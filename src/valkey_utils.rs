use std::cmp::Ordering;
use std::collections::HashMap;

use redis::aio::ConnectionManager;
use redis::{
    Client, ErrorKind, FromRedisValue, RedisError, RedisResult, Value, cmd,
    from_redis_value,
};

const MAX_ELAPSED_MS_S: &str = "60000";

pub(crate) async fn get_conn(valkey_uri: &str) -> ConnectionManager {
    let cli = Client::open(valkey_uri).expect("Error creating REDIS client");
    ConnectionManager::new(cli).await.expect("Error creating REDIS connection manager")
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct EntryId {
    pub timestamp: String,
    pub sequence_number: usize,
}

impl Ord for EntryId {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.timestamp, self.sequence_number)
            .cmp(&(&other.timestamp, other.sequence_number))
    }
}

impl PartialOrd for EntryId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EntryId {
    fn from_str(s: &str) -> RedisResult<Self> {
        if let Some((ts, sequence_number_s)) = s.split_once('-') {
            if let Ok(sequence_number) = sequence_number_s.parse() {
                Ok(EntryId { timestamp: ts.into(), sequence_number })
            } else {
                Err((ErrorKind::TypeError, "bad sequence number").into())
            }
        } else {
            Err((ErrorKind::TypeError, "missing dash").into())
        }
    }
}

impl FromRedisValue for EntryId {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let v: String = from_redis_value(v)?;
        Self::from_str(&v)
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}-{}", self.timestamp, self.sequence_number)
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct PendingIdledEntry {
    pub id: EntryId,
    /// The number of times this message was delivered.
    pub times_delivered: usize,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct Entry {
    pub id: EntryId,
    pub data: HashMap<String, Vec<u8>>,
}

/*
> XREADGROUP GROUP group-1 consumer-1 COUNT 3 STREAMS stream-1 >
1) 1) "stream-1"
   2) 1) 1) "1755085813929-0"
         2) 1) "key1"
            2) "val1"
            3) "key2"
            4) "val2"
      2) 1) "1755085836446-0"
         2) 1) "key1"
            2) "val1"
            3) "key2"
            4) "val2"
*/
#[derive(Default, Debug, Clone)]
pub(crate) struct Entries {
    pub entries: Vec<Entry>,
}

impl FromRedisValue for Entries {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut ret = Self { ..Default::default() };
        type SRRows =
            Vec<HashMap<String, Vec<HashMap<EntryId, HashMap<String, Vec<u8>>>>>>;
        let rows: SRRows = from_redis_value(v)?;
        for row in rows {
            for (_stream, vec_entries_map) in row {
                for entries_map in vec_entries_map {
                    for (id, data) in entries_map {
                        ret.entries.push(Entry { id, data });
                    }
                }
            }
        }
        Ok(ret)
    }
}

/*
> xpending stream1 group1 idle 1000 - + 10
1) 1) "1734356743229-0"
   2) "consumer1"
   3) (integer) 34423
   4) (integer) 1
2) 1) "1734358366260-0"
   2) "consumer1"
   3) (integer) 12255
   4) (integer) 1
3) 1) "1734358368714-0"
   2) "consumer1"
   3) (integer) 10857
   4) (integer) 1
4) 1) "1734358369804-0"
   2) "consumer1"
   3) (integer) 7137
   4) (integer) 1
*/
#[derive(Default, Debug, Clone)]
pub(crate) struct PendingIdled {
    pub ids: Vec<PendingIdledEntry>,
}

impl PendingIdled {
    pub async fn get(
        conn: &mut ConnectionManager,
        group: &str,
        stream: &str,
        count: &str,
    ) -> RedisResult<Option<Self>> {
        match cmd("XPENDING")
            .arg(&[stream, group, "IDLE", MAX_ELAPSED_MS_S, "-", "+", count])
            .query_async::<Self>(conn)
            .await
        {
            Ok(v) => Ok(Some(v)),
            Err(err) => {
                if err.code() == Some("NOGROUP") {
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }
}

impl FromRedisValue for PendingIdled {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut ret = Self::default();
        match v {
            Value::Array(outer_tuple) => {
                for outer in outer_tuple {
                    match outer {
                        Value::Array(inner_tuple) => match &inner_tuple[..] {
                            [entry_id_v, _, _, Value::Int(times_delivered_u64)] => {
                                let id: EntryId = from_redis_value(entry_id_v)?;
                                let times_delivered = *times_delivered_u64 as usize;
                                ret.ids.push(PendingIdledEntry { id, times_delivered });
                            }
                            _ => Err(RedisError::from((
                                ErrorKind::TypeError,
                                "Can't parse redis data 3",
                            )))?,
                        },
                        _ => Err(RedisError::from((
                            ErrorKind::TypeError,
                            "Can't parse redis data 2",
                        )))?,
                    }
                }
            }
            _ => {
                Err(RedisError::from((ErrorKind::TypeError, "Can't parse redis data 1")))?
            }
        };
        Ok(ret)
    }
}

pub(crate) struct GroupInfo {
    pub pending: usize,
    pub lag: usize,
}

impl FromRedisValue for GroupInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let map: HashMap<String, Value> = from_redis_value(v)?;
        let pending: Option<usize> = match map.get("pending") {
            Some(v) => from_redis_value(v)?,
            None => None,
        };
        let lag: Option<usize> = match map.get("lag") {
            Some(v) => from_redis_value(v)?,
            None => None,
        };
        Ok(GroupInfo {
            pending: pending.unwrap_or_default(),
            lag: lag.unwrap_or_default(),
        })
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct ClaimedEntries {
    pub entries: Vec<Entry>,
}

impl ClaimedEntries {
    pub async fn get(
        conn: &mut ConnectionManager,
        group: &str,
        stream: &str,
        consumer: &str,
        ids: &[&str],
    ) -> RedisResult<Self> {
        match cmd("XCLAIM")
            .arg(&[stream, group, consumer, MAX_ELAPSED_MS_S])
            .arg(ids)
            .query_async(conn)
            .await
        {
            Ok(v) => Ok(v),
            Err(err) => {
                if err.code() == Some("NOGROUP") {
                    Ok(ClaimedEntries { entries: vec![] })
                } else {
                    Err(err)
                }
            }
        }
    }
}

impl FromRedisValue for ClaimedEntries {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<EntryId, HashMap<String, Vec<u8>>>> = from_redis_value(v)?;
        let entries: Vec<Entry> = rows
            .into_iter()
            .flat_map(|row| row.into_iter().map(|(id, map)| Entry { id, data: map }))
            .collect();
        Ok(ClaimedEntries { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_entry_ids() {
        let entry_id1 = EntryId::from_str("1734175591978-0").unwrap();
        let entry_id11 = EntryId::from_str("1734175591978-0").unwrap();
        let entry_id2 = EntryId::from_str("1734175591978-1").unwrap();
        assert!(entry_id1 < entry_id2);
        assert!(entry_id2 > entry_id1);
        assert!(entry_id1 == entry_id11);
    }
}
