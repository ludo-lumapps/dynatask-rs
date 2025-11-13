use std::fmt::Display;
use std::io::Write as _;

use chrono::Utc;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use redis::aio::ConnectionManager;
use redis::{Pipeline, RedisError, cmd, pipe};
use tracing::info;

use crate::shared::{
    GROUP, JobStats, active_job_ids_key, job_spans_key, job_stats_key,
    stopping_job_ids_key, stream_key,
};
use crate::valkey_utils::{EntryId, get_conn};
pub(crate) const MAX_PAYLOAD_SIZE: usize = 50_000;


/// Errors returned by `crate::Client`
#[derive(Debug)]
pub enum JobClientError {
    Unhandled { msg: String },
    User { msg: String },
}

impl From<RedisError> for JobClientError {
    fn from(e: RedisError) -> Self {
        Self::Unhandled { msg: format!("valkey error: {e}") }
    }
}

impl std::error::Error for JobClientError {}

impl Display for JobClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JobClientError::Unhandled { msg } => {
                write!(f, "Unhandled error: {msg}")
            }
            JobClientError::User { msg } => {
                write!(f, "User error: {msg}")
            }
        }
    }
}

pub type JobClientResult<T> = Result<T, JobClientError>;
type Jcr<T> = JobClientResult<T>;

fn compress_data(v: &[u8]) -> Jcr<Vec<u8>> {
    let mut e = ZlibEncoder::new(Vec::new(), Compression::default());
    e.write_all(v).map_err(|e| JobClientError::Unhandled {
        msg: format!("Error compressing data: {e}"),
    })?;
    e.finish().map_err(|e| JobClientError::Unhandled {
        msg: format!("Error compressing data: {e}"),
    })
}

fn prep_task_param(data: &[u8]) -> Jcr<Vec<u8>> {
    let ret = compress_data(data)
        .map_err(|e| JobClientError::Unhandled { msg: format!("{e}") })?;
    let payload_size = ret.len();
    if payload_size > MAX_PAYLOAD_SIZE {
        Err(JobClientError::User {
            msg: format!("Task payload {payload_size} > {MAX_PAYLOAD_SIZE}B max"),
        })?;
    }
    Ok(ret)
}

async fn add_task_to_pipeline(
    job_type: &str,
    job_id: i32,
    stream: &str,
    stats_key: &str,
    data: &[u8],
    spans: &[String],
    p: &mut Pipeline,
) -> Jcr<usize> {
    let spans_v = prep_task_param(spans.join("|").as_bytes())?;
    let data_v = prep_task_param(data)?;
    p.cmd("XADD").arg(&[stream, "*"]).arg(&[("data", &data_v), ("spans", &spans_v)]);
    p.cmd("HINCRBY").arg(&[stats_key, "added", "1"]).ignore();
    if !spans.is_empty() {
        let spans_key = job_spans_key(job_type, job_id);
        for span in spans {
            info!("Increasing span {span} by 1");
            p.cmd("HINCRBY").arg(&[&spans_key, span, "1"]).ignore();
        }
    }
    Ok(data_v.len())
}


/// Client that is used to:
///     - start jobs
///     - stop jobs
///     - check if a job is (still) running
///     - get stats about currently running job
#[derive(Clone)]
pub struct Client {
    pub(crate) conn: ConnectionManager,
    pub(crate) job_type: &'static str,
    pub(crate) task_types: &'static [&'static str],
    pub(crate) active_job_ids_key: String,
    pub(crate) stopping_job_ids_key: String,
}

impl Client {
    pub async fn new(
        valkey_uri: &str,
        job_type: &'static str,
        task_types: &'static [&'static str],
    ) -> Self {
        let active_job_ids_key = active_job_ids_key(job_type);
        let stopping_job_ids_key = stopping_job_ids_key(job_type);
        let conn = get_conn(valkey_uri).await;
        Self { conn, job_type, task_types, active_job_ids_key, stopping_job_ids_key }
    }

    pub async fn start_job(&self, job_id: i32, task_type: &str, data: &[u8]) -> Jcr<()> {
        let job_type = &self.job_type;
        // 1- grab a lock
        let lock_key = format!("stream-creation|{job_type}|{job_id}");
        let mut conn = self.conn.clone();
        if cmd("SET")
            .arg(&[&lock_key, "1", "GET", "EX", "30", "NX"])
            .query_async::<Option<String>>(&mut conn)
            .await?
            .is_some()
        {
            Err(JobClientError::User { msg: format!("Job {job_id} already running") })?
        };
        // 2- check if job exists
        let exists: bool = cmd("SISMEMBER")
            .arg(&self.active_job_ids_key)
            .arg(job_id)
            .query_async(&mut conn)
            .await?;
        if exists {
            cmd("DEL").arg(&lock_key).exec_async(&mut conn).await?;
            Err(JobClientError::User { msg: format!("Job {job_id} already running") })?
        }
        let stats_key = job_stats_key(job_type, job_id);
        let mut p = pipe();
        let p2 = &mut p;
        // 3- crate the new job
        p2.atomic();
        p2.cmd("DEL").arg(&stats_key).ignore();
        p2.cmd("SADD").arg(&self.active_job_ids_key).arg(job_id).ignore();
        p2.cmd("SREM").arg(&self.stopping_job_ids_key).arg(job_id).ignore();
        // add the streams
        for tt in self.task_types {
            let stream = stream_key(job_type, job_id, tt);
            p2.cmd("XGROUP").arg(&["CREATE", &stream, GROUP, "0", "MKSTREAM"]).ignore();
        }
        // add the task and release the lock
        let stream = stream_key(job_type, job_id, task_type);
        let pl_len =
            add_task_to_pipeline(job_type, job_id, &stream, &stats_key, data, &[], p2)
                .await?;
        /*
        started_at:
        2025-09-02 11:53:02.386920173 UTC
            -> 2025-09-02 11:53:02
         */
        p2.cmd("HSET")
            .arg(&stats_key)
            .arg("started_at")
            .arg(&Utc::now().to_string()[..19])
            .ignore();
        p2.cmd("DEL").arg(&lock_key).ignore();
        let resp: (EntryId,) = p2.query_async(&mut conn).await?;
        let entry_id = resp.0;
        info!(
            "Added first task {entry_id} to stream {stream} of job {job_id}, size {pl_len}",
        );
        Ok(())
    }

    pub async fn stop_job(&self, job_id: i32) -> Jcr<bool> {
        let mut conn = self.conn.clone();
        let is_member: bool = cmd("SISMEMBER")
            .arg(&self.active_job_ids_key)
            .arg(job_id)
            .query_async(&mut conn)
            .await?;
        if is_member {
            cmd("SADD")
                .arg(&self.stopping_job_ids_key)
                .arg(job_id)
                .exec_async(&mut conn)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn job_is_running(&self, job_id: i32) -> Jcr<bool> {
        let mut conn = self.conn.clone();
        Ok(cmd("SISMEMBER")
            .arg(&self.active_job_ids_key)
            .arg(job_id)
            .query_async(&mut conn)
            .await?)
    }

    pub async fn get_job_stats(&self, job_id: i32) -> JobStats {
        JobStats::get(&mut self.conn.clone(), self.job_type, job_id).await
    }
}

// in task context
pub mod in_task_context {
    use redis::{cmd, pipe};
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;

    use crate::shared::{
        TASK_CONTEXT, job_spans_key, job_stats_key, stopping_job_ids_key, stream_key,
    };
    use crate::valkey_utils::EntryId;

    use super::{Jcr, JobClientError, add_task_to_pipeline};

    pub enum TrackedTasksState {
        JobStopping,
        AllFinished,
        Pending(i32),
    }

    pub async fn add_task(task_type: &str, data: &[u8], track: bool) -> Jcr<()> {
        let task_context = TASK_CONTEXT.get();
        let mut conn = task_context.conn;
        let job_type = &task_context.job_type;
        let job_id = task_context.job_id;
        let stream = stream_key(job_type, job_id, task_type);
        let stats_key = job_stats_key(job_type, job_id);
        let mut p = pipe();
        let parent_spans = task_context.spans;
        let mut sub_task_spans = vec![];
        if let Some(parent_spans) = parent_spans {
            parent_spans.into_iter().for_each(|s| sub_task_spans.push(s));
        }
        if track {
            sub_task_spans.push(task_context.task_id);
        }
        let pl_len = add_task_to_pipeline(
            job_type,
            job_id,
            &stream,
            &stats_key,
            data,
            &sub_task_spans,
            &mut p,
        )
        .await?;
        let res: (Option<EntryId>,) = p.query_async(&mut conn).await?;
        let Some(entry_id) = res.0 else {
            Err(JobClientError::User {
                msg: format!("Stream {stream} for job {job_id} doesn't not exist"),
            })?
        };
        info!("Added {entry_id} to stream {stream}, size {pl_len}");
        Ok(())
    }

    pub async fn job_is_stopping() -> Jcr<bool> {
        let mut task_context = TASK_CONTEXT.get();
        Ok(cmd("SISMEMBER")
            .arg(stopping_job_ids_key(&task_context.job_type))
            .arg(task_context.job_id)
            .query_async(&mut task_context.conn)
            .await?)
    }

    pub async fn get_tracked_tasks_state() -> Jcr<TrackedTasksState> {
        if job_is_stopping().await? {
            return Ok(TrackedTasksState::JobStopping); // no tracking if job is stopping
        }
        let mut task_context = TASK_CONTEXT.get();
        // tracked subtasks have been tagged with span value == this_task_id
        let task_count: Option<String> = cmd("HGET")
            .arg(job_spans_key(&task_context.job_type, task_context.job_id))
            .arg(&task_context.task_id)
            .query_async(&mut task_context.conn)
            .await?;
        match task_count {
            Some(v) => {
                let pending_count = v.parse::<i32>().unwrap_or_default();
                if pending_count == 0 {
                    Ok(TrackedTasksState::AllFinished)
                } else {
                    Ok(TrackedTasksState::Pending(pending_count))
                }
            }
            None => {
                // there were no tracked subtasks, behave as if all are done
                Ok(TrackedTasksState::AllFinished)
            }
        }
    }

    pub async fn wait_until_tracked_tasks_done() -> Jcr<()> {
        loop {
            match get_tracked_tasks_state().await? {
                TrackedTasksState::JobStopping => {
                    info!("Job is stoppping, not waiting for tracked tasks to finish");
                    return Ok(()); // no tracking if job is stopping
                }
                TrackedTasksState::AllFinished => {
                    info!("tracked subtasks have all finished");
                    return Ok(());
                }
                TrackedTasksState::Pending(n) => {
                    info!("{n} tracked subtasks are still pending, sleeping 3s");
                }
            }
            sleep(Duration::from_millis(2000)).await;
        }
    }
}
