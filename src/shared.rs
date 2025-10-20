use std::collections::{HashMap, HashSet};
use std::env::var;
use std::error::Error;
use std::fmt::{Display, Formatter};

use redis::aio::ConnectionManager;
use redis::{RedisError, cmd};
use tokio::task_local;

pub(crate) type JobTaskIds = HashMap<i32, Vec<String>>;
pub(crate) static GROUP: &str = "group-1";

#[derive(Debug)]
pub(crate) enum _Error {
    Valkey(RedisError),
    Unhandled(String),
}

impl _Error {
    pub fn unhandled(msg: String) -> Self {
        Self::Unhandled(msg)
    }
}

impl From<RedisError> for _Error {
    fn from(e: RedisError) -> Self {
        Self::Valkey(e)
    }
}

impl Error for _Error {}

impl Display for _Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Valkey(e) => write!(f, "{e}"),
            Self::Unhandled(e) => write!(f, "{e}"),
        }
    }
}

pub(crate) type _Result<T> = Result<T, _Error>;

/// Configuration for `jobs::start_work`
#[derive(Clone)]
pub struct WorkConf {
    pub(crate) valkey_uri: String,
    pub(crate) job_type: String,
    pub(crate) local_task_type: String,
    pub(crate) task_types: Vec<String>,
    pub(crate) consumer: String,
    pub(crate) max_task_attempts: usize,
    pub(crate) max_tasks_per_job: usize,
    pub(crate) thread_count: usize,
}

/// Configuration builder WorkConf
#[derive(Default)]
pub struct WorkConfBuilder {
    valkey_uri: String,
    job_type: String,
    local_task_type: String,
    task_types: Option<Vec<String>>,
    consumer: Option<String>,
    max_task_attempts: Option<usize>,
    max_tasks_per_job: Option<usize>,
    thread_count: Option<usize>,
}

impl WorkConfBuilder {
    pub fn new(valkey_uri: String, job_type: String, local_task_type: String) -> Self {
        Self {
            valkey_uri,
            job_type,
            local_task_type,
            ..Default::default()
        }
    }

    /// All possible task types
    /// If not set, `local_task_type` will be considered as the sole
    /// task type
    pub fn task_types(mut self, task_types: Vec<String>) -> Self {
        self.task_types = Some(task_types);
        self
    }

    /// Consumer name for this worker
    /// Default is the HOSTNAME env variable if there is one, otherwise "consumer-1"
    pub fn consumer(mut self, consumer: String) -> Self {
        self.consumer = Some(consumer);
        self
    }

    /// Max attempts to process a task
    /// Default is 1
    /// Retries only occur when task fail with `jobs::JobTaskError::CanRetry`
    pub fn max_task_attempts(mut self, max_task_attempts: usize) -> Self {
        self.max_task_attempts = Some(max_task_attempts);
        self
    }

    // Max number of concurrently processed tasks for a given job across all workers
    // Default is 10
    pub fn max_tasks_per_job(mut self, max_tasks_per_job: usize) -> Self {
        self.max_tasks_per_job = Some(max_tasks_per_job);
        self
    }

    // Max number of concurrently processed tasks per worker
    // Default is 10
    pub fn thread_count(mut self, thread_count: usize) -> Self {
        self.thread_count = Some(thread_count);
        self
    }

    pub fn build(self) -> WorkConf {
        let mut task_types = match self.task_types {
            Some(v) => v,
            None => vec![self.local_task_type.clone()],
        };
        let task_types_set: HashSet<_> = task_types.drain(..).collect();
        task_types.extend(task_types_set);
        let consumer = match self.consumer {
            Some(v) => v,
            None => var("HOSTNAME").unwrap_or_else(|_| "consumer-1".to_owned()),
        };
        WorkConf {
            valkey_uri: self.valkey_uri,
            job_type: self.job_type,
            local_task_type: self.local_task_type,
            task_types,
            consumer,
            max_task_attempts: self.max_task_attempts.unwrap_or(1),
            max_tasks_per_job: self.max_tasks_per_job.unwrap_or(10),
            thread_count: self.thread_count.unwrap_or(10),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TaskContext {
    pub conn: ConnectionManager,
    pub job_type: String,
    pub job_id: i32,
    pub task_id: String,
    pub task_type: String,
    pub spans: Option<Vec<String>>,
}
impl TaskContext {
    pub fn new(
        conn: ConnectionManager,
        job_type: String,
        job_id: i32,
        task_id: String,
        task_type: String,
        spans: Option<Vec<String>>,
    ) -> Self {
        Self {
            conn,
            job_type,
            job_id,
            task_id,
            task_type,
            spans,
        }
    }
}

task_local! {
    pub(crate) static TASK_CONTEXT: TaskContext;
}

pub(crate) fn active_job_ids_key(job_type: &str) -> String {
    format!("{job_type}|jobs")
}

pub(crate) fn stopping_job_ids_key(job_type: &str) -> String {
    format!("{job_type}|stopping_jobs")
}

pub(crate) fn job_spans_key(job_type: &str, job_id: i32) -> String {
    format!("{job_type}|{job_id}|spans")
}

pub(crate) fn job_stats_key(job_type: &str, job_id: i32) -> String {
    format!("{job_type}|{job_id}|stats")
}

pub(crate) fn stream_key(job_type: &str, job_id: i32, task_type: &str) -> String {
    format!("{job_type}|{job_id}|{task_type}|stream")
}

pub(crate) fn stream_watch_key(stream: &str) -> String {
    format!("{stream}|watch")
}

/// Running job information returned by `jobs::Client::get_job_stats`
#[derive(Default)]
pub struct JobStats {
    pub started_at: String,
    pub tasks_added: i32,
    pub tasks_read: i32,
    pub tasks_done_ok: i32,
    pub tasks_done_err: i32,
}

impl JobStats {
    pub(crate) async fn get(conn: &mut ConnectionManager, job_type: &str, job_id: i32) -> Self {
        let Ok(exists) = cmd("SISMEMBER")
            .arg(active_job_ids_key(job_type))
            .arg(job_id)
            .query_async::<bool>(conn)
            .await
        else {
            return Default::default();
        };
        if !exists {
            return Default::default();
        }
        let stats_key = job_stats_key(job_type, job_id);
        let Ok(Some(mut st)) = cmd("HGETALL")
            .arg(stats_key)
            .query_async::<Option<HashMap<String, String>>>(conn)
            .await
        else {
            return Default::default();
        };
        Self {
            started_at: st.remove("started_at").unwrap_or_default(),
            tasks_added: st.get("added").map(|v| v.parse().unwrap_or(0)).unwrap_or(0),
            tasks_read: st.get("read").map(|v| v.parse().unwrap_or(0)).unwrap_or(0),
            tasks_done_ok: st
                .get("done_ok")
                .map(|v| v.parse().unwrap_or(0))
                .unwrap_or(0),
            tasks_done_err: st
                .get("done_err")
                .map(|v| v.parse().unwrap_or(0))
                .unwrap_or(0),
        }
    }
}
