use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::{ToRedisArgs, pipe};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::shared::{
    _Result, GROUP, JobTaskIds, PROCESS_CONSUMER, PROCESS_JOB_TYPE, PROCESS_LOCAL_TASK_TYPE,
    stream_key,
};
use crate::valkey_utils::get_conn;

const LOCAL_MONITOR_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) struct LocalMonitor<T>
where
    T: Display + Clone + Eq + Hash + Send + Sync + ToRedisArgs + 'static,
{
    conn: ConnectionManager,
    running_tasks: Arc<Mutex<JobTaskIds<T>>>,
}

impl<T> LocalMonitor<T>
where
    T: Display + Clone + Eq + Hash + Send + Sync + ToRedisArgs + 'static,
{
    pub async fn new(valkey_uri: &str, running_tasks: Arc<Mutex<JobTaskIds<T>>>) -> Self {
        let conn = get_conn(valkey_uri).await;
        Self {
            conn,
            running_tasks,
        }
    }

    pub async fn start(&self, exit_flag: Arc<AtomicBool>) {
        info!("Starting local jobs monitor");
        while !exit_flag.load(Ordering::Relaxed) {
            self.monitor_jobs()
                .await
                .unwrap_or_else(|e| warn!("Error in local jobs monitor: {e}"));
            tokio::time::sleep(LOCAL_MONITOR_INTERVAL).await;
        }
        info!("Exiting local jobs monitor");
    }

    async fn reset_pel_idle_times(&self) -> _Result<()> {
        let mut running_tasks = self.running_tasks.lock().await;
        if running_tasks.is_empty() {
            return Ok(());
        }
        running_tasks.retain(|_, v| !v.is_empty());
        if running_tasks.is_empty() {
            return Ok(());
        }
        let job_type = PROCESS_JOB_TYPE
            .get()
            .expect("PROCESS_JOB_TYPE must be set");
        let consumer = PROCESS_CONSUMER
            .get()
            .expect("PROCESS_CONSUMER must be set");
        let local_task_type = PROCESS_LOCAL_TASK_TYPE
            .get()
            .expect("PROCESS_LOCAL_TASK_TYPE must be set");
        let mut p = pipe();
        let p2 = &mut p;
        for (job_id, ids) in running_tasks.iter() {
            let stream = stream_key(job_type, job_id, local_task_type);
            info!("Resetting entries in {stream}: {ids:?}");
            p2.cmd("XCLAIM")
                .arg(&stream)
                .arg(GROUP)
                .arg(consumer)
                .arg("0")
                .arg(ids)
                .arg("JUSTID")
                .ignore();
        }
        p2.exec_async(&mut self.conn.clone()).await?;
        Ok(())
    }

    async fn monitor_jobs(&self) -> _Result<()> {
        self.reset_pel_idle_times().await?;
        Ok(())
    }
}
