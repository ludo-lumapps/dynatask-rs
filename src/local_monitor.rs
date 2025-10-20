use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::pipe;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::shared::{_Result, GROUP, JobTaskIds, stream_key};
use crate::valkey_utils::get_conn;

const LOCAL_MONITOR_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) struct LocalMonitor {
    conn: ConnectionManager,
    job_type: String,
    local_task_type: String,
    consumer: String,
    running_tasks: Arc<Mutex<JobTaskIds>>,
}

impl LocalMonitor {
    pub async fn new(
        valkey_uri: &str,
        job_type: String,
        local_task_type: String,
        consumer: String,
        running_tasks: Arc<Mutex<JobTaskIds>>,
    ) -> Self {
        let conn = get_conn(valkey_uri).await;
        Self { conn, job_type, local_task_type, consumer, running_tasks }
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
        let mut p = pipe();
        let p2 = &mut p;
        for (job_id, ids) in running_tasks.iter() {
            let stream = stream_key(&self.job_type, *job_id, &self.local_task_type);
            info!("Resetting entries in {stream}: {ids:?}");
            p2.cmd("XCLAIM")
                .arg(stream)
                .arg(GROUP)
                .arg(&self.consumer)
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
