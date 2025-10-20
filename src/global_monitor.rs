use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use redis::aio::ConnectionManager;
use redis::{cmd, pipe};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::shared::{
    _Result, JobStats, WorkConf, active_job_ids_key, stream_watch_key,
    stopping_job_ids_key, stream_key,
};
use crate::valkey_stuff::{GroupInfo, get_conn};

const GLOBAL_MONITOR_INTERVAL: Duration = Duration::from_secs(2);

/// Information sent when calling `job_finished_callback`, if one is provided when
/// calling `jobs::start_work`.
#[derive(Default)]
pub struct FinishedJobInfo {
    pub id: i32,
    pub started_at: String,
    pub tasks_added: i32,
    pub tasks_read: i32,
    pub tasks_done_ok: i32,
    pub tasks_done_err: i32,
}

pub(crate) struct GlobalMonitor<JobIsDone, JobIsDoneFut>
where
    JobIsDone: Fn(FinishedJobInfo) -> JobIsDoneFut + Send,
    JobIsDoneFut: Future<Output = ()> + Send + 'static,
{
    conf: WorkConf,
    conn: ConnectionManager,
    job_is_done_handler: Option<JobIsDone>,
    throttle_key: String,
}

impl<FJobIsDone, FutJobIsDone> GlobalMonitor<FJobIsDone, FutJobIsDone>
where
    FJobIsDone: Fn(FinishedJobInfo) -> FutJobIsDone + Send,
    FutJobIsDone: Future<Output = ()> + Send + 'static,
{
    pub async fn new(conf: WorkConf, job_is_done_handler: Option<FJobIsDone>) -> Self {
        let throttle_key = format!("{}-jobs-monitor-throttle", conf.job_type);
        let conn = get_conn(&conf.valkey_uri).await;
        Self { conf, conn, job_is_done_handler, throttle_key }
    }

    pub async fn start(&self, exit_flag: Arc<AtomicBool>) {
        info!("Starting global jobs monitor");
        while !exit_flag.load(Ordering::Relaxed) {
            self.monitor_jobs().await.unwrap_or_else(|e| {
                warn!("Error in global monitor: {e}");
            });
            sleep(GLOBAL_MONITOR_INTERVAL).await;
        }
        info!("Exiting global jobs monitor");
    }

    async fn job_finished(&self, job_id: i32) -> _Result<()> {
        let job_type = &self.conf.job_type;
        let job_stats = JobStats::get(&mut self.conn.clone(), job_type, job_id).await;
        let streams: Vec<String> = self
            .conf
            .task_types
            .iter()
            .map(|tt| stream_key(job_type, job_id, tt))
            .collect();
        let mut p = redis::pipe();
        let p2 = &mut p;
        p2.cmd("SREM").arg(active_job_ids_key(job_type)).arg(job_id).ignore();
        p2.cmd("SREM").arg(stopping_job_ids_key(job_type)).arg(job_id).ignore();
        for stream in &streams {
            let watch_key = stream_watch_key(stream);
            p2.cmd("DEL").arg(watch_key).ignore();
            p2.cmd("DEL").arg(stream).ignore();
        }
        p2.exec_async(&mut self.conn.clone()).await?;
        if let Some(job_is_done_handler) = &self.job_is_done_handler {
            let job_info = FinishedJobInfo {
                id: job_id,
                started_at: job_stats.started_at,
                tasks_added: job_stats.tasks_added,
                tasks_read: job_stats.tasks_read,
                tasks_done_ok: job_stats.tasks_done_ok,
                tasks_done_err: job_stats.tasks_done_err,
            };
            (job_is_done_handler)(job_info).await;
        }
        Ok(())
    }

    async fn job_is_done(&self, job_id: i32) -> _Result<bool> {
        let job_type = &self.conf.job_type;
        let streams: Vec<String> = self
            .conf
            .task_types
            .iter()
            .map(|t| stream_key(job_type, job_id, t))
            .collect();
        let mut p = pipe();
        let p2 = &mut p;
        p2.cmd("WATCH").arg(&streams).ignore();
        p2.cmd("MULTI").ignore();
        p2.cmd("SISMEMBER").arg(stopping_job_ids_key(job_type)).arg(job_id).ignore();
        for stream in &streams {
            p2.cmd("XINFO").arg(&["GROUPS", stream]).ignore();
        }
        let resp: (Option<(i32, Vec<GroupInfo>)>,) =
            p2.cmd("EXEC").query_async(&mut self.conn.clone()).await?;
        let Some((stopping_res, stream_groups)) = resp.0 else {
            return Ok(false);
        };
        let is_stopping = stopping_res != 0;
        // A job is finished when all its streams are empty
        for group in stream_groups {
            if is_stopping {
                if group.pending > 0 {
                    return Ok(false);
                }
            } else if group.lag > 0 || group.pending > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn monitor_job(&self, job_id: i32) -> _Result<()> {
        info!("Monitoring job {job_id}");
        if self.job_is_done(job_id).await? {
            info!("Job {job_id} has nothing left to do, time to end it");
            self.job_finished(job_id).await?;
        } else {
            info!("Job {job_id} still has tasks, so leave it running");
        }
        Ok(())
    }

    async fn monitor_jobs(&self) -> _Result<()> {
        let mut conn = self.conn.clone();
        let throttle_key = &self.throttle_key;
        if cmd("SET")
            .arg(&[throttle_key, "1", "GET", "EX", "3", "NX"])
            .query_async::<Option<String>>(&mut conn)
            .await?
            .is_some()
        {
            // info!("Global monitor was throttled");
            return Ok(());
        };
        // info!("Global monitor was NOT throttled");
        let job_ids: Vec<i32> = cmd("SMEMBERS")
            .arg(active_job_ids_key(&self.conf.job_type))
            .query_async(&mut conn)
            .await?;
        for job_id in job_ids {
            // SETEX key seconds value
            cmd("SETEX").arg(&[throttle_key, "3", "1"]).exec_async(&mut conn).await?;
            self.monitor_job(job_id).await?;
        }
        Ok(())
    }
}
