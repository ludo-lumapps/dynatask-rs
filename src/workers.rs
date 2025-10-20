use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::Read as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flate2::read::ZlibDecoder;
use rand::seq::SliceRandom;
use redis::aio::ConnectionManager;
use redis::{RedisResult, Value, cmd, pipe};
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::shared::{
    _Error, _Result, GROUP, JobTaskIds, TASK_CONTEXT, TaskContext, WorkConf,
    active_job_ids_key, job_spans_key, job_stats_key, stopping_job_ids_key, stream_key,
    stream_watch_key,
};
use crate::valkey_utils::{
    ClaimedEntries, Entries, PendingIdled, PendingIdledEntry, get_conn,
};

#[derive(Debug)]
pub enum JobTaskError {
    CanRetry,
    CannotRetry,
}

impl Error for JobTaskError {}

impl Display for JobTaskError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            JobTaskError::CanRetry => write!(f, "Error processing task, retry possible"),
            JobTaskError::CannotRetry => {
                write!(f, "Error processing task, retry not possible")
            }
        }
    }
}

fn decompress_data(v: &[u8]) -> _Result<Vec<u8>> {
    let mut z = ZlibDecoder::new(v);
    let mut buffer = Vec::new();
    z.read_to_end(&mut buffer)
        .map_err(|e| _Error::unhandled(format!("Error decompressing data: {e}")))?;
    Ok(buffer)
}

fn get_task_spans(pl: &HashMap<String, Vec<u8>>) -> _Result<Vec<String>> {
    let Some(lzma_bytes) = pl.get("spans") else {
        return Ok(vec![]);
    };
    let str_bytes = decompress_data(lzma_bytes)?;
    let spans_s = String::from_utf8(str_bytes)
        .map_err(|err| _Error::Unhandled(format!("Error getting spans: {err}")))?;
    Ok(match spans_s.is_empty() {
        true => vec![],
        false => spans_s.split("|").map(|s| s.to_owned()).collect(),
    })
}

fn get_task_data(pl: &HashMap<String, Vec<u8>>) -> _Result<Vec<u8>> {
    let Some(lzma_bytes) = pl.get("data") else {
        Err(_Error::Unhandled("payload missing from entry".to_string()))?
    };
    let ret = decompress_data(lzma_bytes)?;
    Ok(ret)
}

struct QueuedTask {
    job_id: i32,
    id: String,
    payload: HashMap<String, Vec<u8>>,
}

async fn xack_entry(
    conn: &mut ConnectionManager,
    job_type: &str,
    local_task_type: &str,
    job_id: i32,
    task_id: &str,
    spans: Option<&Vec<String>>,
    finished_ok: bool,
) -> RedisResult<()> {
    // - decreasing all the spans that have this task
    // - xack the task
    let mut p = pipe();
    let p2 = &mut p;
    if let Some(spans) = spans {
        let spans_key = job_spans_key(job_type, job_id);
        for span in spans {
            info!("Decreasing span {span} by 1");
            p2.cmd("HINCRBY").arg(&[&spans_key, span, "-1"]).ignore();
        }
    }
    let done_field = match finished_ok {
        true => "done_ok",
        false => "done_err",
    };
    let stream = stream_key(job_type, job_id, local_task_type);
    p2.cmd("XACK").arg(&[&stream, GROUP, task_id]).ignore();
    p2.cmd("XDEL").arg(&[&stream, task_id]).ignore();
    let stats_key = job_stats_key(job_type, job_id);
    p2.cmd("HINCRBY").arg(&[&stats_key, done_field, "1"]).ignore();
    p2.exec_async(conn).await?;
    Ok(())
}

async fn process_task<F, FFut>(
    params: Vec<u8>,
    th: F,
    running_tasks: Arc<Mutex<JobTaskIds>>,
) -> _Result<()>
where
    F: Fn(Vec<u8>) -> FFut + Sync,
    FFut: Future<Output = Result<(), JobTaskError>> + Send,
{
    let task_context = TASK_CONTEXT.get();
    let mut conn = task_context.conn;
    let job_type = &task_context.job_type;
    let job_id = task_context.job_id;
    let task_id = &task_context.task_id;
    let task_type = &task_context.task_type;
    info!("Processing job ID {job_id} / task ID {task_id} ");
    {
        let mut running_tasks = running_tasks.lock().await;
        if let Some(ids) = running_tasks.get_mut(&job_id) {
            ids.push(task_id.into());
        } else {
            running_tasks.insert(job_id, vec![task_id.into()]);
        }
    }
    match (th)(params).await {
        Ok(()) => {
            info!("Task {task_id} completed OK");
            xack_entry(
                &mut conn,
                job_type,
                task_type,
                job_id,
                task_id,
                task_context.spans.as_ref(),
                true,
            )
            .await?;
        }
        Err(JobTaskError::CanRetry) => {
            warn!("Task {task_id} failed, will retry if max tries not exceeded");
        }
        Err(JobTaskError::CannotRetry) => {
            warn!("Task {task_id} failed with no retrying");
            xack_entry(
                &mut conn,
                job_type,
                task_type,
                job_id,
                task_id,
                task_context.spans.as_ref(),
                false,
            )
            .await?;
        }
    }
    let mut running_tasks = running_tasks.lock().await;
    if let Some(ids) = running_tasks.get_mut(&job_id) {
        ids.retain(|id| *id != *task_id);
    }
    Ok(())
}

pub(crate) struct WorkDispatcher<F, FFut>
where
    F: Fn(Vec<u8>) -> FFut + Copy + Sync + Send + 'static,
    FFut: Future<Output = Result<(), JobTaskError>> + Send,
{
    conf: WorkConf,
    stopping_job_ids_key: String,
    conn: ConnectionManager,
    running_tasks: Arc<Mutex<JobTaskIds>>,
    task_handler: F,
}

impl<F, FFut> WorkDispatcher<F, FFut>
where
    F: Fn(Vec<u8>) -> FFut + Copy + Sync + Send + 'static,
    FFut: Future<Output = Result<(), JobTaskError>> + Send,
{
    pub async fn new(
        conf: WorkConf,
        running_tasks: Arc<Mutex<JobTaskIds>>,
        task_handler: F,
    ) -> Self {
        let conn = get_conn(&conf.valkey_uri).await;
        let stopping_job_ids_key = stopping_job_ids_key(&conf.job_type);
        Self { conf, stopping_job_ids_key, conn, running_tasks, task_handler }
    }

    async fn clear_abandoned_tasks_of_stopping_job(
        &self,
        job_id: i32,
        stream: &str,
    ) -> _Result<()> {
        // we don't need to handle spans when a job is stopping
        // just xack abandoned tasks
        let Some(pels) =
            PendingIdled::get(&mut self.conn.clone(), GROUP, stream, "10000").await?
        else {
            return Ok(()); // group gone, job was done
        };
        for pel in pels.ids.iter() {
            let task_id = &pel.id.to_string();
            xack_entry(
                &mut self.conn.clone(),
                &self.conf.job_type,
                &self.conf.local_task_type,
                job_id,
                task_id,
                None,
                false,
            )
            .await?;
        }
        Ok(())
    }

    async fn get_next_pel_tasks(
        &self,
        free_slots: usize,
        job_id: i32,
        stream: &str,
        qts: &mut Vec<QueuedTask>,
    ) -> _Result<()> {
        let mut conn = self.conn.clone();
        // get up to `free_slots` tasks from the PEL
        // without checking for the limit of tasks per job
        // because PEL tasks are already counted in the current nb of tasks
        let Some(pels) = PendingIdled::get(
            &mut conn,
            GROUP,
            stream,
            &(free_slots - qts.len()).to_string(),
        )
        .await?
        else {
            return Ok(()); // group gone, job is now done
        };
        if pels.ids.is_empty() {
            return Ok(()); // nothing to claim
        }
        // this "pels -> pel_map -> pel_ids" needs to be simplified
        let pel_map: HashMap<String, PendingIdledEntry> =
            pels.ids.into_iter().map(|p| (p.id.to_string(), p)).collect();
        let pel_ids: Vec<&str> = pel_map.keys().map(|k| k.as_str()).collect();
        let consumer = &self.conf.consumer;
        // claim them all
        // if race occurs, those that got picked up in the
        //   meantime will NOT be returned by XCLAIM
        let claimed =
            ClaimedEntries::get(&mut conn, GROUP, stream, consumer, &pel_ids).await?;
        let max_task_attempts = self.conf.max_task_attempts;
        let job_type = &self.conf.job_type;
        let task_type = &self.conf.local_task_type;
        for entry in claimed.entries {
            let task_id = entry.id.to_string();
            let Some(pel_info) = pel_map.get(&task_id) else {
                continue; // shouldn't happen
            };
            let times_deliv = pel_info.times_delivered;
            // if task was attempted too many times, lets remove it
            if times_deliv >= max_task_attempts {
                warn!("Task {task_id} tried too many times, deleting it");
                let spans = match get_task_spans(&entry.data) {
                    Ok(v) => Some(v),
                    Err(err) => {
                        warn!(
                            "Error '{err}' parsing spans of {task_id}, \
                            will assume no spans to XACK this errored task"
                        );
                        None
                    }
                };
                xack_entry(
                    &mut conn,
                    job_type,
                    task_type,
                    job_id,
                    &task_id,
                    spans.as_ref(),
                    false,
                )
                .await?;
                continue;
            }
            // queue the task to process it again
            qts.push(QueuedTask { job_id, id: task_id, payload: entry.data });
            info!("Entry {} of stream {stream} was claimed by {consumer}", entry.id);
            if qts.len() >= free_slots {
                break;
            }
        }
        Ok(())
    }

    /*
    --- group exists, has picked up entries
    > xpending stream1 group1
    1) (integer) 5
    2) "1734356743229-0"
    3) "1734358381156-0"
    4) 1) 1) "consumer1"
          2) "4"
       2) 1) "consumer2"
          2) "1"


    --- group exists but has no pending entry
    > xpending stream1 group3
    1) (integer) 0
    2) (nil)
    3) (nil)
    4) (nil)
    */
    /// Count of pending entries across all group consumers
    async fn get_pending_count(
        &self,
        conn: &mut ConnectionManager,
        stream: &str,
    ) -> RedisResult<usize> {
        type Inner = (usize, Value, Value, Value);
        let res: Inner = cmd("XPENDING").arg(&[stream, GROUP]).query_async(conn).await?;
        Ok(res.0)
    }

    async fn get_next_new_tasks(
        &self,
        free_slots: usize,
        job_id: i32,
        stream: &str,
        qts: &mut Vec<QueuedTask>,
    ) -> _Result<()> {
        let mut conn = self.conn.clone();
        let watch_key = stream_watch_key(stream);
        cmd("WATCH").arg(&[&watch_key]).exec_async(&mut conn).await?;
        let pending_count = self.get_pending_count(&mut conn, stream).await?;
        if pending_count >= self.conf.max_tasks_per_job {
            return Ok(());
        }
        let left_for_stream = self.conf.max_tasks_per_job - pending_count;
        let max_to_get = std::cmp::min(left_for_stream, free_slots - qts.len());
        let mut p = pipe();
        p.cmd("MULTI").ignore();
        p.cmd("INCR").arg(&watch_key).ignore();
        p.cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(&self.conf.consumer)
            .arg("COUNT")
            .arg(max_to_get)
            .arg("STREAMS")
            .arg(stream)
            .arg(">")
            .ignore();
        let entries_resp_opt: (Option<(i32, Entries)>,) =
            match p.cmd("EXEC").query_async(&mut conn).await {
                Ok(v) => v,
                Err(err) => {
                    if err.code() == Some("NOGROUP") {
                        return Ok(());
                    }
                    warn!("Unexpected error getting new tasks: {err}");
                    return Ok(());
                }
            };
        let Some(entries_resp) = entries_resp_opt.0 else {
            // watched key changed
            // consequence is: this worker moves on to trying to get
            //      tasks of another job
            // if, say, there are not other jobs, this worker
            //      will try to get more tasks from this job in 50ms or so
            return Ok(());
        };
        let read_count = entries_resp.1.entries.len();
        for entry in entries_resp.1.entries {
            qts.push(QueuedTask {
                job_id,
                id: entry.id.to_string(),
                payload: entry.data,
            });
        }
        cmd("HINCRBY")
            .arg(job_stats_key(&self.conf.job_type, job_id))
            .arg("read")
            .arg(read_count)
            .exec_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn is_stopping(&self, job_id: i32) -> _Result<bool> {
        Ok(cmd("SISMEMBER")
            .arg(&self.stopping_job_ids_key)
            .arg(job_id)
            .query_async(&mut self.conn.clone())
            .await?)
    }

    async fn get_next_tasks(
        &self,
        free_slots: usize,
    ) -> _Result<Option<Vec<QueuedTask>>> {
        let job_type = &self.conf.job_type;
        let task_type = &self.conf.local_task_type;
        let mut job_ids: Vec<i32> = cmd("SRANDMEMBER")
            .arg(&[&active_job_ids_key(job_type), "10"])
            .query_async(&mut self.conn.clone())
            .await?;
        if job_ids.is_empty() {
            return Ok(None);
        }
        // srandmember_multiple results themselves are not shuffled
        let mut qts: Vec<QueuedTask> = vec![];
        job_ids.shuffle(&mut rand::rng());
        for job_id in job_ids {
            let stream = stream_key(job_type, job_id, task_type);
            if self.is_stopping(job_id).await? {
                self.clear_abandoned_tasks_of_stopping_job(job_id, &stream).await?;
                continue;
            }
            self.get_next_pel_tasks(free_slots, job_id, &stream, &mut qts).await?;
            if qts.len() >= free_slots {
                break;
            }
            self.get_next_new_tasks(free_slots, job_id, &stream, &mut qts).await?;
            if qts.len() >= free_slots {
                break;
            }
        }
        Ok(Some(qts))
    }

    async fn process_tasks(&self, qts: Vec<QueuedTask>, tasks: &mut Vec<JoinHandle<()>>) {
        let consumer = &self.conf.consumer;
        let job_type = &self.conf.job_type;
        let task_type = &self.conf.local_task_type;
        let th = self.task_handler;
        for qt in qts {
            let running_tasks = self.running_tasks.clone();
            let job_id = qt.job_id;
            let task_id = qt.id.as_str();
            let spans = match get_task_spans(&qt.payload) {
                Ok(v) => Some(v),
                Err(e) => {
                    warn!("Error '{e}' parsing spans of {task_id}");
                    if let Err(err) = xack_entry(
                        &mut self.conn.clone(),
                        job_type,
                        task_type,
                        job_id,
                        task_id,
                        None,
                        false,
                    )
                    .await
                    {
                        warn!("Error XACK'ing task {task_id} of job {job_id}: {err}");
                    }
                    continue;
                }
            };
            match &spans {
                Some(spans) => debug!("Task {task_id} has spans {spans:?}"),
                _ => debug!("Task {task_id} has no spans"),
            }
            let params = match get_task_data(&qt.payload) {
                Ok(v) => v,
                Err(err) => {
                    warn!("Error '{err}' parsing payload of {task_id}");
                    if let Err(err) = xack_entry(
                        &mut self.conn.clone(),
                        job_type,
                        task_type,
                        job_id,
                        task_id,
                        spans.as_ref(),
                        false,
                    )
                    .await
                    {
                        warn!("Error XACK'ing task {task_id} of job {job_id}: {err}");
                    };
                    continue;
                }
            };
            let task_context = TaskContext::new(
                self.conn.clone(),
                self.conf.job_type.clone(),
                job_id,
                task_id.into(),
                task_type.clone(),
                spans.clone(),
            );
            let trace_id = format!("worker-{consumer}-job-{job_id}-task-{task_id}");
            tasks.push(spawn(
                async move {
                    if let Err(err) = TASK_CONTEXT
                        .scope(task_context, process_task(params, th, running_tasks))
                        .await
                    {
                        warn!("Worker error processing entry: {err}");
                    }
                }
                .instrument(info_span!("trace_id", %trace_id)),
            ));
        }
    }

    pub async fn dispatch_work(&self, exit_flag: Arc<AtomicBool>) {
        const ACTIVITY_SLEEP: Duration = Duration::from_millis(75);
        const INACTIVITY_SLEEP: Duration = Duration::from_millis(1000);
        info!("Starting work dispatch");
        let max_slots = self.conf.thread_count;
        let mut tasks: Vec<JoinHandle<()>> = vec![];
        while !exit_flag.load(Ordering::Relaxed) {
            tasks.retain(|t| !t.is_finished());
            let free_slots = max_slots - tasks.len();
            if free_slots == 0 {
                // all workers are working, don't sleep too long...
                sleep(ACTIVITY_SLEEP).await;
                continue;
            }
            match self.get_next_tasks(free_slots).await {
                Ok(Some(qts)) => {
                    if !qts.is_empty() {
                        self.process_tasks(qts, &mut tasks).await;
                    }
                    // stuff is happening, don't sleep too long...
                    sleep(ACTIVITY_SLEEP).await;
                }
                Ok(None) => {
                    // nothing to do, sleep a little long...
                    sleep(INACTIVITY_SLEEP).await;
                }
                Err(err) => {
                    warn!("Error getting up to {free_slots} next tasks: {err}");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
        for jh in tasks {
            let _ = jh.await;
        }
        info!("Exiting work dispatch");
    }
}
