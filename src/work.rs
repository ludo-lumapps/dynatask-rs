use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::spawn;
use tokio::sync::Mutex;
use tracing::info;

use crate::global_monitor::{FinishedJobInfo, GlobalMonitor};
use crate::local_monitor::LocalMonitor;
use crate::shared::{JobTaskIds, WorkConf};
use crate::workers::{JobTaskError, WorkDispatcher};


/// Starts the differents tasks to process jobs
/// This is meant to be typically started from the main thread,
/// but it can be run from any thread.
pub async fn start_work<FTaskHandler, FutTaskHandler, FJobIsDone, FutJobIsDone>(
    conf: WorkConf,
    job_finished_callback: Option<FJobIsDone>,
    task_handler: FTaskHandler,
    exit_flag: Option<Arc<AtomicBool>>,
) where
    FTaskHandler: Fn(Vec<u8>) -> FutTaskHandler + Copy + Sync + Send + 'static,
    FutTaskHandler: Future<Output = Result<(), JobTaskError>> + Send,
    FJobIsDone: (Fn(FinishedJobInfo) -> FutJobIsDone) + Send + Sync + 'static,
    FutJobIsDone: Future<Output = ()> + Send + 'static,
{
    let job_type = &conf.job_type;
    let task_type = &conf.local_task_type;
    info!("Starting [{job_type}] processing [{task_type}] tasks");
    let exit_flag = exit_flag.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    {
        let exit_flag = exit_flag.clone();
        spawn(async move {
            if let Err(err) = tokio::signal::ctrl_c().await {
                eprintln!("Unable to listen for shutdown signal: {err}");
            }
            exit_flag.swap(true, Ordering::Relaxed);
        });
    }
    let global_monitor_handle = {
        let exit_flag = exit_flag.clone();
        let conf = conf.clone();
        let global_monitor = GlobalMonitor::new(conf, job_finished_callback).await;
        spawn(async move { global_monitor.start(exit_flag).await })
    };
    let running_tasks = Arc::new(Mutex::new(JobTaskIds::new()));
    let local_monitor_handle = {
        let local_monitor = LocalMonitor::new(
            &conf.valkey_uri,
            job_type.clone(),
            task_type.clone(),
            conf.consumer.clone(),
            running_tasks.clone(),
        )
        .await;
        let exit_flag = exit_flag.clone();
        spawn(async move { local_monitor.start(exit_flag).await })
    };
    let dispatcher = WorkDispatcher::<_, _>::new(conf, running_tasks, task_handler).await;
    dispatcher.dispatch_work(exit_flag).await;
    let _ = global_monitor_handle.await;
    let _ = local_monitor_handle.await;
    info!("Worker app is exiting");
}
