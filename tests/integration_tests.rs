use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use rand::random_range;
use redis::aio::ConnectionManager;
use redis::{Client, cmd};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, json, to_vec};
use tokio::spawn;
use tokio::time::sleep;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

use dynatask::{FinishedJobInfo, WorkConfBuilder, in_task_context, start_work};

static SETUP: Once = Once::new();

fn setup() {
    SETUP.call_once(|| setup_logging());
}

fn setup_logging() {
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .with_filter(LevelFilter::DEBUG)
        .boxed();
    tracing_subscriber::registry().with(layer).init();
}

static JOB_TYPE: &str = "test_job_trackers";
static LOCAL_TASK_TYPE: &str = "default";

fn get_valkey_uri() -> String {
    std::env::var("VALKEY_URI").unwrap_or_else(|_| "redis://127.0.0.1:6379/3".to_owned())
}

#[derive(Clone)]
struct ValkeyCounter {
    valkey_pool: ConnectionManager,
    key: String,
}

fn get_valkey_cli(valkey_uri: &str) -> Client {
    let cli = Client::open(valkey_uri).unwrap();
    cli
}

async fn get_valkey_pool(valkey_uri: &str) -> ConnectionManager {
    ConnectionManager::new(get_valkey_cli(valkey_uri))
        .await
        .unwrap()
}

impl ValkeyCounter {
    async fn new(key: String) -> Self {
        let valkey_uri = get_valkey_uri();
        Self {
            valkey_pool: get_valkey_pool(&valkey_uri).await,
            key,
        }
    }

    async fn get_conn(&self) -> ConnectionManager {
        self.valkey_pool.clone()
    }

    async fn reset(&self) {
        if let Err(err) = cmd("SET")
            .arg(&self.key)
            .arg(0)
            .exec_async(&mut self.get_conn().await)
            .await
        {
            warn!("Error resetting counter {}: {err}", self.key)
        }
    }

    async fn incr(&self, by: i32) {
        if let Err(err) = cmd("INCRBY")
            .arg(&self.key)
            .arg(by)
            .exec_async(&mut self.get_conn().await)
            .await
        {
            warn!("Error increasing counter {}: {err}", self.key)
        }
    }

    async fn get(&self) -> i32 {
        let v: Option<i32> = match cmd("GET")
            .arg(&self.key)
            .query_async(&mut self.get_conn().await)
            .await
        {
            Ok(v) => v,
            Err(err) => {
                warn!("Error getting counter {}: {err}", self.key);
                return 0;
            }
        };
        v.unwrap_or_default()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum MyMethod {
    FooAll { item_count: i32 },
    FooItem { item_id: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MyContext {
    project_id: i32,
    counter_key: String,
}

struct MyTask {
    ctxt: MyContext,
}

impl MyTask {
    async fn foo_all(&self, item_count: i32) {
        debug!(
            "FooAll method invoked, project_id is: {}",
            self.ctxt.project_id
        );
        for i in 0..item_count {
            let method = MyMethod::FooItem {
                item_id: format!("ITEM_{i}"),
            };
            let params = to_vec(&json!({"context": self.ctxt, "method": method})).unwrap();
            in_task_context::add_task("default", &params, true)
                .await
                .unwrap();
        }
        in_task_context::wait_until_tracked_tasks_done()
            .await
            .unwrap();
    }

    async fn foo_item(&self, item_id: String) {
        debug!(
            "FooItem item_id={item_id} invoked, project_id is: {}",
            self.ctxt.project_id
        );
        let nb_secs = random_range(1..10);
        debug!("Sleeping {nb_secs} seconds");
        sleep(Duration::from_secs(nb_secs)).await;
        debug!("Task is done sleeping");
        ValkeyCounter::new(self.ctxt.counter_key.clone())
            .await
            .incr(1)
            .await;
        debug!("Task is done");
    }
}

async fn job_is_done(job_info: FinishedJobInfo) {
    debug!("set_job_to_done invoked for job id: {}", job_info.id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_tasks_tracking_two_work_threads() {
    // TODO
    setup();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_with_tasks_tracking() {
    setup();
    let task_handler = move |data: Vec<u8>| async move {
        #[derive(Deserialize, Debug)]
        struct TaskData {
            context: MyContext,
            method: MyMethod,
        }

        let task_data: TaskData = match from_slice(&data) {
            Ok(c) => c,
            Err(err) => {
                error!("Error deserializing task data: {err}");
                return Ok(());
            }
        };
        debug!("Handling task {task_data:?}");
        let task = MyTask {
            ctxt: task_data.context,
        };
        match task_data.method {
            MyMethod::FooAll { item_count } => task.foo_all(item_count).await,
            MyMethod::FooItem { item_id } => task.foo_item(item_id).await,
        };
        Ok(())
    };
    let counter_key = "TEST_WITH_TRACKING_COUNTER";
    let valkey_uri = get_valkey_uri();
    let valkey_context = ValkeyCounter::new(counter_key.into()).await;
    valkey_context.reset().await;
    let conf =
        WorkConfBuilder::new(valkey_uri.clone(), JOB_TYPE.into(), LOCAL_TASK_TYPE.into()).build();
    let exit_flag = Arc::new(AtomicBool::new(false));
    let work = start_work(
        conf,
        Some(job_is_done),
        task_handler,
        Some(exit_flag.clone()),
    );
    let workers_handle = spawn(async move { work.await });
    debug!("started workers");
    let item_count_orig = 10;
    let method = MyMethod::FooAll {
        item_count: item_count_orig,
    };
    let data =
        to_vec(&json!({"context": MyContext {project_id:1234, counter_key: counter_key.into() }, "method": method})).unwrap();
    let jobs_client = dynatask::Client::new(&valkey_uri, JOB_TYPE.into(), &["default"]).await;
    jobs_client.start_job(1234, "default", &data).await.unwrap();
    while jobs_client.job_is_running(1234).await.unwrap() {
        sleep(Duration::from_secs(1)).await;
    }
    exit_flag.swap(true, Ordering::Relaxed);
    debug!("Waiting for worker to exit");
    let _ = workers_handle.await;
    debug!("Worker has exited");
    let item_count_final = valkey_context.get().await;
    assert_eq!(item_count_orig, item_count_final);
}

#[tokio::test]
async fn test_watch() {
    setup();
    let valkey_uri = get_valkey_uri();

    let cli1 = Client::open(valkey_uri.as_str()).expect("Error creating Valkey client");
    let mut mgr1 = ConnectionManager::new(cli1)
        .await
        .expect("Error creating Valkey connection manager");
    let cli2 = Client::open(valkey_uri.as_str()).expect("Error creating Valkey client");
    let mut mgr2 = ConnectionManager::new(cli2)
        .await
        .expect("Error creating Valkey connection manager");

    cmd("WATCH")
        .arg(&["fooz1234"])
        .exec_async(&mut mgr1)
        .await
        .unwrap();
    cmd("INCR")
        .arg("fooz1234")
        .exec_async(&mut mgr2)
        .await
        .unwrap();
    cmd("MULTI").exec_async(&mut mgr1).await.unwrap();
    cmd("SET")
        .arg("BLA")
        .arg(1)
        .exec_async(&mut mgr1)
        .await
        .unwrap();
    let res: Option<()> = cmd("EXEC").query_async(&mut mgr1.clone()).await.unwrap();
    assert!(res.is_none());

    cmd("WATCH")
        .arg(&["fooz1234"])
        .exec_async(&mut mgr1)
        .await
        .unwrap();
    cmd("MULTI").exec_async(&mut mgr1).await.unwrap();
    cmd("SET")
        .arg("BLA")
        .arg(1)
        .exec_async(&mut mgr1)
        .await
        .unwrap();
    let res: Option<()> = cmd("EXEC").query_async(&mut mgr1.clone()).await.unwrap();
    assert!(res.is_some());
}
