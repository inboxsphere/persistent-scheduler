use crate::core::error::SchedulerError;
use crate::core::model::{Retry, TaskMeta, TaskStatus};
use crate::core::task_kind::TaskKind;
use native_db::*;
use native_model::{native_model, Model};
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use tracing::error;

pub mod meta;

static DB: OnceCell<Database> = OnceCell::new();

pub static TASK_SCHEDULER_MODELS: Lazy<Models> = Lazy::new(|| {
    let mut models = Models::new();
    models
        .define::<TaskMetaEntity>()
        .expect("Error to define TaskMetaEntity");
    models
});

pub fn init_nativedb(
    db_path: Option<String>,
    cache_size: Option<u64>,
) -> Result<&'static Database<'static>, SchedulerError> {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_memory();

    let cache_size = cache_size.unwrap_or_else(|| sys.free_memory() / 4);

    let db_file = db_path.unwrap_or_else(|| {
        std::env::temp_dir()
            .join("polly-scheduler.db")
            .to_string_lossy()
            .into_owned()
    });

    let database = Builder::new()
        .set_cache_size(cache_size as usize)
        .create(&TASK_SCHEDULER_MODELS, db_file.as_str());

    if let Err(e) = database {
        error!("Error init native db {:?}", e);
        return Err(SchedulerError::StoreInit);
    }

    let _ = DB.set(database.unwrap());
    get_database()
}

pub fn get_database() -> Result<&'static Database<'static>, SchedulerError> {
    DB.get().ok_or_else(|| SchedulerError::StoreInit)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[native_model(id = 2, version = 1)]
#[native_db]
pub struct TaskMetaEntity {
    #[primary_key]
    pub id: String, // Unique identifier for the task
    #[secondary_key]
    pub task_key: String, // Key to identify the specific task
    pub task_params: String, // Arguments for the task, stored as a JSON object
    #[secondary_key]
    pub queue_name: String, // Name of the queue for the task
    pub updated_at: i64,     // Timestamp of the last update
    #[secondary_key]
    pub status: TaskStatus, // Current status of the task
    pub stopped_reason: Option<String>, // Optional reason for why the task was stopped
    pub last_error: Option<String>, // Error message from the last execution, if any
    pub last_run: i64,       // Timestamp of the last run
    pub next_run: i64,       // Timestamp of the next scheduled run
    pub kind: TaskKind,      // Type of the task
    pub success_count: u32,  // Count of successful runs
    pub failure_count: u32,  // Count of failed runs
    pub runner_id: Option<String>, // The ID of the current task runner, may be None
    pub retry_strategy: Retry, // Retry strategy for handling failures
    pub retry_interval: u32, // Interval for retrying the task
    pub base_interval: u32, // Base interval for exponential backoff
    pub delay_seconds: u32,  //Delay before executing a Once task, specified in seconds
    pub max_retries: Option<u32>, // Maximum number of retries allowed
    pub cron_schedule: Option<String>, // Cron expression for scheduling
    pub cron_timezone: Option<String>, // Timezone for the cron schedule (stored as a string)
    pub is_repeating: bool,  // Indicates if the task is repeating
    pub repeat_interval: u32, // Interval for repeating task
    pub heartbeat_at: i64,   // Timestamp of the last heartbeat in milliseconds
}

impl ToKey for TaskStatus {
    fn to_key(&self) -> Key {
        match self {
            TaskStatus::Scheduled => Key::new(vec![0]),
            TaskStatus::Running => Key::new(vec![1]),
            TaskStatus::Success => Key::new(vec![2]),
            TaskStatus::Failed => Key::new(vec![3]),
            TaskStatus::Removed => Key::new(vec![4]),
            TaskStatus::Stopped => Key::new(vec![5]),
        }
    }

    fn key_names() -> Vec<String> {
        vec!["String".to_string(), "&str".to_string()]
    }
}

impl From<TaskMetaEntity> for TaskMeta {
    fn from(entity: TaskMetaEntity) -> Self {
        TaskMeta {
            id: entity.id,
            task_key: entity.task_key,
            task_params: entity.task_params,
            queue_name: entity.queue_name,
            updated_at: entity.updated_at,
            status: entity.status,
            stopped_reason: entity.stopped_reason,
            last_error: entity.last_error,
            last_run: entity.last_run,
            next_run: entity.next_run,
            kind: entity.kind,
            success_count: entity.success_count,
            failure_count: entity.failure_count,
            runner_id: entity.runner_id,
            retry_strategy: entity.retry_strategy,
            retry_interval: entity.retry_interval,
            base_interval: entity.base_interval,
            delay_seconds: entity.delay_seconds,
            max_retries: entity.max_retries,
            cron_schedule: entity.cron_schedule,
            cron_timezone: entity.cron_timezone,
            is_repeating: entity.is_repeating,
            repeat_interval: entity.repeat_interval,
            heartbeat_at: entity.heartbeat_at,
        }
    }
}

impl From<TaskMeta> for TaskMetaEntity {
    fn from(entity: TaskMeta) -> Self {
        TaskMetaEntity {
            id: entity.id,
            task_key: entity.task_key,
            task_params: entity.task_params,
            queue_name: entity.queue_name,
            updated_at: entity.updated_at,
            status: entity.status,
            stopped_reason: entity.stopped_reason,
            last_error: entity.last_error,
            last_run: entity.last_run,
            next_run: entity.next_run,
            kind: entity.kind,
            success_count: entity.success_count,
            failure_count: entity.failure_count,
            runner_id: entity.runner_id,
            retry_strategy: entity.retry_strategy,
            retry_interval: entity.retry_interval,
            base_interval: entity.base_interval,
            delay_seconds: entity.delay_seconds,
            max_retries: entity.max_retries,
            cron_schedule: entity.cron_schedule,
            cron_timezone: entity.cron_timezone,
            is_repeating: entity.is_repeating,
            repeat_interval: entity.repeat_interval,
            heartbeat_at: entity.heartbeat_at,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{fs, path::Path, time::Duration};

    use crate::nativedb::{init_nativedb, TaskMetaEntity};
    use itertools::Itertools;

    fn delete_temp_db() -> Result<(), Box<dyn std::error::Error>> {
        let temp_db_path = std::env::temp_dir().join("polly-scheduler.db");
        if Path::new(&temp_db_path).exists() {
            fs::remove_file(temp_db_path)?;
            println!("File 'polly-scheduler.db' has been deleted.");
        } else {
            println!("File 'polly-scheduler.db' does not exist.");
        }

        Ok(())
    }

    #[tokio::test]
    async fn delete_db_file() {
        delete_temp_db().unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    #[tokio::test]
    async fn test() {
        let db = init_nativedb(None, None).unwrap();
        let r = db.r_transaction().unwrap();

        let list: Vec<TaskMetaEntity> = r
            .scan()
            .primary()
            .unwrap()
            .all()
            .unwrap()
            .try_collect()
            .unwrap();

        println!("{:#?}", list);
    }
}
