use crate::core::cron::next_run;
use crate::core::model::TaskMeta;
use crate::core::model::TaskStatus;
use crate::core::store::is_candidate_task;
use crate::core::store::TaskStore;
use crate::nativedb::get_database;
use crate::nativedb::init_nativedb;
use crate::nativedb::TaskMetaEntity;
use crate::nativedb::TaskMetaEntityKey;
use crate::utc_now;
use async_trait::async_trait;
use itertools::Itertools;
use native_db::Database;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tracing::debug;
use crate::core::task_kind::TaskKind;

#[derive(Error, Debug)]
pub enum NativeDbTaskStoreError {
    #[error("Task not found")]
    TaskNotFound,

    #[error("Invalid task status")]
    InvalidTaskStatus,

    #[error("Task ID conflict: The task with ID '{0}' already exists.")]
    TaskIdConflict(String),

    #[error("NativeDb error: {0:#?}")]
    NativeDb(#[from] native_db::db_type::Error),

    #[error("{0:#?}")]
    Tokio(#[from] tokio::task::JoinError),
}

#[derive(Clone)]
pub struct NativeDbTaskStore {
    pub store: Arc<&'static Database<'static>>,
}

impl Default for NativeDbTaskStore {
    fn default() -> Self {
        NativeDbTaskStore::new(None, None)
    }
}

impl NativeDbTaskStore {
    pub fn new(db_path: Option<String>, cache_size: Option<u64>) -> Self {
        let store = if let Ok(database) = get_database() {
            Arc::new(database)
        } else {
            let database = init_nativedb(db_path, cache_size)
                .expect("Failed to initialize the native database.");
            Arc::new(database)
        };
        Self { store }
    }

    pub fn init(database: &'static Database<'static>) -> Self {
        Self {
            store: Arc::new(database),
        }
    }

    pub fn fetch_and_lock_task(
        db: Arc<&'static Database<'static>>,
        queue: String,
        runner_id: String,
    ) -> Result<Option<TaskMeta>, NativeDbTaskStoreError> {
        // Start the read transaction
        let r = db.r_transaction()?;
        let scan = r
            .scan()
            .secondary::<TaskMetaEntity>(TaskMetaEntityKey::queue_name)?;

        // Start scanning for tasks in the given queue
        let mut iter = scan.start_with(queue)?;

        // Find the first task that meets the candidate criteria and is due to run
        if let Some(task) = iter
            .find(|item| {
                item.as_ref().is_ok_and(|e| {
                    is_candidate_task(&e.kind, &e.status) && e.next_run <= utc_now!()
                })
            })
            .transpose()?
        {
            // Start a read-write transaction to update the task's status
            let rw = db.rw_transaction()?;
            let current = rw.get().primary::<TaskMetaEntity>(task.id)?;

            match current {
                Some(mut current) => {
                    // If the task is still a candidate and ready to run, update it
                    if is_candidate_task(&current.kind, &current.status)
                        && current.next_run <= utc_now!()
                    {
                        let old = current.clone();
                        current.runner_id = Some(runner_id);
                        current.status = TaskStatus::Running;
                        current.updated_at = utc_now!();

                        // Perform the update in the same transaction
                        rw.update(old.clone(), current.clone())?;
                        rw.commit()?;

                        Ok(Some(old.into()))
                    } else {
                        // Task status is not valid, return None
                        Ok(None)
                    }
                }
                None => {
                    // Task not found, return None
                    Ok(None)
                }
            }
        } else {
            // No task found, return None
            Ok(None)
        }
    }

    pub fn fetch_pending_tasks(
        db: Arc<&'static Database<'static>>,
    ) -> Result<Vec<TaskMeta>, NativeDbTaskStoreError> {
        let start = Instant::now();
        let r = db.r_transaction()?;
        let scan = r
            .scan()
            .secondary::<TaskMetaEntity>(TaskMetaEntityKey::candidate_task)?;

        let iter = scan.start_with(true.to_string())?;
        let tasks: Vec<TaskMetaEntity> = iter
            .filter_map(|item| item.ok().filter(|e| e.next_run <= utc_now!()))
            .take(200)
            .collect();

        let rw = db.rw_transaction()?;
        let mut result = Vec::new();
        for entity in tasks.into_iter() {
            let mut updated = entity.clone();
            updated.status = TaskStatus::Running;
            updated.updated_at = utc_now!();
            rw.update(entity.clone(), updated)?;
            result.push(entity.into());
        }
        rw.commit()?;
        debug!(
            "Time taken to fetch task from native_db: {:#?}",
            start.elapsed()
        );

        Ok(result)
    }

    fn update_status(
        db: Arc<&'static Database<'static>>,
        task_id: String,
        is_success: bool,
        last_error: Option<String>,
        next_run: Option<i64>,
    ) -> Result<(), NativeDbTaskStoreError> {
        let rw = db.rw_transaction()?;
        let task = rw.get().primary::<TaskMetaEntity>(task_id)?;

        let task = match task {
            Some(t) => t,
            None => return Err(NativeDbTaskStoreError::TaskNotFound),
        };

        if task.status == TaskStatus::Stopped || task.status == TaskStatus::Removed {
            return Ok(());
        }

        let mut updated_task = task.clone();
        if is_success {
            updated_task.success_count += 1;
            updated_task.status = TaskStatus::Success;
        } else {
            updated_task.failure_count += 1;
            updated_task.status = TaskStatus::Failed;
            updated_task.last_error = last_error;
        }

        if let Some(next_run_time) = next_run {
            updated_task.last_run = updated_task.next_run;
            updated_task.next_run = next_run_time;
        }

        updated_task.updated_at = utc_now!();

        rw.update(task, updated_task)?;
        rw.commit()?;

        Ok(())
    }

    pub fn clean_up(db: Arc<&'static Database<'static>>) -> Result<(), NativeDbTaskStoreError> {
        let rw = db.rw_transaction()?;
        let entities: Vec<TaskMetaEntity> = rw
            .scan()
            .secondary(TaskMetaEntityKey::clean_up)?
            .start_with(true.to_string())?
            .try_collect()?;
        for entity in entities {
            if (utc_now!() - entity.updated_at) > 30 * 60 * 1000 {
                rw.remove(entity)?;
            }
        }
        rw.commit()?;
        Ok(())
    }

    pub fn set_status(
        db: Arc<&'static Database<'static>>,
        task_id: String,
        status: TaskStatus,
    ) -> Result<(), NativeDbTaskStoreError> {
        assert!(matches!(status, TaskStatus::Removed | TaskStatus::Stopped));

        let rw = db.rw_transaction()?;
        let task = rw.get().primary::<TaskMetaEntity>(task_id)?;

        if let Some(mut task) = task {
            let old = task.clone();
            task.status = TaskStatus::Removed;
            task.updated_at = utc_now!();
            rw.update(old, task)?;
            rw.commit()?;
            Ok(())
        } else {
            Err(NativeDbTaskStoreError::TaskNotFound)
        }
    }

    pub fn heartbeat(
        db: Arc<&'static Database<'static>>,
        task_id: String,
        runner_id: String,
    ) -> Result<(), NativeDbTaskStoreError> {
        let rw = db.rw_transaction()?;
        let task = rw.get().primary::<TaskMetaEntity>(task_id)?;

        if let Some(mut task) = task {
            let old = task.clone();
            task.heartbeat_at = utc_now!();
            task.runner_id = Some(runner_id.to_string());
            rw.update(old, task)?;
            rw.commit()?;
            Ok(())
        } else {
            Err(NativeDbTaskStoreError::TaskNotFound)
        }
    }

    pub fn restore(db: Arc<&'static Database<'static>>) -> Result<(), NativeDbTaskStoreError> {
        tracing::info!("starting task restore...");
        let rw = db.rw_transaction()?;
        let entities: Vec<TaskMetaEntity> = rw
            .scan()
            .primary::<TaskMetaEntity>()?
            .all()?
            .try_collect()?;

        // Exclude stopped and Removed tasks
        let targets: Vec<TaskMetaEntity> = entities
            .into_iter()
            .filter(|e| !matches!(e.status, TaskStatus::Removed | TaskStatus::Stopped))
            .collect();
        for entity in targets
            .iter()
            .filter(|e| matches!(e.status, TaskStatus::Running))
        {
            let mut updated_entity = entity.clone(); // Clone to modify
            match updated_entity.kind {
                TaskKind::Cron { .. } | TaskKind::Repeat { .. } => {
                    updated_entity.status = TaskStatus::Scheduled; // Change status to Scheduled for Cron and Repeat
                }
                TaskKind::Once => {
                    updated_entity.status = TaskStatus::Removed; // Remove Once tasks if they didn't complete
                }
            }

            // Handle potential error without using `?` in a map
            rw.update(entity.clone(), updated_entity)?;
        }

        // Handle next run time for repeatable tasks
        for entity in targets
            .iter()
            .filter(|e| matches!(e.kind, TaskKind::Cron { .. } | TaskKind::Repeat { .. }))
        {
            let mut updated = entity.clone();
            match &entity.kind {
                TaskKind::Cron { schedule, timezone } => {
                    updated.next_run = next_run(
                        schedule,
                        timezone,
                        utc_now!(),
                    )
                        .unwrap_or_else(|| {
                            updated.status = TaskStatus::Stopped; // Invalid configuration leads to Stopped
                            updated.stopped_reason = Some("Invalid cron configuration (automatically stopped during task restoration)".to_string());
                            updated.next_run // Keep current next_run
                        });
                }
                TaskKind::Repeat { interval_seconds } => {
                    updated.last_run = updated.next_run;
                    let calculated_next_run =
                        updated.last_run + (interval_seconds * 1000) as i64;
                    updated.next_run = if calculated_next_run <= utc_now!() {
                        utc_now!()
                    } else {
                        calculated_next_run
                    };
                }
                _ => {}
            }

            rw.update(entity.clone(), updated)?;
        }

        rw.commit()?;
        tracing::info!("finished task restore.");
        Ok(())
    }

    pub fn get(
        db: Arc<&'static Database<'static>>,
        task_id: String,
    ) -> Result<Option<TaskMeta>, NativeDbTaskStoreError> {
        let r = db.r_transaction()?;
        Ok(r.get().primary(task_id)?.map(|e: TaskMetaEntity| e.into()))
    }

    pub fn list(
        db: Arc<&'static Database<'static>>,
    ) -> Result<Vec<TaskMeta>, NativeDbTaskStoreError> {
        let r = db.r_transaction()?;
        let list: Vec<TaskMetaEntity> = r.scan().primary()?.all()?.try_collect()?;
        Ok(list.into_iter().map(|e| e.into()).collect())
    }

    pub fn store_one(
        db: Arc<&'static Database<'static>>,
        task: TaskMeta,
    ) -> Result<(), NativeDbTaskStoreError> {
        let rw = db.rw_transaction()?;
        let entity: TaskMetaEntity = task.into();
        rw.insert(entity)?;
        rw.commit()?;
        Ok(())
    }

    pub fn store_many(
        db: Arc<&'static Database<'static>>,
        tasks: Vec<TaskMeta>,
    ) -> Result<(), NativeDbTaskStoreError> {
        let rw = db.rw_transaction()?;
        for task in tasks {
            let entity: TaskMetaEntity = task.into();
            rw.insert(entity)?;
        }
        rw.commit()?;
        Ok(())
    }
}

#[async_trait]
impl TaskStore for NativeDbTaskStore {
    type Error = NativeDbTaskStoreError;

    async fn restore_tasks(&self) -> Result<(), Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::restore(db)).await?
    }

    async fn get(&self, task_id: &str) -> Result<Option<TaskMeta>, Self::Error> {
        let db = self.store.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || Self::get(db, task_id)).await?
    }

    async fn list(&self) -> Result<Vec<TaskMeta>, Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::list(db)).await?
    }

    async fn store_task(&self, task: TaskMeta) -> Result<(), Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::store_one(db, task)).await?
    }

    async fn store_tasks(&self, tasks: Vec<TaskMeta>) -> Result<(), Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::store_many(db, tasks)).await?
    }

    async fn fetch_pending_tasks(&self) -> Result<Vec<TaskMeta>, Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::fetch_pending_tasks(db)).await?
    }

    async fn update_task_execution_status(
        &self,
        task_id: &str,
        is_success: bool,
        last_error: Option<String>,
        next_run: Option<i64>,
    ) -> Result<(), Self::Error> {
        let db = self.store.clone();
        let task_id = task_id.to_string();
        tokio::task::spawn_blocking(move || {
            Self::update_status(db, task_id, is_success, last_error, next_run)
        })
        .await?
    }

    async fn heartbeat(&self, task_id: &str, runner_id: &str) -> Result<(), Self::Error> {
        let db = self.store.clone();
        let task_id = task_id.to_string();
        let runner_id = runner_id.to_string();
        tokio::task::spawn_blocking(move || Self::heartbeat(db, task_id, runner_id)).await?
    }

    async fn set_task_stopped(&self, task_id: &str) -> Result<(), Self::Error> {
        let db = self.store.clone();
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || Self::set_status(db, task_id, TaskStatus::Stopped))
            .await?
    }

    async fn set_task_removed(&self, task_id: &str) -> Result<(), Self::Error> {
        let db = self.store.clone();
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || Self::set_status(db, task_id, TaskStatus::Removed))
            .await?
    }

    async fn cleanup(&self) -> Result<(), Self::Error> {
        let db = self.store.clone();
        tokio::task::spawn_blocking(move || Self::clean_up(db)).await?
    }
}
