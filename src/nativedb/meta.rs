use crate::core::cron::next_run;
use crate::core::model::TaskMeta;
use crate::core::model::TaskStatus;
use crate::core::store::is_candidate_task;
use crate::core::store::TaskStore;
use crate::core::task_kind::TaskKind;
use crate::nativedb::get_database;
use crate::nativedb::init_nativedb;
use crate::nativedb::TaskMetaEntity;
use crate::nativedb::TaskMetaEntityKey;
use crate::utc_now;
use async_trait::async_trait;
use itertools::Itertools;
use native_db::Database;
use std::sync::Arc;
use thiserror::Error;

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
}

fn handle_error<T>(
    result: Result<T, native_db::db_type::Error>,
) -> Result<T, NativeDbTaskStoreError> {
    result.map_err(NativeDbTaskStoreError::from)
}

#[async_trait]
impl TaskStore for NativeDbTaskStore {
    type Error = NativeDbTaskStoreError;

    async fn restore_tasks(&self) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let entities: Vec<TaskMetaEntity> =
            handle_error(rw.scan().primary()?.all()?.try_collect())?;

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
                TaskKind::Cron | TaskKind::Repeat => {
                    updated_entity.status = TaskStatus::Scheduled; // Change status to Scheduled for Cron and Repeat
                }
                TaskKind::Once => {
                    updated_entity.status = TaskStatus::Removed; // Remove Once tasks if they didn't complete
                }
            }

            // Handle potential error without using `?` in a map
            handle_error(rw.update(entity.clone(), updated_entity))?;
        }

        // Handle next run time for repeatable tasks
        for entity in targets
            .iter()
            .filter(|e| matches!(e.kind, TaskKind::Cron | TaskKind::Repeat))
        {
            let mut updated = entity.clone();
            match entity.kind {
                TaskKind::Cron => {
                    if let (Some(cron_schedule), Some(cron_timezone)) =
                        (entity.cron_schedule.clone(), entity.cron_timezone.clone())
                    {
                        updated.next_run = next_run(
                            cron_schedule.as_str(),
                            cron_timezone.as_str(),
                            utc_now!(),
                        )
                        .unwrap_or_else(|| {
                            updated.status = TaskStatus::Stopped; // Invalid configuration leads to Stopped
                            updated.stopped_reason = Some("Invalid cron configuration (automatically stopped during task restoration)".to_string());
                            updated.next_run // Keep current next_run
                        });
                    } else {
                        updated.status = TaskStatus::Stopped; // Configuration error leads to Stopped
                        updated.stopped_reason = Some("Missing cron schedule or timezone (automatically stopped during task restoration)".to_string());
                    }
                }
                TaskKind::Repeat => {
                    updated.last_run = updated.next_run;
                    let calculated_next_run =
                        updated.last_run + (updated.repeat_interval * 1000) as i64;
                    updated.next_run = if calculated_next_run <= utc_now!() {
                        utc_now!()
                    } else {
                        calculated_next_run
                    };
                }
                _ => {}
            }

            handle_error(rw.update(entity.clone(), updated))?;
        }

        handle_error(rw.commit())?;
        Ok(())
    }

    async fn get(&self, task_id: &str) -> Result<Option<TaskMeta>, Self::Error> {
        let r = handle_error(self.store.r_transaction())?;
        Ok(handle_error(r.get().primary(task_id))?.map(|e: TaskMetaEntity| e.into()))
    }

    async fn list(&self) -> Result<Vec<TaskMeta>, Self::Error> {
        let r = handle_error(self.store.r_transaction())?;
        let list: Vec<TaskMetaEntity> = handle_error(r.scan().primary()?.all()?.try_collect())?;
        Ok(list.into_iter().map(|e| e.into()).collect())
    }

    async fn store_task(&self, task: TaskMeta) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let entity: TaskMetaEntity = task.into();
        handle_error(rw.insert(entity))?;
        handle_error(rw.commit())?;
        Ok(())
    }

    async fn fetch_pending_task(
        &self,
        queue: &str,
        runner_id: &str,
    ) -> Result<Option<TaskMeta>, Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let entities: Vec<TaskMetaEntity> = handle_error(
            rw.scan()
                .secondary(TaskMetaEntityKey::queue_name)?
                .start_with(queue)?
                .try_collect(),
        )?;

        if let Some(mut task) = entities
            .into_iter()
            .filter(|e: &TaskMetaEntity| is_candidate_task(&e.kind, &e.status))
            .find(|e| e.next_run <= utc_now!())
        {
            let result = task.clone();
            task.runner_id = Some(runner_id.to_string());
            task.status = TaskStatus::Running;
            task.updated_at = utc_now!();

            handle_error(rw.update(result.clone(), task))?;
            handle_error(rw.commit())?;

            Ok(Some(result.into()))
        } else {
            Ok(None)
        }
    }

    async fn update_task_execution_status(
        &self,
        task_id: &str,
        is_success: bool,
        last_error: Option<String>,
        next_run: Option<i64>,
    ) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let task: Option<TaskMetaEntity> = handle_error(rw.get().primary(task_id))?;

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

        handle_error(rw.update(task, updated_task))?;
        handle_error(rw.commit())?;

        Ok(())
    }

    async fn heartbeat(&self, task_id: &str, runner_id: &str) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let task: Option<TaskMetaEntity> = handle_error(rw.get().primary(task_id))?;

        if let Some(mut task) = task {
            let old = task.clone();
            task.heartbeat_at = utc_now!();
            task.runner_id = Some(runner_id.to_string());
            handle_error(rw.update(old, task))?;
            handle_error(rw.commit())?;
            Ok(())
        } else {
            Err(NativeDbTaskStoreError::TaskNotFound)
        }
    }

    async fn set_task_stopped(&self, task_id: &str) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let task: Option<TaskMetaEntity> = handle_error(rw.get().primary(task_id))?;

        if let Some(mut task) = task {
            let old = task.clone();
            task.status = TaskStatus::Stopped;
            task.updated_at = utc_now!();
            handle_error(rw.update(old, task))?;
            handle_error(rw.commit())?;
            Ok(())
        } else {
            Err(NativeDbTaskStoreError::TaskNotFound)
        }
    }

    async fn set_task_removed(&self, task_id: &str) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let task: Option<TaskMetaEntity> = handle_error(rw.get().primary(task_id))?;

        if let Some(mut task) = task {
            let old = task.clone();
            task.status = TaskStatus::Removed;
            task.updated_at = utc_now!();
            handle_error(rw.update(old, task))?;
            handle_error(rw.commit())?;
            Ok(())
        } else {
            Err(NativeDbTaskStoreError::TaskNotFound)
        }
    }

    async fn cleanup(&self) -> Result<(), Self::Error> {
        let rw = handle_error(self.store.rw_transaction())?;
        let entities: Vec<TaskMetaEntity> = handle_error(
            rw.scan()
                .secondary(TaskMetaEntityKey::status)?
                .start_with(TaskStatus::Removed.to_string().as_str())?
                .try_collect(),
        )?;
        for entity in entities {
            handle_error(rw.remove(entity))?;
        }
        handle_error(rw.commit())?;
        Ok(())
    }
}
