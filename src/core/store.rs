use crate::core::task_kind::TaskKind;
use crate::{
    core::model::{TaskMeta, TaskStatus},
    utc_now,
};
use ahash::AHashMap;
use async_trait::async_trait;
use std::{error::Error, sync::Arc};
use thiserror::Error;
use tokio::sync::RwLock;

#[async_trait::async_trait]
pub trait TaskStore: Clone + Send {
    type Error: Error + Send + Sync;

    /// Restores task states by cleaning up all tasks in a running state and handling their next run times.
    ///
    /// This method performs the following actions:
    /// - Cleans up all tasks that are currently in the `Running` state and may handle their `next_run` fields.
    /// - Additional restoration logic can be added within this method.
    ///
    /// # Returns
    /// Returns a `Result`, which is `Ok(())` if the operation succeeds; otherwise, it returns the appropriate error.
    ///
    /// # Examples
    /// ```
    /// # async fn example() {
    /// #     let store = TaskStore::new();
    /// #     store.restore_tasks().await.unwrap();
    /// # }
    /// ```
    async fn restore_tasks(&self) -> Result<(), Self::Error>;

    /// Retrieves task metadata based on the task ID.
    ///
    /// # Arguments
    ///
    /// * `task_id`: A unique identifier for the task.
    ///
    /// # Returns
    ///
    /// Returns an `Option<TaskMetaEntity>`. If the task is found, it returns `Some(TaskMetaEntity)`, otherwise it returns `None`.
    async fn get(&self, task_id: &str) -> Result<Option<TaskMeta>, Self::Error>;

    /// Lists all task metadata.
    ///
    /// # Returns
    ///
    /// Returns a vector containing all task metadata.
    async fn list(&self) -> Result<Vec<TaskMeta>, Self::Error>;

    /// Stores task metadata.
    ///
    /// # Arguments
    ///
    /// * `task`: The task metadata to be stored.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the task is successfully stored; returns an error if the task ID already exists.
    async fn store_task(&self, task: TaskMeta) -> Result<(), Self::Error>;

    /// Stores tasks metadata.
    ///
    /// # Arguments
    ///
    /// * `tasks`: The task metadata to be stored.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the tasks is successfully stored; returns an error if any task ID already exists.
    async fn store_tasks(&self, tasks: Vec<TaskMeta>) -> Result<(), Self::Error>;

    /// Fetches all pending tasks from the store.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Vec<TaskMeta>` if successful, or an error of type `Self::Error` if fetching tasks fails.
    ///
    /// The returned `Vec<TaskMeta>` contains all tasks that are currently in a pending state, ready for processing.
    ///
    /// # Errors
    ///
    /// This function will return an error of type `Self::Error` if there is an issue querying the task store.
    async fn fetch_pending_tasks(&self) -> Result<Vec<TaskMeta>, Self::Error>;

    /// Updates the execution status of a task.
    ///
    /// # Arguments
    ///
    /// * `task_id`: The ID of the task to update.
    /// * `is_success`: A boolean indicating whether the task succeeded.
    /// * `last_error`: An optional string containing the last error message (if applicable).
    /// * `next_run`: An optional timestamp for the next scheduled run of the task.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update is successful; returns an error if the task is not found or if it is stopped or removed.
    async fn update_task_execution_status(
        &self,
        task_id: &str,
        is_success: bool,
        last_error: Option<String>,
        next_run: Option<i64>,
    ) -> Result<(), Self::Error>;

    /// Updates the heartbeat for a task.
    ///
    /// # Arguments
    ///
    /// * `task_id`: The ID of the task to update.
    /// * `runner_id`: The ID of the runner that is currently executing the task.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update is successful; returns an error if the task is not found.
    async fn heartbeat(&self, task_id: &str, runner_id: &str) -> Result<(), Self::Error>;

    /// Marks a task as stopped.
    ///
    /// # Arguments
    ///
    /// * `task_id`: The ID of the task to mark as stopped.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the task is successfully marked; returns an error if the task is not found.
    async fn set_task_stopped(&self, task_id: &str) -> Result<(), Self::Error>;

    /// Marks a task as removed.
    ///
    /// # Arguments
    ///
    /// * `task_id`: The ID of the task to mark as removed.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the task is successfully marked; returns an error if the task is not found.
    async fn set_task_removed(&self, task_id: &str) -> Result<(), Self::Error>;

    /// Cleans up the task store by removing tasks marked as removed.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the cleanup is successful.
    async fn cleanup(&self) -> Result<(), Self::Error>;
}

#[derive(Error, Debug)]
pub enum InMemoryTaskStoreError {
    #[error("Task not found")]
    TaskNotFound,
    #[error("Task ID conflict: The task with ID '{0}' already exists.")]
    TaskIdConflict(String),
}

#[derive(Clone, Default)]
pub struct InMemoryTaskStore {
    tasks: Arc<RwLock<AHashMap<String, TaskMeta>>>,
}

impl InMemoryTaskStore {
    /// Creates a new instance of `InMemoryTaskStore`.
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(AHashMap::new())),
        }
    }
}

/// Determines if a task can be executed based on its kind and status.
pub fn is_candidate_task(kind: &TaskKind, status: &TaskStatus) -> bool {
    match kind {
        TaskKind::Cron | TaskKind::Repeat => matches!(
            status,
            TaskStatus::Scheduled | TaskStatus::Success | TaskStatus::Failed
        ),
        TaskKind::Once => *status == TaskStatus::Scheduled,
    }
}

#[async_trait]
impl TaskStore for InMemoryTaskStore {
    type Error = InMemoryTaskStoreError;

    async fn restore_tasks(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get(&self, task_id: &str) -> Result<Option<TaskMeta>, Self::Error> {
        let tasks = self.tasks.read().await;
        Ok(tasks.get(task_id).cloned())
    }

    async fn list(&self) -> Result<Vec<TaskMeta>, Self::Error> {
        let tasks = self.tasks.read().await;
        Ok(tasks.values().cloned().collect())
    }

    async fn store_task(&self, task: TaskMeta) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;
        if tasks.contains_key(&task.id) {
            return Err(InMemoryTaskStoreError::TaskIdConflict(task.id.clone()));
        }
        tasks.insert(task.id.clone(), task);
        Ok(())
    }

    async fn store_tasks(&self, tasks: Vec<TaskMeta>) -> Result<(), Self::Error> {
        let mut w_tasks = self.tasks.write().await;
        for task in tasks {
            if w_tasks.contains_key(&task.id) {
                return Err(InMemoryTaskStoreError::TaskIdConflict(task.id.clone()));
            }
            w_tasks.insert(task.id.clone(), task);
        }
        Ok(())
    }

    async fn fetch_pending_tasks(&self) -> Result<Vec<TaskMeta>, Self::Error> {
        let mut tasks = self.tasks.write().await;
        let mut result = Vec::new();
        for task in tasks.values_mut() {
            if is_candidate_task(&task.kind, &task.status) && task.next_run <= utc_now!() {
                let t = task.clone();
                task.status = TaskStatus::Running;
                task.updated_at = utc_now!();
                result.push(t);
            }
        }
        Ok(result)
    }

    async fn update_task_execution_status(
        &self,
        task_id: &str,
        is_success: bool,
        last_error: Option<String>,
        next_run: Option<i64>, // when is None?
    ) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;

        let task = tasks
            .get_mut(task_id)
            .ok_or(InMemoryTaskStoreError::TaskNotFound)?;

        if task.status == TaskStatus::Stopped || task.status == TaskStatus::Removed {
            return Ok(());
        }

        if is_success {
            task.success_count += 1;
            task.status = TaskStatus::Success;
        } else {
            task.failure_count += 1;
            task.status = TaskStatus::Failed;
            task.last_error = last_error;
        }

        if let Some(next_run_time) = next_run {
            println!("now to set next_run={}", next_run_time);
            task.last_run = task.next_run;
            task.next_run = next_run_time;
        }

        task.updated_at = utc_now!();

        Ok(())
    }

    async fn heartbeat(&self, task_id: &str, runner_id: &str) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.heartbeat_at = utc_now!();
            task.runner_id = Some(runner_id.to_string());
            Ok(())
        } else {
            Err(InMemoryTaskStoreError::TaskNotFound)
        }
    }

    async fn set_task_stopped(&self, task_id: &str) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.updated_at = utc_now!();
            task.status = TaskStatus::Stopped;
            Ok(())
        } else {
            Err(InMemoryTaskStoreError::TaskNotFound)
        }
    }

    async fn set_task_removed(&self, task_id: &str) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks.get_mut(task_id) {
            task.updated_at = utc_now!();
            task.status = TaskStatus::Removed;
            Ok(())
        } else {
            Err(InMemoryTaskStoreError::TaskNotFound)
        }
    }

    async fn cleanup(&self) -> Result<(), Self::Error> {
        let mut tasks = self.tasks.write().await;
        tasks.retain(|_, task| task.status != TaskStatus::Removed);
        Ok(())
    }
}
