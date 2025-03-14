use crate::core::task_kind::TaskKind;
use crate::core::{cron::next_run, model::TaskMeta, result::TaskResult, store::TaskStore};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::error;

#[derive(Debug)]
pub enum UpdateRequest {
    PoisonPill,
    Heartbeat(String),
    ExecutionResult(String, TaskMeta, TaskResult),
}

pub struct TaskStatusUpdater {
    sender: mpsc::Sender<UpdateRequest>,
}

impl TaskStatusUpdater {
    pub fn new<T>(task_store: Arc<T>, processor_num: usize) -> Self
    where
        T: TaskStore + Send + Sync + Clone + 'static,
    {
        let (sender, mut receiver) = mpsc::channel::<UpdateRequest>(100);
        let instance = TaskStatusUpdater { sender };

        tokio::spawn({
            async move {
                let mut poison_pill = 0;
                while let Some(request) = receiver.recv().await {
                    let task_store = task_store.clone();
                    match request {
                        UpdateRequest::Heartbeat(task_id) => {
                            if let Err(e) = task_store.heartbeat(&task_id, "").await {
                                tracing::warn!("Failed to heartbeat: {}", e);
                            }
                        }
                        UpdateRequest::ExecutionResult(queue_name, task_meta, task_result) => {
                            if let Err(e) = Self::update_task_execution_status(
                                task_store,
                                task_meta,
                                task_result,
                            )
                            .await
                            {
                                error!(
                                    "Task queue '{}': Failed to update task execution status: {:?}",
                                    queue_name, e
                                );
                            }
                        }
                        UpdateRequest::PoisonPill => {
                            poison_pill += 1;
                            if poison_pill == processor_num {
                                break;
                            }
                        }
                    }
                }
            }
        });

        instance
    }

    pub async fn queue(&self, request: UpdateRequest) {
        if let Err(e) = self.sender.send(request).await {
            error!("Failed to queue task status. Channel error: {:?}", e);
        }
    }

    async fn update_task_execution_status<T>(
        task_store: Arc<T>,
        task: TaskMeta,
        result: TaskResult,
    ) -> Result<(), String>
    where
        T: TaskStore + Send + Clone + 'static,
    {
        // Determine if the task execution was successful
        let is_success = result.is_success();
        let last_duration_ms = result.last_duration_ms;
        let last_retry_count = result.last_retry_count;
        let (last_error, next_run) = Self::handle_task_result(result, &task).await;
        // Update the task execution status in the task store
        task_store
            .update_task_execution_status(
                &task.id,
                is_success,
                last_error,
                Some(last_duration_ms),
                Some(last_retry_count),
                next_run,
            )
            .await
            .map_err(|e| {
                format!(
                    "Failed to update task execution status for task {}: {:?}",
                    task.id, e
                )
            })?;

        Ok(()) // Return Ok if successful
    }

    async fn handle_task_result(
        result: TaskResult,
        task: &TaskMeta,
    ) -> (Option<String>, Option<i64>) {
        // Handle the result of task execution to determine next run time and error status
        match result {
            TaskResult { result: Ok(()), .. } => {
                let next_run = Self::calculate_next_run(task).await; // Calculate next run time on success
                (None, next_run)
            }
            TaskResult {
                result: Err(e),
                task_id,
                ..
            } => {
                // Log the error and return it as last_error
                let last_error = Some(format!("{}, Error message: {}", e.description(), e));
                tracing::error!(
                    "Task execution failed for task {}: {:?}",
                    task_id,
                    last_error
                );
                (last_error, None) // No next run time on failure
            }
        }
    }

    async fn calculate_next_run(task: &TaskMeta) -> Option<i64> {
        // Calculate the next run time based on the task type
        match &task.kind {
            TaskKind::Repeat { interval_seconds } => {
                let next_run = task.next_run + (interval_seconds * 1000) as i64;
                Some(next_run) // Return the next run time for repeat tasks
            }
            TaskKind::Cron { schedule, timezone } => {
                // Calculate next run time for cron jobs using schedule and timezone
                next_run(&*schedule, &*timezone, task.next_run)
            }
            _ => None, // No next run time for one-time tasks
        }
    }
}
