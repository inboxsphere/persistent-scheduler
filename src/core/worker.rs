use crate::core::cron::next_run;
use crate::core::model::TaskMeta;
use crate::core::result::TaskResult;
use crate::core::task_kind::TaskKind;
use crate::core::{handlers::TaskHandlers, store::TaskStore};
use crate::generate_token;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

pub(crate) async fn process_task_worker<T>(
    queue_name: &str,
    handlers: TaskHandlers,
    task_store: Arc<T>,
    shutdown: Arc<RwLock<bool>>,
) where
    T: TaskStore + Send + Clone + 'static,
{
    // Generate a unique identifier for the worker
    let worker_id = generate_token!();
    loop {
        // Check if shutdown is triggered
        let triggered = shutdown.read().await;
        if *triggered {
            break; // Exit loop if shutdown is triggered
        }

        // Fetch and execute a task from the task store
        let fetch =
            fetch_and_execute_task(queue_name, &worker_id, handlers.clone(), task_store.clone())
                .await;

        match fetch {
            // If a task is retrieved, log the information and update its status
            Some((task, result)) => {
                tracing::debug!(
                    worker_id = %worker_id,
                    next_run = ?task.next_run,
                    "Worker received a task for execution, scheduled to run at timestamp"
                );
                if let Err(e) = update_task_execution_status(task_store.clone(), task, result).await
                {
                    tracing::error!("Failed to update task execution status: {:?}", e);
                    break; // Exit the loop on error
                }
            }
            // If no task is available, wait and retry
            None => {
                tracing::debug!(
                    "Worker {} has no task available, waiting before retrying...",
                    worker_id
                );
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                // Sleep for 1 second
            }
        }
    }
    tracing::warn!("Task worker='{worker_id}' is closed"); // Log that the worker has closed
}

async fn fetch_and_execute_task<T>(
    queue_name: &str,
    worker_id: &str,
    handlers: TaskHandlers,
    task_store: Arc<T>,
) -> Option<(TaskMeta, TaskResult)>
where
    T: TaskStore + Send + Clone + 'static,
{
    // Fetch a pending task from the task store
    match task_store.fetch_pending_task(queue_name, worker_id).await {
        Ok(Some(task)) => {
            // Execute the task and monitor its execution
            let result = monitor_task_execution(
                task_store.clone(),
                handlers.execute(task.clone()),
                &task.id,
                worker_id,
                10000,
                &task.task_key,
            )
            .await;
            Some((task, result)) // Return the fetched task and its result
        }
        Ok(None) => None, // No task found
        Err(e) => {
            tracing::error!("Failed to fetch pending task: {}", e);
            None // Return None on error
        }
    }
}

async fn calculate_next_run(task: &TaskMeta) -> Option<i64> {
    // Calculate the next run time based on the task type
    match task.kind {
        TaskKind::Repeat => {
            let next_run = task.next_run + (task.repeat_interval * 1000) as i64;
            Some(next_run) // Return the next run time for repeat tasks
        }
        TaskKind::Cron => {
            // Calculate next run time for cron jobs using schedule and timezone
            if let (Some(cron_schedule), Some(cron_timezone)) =
                (&task.cron_schedule, &task.cron_timezone)
            {
                next_run(cron_schedule, cron_timezone, task.next_run)
            } else {
                None // Return None if schedule or timezone is not defined
            }
        }
        _ => None, // No next run time for one-time tasks
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
    let (last_error, next_run) = handle_task_result(result, &task).await;
    // Update the task execution status in the task store
    task_store
        .update_task_execution_status(&task.id, is_success, last_error, next_run)
        .await
        .map_err(|e| {
            format!(
                "Failed to update task execution status for task {}: {:?}",
                task.id, e
            )
        })?;

    Ok(()) // Return Ok if successful
}

async fn handle_task_result(result: TaskResult, task: &TaskMeta) -> (Option<String>, Option<i64>) {
    // Handle the result of task execution to determine next run time and error status
    match result {
        TaskResult { result: Ok(()), .. } => {
            let next_run = calculate_next_run(task).await; // Calculate next run time on success
            (None, next_run)
        }
        TaskResult {
            result: Err(e),
            task_id,
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

async fn monitor_task_execution<F, T>(
    storage: Arc<T>,
    future: F,
    task_id: &str,
    worker_id: &str,
    heartbeat_interval: u64,
    task_name: &str,
) -> F::Output
where
    F: Future,
    T: TaskStore,
{
    // Monitor the execution of a task and send heartbeats
    let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_interval));
    let mut future = std::pin::pin!(future);
    let start_time = Instant::now(); // Record the start time

    let task_display_name = format!("{{'{}'-{}}}", task_name, task_id);
    let mut tick_count = 0;
    let log_frequency = 5; // Log every 5 heartbeats

    loop {
        tokio::select! {
            output = &mut future => {
                return output; // Return the output when the future completes
            },

            _ = interval.tick() => {
                tick_count += 1; // Increment heartbeat tick count

                // Log elapsed time at specified intervals
                if tick_count % log_frequency == 0 {
                    let elapsed_time = start_time.elapsed();
                    let elapsed_seconds = elapsed_time.as_secs();

                    // Log different formats based on elapsed time
                    if elapsed_seconds >= 3600 {
                        let hours = elapsed_seconds / 3600;
                        let minutes = (elapsed_seconds % 3600) / 60;
                        tracing::info!(
                            "Task {} has been running for {} hours and {} minutes.",
                            task_display_name,
                            hours,
                            minutes
                        );
                    } else if elapsed_seconds >= 60 {
                        let minutes = elapsed_seconds / 60;
                        let seconds = elapsed_seconds % 60;
                        tracing::info!(
                            "Task {} has been running for {} minutes and {} seconds.",
                            task_display_name,
                            minutes,
                            seconds
                        );
                    } else {
                        let seconds = elapsed_seconds;
                        let millis = elapsed_time.subsec_millis();
                        tracing::info!(
                            "Task {} has been running for {} seconds and {} milliseconds.",
                            task_display_name,
                            seconds,
                            millis
                        );
                    }
                }

                // Send heartbeat to task store
                if let Err(e) = storage.heartbeat(task_id, worker_id).await {
                    tracing::warn!("Failed to heartbeat: {}", e);
                }
            }
        }
    }
}
