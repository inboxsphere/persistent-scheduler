use crate::core::handlers::TaskHandlers;
use crate::core::{
    model::TaskMeta,
    status_updater::{self, TaskStatusUpdater},
};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing::{error, warn};

pub enum Package {
    PoisonPill,
    Task(TaskMeta),
}

impl Package {
    pub fn task(task: TaskMeta) -> Self {
        Package::Task(task)
    }
}

pub struct TaskProcessor {
    channel: mpsc::Sender<Package>,
}

impl TaskProcessor {
    /// Creates a new TaskProcessor for a specific queue with a concurrency limit.
    pub fn new<T>(
        queue_name: String,
        limit: usize,
        handlers: Arc<TaskHandlers>,
        status_updater: Arc<TaskStatusUpdater>,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Package>(200);
        let semaphore = Arc::new(Semaphore::new(limit));

        let instance = TaskProcessor { channel: sender };

        tokio::spawn(async move {
            let queue_name = queue_name.clone();
            let mut handlers_in_progress: Vec<JoinHandle<()>> = Vec::new();

            while let Some(package) = receiver.recv().await {
                match package {
                    Package::PoisonPill => {
                        warn!(
                            "Received process exit signal, {} tasks still in progress.",
                            handlers_in_progress.len()
                        );

                        for handler in handlers_in_progress {
                            if let Err(e) = handler.await {
                                error!("Task execution failed: {:?}", e);
                            }
                        }
                        status_updater
                            .queue(status_updater::UpdateRequest::PoisonPill)
                            .await;
                        break;
                    }
                    Package::Task(task) => loop {
                        match semaphore.clone().try_acquire_owned() {
                            Ok(permit) => {
                                handlers_in_progress.retain(|handle| !handle.is_finished());
                                let handlers = handlers.clone();
                                let queue_name = queue_name.clone();
                                let status_updater = status_updater.clone();
                                let handler = Self::spawn_task(
                                    task,
                                    permit,
                                    handlers,
                                    queue_name,
                                    status_updater,
                                );
                                handlers_in_progress.push(handler);
                                break;
                            }
                            Err(_) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            }
                        }
                    },
                }
            }
        });

        instance
    }

    pub async fn accept(&self, package: Package) {
        if let Err(e) = self.channel.send(package).await {
            error!("Failed to queue task status. Channel error: {:?}", e);
        }
    }

    fn spawn_task(
        task: TaskMeta,
        permit: OwnedSemaphorePermit,
        handlers: Arc<TaskHandlers>,
        queue_name: String,
        status_updater: Arc<TaskStatusUpdater>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _permit = permit;
            let task_id = task.id.clone();
            let task_key = task.task_key.clone();
            let result = Self::monitor_task_execution(
                handlers.execute(task.clone()),
                &task_id,
                10000,
                &task_key,
                status_updater.clone(),
            )
            .await;

            status_updater
                .queue(status_updater::UpdateRequest::ExecutionResult(
                    queue_name, task, result,
                ))
                .await
        })
    }

    async fn monitor_task_execution<F>(
        future: F,
        task_id: &str,
        heartbeat_interval: u64,
        task_name: &str,
        status_updater: Arc<TaskStatusUpdater>,
    ) -> F::Output
    where
        F: Future,
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

                    status_updater.queue(status_updater::UpdateRequest::Heartbeat(task_id.into())).await
                }
            }
        }
    }
}
