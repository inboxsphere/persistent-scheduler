use crate::core::model::TaskMeta;
use crate::core::processor::Package;
use crate::core::shutdown::shutdown_signal;
use crate::core::store::TaskStore;
use crate::core::{handlers, processor::TaskProcessor, status_updater::TaskStatusUpdater};
use ahash::AHashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct TaskFlow<T>
where
    T: TaskStore + Send + Sync + Clone + 'static,
{
    task_store: Arc<T>,
    processors: Arc<AHashMap<String, TaskProcessor>>,
    shutdown: Arc<RwLock<bool>>,
}

impl<T> TaskFlow<T>
where
    T: TaskStore + Send + Sync + Clone + 'static,
{
    pub fn new(
        task_store: Arc<T>,
        queue_concurrency: &AHashMap<String, usize>,
        handlers: Arc<handlers::TaskHandlers>,
        status_updater: Arc<TaskStatusUpdater>,
    ) -> Self {
        let mut processors = AHashMap::new();
        for (queue, limit) in queue_concurrency {
            let processor = TaskProcessor::new::<T>(
                queue.clone(),
                *limit,
                handlers.clone(),
                status_updater.clone(),
            );
            processors.insert(queue.clone(), processor);
        }

        Self {
            task_store,
            processors: Arc::new(processors),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn start(self: Arc<Self>) {
        let task_store = self.task_store.clone();
        let processors = self.processors.clone();
        let clone = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                let shutdown = clone.shutdown.read().await;
                if *shutdown {
                    tracing::info!("Stop to fetch pending tasks.");
                    Self::send_poison(processors.clone()).await;
                    break;
                }

                match task_store.clone().fetch_pending_tasks().await {
                    Ok(tasks) => {
                        // Group tasks by `queue_name`.
                        let mut queued_tasks: AHashMap<String, Vec<TaskMeta>> = AHashMap::new();

                        for task in tasks {
                            queued_tasks
                                .entry(task.queue_name.clone())
                                .or_insert_with(Vec::new)
                                .push(task);
                        }

                        // Send tasks to their corresponding channels.
                        for (queue_name, tasks) in queued_tasks {
                            if let Err(e) =
                                Self::send_tasks_to_channel(processors.clone(), &queue_name, tasks)
                                    .await
                            {
                                tracing::error!(
                                    "Error sending tasks to channel for queue '{}': {:?}",
                                    queue_name,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch tasks: {:?}", e);
                    }
                }

                // Simulate a delay before the next fetch.
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        });
        let clone = Arc::clone(&self);
        let signal_handler = async move {
            // Listen for a shutdown signal (Ctrl+C).
            shutdown_signal().await;
            // Notify the task to shut down.
            let mut triggered = clone.shutdown.write().await; // Acquire write lock to set shutdown state
            *triggered = true; // Set the shutdown state to true
        };

        tokio::spawn(signal_handler);
    }

    async fn send_tasks_to_channel(
        processors: Arc<AHashMap<String, TaskProcessor>>,
        queue_name: &str,
        tasks: Vec<TaskMeta>,
    ) -> Result<(), String> {
        let processor = processors.get(queue_name).ok_or_else(|| format!(
            "Unexpected error: failed to find the processor for queue '{}'. This should not happen.",
            queue_name
        ))?;

        for task in tasks {
            processor.accept(Package::task(task)).await;
        }

        Ok(())
    }

    async fn send_poison(processors: Arc<AHashMap<String, TaskProcessor>>) {
        for (_, processor) in processors.iter() {
            processor.accept(Package::PoisonPill).await;
        }
    }
}
