use crate::core::cleaner::TaskCleaner;
use crate::core::cron::next_run;
use crate::core::handlers::TaskHandlers;
use crate::core::model::TaskMeta;
use crate::core::status_updater::TaskStatusUpdater;
use crate::core::store::TaskStore;
use crate::core::task::Task;
use crate::core::task_kind::TaskKind;
use crate::utc_now;
use ahash::AHashMap;
use std::sync::Arc;
use std::time::Duration;

use super::flow::TaskFlow;

pub struct TaskContext<S>
where
    S: TaskStore + Send + Sync + Clone + 'static, // Ensures that S is a type that implements the TaskStore trait
{
    queue_concurrency: AHashMap<String, usize>, // Stores the concurrency level for each task queue
    handlers: TaskHandlers, // Collection of task handlers to process different task types
    store: Arc<S>, // Arc wrapper around the task store, allowing shared ownership across threads
}

impl<S> TaskContext<S>
where
    S: TaskStore + Send + Sync + Clone + 'static, // S must implement TaskStore, and be Sync and Send
{
    /// Creates a new TaskContext with the provided store.
    pub fn new(store: S) -> Self {
        let store = Arc::new(store);
        Self {
            queue_concurrency: AHashMap::new(), // Initialize concurrency map as empty
            handlers: TaskHandlers::new(),      // Create a new TaskHandlers instance
            store: store.clone(),               // Wrap the store in an Arc for shared ownership
        }
    }

    /// Registers a new task type in the context.
    pub fn register<T>(mut self) -> Self
    where
        T: Task, // T must implement the Task trait
    {
        self.handlers.register::<T>(); // Register the task handler
        self.queue_concurrency.insert(T::TASK_QUEUE.to_owned(), 4); // Set default concurrency for the task queue
        self
    }

    /// Sets the concurrency level for a specified queue.
    pub fn set_concurrency(mut self, queue: &str, count: usize) -> Self {
        self.queue_concurrency.insert(queue.to_owned(), count); // Update the concurrency level for the queue
        self
    }

    /// Starts the task cleaner to periodically clean up tasks.
    fn start_task_cleaner(&self) {
        let cleaner = Arc::new(TaskCleaner::new(self.store.clone())); // Create a new TaskCleaner
        cleaner.start(Duration::from_secs(60 * 10)); // Start the cleaner to run every 10 minutes
    }

    /// Starts worker threads for processing tasks in each queue.
    async fn start_flow(&self) {
        let status_updater = Arc::new(TaskStatusUpdater::new(
            self.store.clone(),
            self.queue_concurrency.len(),
        ));

        let flow = Arc::new(TaskFlow::new(
            self.store.clone(),
            &self.queue_concurrency,
            Arc::new(self.handlers.clone()),
            status_updater,
        ));

        flow.start().await;
    }

    /// Starts the task context, including workers and the task cleaner.
    pub async fn start(self) -> Self {
        self.start_flow().await; // Start task workers
        self.start_task_cleaner(); // Start the task cleaner
        self
    }

    /// Adds a new task to the context for execution.
    pub async fn add_task<T>(
        &self,
        task: T,
        kind: TaskKind,
        delay_seconds: Option<u32>
    ) -> Result<(), String>
    where
        T: Task + Send + Sync + 'static, // T must implement the Task trait and be thread-safe
    {
        let mut task_meta = task.new_meta(kind); // Create metadata for the new task
        let next_run = match &task_meta.kind {
            TaskKind::Once | TaskKind::Repeat { .. } => {
                let delay_seconds = delay_seconds.unwrap_or(task_meta.delay_seconds) * 1000;
                utc_now!() + delay_seconds as i64
            } // Set the next run time by adding a delay to the current time, allowing the task to run at a specified future time.
            TaskKind::Cron { schedule, timezone } => {
                // Calculate the next run time based on the cron schedule and timezone
                next_run(&*schedule, &*timezone, 0).ok_or_else(|| {
                    format!("Failed to calculate next run for cron task '{}': invalid schedule or timezone", T::TASK_KEY)
                })?
            }
        };

        task_meta.next_run = next_run;
        task_meta.last_run = next_run;
        self.store
            .store_task(task_meta) // Store the task metadata in the task store
            .await
            .map_err(|e| e.to_string()) // Handle any errors during the store operation
    }

    /// Adds a new task to the context for execution.
    pub async fn add_tasks<T>(&self, tasks: Vec<TaskConfiguration<T>>) -> Result<(), String>
    where
        T: Task + Send + Sync + 'static, // T must implement the Task trait and be thread-safe
    {
        let mut batch: Vec<TaskMeta> = Vec::new();

        for task in tasks {
            let mut task_meta = task.inner.new_meta(task.kind); // Create metadata for the new task
            let next_run = match &task_meta.kind {
                TaskKind::Once | TaskKind::Repeat { .. } => {
                    let delay_seconds =
                        task.delay_seconds.unwrap_or(task_meta.delay_seconds) * 1000;
                    utc_now!() + delay_seconds as i64
                } // Set the next run time by adding a delay to the current time, allowing the task to run at a specified future time.
                TaskKind::Cron { schedule, timezone } => {
                    // Calculate the next run time based on the cron schedule and timezone
                    next_run(&*schedule, &*timezone, 0).ok_or_else(|| {
                    format!("Failed to calculate next run for cron task '{}': invalid schedule or timezone", T::TASK_KEY)
                })?
                }
            };

            task_meta.next_run = next_run;
            task_meta.last_run = next_run;
            batch.push(task_meta);
        }

        self.store
            .store_tasks(batch) // Store the task metadata in the task store
            .await
            .map_err(|e| e.to_string()) // Handle any errors during the store operation
    }
}

pub struct TaskConfiguration<T: Task> {
    pub inner: T,
    pub kind: TaskKind,
    pub delay_seconds: Option<u32>,
}
