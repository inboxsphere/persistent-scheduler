use crate::core::cleaner::TaskCleaner;
use crate::core::cron::next_run;
use crate::core::handlers::TaskHandlers;
use crate::core::store::TaskStore;
use crate::core::task::Task;
use crate::core::task_kind::TaskKind;
use crate::core::worker::process_task_worker;
use crate::utc_now;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::signal;
use tokio::sync::RwLock;

pub struct TaskContext<S>
where
    S: TaskStore, // Ensures that S is a type that implements the TaskStore trait
{
    queue_concurrency: HashMap<String, u32>, // Stores the concurrency level for each task queue
    handlers: TaskHandlers, // Collection of task handlers to process different task types
    shutdown: Arc<RwLock<bool>>, // Shared state to control the shutdown of the system
    store: Arc<S>, // Arc wrapper around the task store, allowing shared ownership across threads
}

impl<S> TaskContext<S>
where
    S: TaskStore + Sync + Send + 'static, // S must implement TaskStore, and be Sync and Send
{
    /// Creates a new TaskContext with the provided store.
    pub fn new(store: S) -> Self {
        Self {
            queue_concurrency: HashMap::new(), // Initialize concurrency map as empty
            handlers: TaskHandlers::new(),     // Create a new TaskHandlers instance
            store: Arc::new(store),            // Wrap the store in an Arc for shared ownership
            shutdown: Arc::new(RwLock::new(false)), // Initialize shutdown state to false
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
    pub fn set_concurrency(mut self, queue: &str, count: u32) -> Self {
        self.queue_concurrency.insert(queue.to_owned(), count); // Update the concurrency level for the queue
        self
    }

    /// Starts the task cleaner to periodically clean up tasks.
    fn start_task_cleaner(&self) {
        let cleaner = Arc::new(TaskCleaner::new(self.store.clone())); // Create a new TaskCleaner
        cleaner.start(Duration::from_secs(60 * 10)); // Start the cleaner to run every 10 minutes
    }

    /// Starts worker threads for processing tasks in each queue.
    fn start_worker(&self) {
        let Self {
            queue_concurrency,
            handlers,
            store,
            shutdown,
        } = self;
        // Spawn workers based on the concurrency settings for each queue
        for (queue, concurrency) in queue_concurrency.iter() {
            for _ in 0..*concurrency {
                let handlers = handlers.clone(); // Clone the handlers for the worker
                let task_store = store.clone(); // Clone the task store for the worker
                let shutdown = shutdown.clone(); // Clone the shutdown flag for the worker
                let queue = queue.clone(); // Clone the queue name for the worker
                tokio::spawn(async move {
                    process_task_worker(queue.as_str(), handlers, task_store, shutdown).await;
                    // Start processing tasks in the worker
                });
            }
        }
    }

    /// Starts a signal listener to handle shutdown signals.
    pub fn start_signal_listener(&self) {
        let shutdown = self.shutdown.clone(); // Clone the shutdown flag
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    println!("Shutdown signal received (Ctrl+C). Terminating all scheduled tasks and shutting down the system...");
                    let mut triggered = shutdown.write().await; // Acquire write lock to set shutdown state
                    *triggered = true; // Set the shutdown state to true
                }
                Err(err) => {
                    eprintln!("Error listening for shutdown signal: {:?}", err);
                    // Log any errors encountered while listening
                }
            }
        });
    }

    /// Starts the task context, including workers and the task cleaner.
    pub fn start(self) -> Self {
        self.start_worker(); // Start task workers
        self.start_task_cleaner(); // Start the task cleaner
        self.start_signal_listener(); // Start the signal listener
        self
    }

    /// Adds a new task to the context for execution.
    pub async fn add_task<T>(&self, task: T) -> Result<(), String>
    where
        T: Task + Send + Sync + 'static, // T must implement the Task trait and be thread-safe
    {
        let mut task_meta = task.new_meta(); // Create metadata for the new task
        let next_run = match T::TASK_KIND {
            TaskKind::Once | TaskKind::Repeat => {
                utc_now!() + (task_meta.delay_seconds * 1000) as i64
            } // Set next run time to now for once or repeat tasks
            TaskKind::Cron => {
                let schedule = T::SCHEDULE
                    .ok_or_else(|| "Cron schedule is required for TaskKind::Cron".to_string())?; // Ensure a cron schedule is provided

                let timezone = T::TIMEZONE
                    .ok_or_else(|| "Timezone is required for TaskKind::Cron".to_string())?; // Ensure a timezone is provided

                // Calculate the next run time based on the cron schedule and timezone
                next_run(schedule, timezone, 0).ok_or_else(|| {
                    format!("Failed to calculate next run for cron task '{}': invalid schedule or timezone", T::TASK_KEY)
                })?
            }
        };

        task_meta.next_run = next_run; // Set the next run time in the task metadata
        task_meta.last_run = next_run; // Set the last run time in the task metadata
        self.store
            .store_task(task_meta) // Store the task metadata in the task store
            .await
            .map_err(|e| e.to_string()) // Handle any errors during the store operation
    }
}
