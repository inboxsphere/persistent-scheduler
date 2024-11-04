use crate::core::periodic::PeriodicTask;
use crate::core::store::TaskStore;
use std::{sync::Arc, time::Duration};

#[derive(Default)]
pub struct TaskCleaner<T>
where
    T: TaskStore,
{
    // Arc pointer to the task store for managing tasks.
    pub task_store: Arc<T>,
    // Arc pointer to the periodic task that will perform the cleanup.
    periodic_task: Arc<PeriodicTask>,
}

impl<T> TaskCleaner<T>
where
    T: TaskStore + Sync + Send + 'static,
{
    /// Creates a new instance of `TaskCleaner`.
    ///
    /// # Arguments
    ///
    /// * `task_store`: An `Arc` pointing to a `TaskStore` implementation for managing tasks.
    ///
    /// # Returns
    ///
    /// Returns a new instance of `TaskCleaner`.
    pub fn new(task_store: Arc<T>) -> Self {
        Self {
            task_store: task_store.clone(),
            periodic_task: Arc::new(PeriodicTask::new("task cleaner")),
        }
    }

    /// Starts the task cleaner, which will perform cleanup tasks at a specified interval.
    ///
    /// # Arguments
    ///
    /// * `interval`: A `Duration` specifying how often the cleanup task should run.
    ///
    /// # Remarks
    ///
    /// This method creates a closure that invokes the cleanup method from the task store,
    /// and schedules it to run periodically.
    pub fn start(self: Arc<Self>, interval: Duration) {
        // Clone the `TaskCleaner` instance to use in the closure.
        let self_clone = Arc::clone(&self);

        // Create a closure that will perform the cleanup task.
        let task = Arc::new(move |_: Option<Arc<String>>| {
            // Clone the task store for the cleanup operation.
            let task_store = Arc::clone(&self_clone.task_store);
            // Return a pinned future that performs the cleanup operation.
            Box::pin(async move { task_store.cleanup().await.map_err(|e| e.into()) })
        });

        // Start the periodic task that will execute the cleanup operation at the specified interval.
        self.periodic_task
            .clone()
            .start_with_signal(task, None, interval);
    }
}
