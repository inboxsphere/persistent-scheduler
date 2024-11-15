use crate::core::error::SchedulerError;
use crate::core::model::TaskMeta;
use crate::core::result::TaskResult;
use crate::core::task::Task;
use ahash::AHashMap;
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin, sync::Arc};
use tracing::{error, info, warn};

// Type alias for a task handler that takes `Value` as input
// and returns a pinned future that resolves to a Result.
pub type Handler = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct TaskHandlers {
    // A hashmap to store task handlers, mapping task keys to their corresponding handlers.
    handlers: AHashMap<String, Handler>,
}

impl TaskHandlers {
    /// Creates a new `TaskHandlers` container, initializing an empty handler map.
    pub fn new() -> Self {
        Self {
            handlers: AHashMap::new(),
        }
    }

    /// Registers a new task type in the handler map.
    ///
    /// # Type Parameter
    /// * `T`: The type of the task being registered, which must implement the `Task` trait.
    pub fn register<T>(&mut self)
    where
        T: Task,
    {
        self.handlers.insert(
            T::TASK_KEY.to_owned(), // Use the task key as the hashmap key.
            Arc::new(|params| process::<T>(params)), // Create a new handler for the task type.
        );
    }

    /// Executes a task based on its metadata.
    ///
    /// # Arguments
    /// * `task_meta`: Metadata of the task to be executed, encapsulated in `TaskMetaEntity`.
    ///
    /// # Returns
    /// * A `TaskResult` indicating the success or failure of the task execution.
    pub async fn execute(&self, task_meta: TaskMeta) -> TaskResult {
        let task_id = task_meta.id.clone(); // Clone the task ID for later use.
        let task_key = task_meta.task_key.clone(); // Clone the task key to retrieve the handler.
        let retry_policy = task_meta.retry_policy(); // Get the retry policy associated with the task.
        let mut attempts = 0; // Initialize the attempt counter.

        // Define an asynchronous block for task execution with retry logic.
        let execution_future = async move {
            loop {
                // Get the handler for the specified task key.
                let handler_option = self
                    .handlers
                    .get(&task_key)
                    .map(|handler| execute(handler.clone(), Arc::new(task_meta.clone())));

                // Attempt to execute the task using the found handler.
                let result = if let Some(execution_future) = handler_option {
                    execution_future.await // Await the execution result.
                } else {
                    error!("Unrecognized Task '{task_key}'. This error should not occur; it may indicate that the task has not been registered by the developer.");
                    TaskResult::failure(task_id.clone(), SchedulerError::unrecognized())
                    // Handle unrecognized task.
                };

                // If the execution was successful, return the result.
                if result.is_success() {
                    return result;
                }

                attempts += 1; // Increment the attempts counter.
                               // Check if there is a maximum retry limit defined.
                if let Some(max) = retry_policy.max_retries {
                    if attempts >= max {
                        warn!(
                            "Task {} has exceeded the maximum retry attempts of {}",
                            task_id, max
                        );
                        return result; // Return the result after exceeding max retries.
                    }
                }
                // Wait for the defined backoff period before retrying.
                let wait = retry_policy.wait_time(attempts);
                tokio::time::sleep(Duration::from_secs(wait as u64)).await;
            }
        };
        execution_future.await // Await the execution future.
    }
}

/// Processes the parameters for a given task type and returns a pinned future.
///
/// # Type Parameter
/// * `T`: The type of the task, which must implement the `Task` trait.
///
/// # Arguments
/// * `params`: The parameters for the task, represented as a `Value`.
///
/// # Returns
/// * A pinned future that resolves to a `Result` indicating the success or failure of the task processing.
pub fn process<T>(
    params: String,
) -> Pin<Box<dyn Future<Output = Result<(), SchedulerError>> + Send>>
where
    T: Task,
{
    Box::pin(async move {
        // Deserialize the parameters into the specific task type `T`.
        let task =
            serde_json::from_str::<T>(params.as_str()).map_err(|_| SchedulerError::InvalidJson)?;
        // Execute the task and return any errors that occur during execution.
        task.run().await.map_err(SchedulerError::Execution)
    })
}

/// Executes a given handler with the specified task metadata.
///
/// # Arguments
/// * `handler`: The task handler to be executed.
/// * `task_meta`: The metadata for the task to be executed.
///
/// # Returns
/// * A `TaskResult` indicating the outcome of the task execution.
async fn execute(handler: Handler, task_meta: Arc<TaskMeta>) -> TaskResult {
    let task_name = task_meta.task_key.clone(); // Clone the task key for logging.
    let task_queue = task_meta.queue_name.clone(); // Clone the task queue name for logging.
    let start = Instant::now(); // Record the start time of the task execution.
    let task_params = task_meta.task_params.clone(); // Clone the task parameters for execution.

    // Spawn a new asynchronous task to execute the handler.
    let task_future = tokio::spawn(async move { (handler)(task_params).await });
    let task_id = task_meta.id.clone();
    match task_future.await {
        Ok(Ok(_)) => {
            let duration = start.elapsed(); // Calculate the duration of the task execution.
            info!(
                "Task '{{{task_name}-{task_id}}}' in queue '{task_queue}' executed successfully, took {:?}",
                duration
            );
            TaskResult::success(task_meta.id.clone()) // Return success result.
        }
        Ok(Err(e)) => {
            warn!("Task '{{{task_name}-{task_id}}}' in queue '{task_queue}' errored, {e:#?}");
            TaskResult::failure(task_meta.id.clone(), e) // Return failure result with the error.
        }
        Err(e) if e.is_panic() => {
            warn!("Task '{{{task_name}-{task_id}}}' in queue '{task_queue}' panicked");
            TaskResult::failure(task_meta.id.clone(), SchedulerError::panic()) // Handle panic case.
        }
        Err(e) => {
            println!(
                "Task '{{{task_name}}}' in queue '{task_queue}' failed unexpectedly: {:?}",
                e
            );
            TaskResult::failure(task_id, SchedulerError::unexpect(e))
            // Handle unexpected failure.
        }
    }
}
