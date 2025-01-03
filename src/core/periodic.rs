use crate::core::error::BoxError;
use crate::core::shutdown::shutdown_signal;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Default)]
pub struct PeriodicTask {
    // Name of the periodic task.
    name: String,
    // A notification mechanism for shutdown signaling.
    shutdown: Arc<RwLock<bool>>,
}

impl PeriodicTask {
    /// Creates a new instance of `PeriodicTask`.
    ///
    /// # Arguments
    ///
    /// * `name`: A string slice that holds the name of the task.
    ///
    /// # Returns
    ///
    /// Returns a new instance of `PeriodicTask`.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Sends a shutdown signal to the task.
    ///
    /// This method notifies the task to stop executing.
    pub async fn shutdown(self: Arc<Self>) {
        let mut triggered = self.shutdown.write().await; // Acquire write lock to set shutdown state
        *triggered = true; // Set the shutdown state to true
    }

    /// Starts the periodic task and sets up a signal handler for shutdown.
    ///
    /// # Arguments
    ///
    /// * `task`: An `Arc` of a function that returns a future to be executed periodically.
    /// * `interval`: A `Duration` specifying how often the task should run.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the future returned by the task function.
    ///
    /// # Requirements
    ///
    /// The future must output a `Result` with a unit value or a `BoxError` on failure.
    pub fn start_with_signal<F, P>(
        self: Arc<Self>,
        task: Arc<dyn Fn(Option<Arc<P>>) -> F + Send + Sync>,
        param: Option<Arc<P>>,
        interval: Duration,
    ) where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
        P: Send + Sync + 'static,
    {
        // Clone the periodic task instance for the task runner.
        let task_clone = Arc::clone(&self);
        let param_clone = param.clone();
        let task_runner = async move {
            // Run the task periodically.
            task_clone.run(task.clone(), param_clone, interval).await;
        };

        // Clone the periodic task instance for the signal handler.
        let signal_clone = Arc::clone(&self);
        let signal_handler = async move {
            // Listen for a shutdown signal (Ctrl+C).
            shutdown_signal().await;
            info!("Shutting down periodic task '{}'...", &self.name);
            // Notify the task to shut down.
            signal_clone.shutdown().await;
        };

        // Spawn the task runner and signal handler as asynchronous tasks.
        tokio::spawn(task_runner);
        tokio::spawn(signal_handler);
    }

    /// Runs the periodic task at the specified interval.
    ///
    /// # Arguments
    ///
    /// * `task`: An `Arc` of a function that returns a future to be executed.
    /// * `interval`: A `Duration` specifying how often the task should run.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the future returned by the task function.
    async fn run<F, P>(
        self: Arc<Self>,
        task: Arc<dyn Fn(Option<Arc<P>>) -> F + Send + Sync>,
        param: Option<Arc<P>>,
        interval: Duration,
    ) where
        F: Future<Output = Result<(), BoxError>> + Send + 'static,
        P: Send + Sync + 'static,
    {
        info!("task '{}' started", &self.name);
        loop {
            // Check if shutdown is triggered
            let triggered = self.shutdown.read().await;
            if *triggered {
                break; // Exit loop if shutdown is triggered
            }

            let task_clone = Arc::clone(&task);
            let param_clone = param.clone();
            let task_future = tokio::spawn(async move {
                task_clone(param_clone).await // Execute the task.
            });

            // Handle the result of the task execution.
            match task_future.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!("task '{}' failed: {:?}", &self.name, e);
                }
                Err(e) if e.is_panic() => {
                    error!("Fatal: task '{}' encountered a panic.", &self.name);
                }
                Err(e) => {
                    error!("task '{}' failed unexpectedly: {:?}", &self.name, e);
                }
            }
            sleep(interval).await;
        }
        info!("task '{}' stopped", &self.name);
    }
}
