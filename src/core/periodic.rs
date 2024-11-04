use crate::core::error::BoxError;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::signal;
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
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!(
                        "Received SIGINT (Ctrl+C), shutting down periodic task '{}'...",
                        &self.name
                    );
                    // Notify the task to shut down.
                    signal_clone.shutdown().await;
                }
                Err(err) => {
                    error!(
                        "Error listening for shutdown signal: {:?}",
                        BoxError::from(err)
                    );
                }
            }
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
        info!("Periodic task '{}' started", &self.name);
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
                Ok(Ok(_)) => {
                    info!("Periodic task '{}' completed successfully.", &self.name);
                }
                Ok(Err(e)) => {
                    warn!("Periodic task '{}' failed: {:?}", &self.name, e);
                }
                Err(e) if e.is_panic() => {
                    error!("Fatal: Periodic task '{}' encountered a panic.", &self.name);
                }
                Err(e) => {
                    error!(
                        "Periodic task '{}' failed unexpectedly: {:?}",
                        &self.name, e
                    );
                }
            }
            sleep(interval).await;
        }
        info!("Periodic task '{}' stopped", &self.name);
    }
}
