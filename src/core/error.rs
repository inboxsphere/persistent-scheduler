use tokio::task::JoinError;

#[derive(Debug, thiserror::Error)]
/// Error type for task processing failures
pub enum SchedulerError {
    /// Error occurred during task execution
    #[error("{0}")]
    Execution(String),

    /// Failed to parse the provided arguments as JSON
    #[error("Invalid JSON format in task arguments")]
    InvalidJson,

    /// Task type not recognized for the client
    #[error("Unrecognized task type for the client")]
    UnrecognizedTask,

    #[error("{0}")]
    Unexpected(String),

    #[error("Can not init task store")]
    StoreInit,
}

impl SchedulerError {
    pub fn panic() -> Self {
        SchedulerError::Execution("task panicked".to_owned())
    }

    pub fn unexpect(e: JoinError) -> Self {
        SchedulerError::Unexpected(format!("task failed unexpectedly: {:?}", e))
    }

    pub fn unrecognized() -> Self {
        SchedulerError::UnrecognizedTask
    }
    pub fn description(&self) -> &'static str {
        match self {
            SchedulerError::Execution(_) => {
                "Execution: Error occurred during task execution or runtime."
            }
            SchedulerError::InvalidJson => {
                "InvalidJson: Error occurred while initializing the task, invalid JSON format."
            }
            SchedulerError::UnrecognizedTask => {
                "UnrecognizedTask: System error, could not find cached job code."
            }
            SchedulerError::Unexpected(_) => "Unexpected: An unexpected error occurred.",

            SchedulerError::StoreInit => {
                "StoreInit: Failed to initialize the task store, possibly due to configuration or connection issues."
            }
        }
    }
}

pub struct BoxError(Box<dyn std::error::Error + Send + Sync>);

impl BoxError {
    pub fn new<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        BoxError(Box::new(error))
    }
}

impl std::fmt::Debug for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<E> From<E> for BoxError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        BoxError::new(error)
    }
}

impl std::ops::Deref for BoxError {
    type Target = dyn std::error::Error + Send + Sync;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
