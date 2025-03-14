use crate::core::error::SchedulerError;

#[derive(Debug)]
pub struct TaskResult {
    pub task_id: String,
    pub last_duration_ms: usize,
    pub last_retry_count: usize,
    pub result: Result<(), SchedulerError>,
}

impl TaskResult {
    /// Create a success result with task_id
    pub fn success(task_id: String, last_duration_ms: usize, last_retry_count: usize) -> Self {
        Self {
            task_id,
            result: Ok(()),
            last_duration_ms,
            last_retry_count,
        }
    }

    /// Create a failure result with task_id and TaskError
    pub fn failure(
        task_id: String,
        error: SchedulerError,
        last_duration_ms: usize,
        last_retry_count: usize,
    ) -> Self {
        Self {
            task_id,
            result: Err(error),
            last_duration_ms,
            last_retry_count,
        }
    }

    /// Check if the result is a success
    pub fn is_success(&self) -> bool {
        self.result.is_ok()
    }
}
