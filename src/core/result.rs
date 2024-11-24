use crate::core::error::SchedulerError;

#[derive(Debug)]
pub struct TaskResult {
    pub task_id: String,
    pub result: Result<(), SchedulerError>,
}

impl TaskResult {
    /// Create a success result with task_id
    pub fn success(task_id: String) -> Self {
        Self {
            task_id,
            result: Ok(()),
        }
    }

    /// Create a failure result with task_id and TaskError
    pub fn failure(task_id: String, error: SchedulerError) -> Self {
        Self {
            task_id,
            result: Err(error),
        }
    }

    /// Check if the result is a success
    pub fn is_success(&self) -> bool {
        self.result.is_ok()
    }
}
