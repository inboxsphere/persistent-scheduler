use crate::core::model::TaskMeta;
use crate::core::retry::{RetryPolicy, RetryStrategy};
use crate::core::task_kind::TaskKind;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::pin::Pin;

use super::cron::{is_valid_cron_string, is_valid_timezone};

pub type TaskFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

pub trait Task: Serialize + DeserializeOwned + 'static {
    /// A unique identifier for this task.
    ///
    /// This name must be unique and is used to identify the task.
    const TASK_KEY: &'static str;

    /// The default queue associated with this task.
    ///
    /// This can be overridden at the individual task level. If a non-existent queue is specified,
    /// the task will not be processed.
    const TASK_QUEUE: &'static str;

    /// Returns the retry policy for this task instance.
    /// Default is exponential backoff with base 2 and max 3 retries.
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            strategy: RetryStrategy::Exponential { base: 2 },
            max_retries: Some(3),
        }
    }

    /// Returns the delay in seconds before executing a Once task.
    /// Default is 3 seconds.
    fn delay_seconds(&self) -> u32 {
        3
    }

    /// Executes the task with the given parameters.
    ///
    /// Contains the logic required to perform the task. Takes parameters of type `Self::Params`
    /// that can be used during execution.
    fn run(self) -> TaskFuture;

    /// Validates the parameters based on the task type.
    ///
    /// Checks that the necessary fields for the specific `TaskKind` are provided.
    fn validate(&self, kind: &TaskKind) -> Result<(), String> {
        if Self::TASK_QUEUE.is_empty() {
            return Err("TASK_QUEUE must not be empty.".into());
        }

        match kind {
            TaskKind::Cron { schedule, timezone } => {
                if !is_valid_cron_string(&*schedule) {
                    return Err(format!("Invalid SCHEDULE: '{}' for Cron tasks.", schedule).into());
                }

                if !is_valid_timezone(&*timezone) {
                    return Err(format!("Invalid TIMEZONE: '{}' for Cron tasks.", schedule).into());
                }
            }
            TaskKind::Repeat { interval_seconds } => {
                if interval_seconds <= &0 {
                    return Err("A valid REPEAT_INTERVAL must be defined for Repeat tasks.".into());
                }
            }
            TaskKind::Once => {
                // No additional checks needed for Once tasks
            }
        }
        Ok(())
    }

    /// Creates a new metadata entry for the task.
    ///
    /// This method generates a `TaskMetaEntity` instance based on the task's properties.
    /// It validates required fields and panics if validation fails.
    fn new_meta(&self, kind: TaskKind) -> TaskMeta {
        self.validate(&kind).unwrap_or_else(|err| {
            panic!(
                "Validation failed for task '{}': {}. This indicates a programming error.",
                Self::TASK_KEY,
                err
            )
        });

        let is_repeating = matches!(kind, TaskKind::Repeat { .. });
        TaskMeta::new(
            Self::TASK_KEY.to_owned(),
            serde_json::to_string(&self).expect(
                "Serialization failed: this should never happen if all fields are serializable",
            ),
            Self::TASK_QUEUE.to_owned(),
            kind,
            self.retry_policy(),
            is_repeating,
            self.delay_seconds(),
        )
    }
}
