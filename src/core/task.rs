use crate::core::model::TaskMeta;
use crate::core::model::TaskKind as ModelTaskKind;
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

    /// The type of task being defined.
    ///
    /// This specifies the task behavior and can be one of the `TaskKind` variants.
    const TASK_KIND: TaskKind;

    /// The retry policy for this task.
    ///
    /// Defines the strategy and maximum retry attempts in case of failure.
    /// Default is exponential backoff with a base of 2 and a maximum of 5 retries.
    const RETRY_POLICY: RetryPolicy = RetryPolicy {
        strategy: RetryStrategy::Exponential { base: 2 },
        max_retries: Some(3),
    };

    /// Delay before executing a Once task, in seconds.
    ///
    /// Specifies the delay before starting a Once task after it is scheduled.
    const DELAY_SECONDS: u32 = 3;

    /// Executes the task with the given parameters.
    ///
    /// Contains the logic required to perform the task. Takes parameters of type `Self::Params`
    /// that can be used during execution.
    fn run(self) -> TaskFuture;

    /// Validates the parameters based on the task type.
    ///
    /// Checks that the necessary fields for the specific `TaskKind` are provided.
    fn validate(&self) -> Result<(), String> {
        if Self::TASK_QUEUE.is_empty() {
            return Err("TASK_QUEUE must not be empty.".into());
        }

        match Self::TASK_KIND {
            TaskKind::Cron { schedule, timezone } => {
                if !is_valid_cron_string(&*schedule) {
                    return Err(
                        format!("Invalid SCHEDULE: '{}' for Cron tasks.", schedule).into(),
                    );
                }

                if !is_valid_timezone(&*timezone) {
                    return Err(
                        format!("Invalid TIMEZONE: '{}' for Cron tasks.", schedule).into(),
                    );
                }
            }
            TaskKind::Repeat { interval_seconds } => {
                if interval_seconds <= 0 {
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
    fn new_meta(&self) -> TaskMeta {
        self.validate().unwrap_or_else(|err| {
            panic!(
                "Validation failed for task '{}': {}. This indicates a programming error.",
                Self::TASK_KEY,
                err
            )
        });

        TaskMeta::new(
            Self::TASK_KEY.to_owned(),
            serde_json::to_string(&self).expect(
                "Serialization failed: this should never happen if all fields are serializable",
            ),
            Self::TASK_QUEUE.to_owned(),
            match Self::TASK_KIND {
                TaskKind::Cron { schedule, timezone } => {
                    ModelTaskKind::Cron {
                        schedule: schedule.into(), timezone: timezone.into(),
                    }
                }
                TaskKind::Repeat { interval_seconds } => {
                    ModelTaskKind::Repeat { interval_seconds }
                }
                TaskKind::Once => ModelTaskKind::Once
            },
            Self::RETRY_POLICY,
            matches!(Self::TASK_KIND, TaskKind::Repeat { .. }),
            Self::DELAY_SECONDS,
        )
    }
}
