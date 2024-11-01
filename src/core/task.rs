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

    /// The type of task being defined.
    ///
    /// This specifies the task behavior and can be one of the `TaskKind` variants.
    const TASK_KIND: TaskKind;

    /// Schedule expression for Cron tasks.
    ///
    /// This field is required if `TASK_KIND` is `TaskKind::Cron`.
    /// It defines the schedule on which the task should run.
    const SCHEDULE: Option<&'static str> = None;

    /// Timezone for the schedule expression.
    ///
    /// This is required for `TaskKind::Cron` and specifies the timezone for the cron schedule.
    const TIMEZONE: Option<&'static str> = None;

    /// Repeat interval for Repeat tasks, in seconds.
    ///
    /// This field is required if `TASK_KIND` is `TaskKind::Repeat`.
    /// It defines the interval between consecutive executions of the task.
    const REPEAT_INTERVAL: Option<u32> = None;

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
            TaskKind::Cron => {
                let schedule = Self::SCHEDULE.as_ref();
                let timezone = Self::TIMEZONE.as_ref();

                if schedule.is_none() || timezone.is_none() {
                    return Err("Both SCHEDULE and TIMEZONE must be defined for Cron tasks.".into());
                }

                let schedule_str = schedule.unwrap();
                let timezone_str = timezone.unwrap();

                if !is_valid_cron_string(schedule_str) {
                    return Err(
                        format!("Invalid SCHEDULE: '{}' for Cron tasks.", schedule_str).into(),
                    );
                }

                if !is_valid_timezone(timezone_str) {
                    return Err(
                        format!("Invalid TIMEZONE: '{}' for Cron tasks.", timezone_str).into(),
                    );
                }
            }
            TaskKind::Repeat => {
                if Self::REPEAT_INTERVAL.unwrap_or(0) <= 0 {
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
            Self::TASK_KIND,
            Self::RETRY_POLICY,
            Self::SCHEDULE.map(|s| s.to_string()),
            Self::TIMEZONE.map(|s| s.to_string()),
            matches!(Self::TASK_KIND, TaskKind::Repeat),
            Self::REPEAT_INTERVAL,
            Self::DELAY_SECONDS,
        )
    }
}
