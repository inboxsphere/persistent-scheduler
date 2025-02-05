use crate::{
    core::{
        retry::{RetryPolicy, RetryStrategy},
    },
    generate_token, utc_now,
};
use serde::{Deserialize, Serialize};
use std::fmt;
type LinearInterval = u32;
type ExponentialBase = u32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMeta {
    pub id: String,                     // Unique identifier for the task
    pub task_key: String,               // Key to identify the specific task
    pub task_params: String,            // Arguments for the task, stored as a JSON object
    pub queue_name: String,             // Name of the queue for the task
    pub updated_at: i64,                // Timestamp of the last update
    pub status: TaskStatus,             // Current status of the task
    pub stopped_reason: Option<String>, // Optional reason for why the task was stopped
    pub last_error: Option<String>,     // Error message from the last execution, if any
    pub last_run: i64,                  // Timestamp of the last run
    pub next_run: i64,                  // Timestamp of the next scheduled run
    pub kind: TaskKind,                 // Type of the task
    pub success_count: u32,             // Count of successful runs
    pub failure_count: u32,             // Count of failed runs
    pub runner_id: Option<String>,      // The ID of the current task runner, may be None
    pub retry_strategy: Retry,          // Retry strategy for handling failures
    pub retry_interval: u32,            // Interval for retrying the task
    pub base_interval: u32,             // Base interval for exponential backoff
    pub delay_seconds: u32,             //Delay before executing a Once task, specified in seconds
    pub max_retries: Option<u32>,       // Maximum number of retries allowed
    pub is_repeating: bool,             // Indicates if the task is repeating
    pub heartbeat_at: i64,              // Timestamp of the last heartbeat in milliseconds
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
/// Defines the type of task to be executed.
pub enum TaskKind {
    /// Represents a cron job, which is scheduled to run at specific intervals.
    Cron {
        /// Schedule expression for Cron tasks.
        schedule: String,
        /// Timezone for the schedule expression.
        timezone: String,
    },

    /// Represents a repeated job that runs at a regular interval.
    Repeat {
        /// Repeat interval for Repeat tasks, in seconds.
        interval_seconds: u32
    },

    /// Represents a one-time job that runs once and then completes.
    #[default]
    Once,
}

#[derive(Clone, Debug, Eq, Default, PartialEq, Serialize, Deserialize, Hash)]
pub enum TaskStatus {
    // Task has been scheduled but has not started executing yet.
    #[default]
    Scheduled,

    // Task is currently running.
    Running,

    // Task has completed successfully.
    Success,

    // Task has failed.
    Failed,

    // Task has been marked for removal and will be cleaned up by a dedicated thread.
    Removed,

    // Task has been stopped (applicable to Repeat and Cron type tasks).
    Stopped,
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_str = match self {
            TaskStatus::Scheduled => "Scheduled",
            TaskStatus::Running => "Running",
            TaskStatus::Success => "Success",
            TaskStatus::Failed => "Failed",
            TaskStatus::Removed => "Removed",
            TaskStatus::Stopped => "Stopped",
        };
        write!(f, "{}", status_str)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum Retry {
    #[default]
    Linear,
    Exponential,
}

impl fmt::Display for Retry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Retry::Linear => write!(f, "Linear"),
            Retry::Exponential => write!(f, "Exponential"),
        }
    }
}

fn to_retry(retry_policy: RetryPolicy) -> (Retry, LinearInterval, ExponentialBase) {
    match retry_policy.strategy {
        RetryStrategy::Linear { interval } => (Retry::Linear, interval, Default::default()),
        RetryStrategy::Exponential { base } => (Retry::Exponential, Default::default(), base),
    }
}

impl TaskMeta {
    pub fn new(
        task_key: String,
        task_params: String,
        queue_name: String,
        kind: TaskKind,
        retry_policy: RetryPolicy,
        is_repeating: bool,
        delay_seconds: u32,
    ) -> Self {
        // Extract retry strategy and intervals from the given retry policy.
        let (retry_strategy, retry_interval, base_interval) = to_retry(retry_policy);
        Self {
            id: generate_token!(),
            task_key,
            task_params,
            queue_name,
            updated_at: utc_now!(),
            status: TaskStatus::Scheduled,
            last_error: Default::default(),
            last_run: Default::default(),
            next_run: Default::default(),
            kind,
            stopped_reason: Default::default(),
            success_count: Default::default(),
            failure_count: Default::default(),
            runner_id: Default::default(),
            retry_strategy,
            retry_interval,
            base_interval,
            max_retries: retry_policy.max_retries,
            is_repeating,
            heartbeat_at: Default::default(),
            delay_seconds,
        }
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        let strategy = match self.retry_strategy {
            Retry::Linear => RetryStrategy::Linear {
                interval: self.retry_interval,
            },
            Retry::Exponential => RetryStrategy::Exponential {
                base: self.base_interval,
            },
        };

        RetryPolicy {
            strategy,
            max_retries: self.max_retries,
        }
    }
}
