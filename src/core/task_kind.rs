use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
/// Defines the type of task to be executed.
pub enum TaskKind {
    /// Represents a cron job, which is scheduled to run at specific intervals.
    Cron {
        /// Schedule expression for Cron tasks.
        schedule: &'static str,
        /// Timezone for the schedule expression.
        timezone: &'static str,
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

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cron { .. } => write!(f, "Cron"),
            TaskKind::Repeat { .. } => write!(f, "Repeat"),
            TaskKind::Once => write!(f, "Once"),
        }
    }
}
