use std::{sync::Arc, time::Duration};

use persistent_scheduler::core::periodic::PeriodicTask;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let task = Arc::new(move || {
        Box::pin(async move {
            println!("this is periodic task.");
            Ok(())
        })
    });
    let periodic_task = Arc::new(PeriodicTask::new("periodic example"));
    periodic_task.start_with_signal(task, Duration::from_secs(2));
    tokio::time::sleep(Duration::from_secs(10 * 60)).await;
}
