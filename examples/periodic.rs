use std::{sync::Arc, time::Duration};

use persistent_scheduler::core::periodic::PeriodicTask;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    #[derive(Debug)]
    struct Test {
        pub name: String,
        pub age: u32,
    }

    let param = Arc::new(Test {
        name: "this is a test name".to_string(),
        age: 34,
    });
    let task = Arc::new(move |param: Option<Arc<Test>>| {
        Box::pin(async move {
            println!("this is periodic task. param={:#?}", param.unwrap());
            Ok(())
        })
    });
    let periodic_task = Arc::new(PeriodicTask::new("periodic example"));
    periodic_task.start_with_signal(task, Some(param), Duration::from_secs(2));
    tokio::time::sleep(Duration::from_secs(10 * 60)).await;
}
