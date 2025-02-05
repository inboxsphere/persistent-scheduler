use std::time::Duration;

use persistent_scheduler::core::{
    context::{TaskAndDelay, TaskContext},
    store::InMemoryTaskStore,
    task::{Task, TaskFuture},
    task_kind::TaskKind,
};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let task_store = InMemoryTaskStore::new();
    let context = TaskContext::new(task_store)
        .register::<MyTask1>()
        .register::<MyTask2>()
        .set_concurrency("default", 10)
        .start()
        .await;
    let mut tasks = Vec::new();
    for _ in 0..100 {
        tasks.push(TaskAndDelay {
            inner: MyTask1::new("name1".to_string(), 32),
            delay_seconds: None,
        });
    }

    tokio::spawn(async move {
        context.add_tasks(tasks).await.unwrap();
    });
    tokio::time::sleep(Duration::from_secs(20)).await;
    
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MyTask1 {
    pub name: String,
    pub age: i32,
}

impl MyTask1 {
    pub fn new(name: String, age: i32) -> Self {
        Self { name, age }
    }
}

impl Task for MyTask1 {
    const TASK_KEY: &'static str = "my_task_a";

    const TASK_QUEUE: &'static str = "default";

    const TASK_KIND: TaskKind = TaskKind::Once;
    //const RETRY_POLICY: RetryPolicy = RetryPolicy::linear(10, Some(5));

    fn run(self) -> TaskFuture {
        Box::pin(async move {
            // println!("{}", self.name);
            // println!("{}", self.age);
            //tokio::time::sleep(Duration::from_secs(15)).await;
            // println!("my task1 is running");
            Ok(())
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MyTask2 {
    pub name: String,
    pub age: i32,
}

impl MyTask2 {
    pub fn new(name: String, age: i32) -> Self {
        Self { name, age }
    }
}

impl Task for MyTask2 {
    const TASK_KEY: &'static str = "my_task_b";
    const TASK_QUEUE: &'static str = "default";
    const TASK_KIND: TaskKind = TaskKind::Repeat {
        interval_seconds: 10
    };

    fn run(self) -> TaskFuture {
        Box::pin(async move {
            println!("{}", self.name);
            println!("{}", self.age);
            tokio::time::sleep(Duration::from_secs(100000)).await;
            println!("my task2 is running");
            Ok(())
        })
    }
}
