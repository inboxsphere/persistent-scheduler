## Rust Persistent Task Scheduling System

### Overview

A high-performance task scheduling system developed in Rust using Tokio. This system supports task persistence, repeatable tasks, Cron-based scheduling, and one-time tasks, ensuring reliability and flexibility for managing time-based operations in various applications.

### Features

- **Task Persistence**: All task information and states are stored persistently, allowing for quick restoration after process restarts.
- **Repeatable Tasks**: Define tasks that execute at specified intervals.
- **Cron Jobs**: Schedule tasks using Cron syntax for precise timing control.
- **One-Time Tasks**: Support for tasks that need to be executed only once.
- **Asynchronous Execution**: Built on Tokio for efficient asynchronous task management.

### Example

```rust
use std::time::Duration;

use persistent_scheduler::{
    core::{
        context::TaskContext,
        store::TaskStore,
        task::{Task, TaskFuture},
        task_kind::TaskKind,
    },
    nativedb::meta::NativeDbTaskStore,
};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let task_store = NativeDbTaskStore::default();
    task_store.restore_tasks().await.unwrap();
    let context = TaskContext::new(task_store)
        .register::<MyTask1>()
        .register::<MyTask2>()
        .set_concurrency("default", 10)
        .start();

    context
        .add_task(MyTask1::new("name1".to_string(), 32))
        .await
        .unwrap();

    context
        .add_task(MyTask2::new("namexxxxxxx".to_string(), 3900))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(100000000)).await;
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
            println!("{}", self.name);
            println!("{}", self.age);

            println!("my task1 is running");
            Err("return error".to_string())
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
    const TASK_KEY: &'static str = "my_task_c";
    const TASK_QUEUE: &'static str = "default";
    const TASK_KIND: TaskKind = TaskKind::Cron;
    const REPEAT_INTERVAL: Option<u32> = Some(2);
    const SCHEDULE: Option<&'static str> = Some("1/2 * * * * *");
    const TIMEZONE: Option<&'static str> = Some("Asia/Shanghai");

    fn run(self) -> TaskFuture {
        Box::pin(async move {
            println!("{}", self.name);
            println!("{}", self.age);
            //tokio::time::sleep(Duration::from_secs(100000)).await;
            println!("my_task_c is running");
            Ok(())
        })
    }
}

```


### License

The MIT License
