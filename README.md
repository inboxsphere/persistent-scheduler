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
1. [Basic Example](https://github.com/inboxsphere/persistent-scheduler/blob/main/examples/basic.rs)
2. [NativeDB Example](https://github.com/inboxsphere/persistent-scheduler/blob/main/examples/nativedb.rs)
3. [Periodic Example](https://github.com/inboxsphere/persistent-scheduler/blob/main/examples/periodic.rs)

### License

The MIT License
