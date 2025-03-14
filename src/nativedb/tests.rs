use itertools::Itertools;
use native_db::Builder;

use crate::{core::model::TaskStatus, generate_token};
use super::{TaskKindEntity, TaskMetaEntity, TaskMetaEntityKey, MODELS};

#[test]
fn test() {
    let database = Builder::new()
        .create_in_memory(&MODELS)
        .unwrap();

    let rw = database.rw_transaction().unwrap();
    let mut task1 = TaskMetaEntity::default();
    task1.id = generate_token!();
    task1.status = TaskStatus::Scheduled;

    let mut task2 = TaskMetaEntity::default();
    task2.id = generate_token!();
    task2.status = TaskStatus::Running;

    let mut task3 = TaskMetaEntity::default();
    task3.id = generate_token!();
    task3.status = TaskStatus::Failed;
    task3.kind = TaskKindEntity::Cron;

    rw.insert(task1).unwrap();
    rw.insert(task2).unwrap();
    rw.insert(task3).unwrap();

    rw.commit().unwrap();

    let r = database.r_transaction().unwrap();

    let result: Vec<TaskMetaEntity> = r
        .scan()
        .secondary::<TaskMetaEntity>(TaskMetaEntityKey::candidate_task)
        .unwrap()
        .start_with("false")
        .unwrap()
        .try_collect()
        .unwrap();
    println!("{:#?}", result);
}
