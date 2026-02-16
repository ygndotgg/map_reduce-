use std::{fs::read_to_string, hash::{DefaultHasher, Hash}};

use crate::models::{KeyValue, TaskData, TaskStatus, TaskType};

struct Worker {
    task_data: TaskData,
    task_type: TaskType,
    status: TaskStatus,
}

impl Worker {
    pub fn new(data: TaskData, typo: TaskType, status: TaskStatus) -> Worker {
        Worker {
            task_data: data,
            task_type: typo,
            status: status,
        }
    }
    pub fn run(&self) {
        match self.task_type {
            TaskType::Map => {
                let data = &self.task_data;
                // we know that we only get a single input_file
                let content = read_to_string(&data.input_files[0]).expect("Invalid File");
                let kvs: Vec<KeyValue> = map(&data.input_files[0], content);
                for kv in kvs {
                    let partion = kv.key 
                }
            }
            TaskType::Reduce => {}
        }
    }
}
fn map(filename: &String, content: String) -> Vec<KeyValue> {
    content
        .split(" ")
        .map(|x| KeyValue {
            key: x.to_string(),
            value: "1".to_string(),
        })
        .collect()
}

pub fn ihash(key:String) -> u32 {
    let hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}
