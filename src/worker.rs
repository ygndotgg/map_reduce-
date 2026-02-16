use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::Write;
use std::{
    fs::read_to_string,
    hash::{DefaultHasher, Hash},
    ptr::hash,
};

use crate::models::{KeyValue, Report, TaskData, TaskStatus, TaskType};

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
    pub fn run(&self) -> Report {
        match self.task_type {
            TaskType::Map => {
                let data = &self.task_data;
                let mut files = HashMap::new();
                let mut hashe = HashSet::new();
                // we know that we only get a single input_file
                let content = read_to_string(&data.input_files[0]).expect("Invalid File");
                let kvs: Vec<KeyValue> = map(&data.input_files[0], content);
                for kv in kvs {
                    let value = format!("{}:{}", kv.key, kv.value);
                    let partion_id = ihash(kv.key) % self.task_data.n_reduce;
                    let filename = format!("mr-{}-{}", self.task_data.task_id, partion_id);
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&filename)
                        .expect("Unable to Open");
                    file.write_all(value.as_bytes())
                        .expect("Failed to write into the file");
                    hashe.insert(filename);
                }
                for file in hashe {
                    files.insert(self.task_data.task_id, file);
                }
                Report::MapDone {
                    taskid: self.task_data.task_id,
                    files,
                }
            }
            TaskType::Reduce => {
                
                unimplemented!("ra")
            }
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

pub fn ihash(key: String) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}
