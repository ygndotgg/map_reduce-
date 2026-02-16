use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::hash::Hasher;
use std::io::Write;
use std::{
    fs::read_to_string,
    hash::{DefaultHasher, Hash},
};

use crate::models::{KeyValue, Report, TaskData, TaskType};

pub struct Worker {
    pub task_data: TaskData,
    pub task_type: TaskType,
}

impl Worker {
    pub fn new(data: TaskData, typo: TaskType) -> Worker {
        Worker {
            task_data: data,
            task_type: typo,
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
                    let value = format!("{},{};", kv.key, kv.value);
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
                for file in &self.task_data.input_files {
                    let mut sorted_data = HashMap::new();
                    let content = read_to_string(&file).expect("A Valid File name is expected");
                    for x in content.split(";") {
                        let mut d = x.split(",");
                        sorted_data
                            .entry(d.next().unwrap().to_string())
                            .or_insert(Vec::new())
                            .push(d.next().unwrap().parse::<u32>().unwrap());
                    }
                    for (k, v) in sorted_data {
                        let value = reduce(k, v);
                        let filename = format!(
                            "{}/mr-out-{}",
                            &self.task_data.output_path, self.task_data.task_id
                        );

                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&filename)
                            .expect("Unable to Open");
                        file.write_all(value.as_bytes())
                            .expect("Failed to write into the file");
                    }
                    return Report::ReducerDone {
                        taskid: self.task_data.task_id,
                    };
                }
                unimplemented!("ra")
            }
        }
    }
}
fn map(_filename: &String, content: String) -> Vec<KeyValue> {
    content
        .split(" ")
        .map(|x| KeyValue {
            key: x.to_string(),
            value: "1".to_string(),
        })
        .collect()
}

fn reduce(key: String, values: Vec<u32>) -> String {
    let val = values.iter().sum::<u32>();
    format!("{}{}", key, val)
}

pub fn ihash(key: String) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}
