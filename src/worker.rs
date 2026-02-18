use std::collections::HashMap;
use std::fs::OpenOptions;
use std::hash::Hasher;
use std::io::Write;
use std::{
    fs::read_to_string,
    hash::{DefaultHasher, Hash},
};

use crate::models::{KeyValue, Report};
use crate::rpc::TaskData;
use crate::rpc::TaskType;

pub struct Worker {
    pub task_data: TaskData,
    pub task_type: TaskType,
}

fn reduce(key: String, values: Vec<u32>) -> String {
    let val = values.iter().sum::<u32>();
    format!("{} {}", key, val)
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
            TaskType::Idle => {
                // Should not happen - worker should not get idle task
                std::thread::sleep(std::time::Duration::from_secs(1));
                Report::Exit
            }
            TaskType::Exit => Report::Exit,
            TaskType::Map => {
                let data = &self.task_data;
                let mut files = HashMap::new();
                // we know that we only get a single input_file
                let content = read_to_string(&data.input_files[0]).expect("Invalid File");
                let kvs: Vec<KeyValue> = map(&data.input_files[0], content);
                std::fs::create_dir_all(&data.output_path).expect("Failed to create_dir");
                for kv in kvs {
                    let value = format!("{},{};", kv.key, kv.value);
                    let partion_id = ihash(kv.key) % self.task_data.n_reduce;
                    let filename =
                        format!("{}/mr-{}-{}", data.output_path, data.task_id, partion_id);
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&filename)
                        .expect("Unable to Open");
                    writeln!(file, "{}", value).expect("Failed to write");
                    files.insert(partion_id, filename);
                }
                Report::MapDone {
                    taskid: self.task_data.task_id,
                    files,
                }
            }

            TaskType::Reduce => {
                let data = &self.task_data;

                // 1. Create output directory
                std::fs::create_dir_all(&data.output_path).expect("Failed to create dir");

                // 2. Read ALL input files and collect key-values
                let mut all_kv: Vec<(String, u32)> = Vec::new();

                for file in &data.input_files {
                    let content = read_to_string(file).expect("Invalid file");
                    for item in content.split(";") {
                        if let Some((k, v)) = item.split_once(",") {
                            if !k.is_empty() {
                                all_kv.push((k.to_string(), v.parse().unwrap_or(0)));
                            }
                        }
                    }
                }

                // 3. Sort by key
                all_kv.sort_by_key(|f| f.0.clone());

                // 4. Group by key and reduce
                let mut i = 0;
                let filename = format!("{}/mr-out-{}", data.output_path, data.task_id);
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&filename)
                    .expect("Unable to open");

                while i < all_kv.len() {
                    let key = all_kv[i].0.clone();
                    let mut values = Vec::new();
                    while i < all_kv.len() && key == all_kv[i].0.clone() {
                        values.push(all_kv[i].1);
                        i += 1;
                    }
                    let result = reduce(key, values);
                    writeln!(file, "{}", result).expect("Failed to write");
                }

                Report::ReducerDone {
                    taskid: data.task_id,
                }
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

pub fn ihash(key: String) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}
