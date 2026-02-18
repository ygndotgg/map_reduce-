use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::{
    fs::{self, read_to_string},
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
                std::thread::sleep(std::time::Duration::from_secs(1));
                Report::Exit
            }
            TaskType::Exit => Report::Exit,
            TaskType::Map => {
                let data = &self.task_data;
                let content = read_to_string(&data.input_files[0]).expect("Invalid File");
                let kvs: Vec<KeyValue> = map(&data.input_files[0], content);
                fs::create_dir_all(&data.output_path).expect("Failed to create_dir");

                let mut partitions: HashMap<u32, Vec<String>> = HashMap::new();
                for kv in kvs {
                    let partition_id = ihash(&kv.key) % data.n_reduce;
                    let value = format!("{},{};", kv.key, kv.value);
                    partitions.entry(partition_id).or_default().push(value);
                }

                let mut files = HashMap::new();
                for (partition_id, values) in partitions {
                    let temp_filename = format!(
                        "{}/mr-{}-{}.tmp",
                        data.output_path, data.task_id, partition_id
                    );
                    let final_filename =
                        format!("{}/mr-{}-{}", data.output_path, data.task_id, partition_id);

                    let mut file =
                        File::create(&temp_filename).expect("Unable to create temp file");
                    for v in &values {
                        writeln!(file, "{}", v).expect("Failed to write");
                    }
                    file.flush().expect("Failed to flush");

                    fs::rename(&temp_filename, &final_filename)
                        .expect("Failed to rename temp file");

                    files.insert(partition_id, final_filename);
                }

                Report::MapDone {
                    taskid: data.task_id,
                    files,
                }
            }

            TaskType::Reduce => {
                let data = &self.task_data;

                fs::create_dir_all(&data.output_path).expect("Failed to create dir");

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

                all_kv.sort_by_key(|f| f.0.clone());

                let temp_filename = format!("{}/mr-out-{}.tmp", data.output_path, data.task_id);
                let final_filename = format!("{}/mr-out-{}", data.output_path, data.task_id);

                let mut file = File::create(&temp_filename).expect("Unable to create temp file");

                let mut i = 0;
                while i < all_kv.len() {
                    let key = all_kv[i].0.clone();
                    let mut values = Vec::new();
                    while i < all_kv.len() && key == all_kv[i].0 {
                        values.push(all_kv[i].1);
                        i += 1;
                    }
                    let result = reduce(key, values);
                    writeln!(file, "{}", result).expect("Failed to write");
                }
                file.flush().expect("Failed to flush");

                fs::rename(&temp_filename, &final_filename).expect("Failed to rename temp file");

                Report::ReducerDone {
                    taskid: data.task_id,
                }
            }
        }
    }
}

fn map(_filename: &String, content: String) -> Vec<KeyValue> {
    let mut d: Vec<(&str, u32)> = content.split(" ").map(|x| (x, 1 as u32)).collect();
    d.sort_by_key(|f| f.0);
    let mut i = 0;
    let mut kvs = Vec::new();
    while i < d.len() {
        let key = d[i].0;
        let mut values = Vec::new();
        while i < d.len() && key == d[i].0 {
            values.push(d[i].1);
            i += 1;
        }
        let result = reduce(key.to_string(), values);
        let kv: KeyValue = result
            .split_once(" ")
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: v.to_string(),
            })
            .expect("Unable to Convert to key Value form");
        kvs.push(kv);
    }
    kvs
}

pub fn ihash(key: &str) -> u32 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as u32
}
