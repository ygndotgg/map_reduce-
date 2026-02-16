// use serde::{Deserialize, Serialize};

pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub type MapFunction = fn(String, String) -> Vec<KeyValue>;

pub type ReduceFunction = fn(String, Vec<String>) -> String;

pub struct TaskData {
    pub task_id: u32,
    pub input_files: Vec<String>,
    pub n_reduce: u32,
    pub output_path: String,
}

pub enum TaskType {
    Map,
    Reduce,
}

pub enum TaskStatus {
    Completed,
    InProgress,
    Idle,
}
