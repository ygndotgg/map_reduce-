// use serde::{Deserialize, Serialize};

use std::collections::HashMap;

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

pub enum Report {
    MapDone {
        taskid: u32,
        files: HashMap<u32, String>,
    },
    ReducerDone {
        taskid: u32,
    },
}
