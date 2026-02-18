use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// worker --> Master

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    GetTask,
    MapDone {
        task_id: u32,
        files: HashMap<u32, String>,
    },
    ReduceDone {
        task_id: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum TaskType {
    Map,
    Reduce,
    Idle,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskStatus {
    Idle,
    InProgress {
        start_time:std::time::Instant
    },
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Phase {
    Map,
    Reduce,
    Done,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskData {
    pub task_id: u32,             // unique id
    pub input_files: Vec<String>, // files for the task to process
    pub n_reduce: u32,            // total number of reduce partitions
    pub output_path: String,      // where to write output files
}

// master -> worker
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Task {
        task_type: TaskType,
        task_data: TaskData,
    },
    NoTask,
    Exit,
}
