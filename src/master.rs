use std::collections::HashMap;

use crate::{
    models::{TaskData, TaskStatus, TaskType},
    worker::Worker,
};

pub enum Phase {
    Map,
    Reduce,
    Done,
}

pub struct Master {
    map_task: HashMap<usize, String>,
    reduce_task: HashMap<usize, String>,
    phase: Phase,
    n_reduce: u32,
    input_files: Vec<String>,
    output: String,
    map_outputs: HashMap<usize, HashMap<usize, String>>,
}

impl Master {
    pub fn new() -> Master {
        Master {
            map_task: HashMap::new(),
            reduce_task: HashMap::new(),
            phase: Phase::Map,
            n_reduce: 2,
            input_files: vec!["abc.txt".to_string(), "bbc.txt".to_string()],
            map_outputs: HashMap::new(),
            output: "output".to_string(),
        }
    }

    pub fn map_task_adder(&self) -> HashMap<usize, String> {
        let mut hs = HashMap::new();
        for (i, file) in self.input_files.iter().enumerate() {
            hs.insert(i, file.clone());
        }
        hs
    }
}

pub fn main_run() {
    let mut a = Master::new();
    a.map_task = a.map_task_adder();
    for i in a.map_task {
        let mut ve = vec![i.1];
        let d = Worker::new(
            TaskData {
                task_id: i.0 as u32,
                input_files: ve,
                n_reduce: a.n_reduce,
                output_path: a.output.clone(),
            },
            TaskType::Map,
            TaskStatus::InProgress,
        );
        let k = d.run();
    }
}
