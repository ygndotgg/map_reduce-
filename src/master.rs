use std::collections::HashMap;

use crate::rpc::{Request, Response, TaskStatus};

pub enum Phase {
    Map,
    Reduce,
    Done,
}

pub struct Master {
    pub map_task: HashMap<u32, TaskStatus>,
    pub reduce_task: HashMap<u32, TaskStatus>,
    pub phase: Phase,
    pub n_reduce: u32,
    pub input_files: Vec<String>,
    pub output: String,
    pub map_outputs: HashMap<u32, HashMap<u32, String>>,
}

impl Master {
    pub fn new(input_files: Vec<String>, n_reduce: u32, output_path: String) -> Master {
        let mut map_task = HashMap::new();
        for (i, _) in input_files.iter().enumerate() {
            map_task.insert(i as u32, TaskStatus::Idle);
        }
        Master {
            map_task,
            reduce_task: HashMap::new(),
            phase: Phase::Map,
            n_reduce,
            input_files,
            map_outputs: HashMap::new(),
            output: output_path,
        }
    }
    pub fn handle_request(&mut self, req: Request) -> Response {
        match req {
            Request::GetTask => self.get_task(),
            Request::MapDone { task_id, files } => {
                self.handle_map_done(task_id, files);
                Response::NoTask
            }
            Request::ReduceDone { task_id } => {
                self.handle_reduce_done(task_id);
                Response::NoTask
            }
        }
    }
    fn get_task(&mut self) -> Response {
        match self.phase {
            Phase::Map => {
                let task_id = self
                    .map_task
                    .iter()
                    .find(|(_, status)| **status == TaskStatus::Idle)
                    .map(|(id, _)| *id);
                if let Some(id) = task_id {
                    self.map_task.insert(id, TaskStatus::InProgress);

                    return Response::Task {
                        task_type: crate::rpc::TaskType::Map,
                        task_data: crate::rpc::TaskData {
                            task_id: id,
                            input_files: vec![self.input_files[id as usize].clone()],
                            n_reduce: self.n_reduce,
                            output_path: self.output.clone(),
                        },
                    };
                }
                Response::NoTask
            }
            Phase::Reduce => {
                let task_id = self
                    .reduce_task
                    .iter()
                    .find(|(_, status)| **status == TaskStatus::Idle)
                    .map(|(id, _)| *id);
                if let Some(id) = task_id {
                    self.reduce_task.insert(id, TaskStatus::InProgress);
                    // collect all intermediate files
                    let mut input_files = Vec::new();
                    for (_map_id, files) in &self.map_outputs {
                        if let Some(file) = files.get(&id) {
                            input_files.push(file.clone());
                        }
                    }
                    return Response::Task {
                        task_type: crate::rpc::TaskType::Reduce,
                        task_data: crate::rpc::TaskData {
                            task_id: id,
                            input_files,
                            n_reduce: self.n_reduce,
                            output_path: self.output.clone(),
                        },
                    };
                }
                Response::NoTask
            }
            Phase::Done => Response::Exit,
        }
    }
    fn handle_map_done(&mut self, task_id: u32, files: HashMap<u32, String>) {
        self.map_task.insert(task_id, TaskStatus::Completed);
        self.map_outputs.insert(task_id, files);
        let all_done = self.map_task.values().all(|s| *s == TaskStatus::Completed);
        if all_done {
            self.phase = Phase::Reduce;
            for i in 0..self.n_reduce {
                self.reduce_task.insert(i, TaskStatus::Idle);
            }
            println!("All map task done switching to Reduce Phase");
        }
    }
    fn handle_reduce_done(&mut self, task_id: u32) {
        self.reduce_task.insert(task_id, TaskStatus::Completed);
        let all_done = self
            .reduce_task
            .values()
            .all(|s| *s == TaskStatus::Completed);
        if all_done {
            self.phase = Phase::Done;
            println!("ALl reduce tasks done, job completed!");
        }
    }
}
