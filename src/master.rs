use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::rpc::{Phase, Request, Response, TaskStatus};
use log;

pub struct Master {
    pub map_task: HashMap<u32, TaskStatus>,
    pub reduce_task: HashMap<u32, TaskStatus>,
    pub phase: crate::rpc::Phase,
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
                    .find(|(_, status)| matches!(status, TaskStatus::Idle))
                    .map(|(id, _)| *id);

                if let Some(id) = task_id {
                    self.map_task.insert(
                        id,
                        TaskStatus::InProgress {
                            start_time: std::time::Instant::now(),
                            backup_scheduled: false,
                        },
                    );

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

                let backup_threshold = Duration::from_secs(10);
                let backup_task = self
                    .map_task
                    .iter()
                    .find(|(_, status)| {
                        matches!(status, TaskStatus::InProgress { start_time, backup_scheduled } 
                            if !backup_scheduled && start_time.elapsed() > backup_threshold)
                    })
                    .map(|(id, _)| *id);

                if let Some(id) = backup_task {
                    log::info!("Scheduling backup for map task {}", id);
                    if let Some(TaskStatus::InProgress { start_time, .. }) = self.map_task.get(&id)
                    {
                        self.map_task.insert(
                            id,
                            TaskStatus::InProgress {
                                start_time: *start_time,
                                backup_scheduled: true,
                            },
                        );
                    }
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
                    .find(|(_, status)| matches!(status, TaskStatus::Idle))
                    .map(|(id, _)| *id);
                if let Some(id) = task_id {
                    self.reduce_task.insert(
                        id,
                        TaskStatus::InProgress {
                            start_time: std::time::Instant::now(),
                            backup_scheduled: false,
                        },
                    );
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

                let backup_threshold = Duration::from_secs(10);
                let backup_task = self
                    .reduce_task
                    .iter()
                    .find(|(_, status)| {
                        matches!(status, TaskStatus::InProgress { start_time, backup_scheduled }
                            if !backup_scheduled && start_time.elapsed() > backup_threshold)
                    })
                    .map(|(id, _)| *id);

                if let Some(id) = backup_task {
                    log::info!("Scheduling backup for reduce task {}", id);
                    if let Some(TaskStatus::InProgress { start_time, .. }) =
                        self.reduce_task.get(&id)
                    {
                        self.reduce_task.insert(
                            id,
                            TaskStatus::InProgress {
                                start_time: *start_time,
                                backup_scheduled: true,
                            },
                        );
                    }
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
        // Check if task is still InProgress (might have been reset by health check)
        if let Some(status) = self.map_task.get(&task_id) {
            if matches!(status, TaskStatus::InProgress { .. }) {
                self.map_task.insert(task_id, TaskStatus::Completed);
                self.map_outputs.insert(task_id, files);
            }
            // If status is Idle, it was already reset by health check - ignore
        }

        // Check if ALL map tasks are completed
        let all_done = self
            .map_task
            .values()
            .all(|s| matches!(s, TaskStatus::Completed));
        if all_done && self.phase == Phase::Map {
            self.phase = Phase::Reduce;
            for i in 0..self.n_reduce {
                self.reduce_task.insert(i, TaskStatus::Idle);
            }
            log::info!("All map tasks complete, switching to Reduce phase");
        }
    }

    fn handle_reduce_done(&mut self, task_id: u32) {
        // Check if task is still InProgress
        if let Some(status) = self.reduce_task.get(&task_id) {
            if matches!(status, TaskStatus::InProgress { .. }) {
                self.reduce_task.insert(task_id, TaskStatus::Completed);
            }
        }

        let all_done = self
            .reduce_task
            .values()
            .all(|s| matches!(s, TaskStatus::Completed));
        if all_done && self.phase == Phase::Reduce {
            self.phase = Phase::Done;
            log::info!("All reduce tasks complete, job finished!");
        }
    }

    /// Health check - resets stuck tasks to Idle
    pub fn health_check(&mut self, timeout_secs: u64) {
        let timeout = Duration::from_secs(timeout_secs);

        // Check map tasks
        for (task_id, status) in &mut self.map_task {
            if let TaskStatus::InProgress { start_time, .. } = status {
                if start_time.elapsed() > timeout {
                    log::warn!("Map task {} timed out, resetting to Idle", task_id);
                    *status = TaskStatus::Idle;
                }
            }
        }

        for (task_id, status) in &mut self.reduce_task {
            if let TaskStatus::InProgress { start_time, .. } = status {
                if start_time.elapsed() > timeout {
                    log::warn!("Reduce task {} timed out, resetting to Idle", task_id);
                    *status = TaskStatus::Idle;
                }
            }
        }
    }
}

/// Start background health check thread
pub fn start_health_check(master: Arc<Mutex<Master>>, timeout_secs: u64, check_interval_secs: u64) {
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(check_interval_secs));

            let mut master = master.lock().unwrap();

            // Only check if not done
            if matches!(master.phase, Phase::Done) {
                log::info!("Health check: Job complete, stopping");
                break;
            }

            log::debug!("Health check: Checking for stuck tasks...");
            master.health_check(timeout_secs);
        }
    });
}
