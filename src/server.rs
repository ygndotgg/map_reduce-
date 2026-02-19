use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::{Response, Status, transport::Server};

use crate::master::Master;
use crate::rpc::{Phase, Request};

pub mod mr {
    tonic::include_proto!("mapreduce");
}

pub struct MapReducer {
    pub master: Arc<Mutex<Master>>,
}

#[tonic::async_trait]
impl mr::map_reduce_server::MapReduce for MapReducer {
    async fn get_task(
        &self,
        _request: tonic::Request<mr::Empty>,
    ) -> Result<Response<mr::TaskResponse>, Status> {
        let mut master = self.master.lock().await;
        let resp = master.handle_request(Request::GetTask);

        let response = match resp {
            crate::rpc::Response::Task {
                task_type,
                task_data,
            } => {
                let tt = match task_type {
                    crate::rpc::TaskType::Map => "map",
                    crate::rpc::TaskType::Reduce => "reduce",
                    crate::rpc::TaskType::Idle => "idle",
                    crate::rpc::TaskType::Exit => "exit",
                };
                mr::TaskResponse {
                    task_type: tt.to_string(),
                    task_id: task_data.task_id,
                    input_files: task_data.input_files,
                    n_reduce: task_data.n_reduce,
                    output_path: task_data.output_path,
                }
            }
            crate::rpc::Response::NoTask => mr::TaskResponse {
                task_type: "idle".to_string(),
                task_id: 0,
                input_files: vec![],
                n_reduce: 0,
                output_path: String::new(),
            },
            crate::rpc::Response::Exit => mr::TaskResponse {
                task_type: "exit".to_string(),
                task_id: 0,
                input_files: vec![],
                n_reduce: 0,
                output_path: String::new(),
            },
        };

        Ok(Response::new(response))
    }

    async fn map_done(
        &self,
        request: tonic::Request<mr::MapDoneRequest>,
    ) -> Result<Response<mr::Empty>, Status> {
        let req = request.into_inner();
        let files: std::collections::HashMap<u32, String> = req.files.into_iter().collect();

        let mut master = self.master.lock().await;
        master.handle_request(Request::MapDone {
            task_id: req.task_id,
            files,
        });

        Ok(Response::new(mr::Empty {}))
    }

    async fn reduce_done(
        &self,
        request: tonic::Request<mr::ReduceDoneRequest>,
    ) -> Result<Response<mr::Empty>, Status> {
        let req = request.into_inner();

        let mut master = self.master.lock().await;
        master.handle_request(Request::ReduceDone {
            task_id: req.task_id,
        });

        Ok(Response::new(mr::Empty {}))
    }
}

pub async fn run_server(
    input_files: Vec<String>,
    n_reduce: u32,
    output_path: String,
    addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let master = Arc::new(Mutex::new(Master::new(input_files, n_reduce, output_path)));

    let master_for_health = Arc::clone(&master);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let mut m = master_for_health.lock().await;
            if matches!(m.phase, Phase::Done) {
                log::info!("Health check: Job complete, stopping");
                break;
            }
            m.health_check(30);
        }
    });

    let mapreducer = MapReducer { master };
    let addr = addr.parse()?;

    log::info!("gRPC Master listening on {}", addr);

    Server::builder()
        .add_service(mr::map_reduce_server::MapReduceServer::new(mapreducer))
        .serve(addr)
        .await?;

    Ok(())
}
