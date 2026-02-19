use std::time::Duration;

use mapreduce::client::{Client, TaskType};
use mapreduce::models::Report;
use mapreduce::rpc::{TaskData, TaskType as RpcTaskType};
use mapreduce::worker::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(|s| s.as_str()).unwrap_or("http://127.0.0.1:50051");

    log::info!("Worker connecting to {}", addr);

    let mut client = Client::connect(addr).await?;

    loop {
        log::info!("Asking for task...");
        let task = client.get_task().await?;

        match task {
            TaskType::Exit => {
                log::info!("Received Exit, shutting down");
                break;
            }
            TaskType::Idle => {
                log::debug!("No task available, sleeping...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            TaskType::Map {
                task_id,
                input_files,
                n_reduce,
                output_path,
            } => {
                let task_data = TaskData {
                    task_id,
                    input_files,
                    n_reduce,
                    output_path,
                };
                let worker = Worker::new(task_data, RpcTaskType::Map);
                let report = worker.run();

                if let Report::MapDone { taskid, files } = report {
                    log::info!("Map task {} complete, sending MapDone...", taskid);
                    client.map_done(taskid, files).await?;
                }
            }
            TaskType::Reduce {
                task_id,
                input_files,
                n_reduce,
                output_path,
            } => {
                let task_data = TaskData {
                    task_id,
                    input_files,
                    n_reduce,
                    output_path,
                };
                let worker = Worker::new(task_data, RpcTaskType::Reduce);
                let report = worker.run();

                if let Report::ReducerDone { taskid } = report {
                    log::info!("Reduce task {} complete, sending ReduceDone...", taskid);
                    client.reduce_done(taskid).await?;
                }
            }
        }
    }

    Ok(())
}
