use std::{
    io::{BufReader, Write},
    net::TcpStream,
};

use mapreduce::{
    rpc::{Request, Response},
    worker::Worker,
};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut connect = TcpStream::connect("127.0.0.1:8080")?;
        let req = Request::GetTask;
        // let buf = BufReader::new(connect);
        serde_json::to_writer(&connect, &req)?;
        connect.flush()?;
        // TcpStream::shutdown(&connect, std::net::Shutdown::Write)?;

        let resp: Response = serde_json::from_reader(connect)?;
        match resp {
            Response::Exit => break,
            Response::NoTask => {
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
            Response::Task {
                task_type,
                task_data,
            } => {
                let worker = Worker::new(task_data, task_type);
                let report = worker.run();
                match report {
                    mapreduce::models::Report::MapDone { taskid, files } => {
                        let req = Request::MapDone {
                            task_id: taskid,
                            files,
                        };
                        serde_json::to_writer(&connect, &req)?;
                    }
                    mapreduce::models::Report::ReducerDone { taskid } => {
                        let req = Request::ReduceDone { task_id: taskid };
                        serde_json::to_writer(&connect, &req)?;
                    }
                }
                connect.flush()?;
            }
        }
    }
}
