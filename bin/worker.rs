use std::{io::Write, net::TcpStream};

use mapreduce::{
    rpc::{Request, Response},
    worker::Worker,
};

fn send_request(req: &Request) -> Result<Response, Box<dyn std::error::Error>> {
    let mut connect = TcpStream::connect("127.0.0.1:7799")?;
    serde_json::to_writer(&connect, &req)?;
    writeln!(&connect)?;
    connect.flush()?;
    TcpStream::shutdown(&connect, std::net::Shutdown::Write)?;
    let resp: Response = serde_json::from_reader(&connect)?;
    Ok(resp)
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        println!("[Worker] Asking for task...");
        let resp = send_request(&Request::GetTask)?;
        println!("[Worker] Got response: {:?}", resp);

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
                        println!("[Worker] Map done, sending MapDone...");
                        let req = Request::MapDone {
                            task_id: taskid,
                            files,
                        };
                        send_request(&req)?;
                    }
                    mapreduce::models::Report::ReducerDone { taskid } => {
                        println!("[Worker] Reduce done, sending ReduceDone...");
                        let req = Request::ReduceDone { task_id: taskid };
                        send_request(&req)?;
                    }
                    mapreduce::models::Report::Exit => {}
                }
            }
        }
    }
    Ok(())
}
