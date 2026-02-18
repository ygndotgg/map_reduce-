use std::{
    env,
    io::{BufRead, BufReader, Write},
    net::TcpStream,
};

use mapreduce::{
    rpc::{Request, Response},
    worker::Worker,
};

fn send_request(
    req: &Request,
    connect: &mut TcpStream,
) -> Result<Response, Box<dyn std::error::Error>> {
    // Send request
    serde_json::to_writer(&*connect, req)?;
    writeln!(&*connect)?;
    connect.flush()?;

    // Read response using read_line (same as master)
    let mut buf = BufReader::new(&*connect);
    let mut line = String::new();
    buf.read_line(&mut line)?;
    let resp: Response = serde_json::from_str(&line)?;
    Ok(resp)
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Vec<String> = env::args().collect();
    let port = args.get(1).map(|s| s.as_str()).unwrap_or("7799");

    let addr = format!("127.0.0.1:{}", port);
    log::info!("Worker connecting to {}", addr);

    let mut connect = TcpStream::connect(addr)?;
    loop {
        log::info!("Asking for task...");
        let resp = send_request(&Request::GetTask, &mut connect)?;

        log::debug!("Got response: {:?}", resp);
        match resp {
            Response::Exit => {
                log::info!("Received Exit, shutting down");
                break;
            }
            Response::NoTask => {
                log::debug!("No task available, sleeping...");
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
                        log::info!("Map task {} complete, sending MapDone...", taskid);
                        let req = Request::MapDone {
                            task_id: taskid,
                            files,
                        };
                        send_request(&req, &mut connect)?;
                    }
                    mapreduce::models::Report::ReducerDone { taskid } => {
                        log::info!("Reduce task {} complete, sending ReduceDone...", taskid);
                        let req = Request::ReduceDone { task_id: taskid };
                        send_request(&req, &mut connect)?;
                    }
                    mapreduce::models::Report::Exit => {}
                }
            }
        }
    }
    Ok(())
}
