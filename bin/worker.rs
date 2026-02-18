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
    let args: Vec<String> = env::args().collect();
    let addr = format!("127.0.0.1:{}", args[1]);
    let mut connect = TcpStream::connect(addr)?;
    loop {
        println!("[Worker] Asking for task...");
        let resp = send_request(&Request::GetTask, &mut connect)?;

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
                        send_request(&req, &mut connect)?;
                    }
                    mapreduce::models::Report::ReducerDone { taskid } => {
                        println!("[Worker] Reduce done, sending ReduceDone...");
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
