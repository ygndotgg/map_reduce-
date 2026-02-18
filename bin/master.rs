use std::{
    env,
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    sync::Arc,
    thread,
};

use mapreduce::{
    master::{Master, start_health_check},
    rpc::Request,
};

fn handle_worker(mut stream: std::net::TcpStream, master: Arc<std::sync::Mutex<Master>>) {
    loop {
        let mut buf = BufReader::new(&mut stream);
        let mut line = String::new();

        match buf.read_line(&mut line) {
            Ok(0) => {
                log::info!("Worker disconnected");
                break;
            }
            Ok(_) => {
                log::debug!("Received: {}", line.trim());
                let req: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        log::error!("Parse error: {}", e);
                        break;
                    }
                };

                // Handle request
                let mut master = master.lock().unwrap();
                let resp = master.handle_request(req);

                // Send response
                serde_json::to_writer(&stream, &resp).unwrap();
                writeln!(&stream).unwrap();
                stream.flush().unwrap();

                match resp {
                    mapreduce::rpc::Response::Exit => {
                        log::info!("Worker exiting");
                        break;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                log::error!("Read error: {}", e);
                break;
            }
        }
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Vec<String> = env::args().collect();
    let port = args.get(1).map(|s| s.as_str()).unwrap_or("7799");
    let n_reduce: u32 = 5;

    let input_files = (1..=5).map(|i| format!("input/file{i}.txt")).collect();
    let output_path = "output".to_string();

    let master = Arc::new(std::sync::Mutex::new(Master::new(
        input_files,
        n_reduce,
        output_path,
    )));

    // Spawn health check thread (30 second timeout, check every 10 seconds)
    let master_for_health = Arc::clone(&master);
    start_health_check(master_for_health, 30, 10);

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr)?;
    log::info!("Master listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                log::info!("New worker connected");
                let master = Arc::clone(&master);

                thread::spawn(move || {
                    handle_worker(stream, master);
                });
            }
            Err(e) => {
                log::error!("Connection error: {}", e);
            }
        }
    }

    Ok(())
}
