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
                println!("[Master] Worker disconnected");
                break;
            }
            Ok(_) => {
                println!("[Master] Received: {}", line.trim());
                let req: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        println!("[Master] Parse error: {}", e);
                        break;
                    }
                };

                // Handle request
                let mut master = master.lock().unwrap();
                println!("[Master] Calling handle_request");
                let resp = master.handle_request(req);
                println!("[Master] Got response, sending...");

                // Send response
                serde_json::to_writer(&stream, &resp).unwrap();
                writeln!(&stream).unwrap();
                stream.flush().unwrap();
                println!("[Master] Response sent");

                match resp {
                    mapreduce::rpc::Response::Exit => {
                        println!("[Master] Worker exiting");
                        break;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                println!("[Master] Read error: {}", e);
                break;
            }
        }
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_files = (1..=5).map(|i| format!("input/file{i}.txt")).collect();
    let args: Vec<String> = env::args().collect();
    let n_reduce = 5;
    let output_path = "output".to_string();

    let master = Arc::new(std::sync::Mutex::new(Master::new(
        input_files,
        n_reduce,
        output_path,
    )));

    // Spawn health check thread (30 second timeout, check every 10 seconds)
    let master_for_health = Arc::clone(&master);
    start_health_check(master_for_health, 30, 10);
    let addr = format!("127.0.0.1:{}", args[1]);
    let listener = TcpListener::bind(&addr)?;
    println!("Master listening on {}", addr);
    println!("Waiting for workers...");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("[Master] New worker connected");
                let master = Arc::clone(&master);

                thread::spawn(move || {
                    handle_worker(stream, master);
                });
            }
            Err(e) => {
                println!("[Master] Connection error: {}", e);
            }
        }
    }

    Ok(())
}
