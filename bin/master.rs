use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use mapreduce::{master::Master, rpc::{Request, Response}};

fn handle_worker(mut stream: std::net::TcpStream, master: Arc<Mutex<Master>>) {
    loop {
        let mut buf = BufReader::new(&mut stream);
        let mut line = String::new();

        match buf.read_line(&mut line) {
            Ok(0) => {
                // Connection closed
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

                // Handle request - lock master
                let mut master = master.lock().unwrap();
                let resp = master.handle_request(req);

                // Send response
                serde_json::to_writer(&stream, &resp).unwrap();
                writeln!(&stream).unwrap();
                stream.flush().unwrap();
                // If Exit, break
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
    let n_reduce = 5;
    let output_path = "output".to_string();

    let master = Arc::new(Mutex::new(Master::new(input_files, n_reduce, output_path)));

    let listener = TcpListener::bind("127.0.0.1:7799")?;
    println!("Master listening on 127.0.0.1:7799");
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
