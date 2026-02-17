use std::{
    io::{BufReader, Write},
    net::TcpListener,
    sync::Mutex,
};

use mapreduce::{master::Master, rpc::Request};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_files = vec!["input/file1.txt".to_string(), "input/file2.txt".to_string()];
    let n_reduce = 5;
    let output_path = "output".to_string();
    let master = Mutex::new(Master::new(input_files, n_reduce, output_path));
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    println!("master listening on 127.0.0.1:8080");
    for stream in listener.incoming() {
        let mut stream = &stream?;
        let buf = BufReader::new(stream);
        let req: Request = serde_json::from_reader(buf)?;
        let mut master = master.lock().unwrap();
        let resp = master.handle_request(req);
        serde_json::to_writer(stream, &resp)?;
        stream.flush()?;
    }
    Ok(())
}
