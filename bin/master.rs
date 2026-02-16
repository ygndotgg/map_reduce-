use std::{io::BufReader, net::TcpListener};

use mapreduce::rpc::{Request, Response};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    for stream in listener.incoming() {
        let stream = stream?;
        let buf = BufReader::new(&stream);
        let req: Request = serde_json::from_reader(buf)?;
        println!("we Go request ra {:?}", req);
        // --- process ----
        let resp = Response::NoTask;
        serde_json::to_writer(stream, &resp)?;
    }
    Ok(())
}
