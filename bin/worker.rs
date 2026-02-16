use std::{io::BufReader, net::TcpStream};

use mapreduce::rpc::{Request, Response};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connect = TcpStream::connect("127.0.0.1:8080")?;
    let req = Request::GetTask;
    // let buf = BufReader::new(connect);
    serde_json::to_writer(&connect, &req)?;
    TcpStream::shutdown(&connect, std::net::Shutdown::Write)?;

    let resp: Response = serde_json::from_reader(connect)?;
    println!("Got Response to the client");
    Ok(())
}
