#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1).map(|s| s.as_str()).unwrap_or("50051");

    let input_files = (1..=5).map(|i| format!("input/file{i}.txt")).collect();
    let n_reduce: u32 = 5;
    let output_path = "output".to_string();
    let addr = format!("127.0.0.1:{}", port);

    mapreduce::server::run_server(input_files, n_reduce, output_path, addr).await
}
