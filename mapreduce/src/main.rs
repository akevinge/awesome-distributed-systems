#![feature(never_type)]
use mapreduce::{coordinator, worker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = ::std::env::args().collect();
    if args.len() >= 2 {
        match &args[1][..] {
            "worker" => return worker::main().await,
            "coordinator" => return coordinator::main().await,
            _ => (),
        }
    }

    Err(format!("usage: {} [client | server] ADDRESS", args[0]).into())
}
