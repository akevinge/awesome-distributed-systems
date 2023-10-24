use clap::{Parser, ValueEnum};
use client::Client;

#[derive(Parser, Debug)]
#[command(name = "cli", version = "0.1.0", about = "CLI for GFS client.")]
struct Args {
    #[arg(value_enum)]
    operation: Operation,
    #[arg(short, long)]
    filename: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Operation {
    Open,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = Client::new().await?;
    match args.operation {
        Operation::Open => client.open(&args.filename).await,
    }
}
