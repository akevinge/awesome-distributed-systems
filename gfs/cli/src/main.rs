use clap::{Parser, ValueEnum};
use client::{Client, Result};

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
    Write,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mut client = Client::new("http://[::1]:50551".into());
    match args.operation {
        Operation::Open => client.open(&args.filename).await?,
        Operation::Write => {
            client
                .write(&args.filename, 0, "010101010".to_string().into_bytes())
                .await?
        }
    }
    Ok(())
}
