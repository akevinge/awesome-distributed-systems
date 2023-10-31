use std::net::SocketAddr;

use chunk_server_impl::ChunkServerImpl;
use clap::Parser;
use proto::grpc::chunk_server_server::ChunkServerServer;
use tonic::transport::Server;
use url::Url;

mod buffer;
mod chunk_file_manager;
mod chunk_server_impl;
mod lease_manager;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, help = "ID of chunk server.")]
    id: u64,
    #[arg(
        short,
        long,
        help = "Address to run chunk server on. Example: [::1]:50052"
    )]
    bind: SocketAddr,
    #[arg(
        short,
        long,
        help = "Address of master server. Example: http://[::1]:50051",
        value_parser = clap::value_parser!(Url)
    )]
    master: Url,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();

    let chunk_server = ChunkServerImpl::new(args.id, &args.master, &args.bind).await?;

    // Start the chunk server.
    Server::builder()
        .add_service(ChunkServerServer::new(chunk_server))
        .serve(args.bind)
        .await?;

    Ok(())
}
