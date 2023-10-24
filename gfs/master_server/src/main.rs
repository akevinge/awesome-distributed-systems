use master_server_impl::MasterImpl;
use proto::grpc::master_server::MasterServer;
use tonic::transport::Server;

mod chunk_server_manager;
mod heartbeat;
mod master_server_impl;
mod metdata_manager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let service = MasterImpl::new().await;

    Server::builder()
        .add_service(MasterServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
