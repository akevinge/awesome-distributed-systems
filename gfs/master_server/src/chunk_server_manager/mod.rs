use proto::common::ChunkServerLocation;
use proto::grpc::chunk_server_client::ChunkServerClient;
use std::str::FromStr;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

mod chunk_server_manager_impl;
mod error;
mod server_priority;

pub use chunk_server_manager_impl::ChunkServerManager;

pub async fn connect_to_chunk_server(
    loc: &ChunkServerLocation,
) -> Result<ChunkServerClient<Channel>, tonic::Status> {
    let endpoint = match Endpoint::from_str(&format!("http://{}:{}", loc.address, loc.port)) {
        Ok(endpoint) => endpoint,
        Err(e) => {
            return Err(tonic::Status::internal(format!(
                "Failed to parse endpoint: {}",
                e
            )))
        }
    };
    let endpoint = endpoint
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(60));

    // Attempt to connect to chunk server and return client.
    ChunkServerClient::connect(endpoint.clone())
        .await
        .map_err(|e| tonic::Status::internal(format!("Failed to connect to chunk server: {}", e)))
}
