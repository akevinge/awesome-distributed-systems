use std::{str::FromStr, time::Duration};

use proto::grpc::chunk_server_client::ChunkServerClient;
use tonic::{
    transport::{Channel, Endpoint},
    Status,
};

pub async fn connect_to_chunk_server(
    addr: String,
    port: u32,
) -> Result<ChunkServerClient<Channel>, Status> {
    let endpoint = match Endpoint::from_str(&format!("http://{}:{}", addr, port)) {
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
