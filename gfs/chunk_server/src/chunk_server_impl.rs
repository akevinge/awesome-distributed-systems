use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use log::{debug, error};
use proto::{
    common::{self, ChunkServerLocation, Empty},
    grpc::{
        chunk_server_server::ChunkServer, master_client::MasterClient, AdvanceChunkVersionRequest,
        ApplyMutationsRequest, GrantLeaseRequest, InitEmptyChunkRequest, WriteChunkRequest,
    },
};
use tokio::sync::Mutex;
use tonic::Request;
use url::Url;

use crate::{chunk_file_manager::ChunkFileManager, lease_manager::LeaseManager};

#[derive(Debug)]
pub struct ChunkServerImpl {
    // Chunk server id. This is a workaround when all chunk servers are running locally and need a
    // persistent value to identify their data directories.
    id: u64,
    // The master client is used to send heartbeats to the master server.
    master_client: Arc<Mutex<MasterClient<tonic::transport::Channel>>>,
    // The chunk server's location.
    // This is sent to the master server in the heartbeat.
    location: ChunkServerLocation,
    // Lease manager.
    lease_manager: LeaseManager,
    /// Chunk file manager.
    file_manager: ChunkFileManager,
}

impl ChunkServerImpl {
    /// Create a new chunk server.
    /// This connects and reports itself to the master server.
    pub async fn new(
        id: u64,
        master_addr: &Url,
        bind_addr: &SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let master_client = MasterClient::connect(master_addr.to_string()).await?;

        let cs = Self {
            id,
            master_client: Arc::new(Mutex::new(master_client)),
            location: ChunkServerLocation {
                address: bind_addr.ip().to_string(),
                port: bind_addr.port() as u32,
            },
            lease_manager: LeaseManager::new(),
            file_manager: ChunkFileManager::default(),
        };
        cs.report_chunk_server().await?;
        Ok(cs)
    }

    async fn report_chunk_server(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut master_client = self.master_client.lock().await;
        let report_request = Request::new(common::ChunkServer {
            available_space_mb: 9999,
            location: Some(self.location.clone()),
        });
        master_client.report_chunk_server(report_request).await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl ChunkServer for ChunkServerImpl {
    async fn heartbeat(
        &self,
        _: tonic::Request<common::Empty>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        println!("Got a heartbeat request.");
        Ok(tonic::Response::new(Empty {}))
    }

    /// Initialize an empty chunk to disk.
    /// This is called by the master server when a new file is created.
    /// If it is called on an existing file, the file is truncated.
    async fn init_empty_chunk(
        &self,
        request: tonic::Request<InitEmptyChunkRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        debug!("Got a init_empty_chunk request: {:?}.", request);
        let chunk_handle = &request.get_ref().chunk_handle;

        self.file_manager
            .init_chunk(self.id, chunk_handle.to_owned(), chunk_handle.to_owned())
            .map_err(|e| {
                error!("Failed to init chunk: {:?}.", e);
                tonic::Status::internal(format!("Failed to init chunk: {}", e))
            })?;

        Ok(tonic::Response::new(Empty {}))
    }

    async fn advance_chunk_version(
        &self,
        request: tonic::Request<AdvanceChunkVersionRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        debug!("Got advance_chunk_version request: {:?}.", request);
        let chunk_handle = &request.get_ref().chunk_handle;
        let new_version = &request.get_ref().new_version;

        self.file_manager
            .advance_chunk_version(chunk_handle, *new_version)
            .map_err(|e| {
                error!("Failed to advance chunk version: {:?}.", e);
                tonic::Status::not_found(format!("Failed to advance chunk version: {}", e))
            })?;

        Ok(tonic::Response::new(Empty {}))
    }

    async fn grant_lease(
        &self,
        request: tonic::Request<GrantLeaseRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let chunk_handle = &request.get_ref().chunk_handle;
        let expiration_unix_secs = request.get_ref().expiration_unix_secs;

        self.lease_manager
            .grant_lease(chunk_handle.clone(), expiration_unix_secs);

        Ok(tonic::Response::new(Empty {}))
    }

    async fn write_chunk(
        &self,
        request: tonic::Request<WriteChunkRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        debug!("Got a write_chunk request: {:?}.", request);
        Ok(tonic::Response::new(Empty {}))
    }

    async fn apply_mutations(
        &self,
        request: tonic::Request<ApplyMutationsRequest>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        debug!("Got a apply_mutations request: {:?}.", request);
        Ok(tonic::Response::new(Empty {}))
    }
}
