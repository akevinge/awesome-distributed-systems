use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};

use ::common::{conn, utils::calc_checksum};
use anyhow::Result;
use log::{debug, error};
use proto::{
    common::{self, ChunkServerLocation, Empty},
    grpc::{
        chunk_server_server::ChunkServer, master_client::MasterClient, send_chunk_data_response,
        AdvanceChunkVersionRequest, ApplyMutationsRequest, ApplyMutationsResponse,
        GrantLeaseRequest, InitEmptyChunkRequest, MutateResponseStatus, SendChunkDataRequest,
        SendChunkDataResponse, WriteChunkRequest, WriteChunkResponse,
    },
};
use tokio::{sync::Mutex, task::JoinSet};
use tonic::{Request, Response};
use url::Url;

use crate::{buffer::Buffer, chunk_file_manager::ChunkFileManager, lease_manager::LeaseManager};

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
    /// Mutation buffer. This is used to store mutations that have not yet been applied (written to disk)
    buffer: Buffer,
}

const BYTES_64_MB: usize = 64 * 1024 * 1024;

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
            buffer: Buffer::new(NonZeroUsize::new(100).unwrap()),
        };
        cs.report_chunk_server().await?;
        Ok(cs)
    }

    // TODO: move to anyhow errors.
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

    async fn send_chunk_data(
        &self,
        request: tonic::Request<SendChunkDataRequest>,
    ) -> Result<tonic::Response<SendChunkDataResponse>, tonic::Status> {
        let data = request.get_ref().data.to_owned();
        let checksum = request.get_ref().checksum.to_owned();

        // Check if data is too big.
        if data.len() > (BYTES_64_MB) {
            return Ok(tonic::Response::new(SendChunkDataResponse {
                status: send_chunk_data_response::Status::TooBig.into(),
            }));
        }

        // Validate checksum.
        if checksum != calc_checksum(data.as_slice()) {
            return Ok(tonic::Response::new(SendChunkDataResponse {
                status: send_chunk_data_response::Status::BadData.into(),
            }));
        }

        // Add data to buffer.
        self.buffer.insert(checksum, data);
        Ok(tonic::Response::new(SendChunkDataResponse {
            status: send_chunk_data_response::Status::Ok.into(),
        }))
    }

    async fn write_chunk(
        &self,
        request: tonic::Request<WriteChunkRequest>,
    ) -> Result<tonic::Response<WriteChunkResponse>, tonic::Status> {
        debug!("Got a write_chunk request: {:?}.", request);
        let header = request.get_ref().header.to_owned().ok_or_else(|| {
            error!("Got a write_chunk request with no header.");
            tonic::Status::internal("Got a write_chunk request with no header.")
        })?;

        // Check if chunk exists and that its version isn't stale.
        match self.file_manager.get_chunk(&header.chunk_handle) {
            Some(chunk_file) if chunk_file.get_version() != header.chunk_version => {
                error!("Got a write_chunk request with a bad version.");
                return Ok(tonic::Response::new(WriteChunkResponse {
                    status: MutateResponseStatus::StaleVersion.into(),
                    replica_statuses: vec![],
                }));
            }
            None => {
                error!("Got a write_chunk request for a chunk that doesn't exist.");
                return Ok(tonic::Response::new(WriteChunkResponse {
                    status: MutateResponseStatus::ChunkNotFound.into(),
                    replica_statuses: vec![],
                }));
            }
            _ => {}
        };

        // Retrieve data from buffer and error if data isn't in the buffer.
        let data = self.buffer.pop(&header.checksum).ok_or_else(|| {
            error!("Got a write_chunk request with no buffered data.");
            tonic::Status::internal("Got a write_chunk request with no buffered data.")
        })?;

        // Write data to disk.
        self.file_manager
            .write_chunk(&header.chunk_handle, header.start_offset, data)
            .map_err(|e| {
                error!("Failed to write chunk: {:?}.", e);
                tonic::Status::internal(format!("Failed to write chunk: {}", e))
            })?;

        // Propagate mutations to replicas.
        let replicas = request.get_ref().replica_locations.to_owned();
        let mut join_set = JoinSet::new();
        for replica in replicas {
            let header = header.clone(); // Clone header for task.
            join_set.spawn(async move {
                let mut client =
                    conn::connect_to_chunk_server(replica.address.to_owned(), replica.port).await?;
                let request = ApplyMutationsRequest {
                    headers: vec![header],
                };
                let res = client.apply_mutations(request).await?;
                Ok(res) as Result<Response<ApplyMutationsResponse>>
            });
        }

        let mut replica_statuses = Vec::new();
        while let Some(Ok(res)) = join_set.join_next().await {
            match res {
                Ok(r) => {
                    replica_statuses.push(r.get_ref().status.into());
                }
                Err(e) => {
                    error!("Failed to apply mutations: {:?}.", e);
                    replica_statuses.push(MutateResponseStatus::InternalError.into());
                }
            }
        }

        Ok(tonic::Response::new(WriteChunkResponse {
            status: MutateResponseStatus::Ok.into(),
            replica_statuses,
        }))
    }

    async fn apply_mutations(
        &self,
        request: tonic::Request<ApplyMutationsRequest>,
    ) -> Result<tonic::Response<ApplyMutationsResponse>, tonic::Status> {
        let headers = request.get_ref().headers.to_owned();
        for header in headers {
            // Check if chunk exists and that its version isn't stale.
            match self.file_manager.get_chunk(&header.chunk_handle) {
                Some(chunk_file) if chunk_file.get_version() != header.chunk_version => {
                    error!("Got a write_chunk request with a bad version.");
                    return Ok(tonic::Response::new(ApplyMutationsResponse {
                        status: MutateResponseStatus::StaleVersion.into(),
                    }));
                }
                None => {
                    error!("Got a write_chunk request for a chunk that doesn't exist.");
                    return Ok(tonic::Response::new(ApplyMutationsResponse {
                        status: MutateResponseStatus::ChunkNotFound.into(),
                    }));
                }
                _ => {}
            };

            // Retrieve data from buffer and error if data isn't in the buffer.
            let data = self.buffer.pop(&header.checksum).ok_or_else(|| {
                error!("Got a write_chunk request with no buffered data.");
                tonic::Status::internal("Got a write_chunk request with no buffered data.")
            })?;

            // Write data to disk.
            self.file_manager
                .write_chunk(&header.chunk_handle, header.start_offset, data)
                .map_err(|e| {
                    error!("Failed to write chunk: {:?}.", e);
                    tonic::Status::internal(format!("Failed to write chunk: {}", e))
                })?;
        }
        Ok(tonic::Response::new(ApplyMutationsResponse {
            status: MutateResponseStatus::Ok.into(),
        }))
    }
}
