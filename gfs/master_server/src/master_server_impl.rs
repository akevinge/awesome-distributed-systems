use std::fs::File;
use std::sync::Arc;
use std::time::Duration;

use super::chunk_server_manager::{connect_to_chunk_server, ChunkServerManager};
use super::heartbeat::Heartbeat;
use super::metdata_manager::MetadataManager;
use proto::{
    common::{ChunkServer, ChunkServerLocation, Empty},
    grpc::{master_server::Master, InitEmptyChunkRequest, OpenFileRequest},
    metadata::FileChunkMetadata,
};
use tonic::Request;

#[derive(Debug)]
pub struct MasterImpl {
    metadata_manager: MetadataManager,
    chunk_server_manager: Arc<ChunkServerManager>,
    _heartbeat: Arc<Heartbeat>,
}

impl MasterImpl {
    pub async fn new() -> Self {
        // Initialize metadata manager, chunk server manager, and heartbeart.
        let metadata_manager = MetadataManager::default();
        let chunk_server_manager = Arc::new(ChunkServerManager::default());
        let heartbeat = Arc::new(Heartbeat::new(
            chunk_server_manager.clone(),
            Duration::from_secs(1),
        ));
        Arc::clone(&heartbeat).init_heartbeat().await;

        Self {
            metadata_manager,
            chunk_server_manager,
            _heartbeat: heartbeat,
        }
    }
}

#[tonic::async_trait]
impl Master for MasterImpl {
    /// Report chunk server to master server.
    /// This is called by chunk servers on boot to register themselves to the master server.
    async fn report_chunk_server(
        &self,
        request: tonic::Request<ChunkServer>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        // Ensure chunk server is valid in request.
        let chunk_server = request.get_ref();

        // Ensure chunk server location is valid in request.
        let location = chunk_server.location.as_ref();
        if location.is_none() {
            return Err(tonic::Status::invalid_argument(
                "Chunk server location not found in heartbeat request",
            ));
        }

        println!("Got a chunk server report from {:?}", location);
        // Register chunk server.
        self.chunk_server_manager
            .register_chunk_server(chunk_server.clone());
        Ok(tonic::Response::new(Empty {}))
    }

    /// Open a new file, create if it doesn't exist.
    /// This is called by clients when they want to create a new file.
    /// The master server will set aside N (configurable) chunk servers to allocate chunks for the file
    /// and calls init_empty_chunk on each of them.
    async fn open_file(
        &self,
        request: tonic::Request<OpenFileRequest>,
    ) -> Result<tonic::Response<FileChunkMetadata>, tonic::Status> {
        let filename = &request.get_ref().filename;
        let chunk_index = &request.get_ref().chunk_index;

        // Check if file already exists.
        if self.metadata_manager.file_exists(filename) {
            let metadata = self
                .metadata_manager
                .get_metadata(filename, *chunk_index)
                .expect("unreachable");
            return Ok(tonic::Response::new(metadata));
        }

        // Create new chunk handle and allocate chunk servers.
        let new_chunk_handle = self.metadata_manager.create_chunk_handle();
        let chunk_locations = self
            .chunk_server_manager
            .allocate_chunk_servers(&new_chunk_handle, 3)
            .map_err(|e| {
                tonic::Status::internal(format!("Failed to allocate chunk servers: {}", e))
            })?;

        // Initialize empty chunk on chunk servers.
        for locations in chunk_locations {
            let loc: ChunkServerLocation = locations.into();

            let mut chunk_server_client = connect_to_chunk_server(&loc).await?;

            let request = Request::new(InitEmptyChunkRequest {
                chunk_handle: new_chunk_handle.clone(),
            });
            println!("Sending init_empty_chunk request to {:?}", loc);
            match chunk_server_client.init_empty_chunk(request).await {
                Ok(_) => (),
                Err(e) => {
                    return Err(tonic::Status::internal(format!(
                        "Failed to initialize empty chunk: {}",
                        e
                    )))
                }
            }
        }

        // Add file to metadata manager.
        let chunk_metadata = FileChunkMetadata {
            chunk_handle: new_chunk_handle.clone(),
            version: 1,
            ..Default::default()
        };
        self.metadata_manager
            .add_file(filename, new_chunk_handle, chunk_metadata.to_owned());

        Ok(tonic::Response::new(chunk_metadata))
    }
}
