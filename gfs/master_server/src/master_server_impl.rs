use std::sync::Arc;
use std::time::Duration;

use super::chunk_server_manager::ChunkServerManager;
use super::heartbeat::Heartbeat;
use super::metdata_manager::MetadataManager;
use common::{conn, utils};
use proto::{
    common::{ChunkServer, ChunkServerLocation, Empty},
    grpc::{
        master_server::Master, open_file_request::OpenMode, AdvanceChunkVersionRequest,
        InitEmptyChunkRequest, OpenFileRequest,
    },
    metadata::FileChunkMetadata,
};
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};

type Result<T> = std::result::Result<T, tonic::Status>;

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

    async fn write_file_chunk(
        &self,
        filename: &String,
        chunk_index: u32,
    ) -> Result<FileChunkMetadata> {
        // Retrieve chunk metadata or create the file if it doesn't exist.
        let chunk_metadata = match self.metadata_manager.get_metadata(filename, chunk_index) {
            Some(m) => m,
            None => self.create_file(filename).await?,
        };

        let server_locations = utils::get_locations(chunk_metadata.clone());
        let mut task_set = JoinSet::new();
        for loc in server_locations {
            let chunk_handle = chunk_metadata.chunk_handle.clone();
            let new_version = chunk_metadata.version;
            task_set.spawn(async move {
                let mut cs_client =
                    conn::connect_to_chunk_server(loc.address.to_owned(), loc.port).await?;

                let advance_version_request = Request::new(AdvanceChunkVersionRequest {
                    chunk_handle,
                    new_version,
                });
                cs_client
                    .advance_chunk_version(advance_version_request)
                    .await?;

                Ok(()) as Result<()>
            });
        }

        while let Some(res) = task_set.join_next().await {
            if res.is_err() {
                // TODO: propagate error.
            }
        }

        Ok(chunk_metadata)
    }

    async fn create_file(&self, filename: &String) -> Result<FileChunkMetadata> {
        // Check if file already exists.
        if self.metadata_manager.file_exists(filename) {
            let metadata = self
                .metadata_manager
                .get_metadata(filename, 0)
                .expect("unreachable");
            return Ok(metadata);
        }

        // Create new chunk handle and allocate chunk servers.
        let new_chunk_handle = self.metadata_manager.create_chunk_handle();
        let allocated_chunk_servers = self
            .chunk_server_manager
            .allocate_chunk_servers(&new_chunk_handle, 3)
            .map_err(|e| {
                tonic::Status::internal(format!("Failed to allocate chunk servers: {}", e))
            })?;

        let mut chunk_locations: Vec<ChunkServerLocation> = vec![];

        // Initialize empty chunk on chunk servers.
        for chunk_server in allocated_chunk_servers {
            chunk_locations.push(chunk_server.to_owned().into());

            let loc: ChunkServerLocation = chunk_server.clone().into();

            let mut chunk_server_client =
                conn::connect_to_chunk_server(loc.address.to_owned(), loc.port).await?;

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

        let primary_location = chunk_locations.get(0).cloned();
        let replica_locations = if chunk_locations.len() > 1 {
            chunk_locations[1..chunk_locations.len()].to_vec()
        } else {
            vec![]
        };

        // Add file to metadata manager.
        let chunk_metadata = FileChunkMetadata {
            chunk_handle: new_chunk_handle.clone(),
            primary_location,
            replica_locations,
            version: 1,
        };
        self.metadata_manager
            .add_file(filename, new_chunk_handle, chunk_metadata.to_owned());
        Ok(chunk_metadata)
    }
}

#[tonic::async_trait]
impl Master for MasterImpl {
    /// Report chunk server to master server.
    /// This is called by chunk servers on boot to register themselves to the master server.
    async fn report_chunk_server(
        &self,
        request: tonic::Request<ChunkServer>,
    ) -> Result<Response<Empty>> {
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
        Ok(Response::new(Empty {}))
    }

    /// Open a new file, create if it doesn't exist.
    /// This is called by clients when they want to create a new file.
    /// The master server will set aside N (configurable) chunk servers to allocate chunks for the file
    /// and calls init_empty_chunk on each of them.
    async fn open_file(
        &self,
        request: tonic::Request<OpenFileRequest>,
    ) -> Result<Response<FileChunkMetadata>> {
        let filename = &request.get_ref().filename;
        let chunk_index = request.get_ref().chunk_index;
        let mode = &request.get_ref().mode();

        match mode {
            OpenMode::Read => Err(Status::unimplemented("unimplemented")),
            OpenMode::Create => {
                let metadata = self.create_file(filename).await?;
                Ok(Response::new(metadata))
            }
            OpenMode::Write => {
                let metadata = self.write_file_chunk(filename, chunk_index).await?;
                Ok(Response::new(metadata))
            }
        }
    }
}
