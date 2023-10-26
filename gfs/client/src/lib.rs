use cache_manager::CacheManager;
use common::{conn, utils};
use log::debug;
use proto::{
    common::Empty,
    grpc::{
        master_client::MasterClient, open_file_request::OpenMode, OpenFileRequest,
        WriteChunkHeader, WriteChunkRequest,
    },
};
use tokio::task::JoinSet;
use tonic::Request;

mod cache_manager;

const CHUNK_SIZE_BYTES: u32 = 64 * 1024 * 1024;
// TODO(FIX): This is a temporary value for testing purposes.)

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Client {
    cache_manager: CacheManager,
    master_addr: String,
}

impl Client {
    pub fn new(master_addr: String) -> Self {
        Self {
            master_addr,
            cache_manager: CacheManager::default(),
        }
    }

    pub async fn read(&mut self, filename: &str, byte_range: (u32, u32)) -> Result<()> {
        if byte_range.0 > byte_range.1 {
            return Err("Invalid byte range".into());
        }

        let byte_size = byte_range.1 - byte_range.0;
        if byte_size > CHUNK_SIZE_BYTES {
            return Err(format!("Byte range too large {byte_size}").into());
        }

        let chunk_index = (byte_range.0 / CHUNK_SIZE_BYTES) - 1;

        Ok(())
    }

    async fn connect_to_master(&self) -> Result<MasterClient<tonic::transport::Channel>> {
        let addr = "http://[::1]:50051";
        Ok(MasterClient::connect(addr).await?)
    }

    async fn open_with_index(
        &self,
        filename: &str,
        chunk_index: u32,
        mode: OpenMode,
    ) -> Result<()> {
        let request = Request::new(OpenFileRequest {
            chunk_index,
            filename: filename.to_string(),
            mode: mode.into(),
        });
        let response = self.connect_to_master().await?.open_file(request).await?;
        self.cache_manager
            .cache_metadata(filename, chunk_index, response.into_inner());
        Ok(())
    }

    pub async fn write(&self, filename: &String, start_offset: u32, data: Vec<u8>) -> Result<()> {
        let starting_chunk = start_offset / CHUNK_SIZE_BYTES; // 0-indexed chunk to start writing to.
        let chunk_count = data.len() as u32 / CHUNK_SIZE_BYTES; // 0-indexed number of chunks to write to.
        let ending_chunk = starting_chunk + chunk_count + 1;
        for i in starting_chunk..ending_chunk {
            let chunk_index = i / CHUNK_SIZE_BYTES;
            let chunk_offset = i % CHUNK_SIZE_BYTES;

            let chunk_metadata = &match self.cache_manager.get_metadata(filename, chunk_index) {
                Some(metadata) => metadata,
                None => {
                    self.open_with_index(filename, chunk_index, OpenMode::Write)
                        .await?;
                    self.cache_manager
                        .get_metadata(filename, chunk_index)
                        .expect("unreachable")
                }
            };

            let chunk_server_locations = utils::get_locations(chunk_metadata.clone());
            let mut task_set = JoinSet::new();
            for loc in chunk_server_locations {
                debug!("Sending write_chunk request to {:?}", loc);
                let chunk_handle = chunk_metadata.chunk_handle.clone();
                let data = data.clone();
                let chunk_version = chunk_metadata.version;
                let replica_locations = chunk_metadata.replica_locations.clone();
                task_set.spawn(async move {
                    let mut conn = conn::connect_to_chunk_server(loc.address, loc.port).await?;
                    let write_request = Request::new(WriteChunkRequest {
                        header: Some(WriteChunkHeader {
                            chunk_version,
                            chunk_handle,
                            data,
                            start_offset: chunk_offset as u64,
                        }),
                        replica_locations,
                    });
                    Ok(conn.write_chunk(write_request).await.unwrap())
                        as Result<tonic::Response<Empty>>
                });
            }

            // Wait for all servers to be written to.
            while let Some(res) = task_set.join_next().await {
                if res.is_err() {
                    // TODO: Propagate error
                }
            }
        }
        Ok(())
    }

    pub async fn open(&mut self, filename: &str) -> Result<()> {
        self.open_with_index(filename, 0, OpenMode::Create).await
    }
}
