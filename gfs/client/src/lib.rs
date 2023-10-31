use cache_manager::CacheManager;
use common::{
    conn,
    utils::{self, calc_checksum},
};
use log::debug;
use proto::grpc::{
    master_client::MasterClient, open_file_request::OpenMode, send_chunk_data_response,
    OpenFileRequest, SendChunkDataRequest, SendChunkDataResponse, WriteChunkHeader,
    WriteChunkRequest,
};
use tokio::task::JoinSet;
use tonic::{Request, Response};

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

    // Write data to chunk at given offset.
    async fn write_chunk(
        &self,
        filename: &String,
        chunk_index: u32,
        start_offset: u32,
        data: &Vec<u8>,
    ) -> Result<()> {
        // Get chunk metadata.
        // Create chunk if it doesn't exist.
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

        // Send data to primary and all replicas.
        let chunk_server_locations = utils::get_locations(chunk_metadata.clone());
        let mut task_set = JoinSet::new();
        let checksum = calc_checksum(data.as_slice());

        for loc in chunk_server_locations {
            debug!("Sending data to {:?}", loc);
            let data = data.clone();
            let checksum = checksum.clone();
            task_set.spawn(async move {
                let mut cs_client = conn::connect_to_chunk_server(loc.address, loc.port).await?;
                let request = SendChunkDataRequest { checksum, data };
                let res = cs_client.send_chunk_data(request).await?;
                Ok(res) as Result<Response<SendChunkDataResponse>>
            });
        }

        // Wait for all servers to be written to.
        while let Some(res) = task_set.join_next().await {
            match res {
                Ok(r) => match r {
                    Ok(r) => {
                        if r.into_inner().status != send_chunk_data_response::Status::Ok.into() {
                            // TODO: Propagate error
                        }
                    }
                    Err(_) => {
                        // TODO: Propagate error
                    }
                },
                Err(e) => {
                    // TODO: Propagate error
                }
                _ => {}
            }
        }

        // Send write chunk request to primary.
        // Connect to primary.
        let primary = chunk_metadata
            .primary_location
            .clone()
            .expect("missing primary");
        let mut cs_client =
            conn::connect_to_chunk_server(primary.address.to_owned(), primary.port).await?;

        // Send write_chunk request to primary.
        let header = WriteChunkHeader {
            chunk_handle: chunk_metadata.chunk_handle.clone(),
            chunk_version: chunk_metadata.version,
            checksum: checksum.to_owned(),
            start_offset,
        };
        let replica_locations = chunk_metadata.replica_locations.clone();
        let request = Request::new(WriteChunkRequest {
            header: Some(header),
            replica_locations,
        });
        cs_client.write_chunk(request).await?;
        Ok(())
    }

    // Write data to file at given offset.
    pub async fn write(&self, filename: &String, start_offset: u32, data: Vec<u8>) -> Result<()> {
        // Calculate chunk index and number of chunks to write to.
        let starting_chunk = start_offset / CHUNK_SIZE_BYTES; // 0-indexed chunk to start writing to.
        let chunk_count = data.len() as u32 / CHUNK_SIZE_BYTES; // 0-indexed number of chunks to write to.
        let ending_chunk = starting_chunk + chunk_count + 1;
        // Write to each chunk.
        for chunk_index in starting_chunk..ending_chunk {
            self.write_chunk(filename, chunk_index, start_offset, &data)
                .await?;
        }
        Ok(())
    }

    pub async fn open(&mut self, filename: &str) -> Result<()> {
        self.open_with_index(filename, 0, OpenMode::Create).await
    }
}
