use cache_manager::CacheManager;
use proto::grpc::{master_client::MasterClient, OpenFileRequest};
use tonic::Request;

mod cache_manager;

const CHUNK_SIZE_BYTES: u32 = 64 * 1024 * 1024;
// TODO(FIX): This is a temporary value for testing purposes.)

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

    pub async fn read(
        &mut self,
        filename: &str,
        byte_range: (u32, u32),
    ) -> Result<(), Box<dyn std::error::Error>> {
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

    async fn connect_to_master(
        &self,
    ) -> Result<MasterClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
        let addr = "http://[::1]:50051";
        Ok(MasterClient::connect(addr).await?)
    }

    async fn open_with_index(
        &mut self,
        filename: &str,
        chunk_index: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::new(OpenFileRequest {
            chunk_index,
            filename: filename.to_string(),
        });
        let response = self.connect_to_master().await?.open_file(request).await?;
        self.cache_manager
            .cache_metadata(filename, chunk_index, response.into_inner());
        Ok(())
    }

    pub async fn write(
        &mut self,
        filename: &String,
        start_offset: u32,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for i in (start_offset as usize)..data.len() {
            let chunk_index = i as u32 / CHUNK_SIZE_BYTES;
            let chunk_offset = i as u32 % CHUNK_SIZE_BYTES;

            let chunk_metadata = match self.cache_manager.get_metadata(filename, chunk_index) {
                Some(metadata) => metadata,
                None => {
                    self.open_with_index(filename, chunk_index).await?;
                    self.cache_manager
                        .get_metadata(filename, chunk_index)
                        .expect("unreachable")
                }
            };
        }
        Ok(())
    }

    pub async fn open(&mut self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.open_with_index(filename, 0).await
    }
}
