use proto::grpc::{master_client::MasterClient, CreateFileRequest};
use tonic::Request;

// const CHUNK_SIZE_BYTES: u32 = 64 * 1024 * 1024;
// TODO(FIX): This is a temporary value for testing purposes.)
const CHUNK_SIZE_BYTES: u32 = 1;

pub struct Client {
    master_client: MasterClient<tonic::transport::Channel>,
}

impl Client {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let addr = "http://[::1]:50051";
        let master_client = MasterClient::connect(addr).await?;
        Ok(Self { master_client })
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

    pub async fn open(&mut self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let request = Request::new(CreateFileRequest {
            filename: filename.to_string(),
        });
        let response = self.master_client.create_file(request).await?;
        println!("RESPONSE={:?}", response);
        Ok(())
    }
}
