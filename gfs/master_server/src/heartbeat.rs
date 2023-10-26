use std::{sync::Arc, thread, time::Duration};

use common::conn;
use proto::common::Empty;

use crate::chunk_server_manager::ChunkServerManager;

/// Hearbeat is a background thread that periodically sends heartbeat to all chunk servers.
#[derive(Debug)]
pub struct Heartbeat {
    /// The chunk server manager.
    /// We need this to get a list of chunk servers to send heartbeat to.
    cs_manager: Arc<ChunkServerManager>,

    /// The interval between each heartbeat.
    interval: Duration,
}

impl Heartbeat {
    pub fn new(cs_manager: Arc<ChunkServerManager>, interval: Duration) -> Self {
        Self {
            cs_manager,
            interval,
        }
    }

    /// Start the heartbeat background thread.
    pub async fn init_heartbeat(self: Arc<Self>) {
        let _ = tokio::spawn(async move {
            loop {
                thread::sleep(self.interval);

                for (server, _) in self.cs_manager.server_iter() {
                    let cs_client = match server.borrow_inner().location.as_ref() {
                        Some(loc) => {
                            conn::connect_to_chunk_server(loc.address.to_owned(), loc.port)
                                .await
                                .map_err(|e| {
                                    // TODO: implement retry/cleanup.
                                    println!("Failed to connect to chunk server: {}", e);
                                })
                        }
                        None => unreachable!("Chunk server location not provided."),
                    };

                    if let Ok(mut client) = cs_client {
                        let response = client.heartbeat(tonic::Request::new(Empty {})).await;
                        match response {
                            Ok(_) => (),
                            Err(e) => {
                                // TODO: implement retry/cleanup.
                                println!("Failed to send heartbeat: {}", e);
                            }
                        }
                    }
                }
            }
        });
    }
}
