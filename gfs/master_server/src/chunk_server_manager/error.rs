use std::fmt::Display;

#[derive(Debug, Clone)]

pub enum ChunkAllocationError {
    AlreadyExists,
    NoAvailableChunkServers,
}

impl Display for ChunkAllocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkAllocationError::AlreadyExists => f.write_str("chunk already exists."),
            ChunkAllocationError::NoAvailableChunkServers => {
                f.write_str("no available chunk servers.")
            }
        }
    }
}
