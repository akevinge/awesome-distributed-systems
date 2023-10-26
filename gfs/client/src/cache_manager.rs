use std::collections::HashMap;

use proto::metadata::{FileChunkMetadata, FileMetadata};

#[derive(Debug, Default)]
pub struct CacheManager {
    file_metadata: HashMap<String, FileMetadata>,
    chunk_metadata: HashMap<String, FileChunkMetadata>,
}

impl CacheManager {
    pub fn cache_metadata(
        &mut self,
        filename: &str,
        chunk_index: u32,
        chunk_metadata: FileChunkMetadata,
    ) {
        let chunk_handle = chunk_metadata.to_owned().chunk_handle;
        self.file_metadata.insert(
            filename.to_owned(),
            FileMetadata {
                filename: filename.to_owned(),
                chunks: HashMap::from([(chunk_index, chunk_handle.to_owned())]), // Initial empty chunk at index 0.
            },
        );
        self.chunk_metadata.insert(chunk_handle, chunk_metadata);
    }

    pub fn get_metadata(&self, filename: &String, chunk_index: u32) -> Option<FileChunkMetadata> {
        self.file_metadata
            .get(filename)
            .and_then(|metadata| metadata.chunks.get(&chunk_index).cloned())
            .and_then(|chunk_handle| self.chunk_metadata.get(&chunk_handle).cloned())
    }
}
