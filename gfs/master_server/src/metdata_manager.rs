use common::concurrent_hash_map::ConcurrentHashMap;
use proto::metadata::{FileChunkMetadata, FileMetadata};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

#[derive(Debug, Default)]
pub struct MetadataManager {
    global_chunk_handle: Arc<AtomicU32>,
    file_metadata: ConcurrentHashMap<String, FileMetadata>,
    chunk_metadata: ConcurrentHashMap<String, FileChunkMetadata>,
}

impl MetadataManager {
    pub fn file_exists(&self, filename: &String) -> bool {
        self.file_metadata.contains_key(filename)
    }

    pub fn create_chunk_handle(&self) -> String {
        let handle = self.global_chunk_handle.load(Ordering::SeqCst).to_string();
        self.global_chunk_handle.fetch_add(1, Ordering::SeqCst);
        handle
    }

    pub fn get_metadata(&self, filename: &String, chunk_index: u32) -> Option<FileChunkMetadata> {
        self.file_metadata
            .get(filename)
            .and_then(|metadata| metadata.chunks.get(&chunk_index).cloned())
            .and_then(|chunk_handle| self.chunk_metadata.get(&chunk_handle))
    }

    pub fn add_file(
        &self,
        filename: &str,
        chunk_handle: String,
        chunk_metadata: FileChunkMetadata,
    ) {
        self.file_metadata.insert(
            filename.to_owned(),
            FileMetadata {
                filename: filename.to_owned(),
                chunks: HashMap::from([(0, chunk_handle.clone())]), // Initial empty chunk at index 0.
            },
        );
        self.chunk_metadata.insert(chunk_handle, chunk_metadata);
    }
}
