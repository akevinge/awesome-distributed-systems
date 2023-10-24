use std::{
    fs::{create_dir_all, File},
    io,
};

use anyhow::{anyhow, Result};
use common::concurrent_hash_map::ConcurrentHashMap;
use thiserror::Error;

#[derive(Debug, Clone, Default)]
pub struct ChunkFile {
    filename: String,
    version: u64,
    disk_path: String,
}

#[derive(Debug, Default)]
pub struct ChunkFileManager {
    /// chunk_handle -> ChunkFile
    chunk_files: ConcurrentHashMap<String, ChunkFile>,
}

// TODO: Move this to a config file.
// Temporary directory for storing chunk data when running locally.
// All chunk servers will create a subdirectory here.
const GLOBALCHUNK_SERVER_DIR: &str = "global_disk/data/";
const BYTES_64_MB: &u64 = &(64 * 1024 * 1024);

impl ChunkFileManager {
    pub fn advance_chunk_version(&self, chunk_handle: &String, new_version: u64) -> Result<()> {
        let mut chunk_file = self.chunk_files.get(chunk_handle).ok_or(anyhow!(
            "chunk handle {} not found for version",
            chunk_handle
        ))?;

        chunk_file.version = new_version;
        Ok(())
    }
    pub fn init_chunk(&self, id: u64, chunk_handle: String, filename: String) -> Result<()> {
        if self.chunk_files.contains_key(&chunk_handle) {
            return Err(InitChunkError::ChunkExists.into());
        }

        let dir_path = format!("{}/{}", GLOBALCHUNK_SERVER_DIR, id);
        let file_path = format!("{}/{}", dir_path, filename);

        // Create the data directory if it doesn't exist.
        create_dir_all(&dir_path).map_err(|e| InitChunkError::CreateDirError {
            dir: dir_path.to_owned(),
            source: e,
        })?;

        // Create empty file.
        let file = File::create(&file_path).map_err(|e| InitChunkError::CreateFileError {
            file: file_path.to_owned(),
            source: e,
        })?;

        // Allocate 64 MB. Calls ftruncate under the hood.
        // To see actual size (not disk usage), run `du -sh --apparent-size tmp/data/$file`.
        file.set_len(*BYTES_64_MB)
            .map_err(|e| InitChunkError::AllocateSizeError {
                file: file_path.to_owned(),
                size: *BYTES_64_MB,
                source: e,
            })?;

        let chunk = ChunkFile {
            filename,
            version: 1,
            disk_path: file_path,
        };
        self.chunk_files.insert(chunk_handle, chunk);
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum InitChunkError {
    #[error("chunk already exists.")]
    ChunkExists,
    #[error("error creating directory: {dir}, err: {source}.")]
    CreateDirError {
        dir: String,
        #[source]
        source: io::Error,
    },
    #[error("error creating file: {file}, err: {source}.")]
    CreateFileError {
        file: String,
        #[source]
        source: io::Error,
    },
    #[error("error allocating file: {file} of size: {size}, err: {source}.")]
    AllocateSizeError {
        file: String,
        size: u64,
        #[source]
        source: io::Error,
    },
}
