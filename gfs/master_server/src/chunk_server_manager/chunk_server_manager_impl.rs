use common::concurrent_hash_map::ConcurrentHashMap;
use priority_queue::priority_queue::iterators::IntoSortedIter;
use proto::common::ChunkServer;
use std::{collections::HashSet, fmt::Debug, sync::Mutex};

use super::error::ChunkAllocationError;
use super::server_priority::{ComparableChunkServer, ServerPriorityQueue};

/// Manages chunk servers and their locations.
#[derive(Debug, Default)]
pub struct ChunkServerManager {
    server_priority: Mutex<ServerPriorityQueue>,
    chunk_locations: ConcurrentHashMap<String, HashSet<ComparableChunkServer>>,
}

impl ChunkServerManager {
    pub fn server_iter(&self) -> IntoSortedIter<ComparableChunkServer, u64> {
        let servers = self.server_priority.lock().unwrap();
        servers.clone().into_iter()
    }
    /// Registers a chunk server with the manager.
    /// If the chunk server is already registered, this is a no-op.
    /// Otherwise, the chunk server is added to the priority queue.
    pub fn register_chunk_server(&self, chunk_server: ChunkServer) {
        let mut server_priority = self.server_priority.lock().unwrap();

        // Check if chunk server is already registered.
        if server_priority.contains(&chunk_server) {
            return;
        }

        // Register chunk server and add to priority list.
        let priority = chunk_server.available_space_mb;
        server_priority.push(chunk_server.into(), priority);
    }

    /// Allocates chunk servers for a new chunk.
    /// Returns an error if the chunk already exists or if there are no available chunk servers.
    /// Otherwise, returns a set of chunk servers that the chunk should be replicated on.
    /// The number of chunk servers returned is UP to the requested server count, but will never be 0 (error if none).
    pub fn allocate_chunk_servers(
        &self,
        chunk_handle: &String,
        requested_server_count: usize,
    ) -> Result<HashSet<ComparableChunkServer>, ChunkAllocationError> {
        if self.chunk_locations.contains_key(chunk_handle) {
            return Err(ChunkAllocationError::AlreadyExists);
        }

        let mut chunk_locs: HashSet<ComparableChunkServer> = HashSet::new();

        let mut servers = self.server_priority.lock().unwrap();
        let available = servers.pop_n(requested_server_count);

        if available.len() == 0 {
            return Err(ChunkAllocationError::NoAvailableChunkServers);
        }

        for (server, _) in available.iter() {
            let mut modified_server = server.clone();
            modified_server.borrow_inner_mut().available_space_mb -= 64;
            servers.push(
                modified_server.to_owned(),
                modified_server.borrow_inner().available_space_mb,
            );
            chunk_locs.insert(modified_server);
        }

        self.chunk_locations
            .insert(chunk_handle.clone(), chunk_locs.clone());

        Ok(chunk_locs)
    }
}

#[cfg(test)]
mod chunk_server_manager_test {
    use super::*;
    use proto::common::*;

    macro_rules! test_allocate_chunk_servers {
        (
          $name:ident,
          $chunk_handle:expr,
          available_servers:$servers:expr,
          requested_server_count:$requested_server_count:expr,
          expected_alloc_count:$alloc_count:expr,
          server_priority_by_space:$server_priority_by_space:expr) => {
            test_allocate_chunk_servers!(
                $name,
                $chunk_handle,
                available_servers:$servers,
                requested_server_count:$requested_server_count,
                expected_error:false,
                expected_alloc_count:$alloc_count,
                server_priority_by_space:$server_priority_by_space
            );
        };
        (
          $name:ident,
          $chunk_handle:expr,
          available_servers:$servers:expr,
          requested_server_count:$requested_server_count:expr,
          expected_error:$expected_error:expr,
          expected_alloc_count:$alloc_count:expr,
          server_priority_by_space:$server_priority_by_space:expr) => {
            #[test]
            fn $name() {
                let chunk_server_manager = ChunkServerManager {
                    chunk_locations: ConcurrentHashMap::default(),
                    server_priority: Mutex::new($servers.into()),
                };
                let chunk_handle = String::from($chunk_handle);

                // Allocate new chunk handle.
                let chunk_locations = chunk_server_manager
                    .allocate_chunk_servers(&chunk_handle, $requested_server_count);
                assert_eq!(chunk_locations.is_err(), $expected_error);
                if $expected_error {
                    return;
                }
                assert_eq!(chunk_locations.unwrap().len(), $alloc_count);
                assert_eq!(chunk_server_manager.chunk_locations.len(), 1);

                // Attempting to allocate existing chunk handle should return an error.
                let chunk_locations = chunk_server_manager
                    .allocate_chunk_servers(&chunk_handle, 3);
                assert!(chunk_locations.is_err());

                // Compare top N server priorities.
                let mut server_priority = chunk_server_manager.server_priority.lock().unwrap();
                println!("{:?}", server_priority);
                for i in 0..$server_priority_by_space.len() {
                    let server = server_priority.pop();
                    assert!(server.is_some());
                    assert_eq!(server.unwrap().1, $server_priority_by_space[i]);
                }
            }
        };
    }

    test_allocate_chunk_servers!(
        alloc_chunk_but_no_servers,
        "chunk_handle",
        available_servers: Vec::<ChunkServer>::new(),
        requested_server_count: 3,
        expected_error: true,
        expected_alloc_count: 0,
        server_priority_by_space: Vec::<u64>::new()
    );

    test_allocate_chunk_servers!(
        alloc_3_servers_but_only_1_available,
        "chunk_handle",
        available_servers: vec![
            ChunkServer {
                available_space_mb: 164,
                location: Some(ChunkServerLocation {
                    address: "127.0.0.1".into(),
                    port: 1,
                }),
                ..Default::default()
            }
        ],
        requested_server_count: 3,
        expected_alloc_count: 1,
        server_priority_by_space: vec![100]
    );

    test_allocate_chunk_servers!(
        allocate_3_chunks,
        "chunk_handle",
        available_servers: vec![
           ChunkServer {
                available_space_mb: 164,
                location: Some(ChunkServerLocation {
                    address: "127.0.0.1".into(),
                    port: 1,
                }),
                ..Default::default()
            },
            ChunkServer {
                available_space_mb: 80,
                location: Some(ChunkServerLocation {
                    address: "127.0.0.1".into(),
                    port: 2,
                }),
                ..Default::default()
            },
            ChunkServer {
                available_space_mb: 64,
                location: Some(ChunkServerLocation {
                    address: "127.0.0.1".into(),
                    port: 3,
                }),
                ..Default::default()
            }
        ],
        requested_server_count: 3,
        expected_alloc_count: 3,
        server_priority_by_space: vec![100, 16, 0]
    );

    #[test]
    fn test_register_chunk_server() {
        let chunk_server_manager = ChunkServerManager::default();
        chunk_server_manager.register_chunk_server(ChunkServer {
            location: Some(ChunkServerLocation {
                address: "127.0.0.1".into(),
                port: 1,
            }),
            available_space_mb: 100,
        });

        assert!(chunk_server_manager.server_priority.lock().unwrap().len() == 1);

        chunk_server_manager.register_chunk_server(ChunkServer {
            location: Some(ChunkServerLocation {
                address: "127.0.0.1".into(),
                port: 2,
            }),
            available_space_mb: 164,
        });

        let server_priority = chunk_server_manager.server_priority.lock().unwrap();

        assert!(server_priority.len() == 2);
        let top_chunk = server_priority.peek().unwrap();
        assert_eq!(*top_chunk.clone().1, 164);
    }
}
