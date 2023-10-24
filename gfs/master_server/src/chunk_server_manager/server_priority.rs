use std::{fmt::Display, hash::Hash};

use priority_queue::{priority_queue::iterators::IntoSortedIter, PriorityQueue};
use proto::common::{ChunkServer, ChunkServerLocation};

#[derive(Debug, Clone)]
pub struct ComparableChunkServer(ChunkServer);

impl ComparableChunkServer {
    pub fn borrow_inner(&self) -> &ChunkServer {
        &self.0
    }
    pub fn borrow_inner_mut(&mut self) -> &mut ChunkServer {
        &mut self.0
    }
}

impl From<ChunkServer> for ComparableChunkServer {
    fn from(chunk_server: ChunkServer) -> Self {
        ComparableChunkServer(chunk_server)
    }
}

impl Into<ChunkServer> for ComparableChunkServer {
    fn into(self) -> ChunkServer {
        self.0.clone()
    }
}

impl Into<ChunkServerLocation> for ComparableChunkServer {
    fn into(self) -> ChunkServerLocation {
        match self.0.clone().location {
            Some(loc) => loc,
            None => unreachable!("Chunk server location not provided."),
        }
    }
}

impl Eq for ComparableChunkServer {}

impl Hash for ComparableChunkServer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let location = format!("{:?}", self.0.location);
        location.hash(state)
    }
}

impl Display for ComparableChunkServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let loc = match self.0.location.as_ref() {
            Some(location) => location,
            None => unreachable!("Chunk server location not provided."),
        };

        f.write_fmt(format_args!("{}:{}", loc.address, loc.port))
    }
}

impl PartialEq for ComparableChunkServer {
    fn eq(&self, other: &Self) -> bool {
        format!("{}", self) == format!("{}", other)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ServerPriorityQueue {
    server_priority: PriorityQueue<ComparableChunkServer, u64>,
}

impl Into<Vec<ComparableChunkServer>> for ServerPriorityQueue {
    fn into(self) -> Vec<ComparableChunkServer> {
        self.server_priority.into_sorted_vec()
    }
}

impl IntoIterator for ServerPriorityQueue {
    type IntoIter = IntoSortedIter<ComparableChunkServer, u64>;
    type Item = (ComparableChunkServer, u64);

    fn into_iter(self) -> Self::IntoIter {
        self.server_priority.into_sorted_iter()
    }
}

impl ServerPriorityQueue {
    pub fn len(&self) -> usize {
        self.server_priority.len()
    }

    pub fn peek(&self) -> Option<(&ComparableChunkServer, &u64)> {
        self.server_priority.peek()
    }

    pub fn pop(&mut self) -> Option<(ComparableChunkServer, u64)> {
        self.server_priority.pop()
    }

    pub fn contains(&self, chunk_server: &ChunkServer) -> bool {
        self.server_priority
            .get(&ComparableChunkServer(chunk_server.clone()))
            .is_some()
    }

    pub fn push(&mut self, chunk_server: ComparableChunkServer, priority: u64) {
        self.server_priority.push(chunk_server, priority);
    }

    pub fn pop_n(&mut self, n: usize) -> Vec<(ComparableChunkServer, u64)> {
        let mut popped_servers = Vec::new();
        for _ in 0..n {
            if let Some((server, priority)) = self.pop() {
                popped_servers.push((server, priority));
            } else {
                break;
            }
        }
        popped_servers
    }
}

impl From<Vec<ChunkServer>> for ServerPriorityQueue {
    fn from(value: Vec<ChunkServer>) -> Self {
        let mut server_priority = ServerPriorityQueue::default();
        for chunk_server in value {
            let priority = chunk_server.available_space_mb;
            server_priority.push(chunk_server.into(), priority);
        }
        server_priority
    }
}
