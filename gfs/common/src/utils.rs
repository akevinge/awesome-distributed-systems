use proto::{common::ChunkServerLocation, metadata::FileChunkMetadata};

pub fn get_locations(cs: FileChunkMetadata) -> Vec<ChunkServerLocation> {
    let mut locations = Vec::from(cs.replica_locations);
    locations.push(
        cs.primary_location
            .expect("unreachable: primary location should always exist"),
    );
    locations
}
