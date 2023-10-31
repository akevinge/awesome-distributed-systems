use base64ct::{Base64, Encoding};
use proto::{common::ChunkServerLocation, metadata::FileChunkMetadata};
use sha2::{Digest, Sha256};

pub fn get_locations(cs: FileChunkMetadata) -> Vec<ChunkServerLocation> {
    let mut locations = Vec::from(cs.replica_locations);
    locations.push(
        cs.primary_location
            .expect("unreachable: primary location should always exist"),
    );
    locations
}

pub fn calc_checksum(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    Base64::encode_string(&hash).to_string()
}
