const PROTOS: &[&str] = &[
    "protobuf/metadata.proto",
    "protobuf/common/empty.proto",
    "protobuf/common/chunk_server.proto",
    "protobuf/grpc/master_server_service.proto",
    "protobuf/grpc/chunk_server_service.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(PROTOS, &["protobuf/"])?;
    Ok(())
}
