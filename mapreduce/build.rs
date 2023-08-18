fn main() {
    let mut compile_cmd = capnpc::CompilerCommand::new();

    for entry in glob::glob("src/proto/*.capnp").expect("Failed to read glob pattern") {
        if let Ok(path) = entry {
            compile_cmd.file(path.display().to_string());
            println!("{}", path.display());
        }
    }

    compile_cmd.run().unwrap();
}
