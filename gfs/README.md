# Rust Implementation of GFS
A learning project with the end goal of a fully functioning implementation of GFS.

## Resources 
- [Original paper](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf)
- [GFS2.0](https://www.scs.stanford.edu/20sp-cs244b/projects/GFS%202_0.pdf)

## How to run
At the moment, only `OPEN` works.

1. Run the master
```bash
cargo run --bin master_server
```

2. Run chunk servers
```bash
cargo run --bin chunk_server -- --bind=127.0.0.1:50052 --master=http://[::1]:50051 --id=1 &
cargo run --bin chunk_server -- --bind=127.0.0.1:50053 --master=http://[::1]:50051 --id=2 &
cargo run --bin chunk_server -- --bind=127.0.0.1:50054 --master=http://[::1]:50051 --id=3 &
cargo run --bin chunk_server -- --bind=127.0.0.1:50054 --master=http://[::1]:50051 --id=4 &
```

3. Run client
```bash
cargo run --bin cli -- open --filename abc.txt
```

