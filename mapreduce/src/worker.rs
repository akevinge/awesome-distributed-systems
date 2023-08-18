use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufReader, Write},
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs},
};

use capnp::{
    message::{ReaderOptions, TypedBuilder},
    serialize_packed,
};
use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};

use crate::{
    kv,
    proto::mapreduce_capnp::{self as proto},
    utils,
};

pub trait MapFn<'a> = Fn(&str, &str) -> Vec<kv::KeyValue>;
pub trait ReduceFn = Fn(&str, &[&str]) -> String;

pub struct Worker<'a, T, K>
where
    T: MapFn<'a>,
    K: ReduceFn,
{
    mapf: T,
    reducef: K,
    rpc_system: RpcSystem<rpc_twoparty_capnp::Side>,
    marker: PhantomData<&'a ()>,
}

impl<'a, T, K> Worker<'a, T, K>
where
    T: MapFn<'a>,
    K: ReduceFn,
{
    /// Connect to a coordinator at the given address.
    /// If successful, returned Worker will be connected but not initialized.
    /// Call spawn to start requesting tasks.
    pub async fn connect(
        addr: &SocketAddr,
        mapf: T,
        reducef: K,
    ) -> Result<Worker<'a, T, K>, Box<dyn std::error::Error>> {
        let stream = tokio::net::TcpStream::connect(&addr).await?;
        stream.set_nodelay(true)?;
        let (reader, writer) = tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
        let rpc_network = Box::new(twoparty::VatNetwork::new(
            reader,
            writer,
            capnp_rpc::rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let rpc_system = RpcSystem::new(rpc_network, None);

        Ok(Self {
            mapf,
            reducef,
            rpc_system,
            marker: PhantomData,
        })
    }

    /// Spawn kicks off already connected worker to start requestesting tasks.
    /// This function will block until the worker is shut down.
    pub async fn spawn(mut self, out_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
        tokio::task::LocalSet::new()
            .run_until(async move {
                let coordinator: proto::coordinator::Client =
                    self.rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
                // The spawning of rpc_system need to happen in the same LocalSet,
                // which is it occurs here instead of Worker::connect().
                tokio::task::spawn_local(self.rpc_system.map_err(|e| println!("error: {e:?}")));

                loop {
                    let response = coordinator.assign_task_request().send().promise.await;

                    if response.is_err() {
                        match coordinator.health_check_request().send().promise.await {
                            // Response may have errored but coordiantor is still up.
                            // Coordinator might have no tasks to delegate at the moment.
                            Ok(_) => continue,
                            // If health check fails, coordinator has died and worker should shut down.
                            // TODO: Add retries.
                            Err(_) => {
                                eprintln!("coordinator has died, worker gracefully shutting down");
                                return Ok(());
                            }
                        }
                    }

                    // Map tasks to their respective handler.
                    match response?.get()?.get_response()?.get_task()?.which()? {
                        proto::task::Which::Map(task) => {
                            Worker::<T, K>::handle_map_task(
                                &coordinator,
                                task?,
                                &self.mapf,
                                out_dir,
                            )
                            .await?;
                        }

                        proto::task::Which::Reduce(task) => {
                            Worker::<T, K>::handle_reduce_task(
                                &coordinator,
                                task?,
                                &self.reducef,
                                out_dir,
                            )
                            .await?;
                        }
                    };
                }
            })
            .await
    }

    async fn handle_map_task(
        coordinator: &proto::coordinator::Client,
        task: proto::map_task::Reader<'_>,
        mapf: &T,
        out_dir: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let map_id = task.get_id();
        let filename = task.get_file()?;
        let partition_count = task.get_partitions();

        // Read input file and produce key-value pairs.
        let contents = fs::read_to_string(filename)?;
        let kva: Vec<kv::KeyValue> = (mapf)(filename, &contents); // Explicit typing to force intellisense to work for trait alias Fn.

        // Partition key-value pairs into an intermediate buffer.
        let mut intermediates: HashMap<u64, Vec<&kv::KeyValue>> = HashMap::new();
        for kv in &kva {
            let reduce_id = kv.calc_hash(partition_count as u64);
            intermediates.entry(reduce_id).or_default().push(kv);
        }

        // Write intermediate buffer to disk and
        // build a request to send intermediate disk locations to coordinator.
        let mut finish_task_req = coordinator.finished_task_request();
        let mut req_builder = finish_task_req.get().init_request();
        req_builder.set_id(map_id);
        let mut intermediate_location_builder = req_builder.init_map(intermediates.len() as u32);

        for (i, (reduce_id, kv)) in intermediates.iter().enumerate() {
            // Write intermediate to disk.
            let intermediate_filename = format!("{out_dir}/tmp-{map_id}-{reduce_id}");
            let out_file = utils::create_file_ensured(&intermediate_filename, false)?;
            let intermediates: TypedBuilder<proto::intermediate::Owned> =
                kv::IntermediateKeyValues(kv).into();
            serialize_packed::write_message(out_file, intermediates.borrow_inner())?;

            // Append intermediate location proto to be sent to coordinator.
            let mut location_builder = intermediate_location_builder.reborrow().get(i as u32);
            location_builder.set_intermediate(&intermediate_filename);
            location_builder.set_reduce_id(*reduce_id);
        }

        finish_task_req.send().promise.await?;
        Ok(())
    }

    async fn handle_reduce_task(
        coordinator: &proto::coordinator::Client,
        task: proto::reduce_task::Reader<'_>,
        reducef: &K,
        out_dir: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reduce_id = task.get_id();

        // Read all intermediates from disk to memory.
        let intermediates_reader = task.get_intermediates()?;
        let mut intermediates: Vec<kv::KeyValue> = vec![];
        for intermediate in intermediates_reader.iter() {
            let intermediate = intermediate?;
            let file = File::open(intermediate)?;
            let buf = BufReader::new(file);
            let message_reader = serialize_packed::read_message(buf, ReaderOptions::new())?;
            let intermediate_reader =
                message_reader.get_root::<proto::intermediate::Reader<'_>>()?;

            for entry_reader in intermediate_reader.get_entries()?.iter() {
                let key = entry_reader.get_key()?;
                let value = entry_reader.get_value()?;
                intermediates.push(kv::KeyValue {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
        }

        // Sort intermediates by key.
        intermediates.sort_by(|a, b| a.key.cmp(&b.key));

        // Write reduce output to disk.
        let out_filename = format!("{out_dir}/out-{reduce_id}");
        let mut out_file = utils::create_file_ensured(&out_filename, true)?;
        let mut i = 0;
        while i < intermediates.len() {
            let mut j = i + 1;
            while j < intermediates.len() && intermediates[i].key == intermediates[j].key {
                j += 1;
            }

            let values: Vec<&str> = intermediates[i..j]
                .iter()
                .map(|kv| kv.value.as_str())
                .collect();
            let output: String = (reducef)(&intermediates[i].key, &values);
            writeln!(out_file, "{} {}", intermediates[i].key, output)?;

            i = j;
        }

        let mut finished_task_req = coordinator.finished_task_request();
        let mut req_builder = finished_task_req.get().init_request();
        req_builder.set_id(reduce_id);
        req_builder.set_reduce(());
        finished_task_req.send().promise.await?;

        Ok(())
    }
}

pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err(format!("usage: {} ADDRESS[:PORT]", args[0]).into());
    }

    let addr = args[2]
        .to_socket_addrs()?
        .next()
        .expect("could not parse address");

    Worker::connect(
        &addr,
        |_filename, _contents| Vec::new(),
        |_key, _values| String::new(),
    )
    .await?
    .spawn("")
    .await
}
