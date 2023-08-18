use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Mutex,
    time::Duration,
};

use capnp::capability::Promise;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::{AsyncReadExt, TryFutureExt};

use crate::{
    proto::mapreduce_capnp as proto,
    state::{self, TaskState},
    utils,
};

pub struct Coordinator {
    /// Defines the number of reduce tasks to be assigned, which
    /// is also the number of partitions used by map tasks.
    n_reduce: u32,
    /// The state of tasks that the coordinator delegates.
    state: Mutex<CoordinatorTaskState>,
    /// Max timeout a worker is expected to finish a task
    /// before it is redelegated.
    task_timeout: Duration,
}

struct CoordinatorTaskState {
    map_tasks: HashMap<u64, state::MapTaskState>,
    reduce_tasks: HashMap<u64, state::ReduceTaskState>,
}

impl CoordinatorTaskState {
    pub fn new(files: Vec<String>, n_reduce: u32) -> Self {
        // Create a map state for each input file.
        let mut map_state = HashMap::new();
        for (i, file) in files.into_iter().enumerate() {
            map_state.insert(i as u64, state::MapTaskState::new(file));
        }

        // Create a reduce state for each partition.
        let mut reduce_state = HashMap::new();
        for i in 0..n_reduce {
            reduce_state.insert(i as u64, state::ReduceTaskState::new());
        }

        Self {
            map_tasks: map_state,
            reduce_tasks: reduce_state,
        }
    }
}

impl Coordinator {
    /// Creates a new coordinator but does not initialize it.
    /// Call spawn to start listening for connections.
    pub fn new(files: Vec<String>, n_reduce: u32, task_timeout: Duration) -> Self {
        Self {
            n_reduce,
            task_timeout,
            state: Mutex::new(CoordinatorTaskState::new(files, n_reduce)),
        }
    }

    /// Spawns the coordinator to listen for connections.
    /// This method will block until the coordinator is terminated.
    pub async fn spawn(self, addr: &SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        tokio::task::LocalSet::new()
            .run_until(async move {
                let listener = tokio::net::TcpListener::bind(&addr).await?;
                let coordinator: proto::coordinator::Client = capnp_rpc::new_client(self);

                loop {
                    let (stream, _) = listener.accept().await?;
                    stream.set_nodelay(true)?;

                    let (reader, writer) =
                        tokio_util::compat::TokioAsyncReadCompatExt::compat(stream).split();
                    let network = twoparty::VatNetwork::new(
                        reader,
                        writer,
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system =
                        RpcSystem::new(Box::new(network), Some(coordinator.clone().client));
                    tokio::task::spawn_local(rpc_system.map_err(|e| println!("error: {e:?}")));
                }
            })
            .await
    }

    pub fn map_tasks_done(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .map_tasks
            .values()
            .all(|map_state| map_state.is_done())
    }

    pub fn reduce_tasks_done(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .reduce_tasks
            .values()
            .all(|reduce_state| reduce_state.is_done())
    }
}

impl proto::coordinator::Server for Coordinator {
    fn health_check(
        &mut self,
        _: proto::coordinator::HealthCheckParams,
        _: proto::coordinator::HealthCheckResults,
    ) -> Promise<(), capnp::Error> {
        // Trick the workers into thinking the coordinator has died when all tasks are done.
        // This is a hack to make the tests pass and assumes all are run on the same thread
        // (i.e. main thread should exit when workers exit).
        if self.map_tasks_done() && self.reduce_tasks_done() {
            return Promise::err(capnp::Error::failed("all tasks are done".into()));
        }

        Promise::ok(())
    }

    fn assign_task(
        &mut self,
        _: proto::coordinator::AssignTaskParams,
        mut reply: proto::coordinator::AssignTaskResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let mut state = self.state.lock().unwrap();

        let should_delegated = |task: &dyn TaskState| {
            // A finished task is never delegated again.
            if task.is_done() {
                return false;
            }

            if !task.is_delegated() {
                return true;
            }

            // A task is delegated again if it is unfinished, was delegated, and it has exceeded the timeout.
            if task.elapsed_time().map_or(false, |t| t > self.task_timeout) {
                return true;
            }

            false
        };

        let mut all_map_tasks_done = true;
        // Iterator over map tasks and assign the first one that is neither done nor delegated.
        for (map_id, map_state) in state.map_tasks.iter_mut() {
            if !map_state.is_done() {
                all_map_tasks_done = false;
            }

            if should_delegated(map_state) {
                let response = reply.get().init_response();
                let mut map_task = response.init_task().init_map();
                map_task.set_partitions(self.n_reduce);
                map_task.set_file(map_state.get_filename());
                map_task.set_id(*map_id);

                map_state.set_delegated();

                return Promise::ok(());
            }
        }

        // With this implementation, all map tasks need to be finished before
        // reduce tasks are delegated.
        // Apparently this can be improved.
        if !all_map_tasks_done {
            return Promise::err(capnp::Error::failed(
                "waiting for all map tasks to finish".into(),
            ));
        }

        // Iterator over reduce tasks and assign the first one that is neither done nor delegated.
        for (reduce_id, reduce_state) in state.reduce_tasks.iter_mut() {
            if should_delegated(reduce_state) {
                let response = reply.get().init_response();
                let mut reduce_task = response.init_task().init_reduce();
                reduce_task.set_id(*reduce_id);
                let mut intermediates =
                    reduce_task.init_intermediates(reduce_state.len_intermediates() as u32);
                for (i, location) in reduce_state.get_intermediates().iter().enumerate() {
                    intermediates.set(i as u32, location);
                }

                reduce_state.set_delegated();

                return Promise::ok(());
            }
        }

        Promise::err(capnp::Error::failed("no tasks to delegate".into()))
    }

    fn finished_task(
        &mut self,
        request: proto::coordinator::FinishedTaskParams,
        _: proto::coordinator::FinishedTaskResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        let task = pry!(pry!(request.get()).get_request());
        match pry!(task.which()) {
            proto::coordinator::finished_task_request::Which::Map(map) => {
                let map_finish_request = pry!(map);
                let map_id = task.get_id();

                let state = &mut self.state.lock().unwrap();
                println!("Map task {} finished.", map_id);

                state
                    .map_tasks
                    .entry(map_id)
                    .and_modify(|map_state| map_state.set_done());

                for intermediate in map_finish_request.iter() {
                    let reduce_id = intermediate.get_reduce_id();
                    let location = pry!(intermediate.get_intermediate()).to_string();
                    state
                        .reduce_tasks
                        .entry(reduce_id)
                        .and_modify(|reduce_state| reduce_state.append_intermediate(location));
                }
            }
            proto::coordinator::finished_task_request::Which::Reduce(_) => {
                let reduce_id = task.get_id();
                let state = &mut self.state.lock().unwrap();
                println!("Reduce task {} finished.", reduce_id);

                state
                    .reduce_tasks
                    .entry(reduce_id)
                    .and_modify(|reduce_state| reduce_state.set_done());
            }
        }

        Promise::ok(())
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

    Coordinator::new(
        utils::get_input_files("artifacts/")?,
        10,
        Duration::from_secs(10),
    )
    .spawn(&addr)
    .await
}
