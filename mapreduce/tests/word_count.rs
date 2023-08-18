use std::{fs, net::ToSocketAddrs, path::Path, time::Duration};

use mapreduce::{coordinator::Coordinator, kv, mapreduce_seq, utils, worker::Worker};
use test_utils::{self, glob::glob};

#[tokio::test]
async fn word_count() -> Result<(), Box<dyn std::error::Error>> {
    fn mapf(_filename: &str, value: &str) -> Vec<kv::KeyValue> {
        let mut result = Vec::new();
        for word in value.split(|c: char| !c.is_alphabetic()) {
            if word != "" {
                result.push(kv::KeyValue {
                    key: word.to_string(),
                    value: "1".to_string(),
                });
            }
        }
        result
    }

    fn reducef(_key: &str, values: &[&str]) -> String {
        values.len().to_string()
    }

    let out_dir = "out-tmp";
    let correct_outfile = format!("{out_dir}/correct-out.txt");
    let n_reduce: u32 = 10;
    let task_timeout = Duration::from_secs(5);

    let files = utils::get_input_files("artifacts/")?;
    let _ = fs::remove_dir_all(Path::new(out_dir));

    // Run sequential version of "MapReduce".
    mapreduce_seq::mapreduce_seq(files.clone(), mapf, reducef, &correct_outfile)?;

    let addr = "0.0.0.0:8001".to_socket_addrs().unwrap().next().unwrap();
    let spawn_coordinator = move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime
            .block_on(Coordinator::new(files, n_reduce, task_timeout).spawn(&addr))
            .unwrap();
    };
    let spawn_worker = move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime
            .block_on(async move {
                let worker = Worker::connect(&addr, mapf, reducef).await?;
                worker.spawn(&out_dir).await
            })
            .unwrap()
    };

    std::thread::spawn(|| spawn_coordinator());

    // Sleep for a while to make sure coordinator is ready.
    // Not elegant, could be improved.
    std::thread::sleep(Duration::from_secs(2));

    // Spawn same amount of workers as number of reduce tasks.
    // It doesn't need to be 1:1 and can be any number > 0.
    let mut handles = vec![];
    for _ in 0..n_reduce {
        handles.push(std::thread::spawn(move || spawn_worker()));
    }

    // Wait for only the workers.
    for handle in handles {
        let _ = handle.join();
    }

    test_utils::verify_output(
        &correct_outfile,
        glob(format!("{out_dir}/out-*").as_str()).unwrap(),
    )

    // std::thread::scope(|scope| {
    //     let coordinator_thread = scope.spawn(|| spawn_coordinator());

    //     // Sleep for a while to make sure coordinator is ready.
    //     // Not elegant, could be improved.
    //     std::thread::sleep(Duration::from_secs(2));

    //     // Spawn same amount of workers as number of reduce tasks.
    //     // It doesn't need to be 1:1 and can be any number > 0.
    //     for _ in 0..n_reduce {
    //         scope.spawn(|| spawn_worker());
    //     }
    //     panic!("rhig")
    //     // test_utils::verify_output(
    //     //     &correct_outfile,
    //     //     glob(format!("{out_dir}/out-*").as_str()).unwrap(),
    //     // )
    //     // .unwrap()
    // });
}
