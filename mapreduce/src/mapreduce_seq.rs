use std::{fs, io::Write, path::Path};

use crate::{
    kv, utils,
    worker::{MapFn, ReduceFn},
};

pub fn mapreduce_seq<'a, T, K>(
    files: Vec<String>,
    mapf: T,
    reducef: K,
    out_file: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: MapFn<'a>,
    K: ReduceFn,
{
    let mut intermediates = Vec::<kv::KeyValue>::new();
    for file in files {
        let path = Path::new(file.as_str());
        let content = fs::read_to_string(path)?;
        let filename = path.file_name().unwrap().to_str().unwrap();
        let kva: Vec<kv::KeyValue> = mapf(filename, &content);
        intermediates.extend(kva);
    }

    intermediates.sort_by(|a, b| a.key.cmp(&b.key));

    let mut out_file = utils::create_file_ensured(out_file, false)?;
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
        let output: String = reducef(&intermediates[i].key, &values);

        writeln!(out_file, "{} {}", intermediates[i].key, output)?;
        i = j;
    }

    Ok(())
}
