use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

pub use glob;

pub fn verify_output(
    correct_out_path: &str,
    test_out_paths: glob::Paths,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("verifying output...");
    let mut correct_output = process_test_output(correct_out_path)?;

    let mut test_output: Vec<(String, u32)> = vec![];
    for test_out_path in test_out_paths {
        let path_buf = test_out_path?;
        if let Some(path) = path_buf.to_str() {
            test_output.extend_from_slice(&process_test_output(path)?);
        }
    }

    correct_output.sort_by(|a, b| a.0.cmp(&b.0));
    test_output.sort_by(|a, b| a.0.cmp(&b.0));

    assert!(correct_output == test_output);

    Ok(())
}

pub fn process_test_output(path: &str) -> Result<Vec<(String, u32)>, Box<dyn std::error::Error>> {
    let mut output: Vec<(String, u32)> = vec![];
    let path = Path::new(path);
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.len() >= 2 {
            let key = parts[0].to_string();
            let value = parts[1].parse::<u32>().unwrap_or(0);
            output.push((key, value));
        }
    }

    Ok(output)
}
