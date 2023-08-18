use std::{
    fs::{self, File, OpenOptions},
    path::Path,
};

pub fn create_file_ensured(path: &str, append: bool) -> std::io::Result<File> {
    let path = Path::new(path);
    if !path.parent().is_some_and(|p| p.exists()) {
        fs::create_dir_all(path.parent().unwrap())?;
    }
    OpenOptions::new()
        .write(true)
        .append(append)
        .create(true)
        .open(path)
}

pub fn get_input_files(input_dir: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(input_dir)? {
        let path = entry?.path();
        let file = path.into_os_string().into_string().unwrap();
        files.push(file);
    }
    Ok(files)
}
