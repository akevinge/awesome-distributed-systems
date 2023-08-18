use std::time::{self, Duration};

pub trait TaskState {
    fn is_done(&self) -> bool;
    fn set_done(&mut self);
    fn is_delegated(&self) -> bool;
    fn set_delegated(&mut self);
    fn elapsed_time(&self) -> Option<Duration>;
}

pub struct MapTaskState {
    filename: String,
    done: bool,
    time: Option<time::Instant>,
}

impl MapTaskState {
    pub fn new(filename: String) -> Self {
        Self {
            filename,
            done: false,
            time: None,
        }
    }
    pub fn get_filename(&self) -> &str {
        &self.filename
    }
}

impl TaskState for MapTaskState {
    fn is_done(&self) -> bool {
        self.done
    }

    fn set_done(&mut self) {
        self.done = true;
    }

    fn is_delegated(&self) -> bool {
        self.time.is_some()
    }

    fn set_delegated(&mut self) {
        self.time = Some(time::Instant::now());
    }

    fn elapsed_time(&self) -> Option<Duration> {
        self.time.map(|time| time.elapsed())
    }
}

pub struct ReduceTaskState {
    locations: Vec<String>,
    done: bool,
    time: Option<time::Instant>,
}

impl ReduceTaskState {
    pub fn new() -> Self {
        Self {
            locations: Vec::new(),
            done: false,
            time: None,
        }
    }

    pub fn get_intermediates(&self) -> &Vec<String> {
        &self.locations
    }

    pub fn append_intermediate(&mut self, location: String) {
        self.locations.push(location);
    }

    pub fn len_intermediates(&self) -> usize {
        self.locations.len()
    }
}

impl TaskState for ReduceTaskState {
    fn is_done(&self) -> bool {
        self.done
    }

    fn set_done(&mut self) {
        self.done = true;
    }

    fn is_delegated(&self) -> bool {
        self.time.is_some()
    }

    fn set_delegated(&mut self) {
        self.time = Some(time::Instant::now());
    }

    fn elapsed_time(&self) -> Option<Duration> {
        self.time.map(|time| time.elapsed())
    }
}
