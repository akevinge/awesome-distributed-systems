#![feature(trait_alias)]
#![feature(never_type)]
pub mod coordinator;
pub mod kv;
pub mod mapreduce_seq;
mod proto;
mod state;
pub mod utils;
pub mod worker;
