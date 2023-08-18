use std::{
    collections::hash_map,
    fmt,
    hash::{Hash, Hasher},
};

use capnp::message::TypedBuilder;

use crate::proto::mapreduce_capnp;

#[derive(Debug, PartialEq, PartialOrd)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl KeyValue {
    pub fn calc_hash(&self, n: u64) -> u64 {
        let mut hasher = hash_map::DefaultHasher::new();
        self.key.hash(&mut hasher);
        (hasher.finish() & 0x7FFFFFFF) % n
    }
}

impl fmt::Display for KeyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.key, self.value)
    }
}

pub struct IntermediateKeyValues<'a>(pub &'a Vec<&'a KeyValue>);

impl<'a> From<IntermediateKeyValues<'a>> for TypedBuilder<mapreduce_capnp::intermediate::Owned> {
    fn from(value: IntermediateKeyValues<'a>) -> Self {
        let mut message = TypedBuilder::<mapreduce_capnp::intermediate::Owned>::new_default();
        let intermediate = message.init_root();
        let mut entries = intermediate.init_entries(value.0.len() as u32);
        for (i, kv) in value.0.iter().enumerate() {
            let mut kv_proto = entries.reborrow().get(i as u32);
            kv_proto.set_key(&kv.key);
            kv_proto.set_value(&kv.value);
        }

        message
    }
}
