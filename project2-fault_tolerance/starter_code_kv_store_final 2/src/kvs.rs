extern crate serde_json;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

pub type Pid = u32;
pub type ShardId = u32;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardLoc {
    pub primary: Pid,
    pub secondaries: Vec<Pid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShardInfo {
    pub locations: HashMap<ShardId, ShardLoc>,
}

impl ShardInfo {
    // Constructor function
    pub fn new() -> Self {
        ShardInfo {
            locations: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    // client -> server
    Get(String, usize), // (key, seq_no); seq_no is the sequence number of the client's request
    Put(String, String, usize), // (key, value, seq_no)
    Delete(String, usize), // (key, value)

    // server -> client
    GetRes(Option<String>, usize), // (value, seq_no); seq_no should be the same seq_no of the corresponding Get
    PutRes(Option<String>, usize), // (old_value, seq_no); old_value is the previous value of the key before the current Put
    DeleteRes(Option<String>, usize), // (old_value, seq_no); old_value is the previous value of the key before the current Delete

    // client -> controller
    GetShardInfo(),

    // controller -> client
    GetShardInfoRes(ShardInfo),

    // controller -> server
    PutShardInfo(ShardInfo),

    // server -> controller
    PutShardInfoRes(),

    // server -> server
    Replicate(String, Option<String>), // (key, value); none value indicate delete
    ReplicateRes(String, Option<String>), // (key, old_value);

    // test -> server
    Snapshot(),
    SnapshotRes(KVSSnapshot),
}

#[derive(Clone)]
pub struct KVSResult {
    pub operation: String,
    pub key: String,
    pub observed_value: String,
    pub new_value: String,
    pub begin_time: u128,
    pub end_time: u128,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVSSnapshot {
    pub primary_shards: HashMap<String, String>,
    pub secondary_shards: HashMap<String, String>,
}

impl KVSSnapshot {
    // Constructor function
    pub fn new() -> Self {
        KVSSnapshot {
            primary_shards: HashMap::new(),
            secondary_shards: HashMap::new(),
        }
    }
}

pub fn get_shard_id_from_key(key: &str, num_shards: usize) -> ShardId {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash_value = hasher.finish();
    ((hash_value as usize) % num_shards).try_into().unwrap()
}

#[derive(Clone)]
pub struct KVS<K, V> {
    table: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> KVS<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: std::hash::Hash + Clone,
{
    pub fn new() -> Self {
        KVS {
            table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let mut table = self.table.write().unwrap();
        table.insert(key, value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let table = self.table.read().unwrap();
        table.get(key).cloned()
    }

    pub fn delete(&self, key: &K) -> Option<V> {
        let mut table = self.table.write().unwrap();
        table.remove(key)
    }

    pub fn inner_table(&self) -> HashMap<K, V> {
        let locked_table = self.table.read().unwrap();
        locked_table.clone()
    }
}
