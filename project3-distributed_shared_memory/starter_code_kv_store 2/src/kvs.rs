extern crate serde_json;

use crate::network::*; // Assuming network.rs is in the same crate
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::ops::DerefMut;
use std::sync::{mpsc, Arc, Mutex, RwLock};

pub type Pid = u32;
pub type ShardId = u32;
pub type Message = (Pid, Operation);

/* Use this value to enable/disable caching in the Key-Value Store */
pub const USE_CACHE: bool = true;

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
pub enum InvType {
    ToInv,
    ToShared
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    // client -> server
    Get(String, usize),
    Put(String, String, usize),
    Delete(String, usize),

    // server -> client
    GetRes(Option<String>, usize),
    PutRes(Option<String>, usize),
    DeleteRes(Option<String>, usize),

    // client -> controller
    GetShardInfo(Option<ShardId>),

    // controller -> client
    GetShardInfoRes(ShardInfo),

    // controller -> server
    PutShardInfo(ShardInfo),

    // server -> controller
    PutShardInfoRes(),

    // server (kvs_handler) -> server (kvs)
    ShmemGet(Pid, String, usize),              // client pid + key + psn
    ShmemPut(Pid, String, String, usize),      // client pid + key + value + psn
    ShmemDelete(Pid, String, usize),            // client pid + key + psn

    // server (kvs) -> server (kvs_handler)
    ShmemGetRes(Pid, String, Option<String>, usize),   // client pid + key + value + psn
    ShmemPutRes(Pid, String, Option<String>, String, usize),   // client pid + key + old value + new value + psn
    ShmemDeleteRes(Pid, String, Option<String>, usize), // client pid + key + old value + psn

    // Invalidate message
    // server (kvs) -> server (kvs_handler)
    ShmemInv(String, InvType),                   // key
    // server (kvs_handler) -> server (kvs)
    ShmemInvRes(Option<String>),                // cached value


    // test -> server
    Snapshot(), // none value indicate delete
    SnapshotRes(KVSSnapshot), // old value

                // TODO: add operations for cache maintainance, like invalidation
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub enum Perm {
    Shared,
    Exclusive,
}

#[derive(Clone, Debug)]
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
    assert!(num_shards > 0);

    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash_value = hasher.finish();
    ((hash_value as usize) % num_shards).try_into().unwrap()
}

pub struct KVSHandle {
    pub shard_info: ShardInfo,
    pub shmem_res_rx: Arc<Mutex<mpsc::Receiver<Message>>>,
    pub ctx: Arc<Mutex<NetworkContext>>,
    pub cache: HashMap<String, (Perm, Option<String>)>,
    pub pending_ops: HashMap<String, VecDeque<(Pid, Operation)>> // (Key, Waiting Operations)
}

impl KVSHandle {
    pub fn new(
        shard_info: ShardInfo,
        shmem_res_rx: Arc<Mutex<mpsc::Receiver<Message>>>,
        ctx: Arc<Mutex<NetworkContext>>,
    ) -> Self {
        KVSHandle {
            shard_info: shard_info,
            shmem_res_rx: shmem_res_rx,
            ctx: ctx,
            cache: HashMap::new(),
            pending_ops: HashMap::new()
        }
    }

    pub fn get_cache_snapshot(&self) -> HashMap<String, (Perm, Option<String>)> {
        self.cache.clone()
    }

    fn run_popped_operation(&mut self, client_pid: Pid, operation: Operation) -> Option<String>{
        match operation {
            Operation::Get(key, psn) => {
                self.get(&key, client_pid, psn, true)
            },
            Operation::Put(key, value, psn) => {
                self.put(key, value, client_pid, psn, true)
            },
            Operation::Delete(key, psn) => {
                self.delete(&key, client_pid, psn, true)
            },
            _ => {
                println!("Unreachable hand");
                return None;
            }
        }
    }

    pub fn shmem_put_res(&mut self, key: &String, old_value: Option<String>, new_value: String, client_pid: Pid, psn: usize) {
        let _res = send(
            &mut self.ctx.lock().unwrap(),
            &client_pid, 
            &Operation::PutRes(old_value.clone(), psn)
        );

        // println!("*** Before PUT Cache = {:?}", self.cache);
        // Update cache
        if let Some(val) = self.cache.get_mut(key) {
            val.0 = Perm::Exclusive;
            val.1 = Some(new_value);
        }
        else {
            self.cache.insert(key.clone(), (Perm::Exclusive, Some(new_value)));
        }
        // println!("*** After PUT Cache = {:?}", self.cache);

        loop {
            if let Some(ops) = self.pending_ops.get_mut(key) {
                if ops.len() == 0 {
                    self.pending_ops.remove(key);
                    break;
                }
                else {
                    // Pop element
                    let (client_pid, op) = ops.pop_front().unwrap();
    
                    if self.run_popped_operation(client_pid, op).is_none() {
                        break;
                    }
                }
            }
            else {
                println!("Should not reach here because a result has come");
                break;
            }
        }
    }   

    pub fn shmem_get_res(&mut self, key: &String, value: Option<String>, client_pid: Pid, psn: usize) {
        let _res = send(
            &mut self.ctx.lock().unwrap(),
            &client_pid, 
            &Operation::GetRes(value.clone(), psn)
        );

        // println!("*** Before GET Cache = {:?}", self.cache);
        // Update cache
        if let Some(val) = self.cache.get_mut(key) {
            val.1 = value.clone();
        }
        else {
            self.cache.insert(key.clone(), (Perm::Shared, value.clone()));
        }
        // println!("*** After GET Cache = {:?}", self.cache);


        loop {
            if let Some(ops) = self.pending_ops.get_mut(key) {
                if ops.len() == 0 {
                    self.pending_ops.remove(key);
                    break;
                }
                else {
                    // Pop element
                    let (client_pid, op) = ops.pop_front().unwrap();
    
                    if self.run_popped_operation(client_pid, op).is_none() {
                        break;
                    }
                }
            }
            else {
                println!("Should not reach here because a result has come");
                break;
            }
        }
    }

    pub fn shmem_delete_res(&mut self, key: &String, value: Option<String>, client_pid: Pid, psn: usize) {
        let _res = send(
            &mut self.ctx.lock().unwrap(),
            &client_pid, 
            &Operation::DeleteRes(value, psn)
        );

        // Update cache
        if let Some(val) = self.cache.get_mut(key) {
            val.0 = Perm::Exclusive;
            val.1 = None;
        }
        else {
            self.cache.insert(key.clone(), (Perm::Exclusive, None));
        }

        loop {
            if let Some(ops) = self.pending_ops.get_mut(key) {
                if ops.len() == 0 {
                    self.pending_ops.remove(key);
                    break;
                }
                else {
                    // Pop element
                    let (client_pid, op) = ops.pop_front().unwrap();
    
                    if self.run_popped_operation(client_pid, op).is_none() {
                        break;
                    }
                }
            }
            else {
                println!("Should not reach here because a result has come");
                break;
            }
        }
    }

    pub fn invalid_cache(&mut self, key: String, inv_type:InvType, home_pid: Pid) -> Option<String>{

        let mut result: Option<String> = None;

        // println!("Got INV message. Cache = {:?}", self.cache);


        match inv_type {
            InvType::ToInv => {
                let mut must_remove = false;

                if let Some((_perm, value)) = self.cache.get(&key) {
                    must_remove = true;
                    result = value.clone();
                }

                if must_remove {
                    let removed = self.cache.remove(&key);
                    // println!("Removing cache -> {:?}, cache={:?}", removed, self.cache);


                }
            },
            InvType::ToShared => {
                if let Some(pair) = self.cache.get_mut(&key) {
                    pair.0 = Perm::Shared;
                    result = pair.1.clone();
                }

            },
        };

        // println!("Got INV message. Returning {:?}", result.clone());

        let _res = send(
            &mut self.ctx.lock().unwrap(),
            &home_pid, 
            &Operation::ShmemInvRes(result.clone())
        );

        return None;        
    }

    pub fn put_no_cache(&mut self, key: String, value: String, client_pid: Pid, psn: usize) {
        let shard_id = get_shard_id_from_key(&key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;
        
        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemPut(client_pid, key.clone(), value.clone(), psn),
        ) {
            println!("Failed to send shmem put: {:?}", e);
        }

        let (_pid, operation) =  self.shmem_res_rx.lock().unwrap().recv().unwrap();
        match operation {
            Operation::ShmemPutRes(_pid, _key, old_value, _value, psn) => {
                let _res = send(
                    self.ctx.lock().unwrap().deref_mut(), 
                    &client_pid, 
                    &Operation::PutRes(old_value, psn)
                );
            },
            _ => {
                println!("NOT REACHING HERE");
            }
        }
    }

    pub fn put(&mut self, key: String, value: String, client_pid: Pid, psn: usize, bypass_pending_check: bool) -> Option<String> {
        if !USE_CACHE {
            self.put_no_cache(key, value, client_pid, psn);
            return None;
        }
        
        if !bypass_pending_check && self.pending_ops.contains_key(&key) {
            let ops = self.pending_ops.get_mut(&key).unwrap();
            ops.push_back((client_pid, Operation::Put(key, value, psn)));
            return None;
        }

        if self.cache.contains_key(&key) && self.cache.get(&key).unwrap().0 == Perm::Exclusive {
            // TODO: write to cache and return
            let cache_line = self.cache.get_mut(&key).unwrap();

            let old_value = cache_line.1.clone();

            cache_line.1 = Some(value.clone());

            // println!("Cache update to {:?}", value.clone());
            
            let _ = send(
                self.ctx.lock().unwrap().deref_mut(), 
                &client_pid, 
                &Operation::PutRes(old_value, psn)
            );
            return Some("Query Answered".to_string());

        }

        let shard_id = get_shard_id_from_key(&key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;

        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemPut(client_pid, key.clone(), value.clone(), psn),
        ) {
            println!("Failed to send shmem put: {:?}", e);
            return None;
        }

        if !bypass_pending_check {
            self.pending_ops.insert(key.to_string(), VecDeque::new());
        }

        return None;
        
    }

    pub fn get_no_cache(&mut self, key: &str, client_pid: Pid, psn: usize) {
        let shard_id = get_shard_id_from_key(&key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;
        
        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemGet(client_pid, key.to_string(), psn),
        ) {
            println!("Failed to send shmem put: {:?}", e);
        }

        let (_pid, operation) =  self.shmem_res_rx.lock().unwrap().recv().unwrap();
        match operation {
            Operation::ShmemGetRes(_pid, _key, value, psn) => {
                let _res = send(
                    self.ctx.lock().unwrap().deref_mut(), 
                    &client_pid, 
                    &Operation::GetRes(value, psn)
                );
            },
            _ => {
                println!("NOT REACHING HERE");
            }
        }
    }

    pub fn get(&mut self, key: &str, client_pid: Pid, psn: usize, bypass_pending_check: bool) -> Option<String> {
        if !USE_CACHE {
            self.get_no_cache(key, client_pid, psn);
            return None;
        }

        if !bypass_pending_check && self.pending_ops.contains_key(key) {
            let ops = self.pending_ops.get_mut(key).unwrap();
            ops.push_back((client_pid, Operation::Get(key.to_string(), psn)));
            return None;
        }

        if self.cache.contains_key(key) {
            // TODO: read from cache and return
            let value = self.cache.get(key).unwrap().1.clone();
            let _ = send(
                self.ctx.lock().unwrap().deref_mut(), 
                &client_pid, 
                &Operation::GetRes(value.clone(), psn)
            );

            return Some("Query Answered".to_string());
        }

        let shard_id = get_shard_id_from_key(key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;

        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemGet(client_pid, key.to_string(), psn),
        ) {
            println!("Failed to send shmem get: {:?}", e);
            return None;
        } else {

            if !bypass_pending_check {
                self.pending_ops.insert(key.to_string(), VecDeque::new());
            }
            
            return None;
        }
    }

    pub fn delete_no_cache(&mut self, key: &str, client_pid: Pid, psn: usize) {
        let shard_id = get_shard_id_from_key(&key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;
        
        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemDelete(client_pid, key.to_string(), psn),
        ) {
            println!("Failed to send shmem put: {:?}", e);
        }

        let (_pid, operation) =  self.shmem_res_rx.lock().unwrap().recv().unwrap();
        match operation {
            Operation::ShmemDeleteRes(_pid, _key, old_value, psn) => {
                let _res = send(
                    self.ctx.lock().unwrap().deref_mut(), 
                    &client_pid, 
                    &Operation::DeleteRes(old_value, psn)
                );
            },
            _ => {
                println!("NOT REACHING HERE");
            }
        }
    }

    pub fn delete(&mut self, key: &str, client_pid: Pid, psn: usize, bypass_pending_check: bool) -> Option<String> {
        if !USE_CACHE {
            self.delete_no_cache(key, client_pid, psn);
            return None;
        }

        if !bypass_pending_check && self.pending_ops.contains_key(key) {
            let ops = self.pending_ops.get_mut(key).unwrap();
            ops.push_back((client_pid, Operation::Delete(key.to_string(), psn)));
            return None;
        }

        if self.cache.contains_key(key) && self.cache.get(key).unwrap().0 == Perm::Exclusive {
            // TODO: delete from cache and return
            let value = self.cache.get_mut(key).unwrap();
            
            let old_value = value.1.clone();

            value.1 = None;

            let _ = send(
                self.ctx.lock().unwrap().deref_mut(), 
                &client_pid, 
                &Operation::DeleteRes(old_value, psn)
            );

            return Some("Query Answered".to_string());
        }

        let shard_id = get_shard_id_from_key(key, self.shard_info.locations.len());
        let home = self.shard_info.locations[&shard_id].primary;

        if let Err(e) = send(
            self.ctx.lock().unwrap().deref_mut(),
            &home,
            &Operation::ShmemDelete(client_pid, key.to_string(), psn),
        ) {
            println!("Failed to send shmem delete: {:?}", e);
            return None;
        }

        else {
            if !bypass_pending_check {
                self.pending_ops.insert(key.to_string(), VecDeque::new());
            }

            return None;
        }

        
    }
}

#[derive(Clone)]
pub struct KVS {
    table: Arc<RwLock<HashMap<String, String>>>,
    directory: Arc<RwLock<HashMap<String, HashSet<(Perm, Pid)>>>>, // directory
    dir_res_rx: Arc<Mutex<mpsc::Receiver<Message>>>, //data
}

impl KVS {
    pub fn new(dir_res_rx: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        KVS {
            table: Arc::new(RwLock::new(HashMap::new())),
            directory: Arc::new(RwLock::new(HashMap::new())),
            dir_res_rx: dir_res_rx
        }
    }
    pub fn put_no_cache(&self, key: String, value: String, requestor_pid: Pid, client_pid: Pid, 
                            psn: usize, ctx: &mut NetworkContext) {
        // record the value in table
        let mut table = self.table.write().unwrap();
        let old_value = table.insert(key.clone(), value.clone());

        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemPutRes(client_pid, key.clone(), old_value.clone(), value.clone(), psn)
        );
    }
    
    pub fn put(&self, key: String, new_value: String,
                requestor_pid: Pid, client_pid: Pid, psn: usize,
                ctx: &mut NetworkContext) -> Option<String> 
    {
        if !USE_CACHE {
            self.put_no_cache(key, new_value, requestor_pid, client_pid, psn, ctx);
            return None;
        }

        // TODO:
        // invalidate any cache copies
        let mut directory = self.directory.write().unwrap();

        if !directory.contains_key(&key) {
            directory.insert(key.clone(), HashSet::new());
        }

        // println!("PUT, directory = {:?}", directory);

        let mut key_directory = directory.get_mut(&key).unwrap();


        let mut old_value: Option<String> = None;

        let mut counter = 0;

        for (perm, pid) in key_directory.iter() {
            // send invalid message to pids
            if pid.eq(&requestor_pid) {
                continue;
            }

            if perm.eq(&Perm::Shared) {
                if let Err(_) = send(
                    ctx, 
                    pid, 
                    &Operation::ShmemInv(key.clone(), InvType::ToInv)
                ) {
                    println!("Error sending invalid message 1");
                }

                // println!("Sending to shared pid={}", pid);

                counter += 1;
            }
            else if perm.eq(&Perm::Exclusive){

                if let Err(_) = send(
                    ctx, 
                    pid, 
                    &Operation::ShmemInv(key.clone(), InvType::ToInv)
                ) {
                    println!("Error sending invalid message 2");
                }
                else {
                    //  TODO: Wait for InvRes response and get value
                    // println!("Sending to exclusive pid={}, requestor={}", pid, requestor_pid);

                    // let mut pid2;
                    // let mut operation;

                    let (pid2, operation) = self.dir_res_rx.lock().unwrap().recv().unwrap();
                    // loop {
                    //     if pid2.eq(pid) {
                    //         break;
                    //     }
                    //     else {
                    //         println!(":(");
                    //     }
                    // }


                    match operation {
                        Operation::ShmemInvRes(value) => {
                            // println!("pid={}, received from exclusive = {:?}", pid2, value.clone());
                            old_value = value.clone();
                        },
                        _ => {
                            println!("Wait WHAT??");
                        }
                    }
                }
            }
            else {
                println!("Should not reach here!!!");
            }
        }

        while counter > 0 {
            let (_pid, operation) = self.dir_res_rx.lock().unwrap().recv().unwrap();
            match operation {
                Operation::ShmemInvRes(value) => {
                    // println!("received from shared = {:?}", value.clone());
                    old_value = value.clone();
                    counter -= 1;
                },
                _ => {
                    println!("Wait WHAT??");
                }
            }
        }
        
        // Send old_value and ack to the source
        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemPutRes(client_pid, key.clone(), old_value.clone(), new_value.clone(), psn),
        );

        // println!("Res: old={:?}, new={:?}", old_value.clone(), new_value.clone());

        // record that the requestor have the exclusive copy
        key_directory.clear();
        key_directory.insert((Perm::Exclusive, requestor_pid));

        // record the value in table
        let mut table = self.table.write().unwrap();
        table.insert(key, new_value.clone())
    }

    pub fn get_no_cache(&self, key: &str, requestor_pid: Pid, client_pid: Pid, psn: usize, ctx: &mut NetworkContext) {
        // Get the value from table
        let table = self.table.read().unwrap();
        let value = table.get(key.clone());

        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemGetRes(client_pid, key.to_string(), value.cloned(), psn)
        );
    }

    pub fn get(&self, key: &str, requestor_pid: Pid, client_pid: Pid, psn: usize, ctx: &mut NetworkContext) -> Option<String> {
        if !USE_CACHE {
            self.get_no_cache(key, requestor_pid, client_pid, psn, ctx);
            return None;
        }

        // TODO:
        // invalidate any exclusive copies
        let mut directory = self.directory.write().unwrap();

        // println!("GET, directory = {:?}", directory);

        let mut to_remove: Vec<(Perm, u32)> = vec![];

        let mut value_final:Option<String> = None;
        
        if let Some(k) = directory.get_mut(key.clone()) {
            for (perm, pid) in k.iter() {

                // send invalid message to pids
                if pid.eq(&requestor_pid) {
                    to_remove.push((perm.clone(), pid.clone()));
                    continue;
                }
                if perm.eq(&Perm::Shared) {
                    continue;
                }

                if let Err(_) = send(
                    ctx, 
                    pid, 
                    &Operation::ShmemInv(key.to_string(), InvType::ToShared)
                ) {
                    println!("Error sending invalid message");
                }

                let (_pid, operation) = self.dir_res_rx.lock().unwrap().recv().unwrap();
                match operation {
                    Operation::ShmemInvRes(val) => {
                        value_final = val.clone();
                        // println!("Got this value in GET processing: {:?}", value_final);


                        if let Some(value) = val {
                            self.table.write().unwrap().insert(key.to_string(), value);
                        }
                        else {
                            self.table.write().unwrap().remove(key);
                        }
                    },
                    _ => {
                        println!("Wait WHAT?? 2");
                    }
                }
            }

            if to_remove.len() > 1 {
                println!("Weird Thing!!! More that one exclusive");
            }
            else if to_remove.len() > 0 {
                for pair in to_remove {
                    k.remove(&pair);
                    k.insert((Perm::Shared, pair.1));
                }
            }
            // record that the requestor have a shared copy
            k.insert((Perm::Shared, requestor_pid));

        }
        else {
            // record that the requestor have a shared copy
            let mut set = HashSet::new();
            set.insert((Perm::Shared, requestor_pid));

            directory.insert(key.to_string(), set);

        }


        let table = self.table.read().unwrap();
        let result = table.get(key).cloned();

        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemGetRes(client_pid, key.to_string(), result.clone(), psn)
        );

        // println!("Res={:?}", result);

        result.clone()
    }

    pub fn delete_no_cache(&self, key: &str, requestor_pid: Pid, client_pid: Pid, psn: usize, ctx: &mut NetworkContext) {
        // remove the value in table
        let mut table = self.table.write().unwrap();
        let old_value = table.remove(key.clone());

        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemDeleteRes(client_pid, key.to_string(), old_value.clone(), psn)
        );
    }

    pub fn delete(&self, key: &str, requestor_pid: Pid, client_pid: Pid, psn: usize, ctx: &mut NetworkContext) -> Option<String> {
        if !USE_CACHE {
            self.delete_no_cache(key, requestor_pid, client_pid, psn, ctx);
            return None;
        }
        // TODO:
        // invalidate any cache copies

        // println!("Received DELETE!!!");



        let mut directory = self.directory.write().unwrap();

        let mut old_value: Option<String> = None;

        if let Some(k) = directory.get(key) {

            let mut counter = 0;

            for (perm, pid) in k {
                // send invalid message to pids
                if pid.eq(&requestor_pid) {
                    continue;
                }

                if perm.eq(&Perm::Shared) {
                    if let Err(_) = send(
                        ctx, 
                        pid, 
                        &Operation::ShmemInv(key.to_string(), InvType::ToInv)
                    ) {
                        println!("Error sending invalid message 3");
                    }

                    counter += 1;
                }
                else if perm.eq(&Perm::Exclusive){

                    if let Err(_) = send(
                        ctx, 
                        pid, 
                        &Operation::ShmemInv(key.to_string(), InvType::ToInv)
                    ) {
                        println!("Error sending invalid message 4");
                    }
                    else {
                        //  TODO: Wait for InvRes response and get value
                        let (_pid, operation) = self.dir_res_rx.lock().unwrap().recv().unwrap();

                        match operation {
                            Operation::ShmemInvRes(value) => {
                                old_value = value.clone();
                            },
                            _ => {
                                println!("Wait WHAT?? 5");
                            }
                        }
                    }
                }
                else {
                    println!("Should not reach here!!!");
                }
            }

            while counter > 0 {
                let (_pid, operation) = self.dir_res_rx.lock().unwrap().recv().unwrap();
                match operation {
                    Operation::ShmemInvRes(value) => {
                        old_value = value.clone();
                        counter -= 1;
                    },
                    _ => {
                        println!("Wait WHAT??");
                    }
                }
            }
        }

        // Send old_value and ack to the source
        let _res = send(
            ctx,
            &requestor_pid,
            &Operation::ShmemDeleteRes(client_pid, key.to_string(), old_value, psn),
        );

        // record that the requestor have the exclusive copy
        directory.remove(key);
        let mut new_set: HashSet<(Perm, u32)> = HashSet::new();
        new_set.insert((Perm::Exclusive, requestor_pid));
        directory.insert(key.to_string(), new_set);

        // record the value in table
        let mut table = self.table.write().unwrap();
        table.remove(key)


    }

    pub fn inner_table(&self) -> HashMap<String, String> {
        let locked_table = self.table.read().unwrap();
        locked_table.clone()
    }
}
