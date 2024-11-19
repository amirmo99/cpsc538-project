use kv_store::kvs::KVS;
use kv_store::kvs::{get_shard_id_from_key, KVSSnapshot, Operation, Pid, ShardInfo};
use kv_store::network::{create_network_context, recv, send, NetworkContext}; // Assuming network.rs is in the same crate
use std::collections::{HashSet, HashMap};
use std::env;
use std::io::{Error, ErrorKind};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

type Message = (Pid, Operation);

fn handle_get(
    _info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS<String, String>,
    key: &String,
    psn: usize,
) -> Result<Operation, Error> {
    let value = kvs.get(key);
    return Ok(Operation::GetRes(value, psn));
}

fn handle_put(
    ctx: &mut NetworkContext,
    info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS<String, String>,
    key: String,
    value: String,
    psn: usize,
    rx: &mpsc::Receiver<Message>,
) -> Result<Operation, Error> {
    // TODO: replication
    // Send Replicate to all secondary servers of the key
    // and wait for ReplicateRes from all of them before
    // sending response to the client
    let old_value = kvs.put(key.clone(), value.clone());

    let shard_info = info.read().unwrap().clone();
    let shard_id = get_shard_id_from_key(&key, shard_info.locations.len());
    
    // Phase 0:
    let secondaries = get_secondaries_by_key(info, &key);

    // Phase 1: Send replicate to all
    for pid in &secondaries {
        let msg = Operation::Replicate(key.clone(), Some(value.clone()));
        let res = send(ctx, pid, &msg);
        match res {
            Ok(_) => {},
            Err(_) => {
                println!("Could not send replicated_put message to secondary");
            },
        }
    }

    return Ok(Operation::PutRes(old_value, psn));
}


fn handle_delete(
    ctx: &mut NetworkContext,
    info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS<String, String>,
    key: &String,
    psn: usize,
    rx: &mpsc::Receiver<Message>,
) -> Result<Operation, Error> {
    // TODO: replication
    // Send Replicate to all secondary servers of the key
    // and wait for ReplicateRes from all of them before
    // sending response to the client
    let old_value = kvs.delete(key);

    let shard_info = info.read().unwrap().clone();
    let shard_id = get_shard_id_from_key(&key, shard_info.locations.len());
    
    // Phase 0:
    let self_pid = shard_info.locations[&shard_id].primary;
    let mut secondaries = shard_info.locations[&shard_id].secondaries.clone();
    secondaries.sort();

    // Phase 1: Send replicate to all
    for pid in &secondaries {
        let msg = Operation::Replicate(key.clone(), None);
        let res = send(ctx, pid, &msg);
        match res {
            Ok(_) => {},
            Err(_) => {
                println!("Could not send replicated_put message to secondary");
            },
        }
    }

    return Ok(Operation::DeleteRes(old_value, psn));
}

fn handle_replicate(
    _info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS<String, String>,
    key: String,
    value: Option<String>,
) -> Result<Operation, Error> {
    let result = match value {
        Some(_) => {
            let old_value = kvs.put(key.clone(), value.unwrap());
            Operation::ReplicateRes(key, old_value)
        }
        None => {
            let old_value = kvs.delete(&key);
            Operation::ReplicateRes(key, old_value)
        }
    };
    Ok(result)
}

fn handle_snapshot(
    info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS<String, String>,
    self_pid: &Pid,
) -> Result<Operation, Error> {
    let mut snapshot = KVSSnapshot::new();
    let shard_info = info.read().unwrap();
    for (k, v) in kvs.inner_table() {
        let shard_id = get_shard_id_from_key(&k, shard_info.locations.len());
        if shard_info.locations[&shard_id].primary == *self_pid {
            snapshot.primary_shards.insert(k.clone(), v.clone());
        } else if shard_info.locations[&shard_id]
            .secondaries
            .contains(self_pid)
        {
            snapshot.secondary_shards.insert(k.clone(), v.clone());
        }
    }
    Ok(Operation::SnapshotRes(snapshot))
}

fn handle_put_shard_info(
    info: &Arc<RwLock<ShardInfo>>,
    new_shard_info: &ShardInfo,
) -> Result<Operation, Error> {
    let mut shard_info = info.write().unwrap();
    *shard_info = new_shard_info.clone();
    return Ok(Operation::PutShardInfoRes());
}

fn get_secondaries_by_key(
    info: &Arc<RwLock<ShardInfo>>,
    key: &String
) -> Vec<Pid>
{
    let shard_info = info.read().unwrap().clone();
    let shard_id = get_shard_id_from_key(&key, shard_info.locations.len());
    let mut secondaries = shard_info.locations[&shard_id].secondaries.clone();
    secondaries.sort();
    secondaries
}

struct KeyInfo {
    client_pid: Pid,
    pending_secondaries: Vec<Pid>,
    delayed_operations: Vec<(Pid, Operation)>,
    response: Operation
}

fn run_server(self_pid: Pid, controller_pid: Pid, client_pids: Vec<Pid>) {
    let kvs: KVS<String, String> = KVS::new();
    let shard_info = Arc::new(RwLock::new(ShardInfo::new()));
    let ctx = create_network_context(&self_pid).unwrap();
    let (tx, rx) = mpsc::channel::<Message>(); // net -> worker

    let arc_tx = Arc::new(tx);
    let network_tx = arc_tx.clone();
    let worker_tx = arc_tx.clone();
    // TODO: implements the worker thread.
    // it should contigously try to retrieve messages from the network thread using rx
    // for each message it retrieves, it should call the corresponding handler to handle it.
    // after the request is handled, it should send a corresponding Res back to the requestor
    // Please read the enum Operation in kvs.rs to understand the content of each operation.
    let mut ctx_worker = ctx.clone();
    let worker = thread::spawn(move || {
        let mut pending_keys: HashMap<String, KeyInfo> = HashMap::new();

        loop {

            let (pid, operation) = rx.recv().unwrap();
            
            // println!("Server {} Received: pid={}, op={:?}", self_pid, pid, operation);

            match operation.clone() {
                Operation::Put(key, value, seq_no) => {
                    if pending_keys.contains_key(&key) {
                        // println!("WARNING! PUT operation delayed...");
                        let _ = worker_tx.send((pid, operation));
                        continue;
                    }

                    let op = handle_put(&mut ctx_worker, &shard_info, &kvs, key.clone(), value, seq_no, &rx).unwrap();
                    
                    let secondaries = get_secondaries_by_key(&shard_info, &key);
                    if secondaries.len() > 0 {
                        pending_keys.insert(key.clone(), KeyInfo { 
                            client_pid: pid, 
                            pending_secondaries: get_secondaries_by_key(&shard_info, &key), 
                            delayed_operations: vec![],
                            response: op
                        });
                    }
                    else {
                        let _ = send(&mut ctx_worker, &pid, &op);
                    }
                },
                Operation::Get(key, seq_no) => {
                    if pending_keys.contains_key(&key) {
                        // println!("WARNING! GET operation delayed...");
                        let _ = worker_tx.send((pid, operation));
                        continue;
                    }

                    let op = handle_get(&shard_info, &kvs, &key, seq_no).unwrap();
                    let _ = send(&mut ctx_worker, &pid, &op);
                },
                Operation::Delete(key, seq_no) => {
                    if pending_keys.contains_key(&key) {
                        // println!("WARNING! DELETE operation delayed...");
                        let _ = worker_tx.send((pid, operation));
                        continue;
                    }

                    let op = handle_delete(&mut ctx_worker, &shard_info, &kvs, &key, seq_no, &rx).unwrap();

                    let secondaries = get_secondaries_by_key(&shard_info, &key);
                    if secondaries.len() > 0 {
                        pending_keys.insert(key.clone(), KeyInfo { 
                            client_pid: pid, 
                            pending_secondaries: get_secondaries_by_key(&shard_info, &key), 
                            delayed_operations: vec![],
                            response: op
                        });
                    }
                    else {
                        let _ = send(&mut ctx_worker, &pid, &op);
                    }
                },

                Operation::PutShardInfo(rcvd_shard_info) => {
                    let op = handle_put_shard_info(&shard_info, &rcvd_shard_info).unwrap();
                    let _ = send(&mut ctx_worker, &pid, &op);
                },

                Operation::Replicate(key, value) => {
                    let op = handle_replicate(&shard_info, &kvs, key, value).unwrap();
                    let _ = send(&mut ctx_worker, &pid, &op);
                },
                Operation::ReplicateRes(key, old_value) => {
                    let key_info = pending_keys.get_mut(&key).unwrap();

                    let found: Result<usize, usize> = key_info.pending_secondaries.binary_search(&pid);
                    if let Ok(id) = found {
                        key_info.pending_secondaries.remove(id);
                    } 
                    else {
                        println!("Unexpected behaviour... secondary pid not found in pending list");
                    }

                    if key_info.pending_secondaries.is_empty() {
                        let _ = send(&mut ctx_worker, &key_info.client_pid, &key_info.response);
                        pending_keys.remove(&key);
                    }
                },

                Operation::Snapshot() => {
                    let op = handle_snapshot(&shard_info, &kvs, &self_pid).unwrap();
                    let _ = send(&mut ctx_worker, &pid, &op);
                },

                // Operation::SnapshotRes(snapshot) => {},

                _op => {
                    let error = format!("Unexpected operation received at server, op={:?}", _op);
                    println!("{}", error);
                }

            };
        }
    });

    // network thread to poll messages and dispatch to worker threads
    let mut poll_pids = client_pids.clone();
    let mut ctx_server = ctx.clone();
    poll_pids.push(controller_pid);
    let network_thread = thread::spawn(move || {
        loop {
            match recv(&mut ctx_server, 1000) {
                Ok(messages) => {
                    for (pid, operation) in messages {
                        network_tx.send((pid, operation)).unwrap();
                    }
                }
                Err(_) => {
                    // println!("Server timed out");
                }
            }
        }
    });

    worker.join().unwrap();
    network_thread.join().unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        eprintln!("Usage: server <self_pid > <controller_pid> <client_pid_list>");
        return;
    }

    let self_pid: Pid = match args[1].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <client_pid_list>");
            return;
        }
    };

    let controller_pid: Pid = match args[2].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <client_pid_list>");
            return;
        }
    };

    let client_pids: Result<Vec<Pid>, _> = args[3..].iter().map(|x| x.parse()).collect();

    let client_pids = match client_pids {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <client_pid_list>");
            return;
        }
    };

    run_server(self_pid, controller_pid, client_pids);
}
