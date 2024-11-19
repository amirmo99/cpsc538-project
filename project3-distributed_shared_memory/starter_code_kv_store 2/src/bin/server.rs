use kv_store::kvs::*;
use kv_store::network::*; // Assuming network.rs is in the same crate
// use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{Error, ErrorKind};
use std::ops::DerefMut;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;



fn  handle_invalid(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    src_pid: Pid,
    _info: &Arc<RwLock<ShardInfo>>,
    key: String,
    inv_type: InvType,
) -> Result<Operation, Error> {
    let value = kvs_handle.invalid_cache(key, inv_type, src_pid);
    // return Ok(Operation::GetRes(value, psn));
    return Err(Error::new(ErrorKind::Other, "error"));
}

// todo reject invalid shards
fn  handle_get(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    _info: &Arc<RwLock<ShardInfo>>,
    key: &String,
    psn: usize,
) -> Result<Operation, Error> {
    let _value = kvs_handle.get(key, client_pid, psn, false);
    // return Ok(Operation::GetRes(value, psn));
    return Err(Error::new(ErrorKind::Other, "error"));
}

fn handle_put(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    _info: &Arc<RwLock<ShardInfo>>,
    key: String,
    value: String,
    psn: usize,
) -> Result<Operation, Error> {
    let _old_value = kvs_handle.put(key, value, client_pid, psn, false);
    // Ok(Operation::PutRes(old_value, psn))
    return Err(Error::new(ErrorKind::Other, "error"));
}

fn handle_delete(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    _info: &Arc<RwLock<ShardInfo>>,
    key: &String,
    psn: usize,
) -> Result<Operation, Error> {
    let _old_value = kvs_handle.delete(&key, client_pid, psn, false);
    // Ok(Operation::DeleteRes(old_value, psn))
    return Err(Error::new(ErrorKind::Other, "error"));
}

fn  handle_shmem_get_res(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    key: &String,
    value: Option<String>,
    psn: usize,
) -> Result<Operation, Error> {
    kvs_handle.shmem_get_res(key, value, client_pid, psn);

    // return Ok(Operation::GetRes(value, psn));
    return Err(Error::new(ErrorKind::Other, "error"));
}

fn  handle_shmem_put_res(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    key: &String,
    old_value: Option<String>,
    new_value: String,
    psn: usize,
) -> Result<Operation, Error> {
    kvs_handle.shmem_put_res(key, old_value, new_value, client_pid, psn);

    return Err(Error::new(ErrorKind::Other, "error"));
}

fn  handle_shmem_delete_res(
    kvs_handle: &mut KVSHandle,
    _self_pid: &Pid,
    client_pid: Pid,
    key: &String,
    old_value: Option<String>,
    psn: usize,
) -> Result<Operation, Error> {
    kvs_handle.shmem_delete_res(key, old_value, client_pid, psn);

    return Err(Error::new(ErrorKind::Other, "error"));
}

fn handle_shmem_put(
    kvs: &KVS,
    key: String,
    value: String,
    requestor: Pid,
    client: Pid,
    psn: usize,
    ctx: &mut NetworkContext,
) -> Result<Operation, Error> {
    let _old_value = kvs.put(key.clone(), value.clone(), requestor,
                                             client, psn, ctx);
    // Ok(Operation::ShmemPutRes(uid, old_value))
    return Err(Error::new(ErrorKind::Other, "error"));

}

fn handle_shmem_get(
    kvs: &KVS, 
    key: String, 
    requestor: Pid,
    client: Pid,
    psn: usize,
    ctx: &mut NetworkContext,
) -> Result<Operation, Error> {
    let _value = kvs.get(&key, requestor, client, psn, ctx);
    // Ok(Operation::ShmemGetRes(uid, value))
    return Err(Error::new(ErrorKind::Other, "error"));

}

fn handle_shmem_delete(
    kvs: &KVS, 
    key: String, 
    requestor: Pid,
    client: Pid,
    psn: usize,
    ctx: &mut NetworkContext,
) -> Result<Operation, Error> {
    let _old_value = kvs.delete(&key, requestor, client, psn, ctx);
    // Ok(Operation::ShmemDeleteRes(uid, old_value))
    return Err(Error::new(ErrorKind::Other, "error"));

}

fn handle_snapshot(
    info: &Arc<RwLock<ShardInfo>>,
    kvs: &KVS,
    self_pid: &Pid
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
    kvs_handle: &mut KVSHandle,
    info: &Arc<RwLock<ShardInfo>>,
    new_shard_info: &ShardInfo,
) -> Result<Operation, Error> {
    let mut shard_info = info.write().unwrap();
    *shard_info = new_shard_info.clone();
    kvs_handle.shard_info = new_shard_info.clone();
    return Ok(Operation::PutShardInfoRes());
}

fn run_server(self_pid: Pid, _controller_pid: Pid, _server_pids: Vec<Pid>) {
    let ctx = create_network_context(&self_pid).unwrap();

    let shard_info = Arc::new(RwLock::new(ShardInfo::new()));

    let (client_req_tx, client_req_rx) = mpsc::channel::<Message>(); // net -> worker
    let (shmem_req_tx, shmem_req_rx) = mpsc::channel::<Message>(); // net -> shmem_manager
    let (shmem_res_tx, shmem_res_rx) = mpsc::channel::<Message>(); // net -> worker
    let (dir_res_tx, dir_res_rx) = mpsc::channel::<Message>(); // net -> shmem_manager

    let ctx_worker = ctx.clone();
    let shard_info_worker = shard_info.clone();

    let shared_shmem_res_rx = Arc::new(Mutex::new(shmem_res_rx));
    let shared_ctx_worker = Arc::new(Mutex::new(ctx_worker));
    let mut kvs_handle = KVSHandle::new(
        shard_info_worker.write().unwrap().clone(),
        shared_shmem_res_rx.clone(),
        shared_ctx_worker.clone(),
    );

    let worker = thread::spawn(move || {
        loop {

            let (pid, operation) = client_req_rx.recv().unwrap();

            let result = match operation {
                Operation::Get(key, psn) => {
                    handle_get(&mut kvs_handle, &self_pid, pid, &shard_info_worker, &key, psn)
                },
                Operation::Put(key, value, psn) => {
                    handle_put(&mut kvs_handle, &self_pid, pid, &shard_info_worker, key, value, psn)
                }
                Operation::Delete(key, psn) => {
                    handle_delete(&mut kvs_handle, &self_pid, pid, &shard_info_worker, &key, psn)
                },
                Operation::PutShardInfo(new_shard_info) => {

                    handle_put_shard_info(&mut kvs_handle, &shard_info_worker, &new_shard_info)
                },
                Operation::ShmemInv(key, inv_type) => {
                    handle_invalid(&mut kvs_handle, &self_pid, pid, &shard_info_worker, key, inv_type)
                },  
                Operation::ShmemGetRes(pid, key, value, psn) => {
                    handle_shmem_get_res(&mut kvs_handle, &self_pid, pid, &key, value, psn)
                },
                Operation::ShmemPutRes(pid, key, old_value, new_value, psn) => {
                    handle_shmem_put_res(&mut kvs_handle, &self_pid, pid, &key, old_value, new_value, psn)
                },
                Operation::ShmemDeleteRes(pid, key, value, psn) => {
                    handle_shmem_delete_res(&mut kvs_handle, &self_pid, pid, &key, value, psn)
                },

                // Handle other opcodes here
                _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid operation")),
            };

            match result {
                Ok(operation) => {
                    if let Err(e) = send(
                        shared_ctx_worker.clone().lock().unwrap().deref_mut(),
                        &pid,
                        &operation,
                    ) {
                        println!("Server failed to send response: {:?}", e);
                    }
                }
                Err(_) => {
                    // println!("Server fail to generate response.");
                }
            }
        }
    });

    let mut ctx_shmem_manager = ctx.clone();
    let shard_info_shmem_manager = shard_info.clone();
    let shared_shmem_data_rx = Arc::new(Mutex::new(dir_res_rx));
    let kvs = KVS::new(shared_shmem_data_rx);

    let shmem_manager = thread::spawn(move || {
        loop {
            let (pid, operation) = shmem_req_rx.recv().unwrap();

            let result = match operation {
                Operation::ShmemPut(client_pid, key, value, psn) => {
                    handle_shmem_put(&kvs, key, value, pid, client_pid, psn,  &mut ctx_shmem_manager)
                },
                Operation::ShmemGet(client_pid, key, psn) => {
                    handle_shmem_get(&kvs, key, pid, client_pid, psn,  &mut ctx_shmem_manager)
                },
                Operation::ShmemDelete(client_pid, key, psn) => {
                    handle_shmem_delete(&kvs, key, pid, client_pid, psn,  &mut ctx_shmem_manager)
                },
                Operation::Snapshot() => {
                    handle_snapshot(&shard_info_shmem_manager, &kvs, &self_pid)
                }
                // Handle other opcodes here
                _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid operation")),
            };
            
            match result {
                Ok(operation) => {
                    match operation {
                        Operation::Snapshot() => {
                            if let Err(e) = send(&mut ctx_shmem_manager, &pid, &operation) {
                                println!("Server failed to send response: {:?}", e);
                            }
                        },
                        _ => {}
                    }
                }
                Err(_) => {
                    // println!("Server fail to generate response.");
                }
            }
        }
    });

    // Server thread to poll messages and dispatch to worker threads
    let mut ctx_server = ctx.clone();
    let server = thread::spawn(move || {
        loop {
            match recv(&mut ctx_server, 1000) {
                Ok(messages) => {
                    for (pid, operation) in messages {
                        match operation {
                            Operation::ShmemPut(_, _, _, _) => {
                                shmem_req_tx.send((pid, operation)).unwrap()
                            }
                            Operation::ShmemGet(_, _, _) => {
                                shmem_req_tx.send((pid, operation)).unwrap()
                            },
                            Operation::ShmemDelete(_, _, _) => {
                                shmem_req_tx.send((pid, operation)).unwrap()
                            },
                            Operation::Snapshot() => {
                                shmem_req_tx.send((pid, operation)).unwrap()
                            },
                            Operation::ShmemInvRes(_) => {
                                dir_res_tx.send((pid, operation)).unwrap()
                            },
                            Operation::ShmemPutRes(_, _, _, _, _) => {
                                if USE_CACHE {
                                    client_req_tx.send((pid, operation)).unwrap()
                                }
                                else {
                                    shmem_res_tx.send((pid, operation)).unwrap()
                                }
                            },
                            Operation::ShmemGetRes(_, _, _, _) => {
                                if USE_CACHE {
                                    client_req_tx.send((pid, operation)).unwrap()
                                }
                                else {
                                    shmem_res_tx.send((pid, operation)).unwrap()
                                }
                            },
                            Operation::ShmemDeleteRes(_, _, _, _) => {
                                if USE_CACHE {
                                    client_req_tx.send((pid, operation)).unwrap()
                                }
                                else {
                                    shmem_res_tx.send((pid, operation)).unwrap()
                                }
                            },
                            _ => {
                                client_req_tx.send((pid, operation)).unwrap()
                            },
                        }
                    }
                }
                Err(_) => {
                    // println!("Server timed out");
                }
            }
        }
    });

    worker.join().unwrap();
    shmem_manager.join().unwrap();
    server.join().unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        eprintln!("Usage: server <self_pid > <controller_pid> <server_pid_list>");
        return;
    }

    let self_pid: Pid = match args[1].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <server_pid_list>");
            return;
        }
    };

    let controller_pid: Pid = match args[2].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <server_pid_list>");
            return;
        }
    };

    let server_pids: Result<Vec<Pid>, _> = args[3..].iter().map(|x| x.parse()).collect();

    let server_pids = match server_pids {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Usage: server <self_pid> <controller_pid> <server_pid_list>");
            return;
        }
    };

    run_server(self_pid, controller_pid, server_pids);
}
