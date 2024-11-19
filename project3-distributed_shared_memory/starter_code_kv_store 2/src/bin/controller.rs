use kv_store::kvs::*;
use kv_store::network::*; // Assuming network.rs is in the same crate
use std::collections::HashSet;
use std::env;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::thread;

fn assign_shards_to_servers(
    shard_info: &Arc<Mutex<ShardInfo>>,
    num_shards: usize,
    server_pids: &Vec<Pid>,
) {
    let mut info = shard_info.lock().unwrap();
    for shard_id in 0..num_shards {
        let primary = server_pids[shard_id];
        let secondaries = server_pids
            .clone()
            .into_iter()
            .filter(|&x| x != primary)
            .collect();
        info.locations
            .entry(shard_id as ShardId)
            .or_insert(ShardLoc {
                primary: primary,
                secondaries: secondaries,
            });
    }
}

fn run_controller(
    controller_pid_for_clients: Pid,
    controller_pid_for_servers: Pid,
    _client_pids: Vec<Pid>,
    server_pids: Vec<Pid>,
) {
    let shard_info: Arc<Mutex<ShardInfo>> = Arc::new(Mutex::new(ShardInfo::new()));
    assign_shards_to_servers(&shard_info, server_pids.len(), &server_pids);

    // Spawn shardinfo server thread
    let mut shard_info_ = shard_info.clone();
    let shardinfo_server = thread::spawn(move || {
        let mut ctx = create_network_context(&controller_pid_for_clients).unwrap();
        loop {
            match recv(&mut ctx, 1000) {
                Ok(messages) => {
                    for (pid, operation) in &messages {
                        let result: Result<Operation, Error> = match operation {
                            Operation::GetShardInfo(_shard_id) => Ok(Operation::GetShardInfoRes(
                                shard_info_.lock().unwrap().clone(),
                            )),
                            _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid operation")),
                        };
                        match result {
                            Ok(operation_res) => {
                                if let Err(e) = send(&mut ctx, &pid, &operation_res) {
                                    println!("Controller failed to send response: {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("Controller fail to generate response. {:?}", e);
                            }
                        }
                    }
                }
                Err(_) => {
                    // println!("Controller idle");
                }
            }
        }
    });

    // Spawn server_monitor thread
    // let self_pid = self_pid.clone();
    shard_info_ = shard_info.clone();
    let server_monitor = thread::spawn(move || {
        let mut ctx = create_network_context(&controller_pid_for_servers).unwrap();
        loop {
            for server_pid in &server_pids {
                if let Err(e) = send(
                    &mut ctx,
                    &server_pid,
                    &Operation::PutShardInfo(shard_info_.lock().unwrap().clone()),
                ) {
                    println!("Controller failed to send heartbeat: {:?}", e);
                }
            }
            let mut poll_pids: HashSet<Pid> = server_pids.clone().into_iter().collect();
            while poll_pids.len() > 0 {
                match recv(&mut ctx, 1000) {
                    Ok(messages) => {
                        for (pid, _operation) in &messages {
                            poll_pids.remove(&pid);
                        }
                    }
                    Err(e) => {
                        println!("Controller heartbeat error {:?}", e);
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });

    shardinfo_server.join().unwrap();
    server_monitor.join().unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 6 {
        eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
        return;
    }

    let controller_pid_for_clients: Pid = match args[1].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    let controller_pid_for_servers: Pid = match args[2].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    let num_clients: usize = match args[3].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    let _num_servers: usize = match args[4].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    let client_pids: Result<Vec<Pid>, _> = args[5..(5 + num_clients)]
        .iter()
        .map(|x| x.parse())
        .collect();

    let client_pids = match client_pids {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    let server_pids: Result<Vec<Pid>, _> = args[(5 + num_clients)..]
        .iter()
        .map(|x| x.parse())
        .collect();

    let server_pids = match server_pids {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Usage: controller <controller_pid_for_clients> <controller_pid_for_servers> <num_clients> <num_servers> <client_pid_list> <server_pid_list>");
            return;
        }
    };

    run_controller(
        controller_pid_for_clients,
        controller_pid_for_servers,
        client_pids,
        server_pids,
    );
}
