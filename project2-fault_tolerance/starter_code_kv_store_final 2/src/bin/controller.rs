use kv_store::kvs::{Operation, Pid, ShardId, ShardInfo, ShardLoc};
use kv_store::network::{create_network_context, recv, send}; // Assuming network.rs is in the same crate
use std::collections::HashSet;
use std::env;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::Duration;

fn assign_shards_to_servers(
    shard_info: &Arc<Mutex<ShardInfo>>,
    num_shards: usize,
    server_pids: &Vec<Pid>,
) {
    // TODO: assign one primary for each shard in round-robin manner
    // for each shard, assign all remaining servers as secondaries
    // you should update the shard_info argument in place
    let mut info = shard_info.lock().unwrap();
    info.locations.clear();
    
    for i in 0..num_shards {
        let primary = server_pids[i];

        let mut secondaries = server_pids.clone();
        secondaries.remove(i);

        info.locations.insert(
            i as u32, ShardLoc { 
                primary, 
                secondaries
            }
        );
    }

    println!("Shard Assignments: {:?}", info.locations);
}

fn run_controller(
    controller_pid_for_clients: Pid,
    controller_pid_for_servers: Pid,
    _client_pids: Vec<Pid>,
    server_pids: Vec<Pid>,
) {
    const PERIOD: Duration = Duration::new(5, 0);

    let shard_info: Arc<Mutex<ShardInfo>> = Arc::new(Mutex::new(ShardInfo::new()));
    assign_shards_to_servers(&shard_info, server_pids.len(), &server_pids);

    // Create references for shard_info to be used by threads
    let shard_info_1: Arc<Mutex<ShardInfo>> = Arc::clone(&shard_info);
    let shard_info_2 = Arc::clone(&shard_info);

    // TODO: implement the shardinfo_server thread.
    // it should first create a network context with create_network_context using controller_pid_for_client
    // it should then use recv to poll GetShardInfo from clients, and reply to clients using send
    // with a GetShardInfoRes, which contains the up-to-date shard info
    let shardinfo_server = thread::spawn(move || {
        let mut ctx = create_network_context(&controller_pid_for_clients).unwrap();

        loop {
            match recv(&mut ctx, 10000) {
                Ok(msg) => {
                    for (id, operation) in msg {
                        match operation {
                            Operation::GetShardInfo() => {
                                let info = shard_info_1.lock().unwrap().clone();
                                let message = Operation::GetShardInfoRes(info);
                                
                                let res = send(&mut ctx, &id, &message);
                                match res {
                                    Ok(_) => {},
                                    Err(_) => {
                                        println!("Failed to send GetShardInfoRes from controller to id={}", id);
                                    },
                                }
                            },
                            _ => {
                                println!("(shardinfo_server) Unexpected operation received at controller, id={}, op={:?}", 
                                                id, operation);
                            }
                        }
                    }
                },
                Err(_) => {},
            }
        }
    });

    // TODO: implement server_monitor thread.
    // it should first create a network context with create_network_context using controller_pid_for_server
    // it should then periodically send a PutShardInfo to each server and use recv to wait for their response (PutShardInfoRes)
    // if you want to do part C, this also serves as a heartbeat to detect server failures
    let server_monitor = thread::spawn(move || {
        let mut ctx = create_network_context(&controller_pid_for_servers).unwrap();

        loop {
            // Phase 1: Sending PutShardInfo
            for id in &server_pids {
                let info = shard_info_2.clone().lock().unwrap().clone();
                let message = Operation::PutShardInfo(info);

                let res = send(&mut ctx, id, &message);
                match res {
                    Ok(_) => {},
                    Err(_) => {
                        println!("Failed to send PutShardInfo from controller to id={}", id);
                    },
                }
            }

            // Phase 2: Collecting PutShardInfoRes
            let mut servers_pending = server_pids.clone();
            servers_pending.sort();

            while !servers_pending.is_empty() {
                match recv(&mut ctx, 2000) {
                    Ok(msg) => {
                        for (id, operation) in msg {
                            match operation {
                                Operation::PutShardInfoRes() => {
                                    let index = servers_pending.binary_search(&id).unwrap();
                                    let removed = servers_pending.remove(index);
                                    assert_eq!(removed, id);
                                },
                                _ => {
                                    println!("(server_monitor) Unexpected operation received at controller, id={}, op={:?}", 
                                                id, operation);
                                },
                            }
                        }
                    },
                    Err(_) => {
                       println!("Timeout! Pending IDs: {:?}", servers_pending); 
                       break;
                    },
                }
            }

            // Phase 3: Wait for next iteration
            sleep(PERIOD);
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
