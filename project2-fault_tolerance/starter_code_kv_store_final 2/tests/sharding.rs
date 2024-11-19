mod utils;

use kv_store::kvs::{KVSResult, Pid};
use std::collections::HashMap;
use std::process::Child;
use std::thread::sleep;
use std::time::Duration;
use utils::{get_server_snapshots, launch_client, launch_controller, launch_server, read_result};

fn get_latest_kv_from_input(result: &Vec<Vec<KVSResult>>) -> HashMap<String, String> {
    let mut latest_kv = HashMap::new();
    for client_result in result {
        for op_result in client_result.into_iter().rev() {
            if op_result.operation == "put" {
                latest_kv
                    .entry(op_result.key.clone())
                    .or_insert(op_result.new_value.clone());
            } else if op_result.operation == "delete" {
                latest_kv
                    .entry(op_result.key.clone())
                    .or_insert("".to_string());
            }
        }
    }
    latest_kv
}

fn verify_result(result: &Vec<Vec<KVSResult>>, server_pids: &Vec<Pid>) {
    let latest_kv = get_latest_kv_from_input(result);
    let snapshots = get_server_snapshots(server_pids).unwrap();

    // verify keys are sharded
    for i in 0..snapshots.len() {
        let snapshot_a = &snapshots[i].primary_shards;
        for j in (i + 1)..snapshots.len() {
            let snapshot_b = &snapshots[j].primary_shards;
            for k_a in snapshot_a.keys() {
                assert!(!snapshot_b.contains_key(k_a));
            }
        }
    }

    // verify keys are complete & no write is lost
    let num_keys: usize = snapshots.iter().map(|map| map.primary_shards.len()).sum();
    assert!(num_keys == latest_kv.len());
    for snapshot in &snapshots {
        let primary = &snapshot.primary_shards;
        for (k, v) in primary.iter() {
            assert!(latest_kv.contains_key(k) && latest_kv.get(k).unwrap() == v);
        }
    }
}

fn test_sharding_common(num_clients: usize, num_servers: usize) {
    // binary location. binaries include controller and worker
    let bin_dir = "./target/debug/";
    let workload = "sharding";
    // number pf worker processes to run
    let input_dir = format!("./data/input/{}", workload);
    let result_dir = format!("./data/result/{}", workload);

    let controller_pid_for_clients: usize = 0;
    let controller_pid_for_servers: usize = 1;
    let client_pids: Vec<usize> = (2..num_clients + 2).collect();
    let server_pids: Vec<usize> = ((num_clients + 2)..=(num_clients + num_servers + 1)).collect();

    // Launch servers.
    println!("launching servers");
    let mut servers: Vec<Child> = Vec::new();
    for server_pid in &server_pids {
        let server = launch_server(
            &bin_dir,
            &server_pid,
            &controller_pid_for_servers,
            &server_pids,
        )
        .expect("Failed to launch client");
        servers.push(server);
    }

    // Launch controller.
    println!("launching controllers");
    let mut controller = launch_controller(
        &bin_dir,
        &controller_pid_for_clients,
        &controller_pid_for_servers,
        &num_clients,
        &num_servers,
        &client_pids,
        &server_pids,
    )
    .expect("Failed to launch controller");

    // Launch clients.
    println!("launching clients");
    let mut clients: Vec<Child> = Vec::new();
    for client_pid in &client_pids {
        let client = launch_client(
            &bin_dir,
            &client_pid,
            &controller_pid_for_clients,
            &input_dir,
            &server_pids,
            &result_dir,
        )
        .expect("Failed to launch client");
        clients.push(client);
    }

    // Sleep for enough time to let the system run
    sleep(Duration::from_secs(10));

    // Clean up (kill all remaining processes)
    controller.kill().expect("Failed to kill controller");
    for mut client in clients {
        let _ = client.kill();
    }

    let mut result_paths = Vec::new();
    for client_pid in &client_pids {
        let result_file = format!("{}{}.txt", result_dir, &client_pid); // Combine the prefix with the index
        result_paths.push(result_file);
    }
    // verify result
    match read_result(result_paths) {
        Ok(result) => {
            verify_result(&result, &server_pids.iter().map(|&x| x as Pid).collect());
            // panic!("debug");
        }
        Err(_) => {
            panic!("Fail to read result file");
        }
    }

    for mut server in servers {
        let _ = server.kill();
    }
}

#[test]
fn test_sharding() {
    test_sharding_common(1, 3);
    test_sharding_common(3, 3);
    test_sharding_common(2, 5);
    test_sharding_common(5, 2);
}
