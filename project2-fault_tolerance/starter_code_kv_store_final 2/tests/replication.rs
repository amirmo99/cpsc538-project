mod utils;

use kv_store::kvs::Pid;
use std::process::Child;
use std::thread::sleep;
use std::time::Duration;
use utils::{get_server_snapshots, launch_client, launch_controller, launch_server};

fn verify_result(server_pids: &Vec<Pid>) {
    let snapshots = get_server_snapshots(server_pids).unwrap();

    for i in 0..snapshots.len() {
        let snapshot_a = &snapshots[i];
        for (pk, pv) in snapshot_a.secondary_shards.iter() {
            let mut primary_cnt = 0;
            for j in 0..snapshots.len() {
                if i == j {
                    continue;
                }
                let snapshot_b = &snapshots[j];
                if snapshot_b.primary_shards.contains_key(pk) {
                    assert!(*pv == *snapshot_b.primary_shards.get(pk).unwrap());
                    primary_cnt += 1;
                }
            }
            assert!(primary_cnt == 1);
        }
    }
}

fn test_replication_common(num_clients: usize, num_servers: usize) {
    // binary location. binaries include controller and worker
    let bin_dir = "./target/debug/";
    let workload = "rep";
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
    verify_result(&server_pids.iter().map(|&x| x as Pid).collect());

    for mut server in servers {
        let _ = server.kill();
    }
}

#[test]
fn test_replication() {
    test_replication_common(1, 3);
    test_replication_common(3, 3);
    test_replication_common(2, 5);
    test_replication_common(5, 2);
}
