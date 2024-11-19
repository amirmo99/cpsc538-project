mod utils;

use kv_store::kvs::KVSResult;
use std::process::Child;
use std::thread::sleep;
use std::time::Duration;
use utils::{group_result_by_key, launch_client, launch_controller, launch_server, read_result};

fn verify_result(result: &Vec<Vec<KVSResult>>) {
    for client_result in result {
        let mut prev_put_value = "".to_string();

        for op_result in client_result {

            // println!("{:?}", op_result);

            if op_result.operation == "put" {
                prev_put_value = op_result.new_value.clone();
            } 
            else if op_result.operation == "delete" {
                prev_put_value = "".to_string();
            } 
            else {
                assert!(op_result.operation == "get");
                assert!(op_result.observed_value == prev_put_value, "observed = {},  prev = {}",
                             op_result.observed_value, prev_put_value);
            }
        }
    }
}

fn test_read_your_write_common(num_clients: usize, num_servers: usize) {
    // binary location. binaries include controller and worker
    let bin_dir = "./target/debug/";
    let workload = "ryw";
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
    for mut server in servers {
        let _ = server.kill();
    }

    let mut result_paths = Vec::new();
    for client_pid in &client_pids {
        let result_file = format!("{}{}.txt", result_dir, &client_pid); // Combine the prefix with the index
        result_paths.push(result_file);
    }
    // verify result
    match read_result(result_paths) {
        Ok(result) => {
            let grouped_result = group_result_by_key(result).unwrap();
            for (key, key_result) in grouped_result.iter() {
                println!("verify read-your-write for key:{}", key);
                verify_result(key_result);
            }
            // panic!("debug");
        }
        Err(_) => {
            panic!("Fail to read result file");
        }
    }
}

#[test]
fn test_read_your_write() {
    test_read_your_write_common(1, 1);
    test_read_your_write_common(3, 1);
    test_read_your_write_common(1, 3);
    test_read_your_write_common(3, 3);
}
