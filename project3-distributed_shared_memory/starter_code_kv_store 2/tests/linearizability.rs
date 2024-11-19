mod utils;

use kv_store::kvs::KVSResult;
use std::collections::{HashMap, HashSet};
use std::process::Child;
use std::thread::sleep;
use std::time::Duration;
use utils::{group_result_by_key, launch_client, launch_controller, launch_server, read_result};


fn remove_val(get_results: &mut Vec<KVSResult>, val: &String) {
    let mut to_remove = vec![];
    
    for i in 0..get_results.len() {
        if &get_results[i].observed_value == val {
            to_remove.push(i);
        }
    }
    to_remove.sort_by(|a, b| b.cmp(a));

    for index in to_remove {
        get_results.remove(index);
    }
}


fn verify_result(result: &Vec<Vec<KVSResult>>) {
    // check this post for the checking algorithm used
    // http://rystsov.info/2017/07/16/linearizability-testing.html.

    let mut flattened_results: Vec<KVSResult> = Vec::new();
    for inner_vec in result {
        flattened_results.extend_from_slice(inner_vec);
    }

    println!("Start sorting...");

    flattened_results.sort_by_key(|k| k.begin_time);

    let put_results: Vec<KVSResult> = flattened_results
        .iter()
        .filter(|&result| result.operation == "put")
        .cloned()
        .collect();

    let mut get_results: Vec<KVSResult> = flattened_results
        .iter()
        .filter(|&result| result.operation == "get")
        .cloned()
        .collect();

    let mut ordered_results: Vec<KVSResult> = Vec::new();

    let mut prev_val = "".to_string();
    remove_val(&mut get_results, &prev_val);

    loop {
        let before_len = ordered_results.len();
        if before_len == put_results.len() {
            break;
        }

        for i in 0..put_results.len() {
            let op = &put_results[i];
            if op.observed_value == prev_val {
                ordered_results.push(op.clone());
                prev_val = op.new_value.clone();

                remove_val(&mut get_results, &prev_val);
                println!("Found value {}", &prev_val);
            }
        }
        
        assert!(before_len != ordered_results.len(), "before_len={}, after={}", before_len, ordered_results.len());
    }

    assert!(ordered_results.len() == put_results.len());
    assert!(get_results.is_empty());
}

#[test]
fn test_linearizability() {
    // binary location. binaries include controller and worker
    let bin_dir = "./target/debug/";
    let workload = "long";
    // number pf worker processes to run
    let num_clients = 2;
    let num_servers = 3;
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
                println!("verify linearizability for key:{}", key);
                verify_result(key_result);
            }
            // panic!("debug");
        }
        Err(_) => {
            panic!("Fail to read result file");
        }
    }
}
