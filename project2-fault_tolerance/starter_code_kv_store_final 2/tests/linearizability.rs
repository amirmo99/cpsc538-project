mod utils;

use kv_store::kvs::KVSResult;
use std::cmp::Ordering;
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
    // TODO: complete this function
    // check this post for the checking algorithm used (http://rystsov.info/2017/07/16/linearizability-testing.html).
    // The input trace "long" does not contain any delete operation, and
    // it garuantees that each put to the same key write a unique value.
    println!("Start verification...");

    /*
    let num_client = result.len();

    let mut client_index = vec![0 as usize; num_client];

    let global_order: Vec<KVSResult> = vec![];

    
    for i in 0..result.len() {
        println!("{}", result[i].len());
    }
    
    // assert!(1 == 2);
    let mut prev_val = "".to_string();
    
    loop {
        // Phase 1: Make sure all GETs for the previously PUT value are handled
        let mut found_get = false;

        for i in 0..num_client {
            let op_index = client_index[i];
            if op_index >= result[i].len() {
                continue;
            }

            let op_result = result[i][op_index].clone();

            if op_result.operation == "get" && op_result.observed_value == prev_val {
                found_get = true;
                client_index[i] += 1;
            }
        }

        if found_get {
            continue;
        }

        // Phase 2: Check for new PUT requests
        let mut found_put = false;

        for i in 0..num_client {
            let op_index = client_index[i];
            if op_index >= result[i].len() {
                continue;
            }

            let op_result = result[i][op_index].clone();

            if op_result.operation == "put" && op_result.observed_value == prev_val {
                client_index[i] += 1;
                prev_val = op_result.observed_value.clone();
                found_put = true;
                break;
            }
        }

        if found_put {
            continue;
        }


        // Phase 3: Make sure we reach here only when all transactions are visited and were linear
        for i in 0..num_client {
            println!("{}", client_index[i]);
        }
        for i in 0..num_client {
            assert!(client_index[i] == result[i].len());
        }

        break;
    }
    */

    
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
            }
        }

        if before_len == ordered_results.len() {
            assert!(false);
        }
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
    let num_clients = 8;
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
    sleep(Duration::from_secs(90));

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
        }
        Err(_) => {
            panic!("Fail to read result file");
        }
    }
}
