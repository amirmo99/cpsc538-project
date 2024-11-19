mod utils;

use kv_store::kvs::KVSResult;
use std::process::Child;
use std::thread::sleep;
use std::time::Duration;
use utils::{launch_client, launch_controller, launch_server, read_result};

fn compute_average(data: &[f64]) -> f64 {
    let sum: f64 = data.iter().sum();
    let count = data.len();
    sum / count as f64
}

fn compute_percentile(data: &[f64], percentile: f64) -> f64 {
    let len = data.len();
    if len == 0 {
        return 0.0;
    }
    let mut index = (percentile / 100.0 * (len as f64 - 1.0)).round() as usize;
    index = usize::min(len - 1, usize::max(0, index)); // Clamp index to be in bounds
    data[index]
}

fn print_performance(result: &Vec<Vec<KVSResult>>) {
    // calculate latency
    let mut latencies = Vec::new();
    for client_result in result.iter() {
        for op_result in client_result.iter() {
            let latency_us = (op_result.end_time - op_result.begin_time) as f64 / 1000 as f64;
            latencies.push(latency_us);
        }
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let average = compute_average(&latencies);
    let median = compute_percentile(&latencies, 50.0);
    let p95 = compute_percentile(&latencies, 95.0);
    let p99 = compute_percentile(&latencies, 99.0);

    // calculate throughput, only count the part where all clients run concurrently
    let mut num_concurrent_op = 0;
    let latest_begin = result
        .iter()
        .filter_map(|v| Some(v.first().unwrap().begin_time))
        .max()
        .unwrap();
    let earliest_end = result
        .iter()
        .filter_map(|v| Some(v.last().unwrap().end_time))
        .min()
        .unwrap();
    for i in 0..result.len() {
        let client_result = &result[i];
        let mut begin_idx = 0;
        let mut end_idx = client_result.len() - 1;
        while begin_idx < client_result.len() && client_result[begin_idx].begin_time < latest_begin
        {
            begin_idx += 1;
        }
        assert!(begin_idx != client_result.len());
        while end_idx >= 0 && client_result[end_idx].end_time > earliest_end {
            end_idx -= 1;
        }
        assert!(end_idx >= 0);
        let num_concurrent_op_client = end_idx - begin_idx;
        num_concurrent_op += num_concurrent_op_client;
        println!("concurrent op for client:{} is {}", i, num_concurrent_op);
    }
    let throughput_mops =
        num_concurrent_op as f64 / ((earliest_end - latest_begin) as f64 / 1000 as f64);
    println!("Throughput(Mops):{}", throughput_mops);
    println!(
        "Latency(us): average:{}, median:{}, 95%:{}, 99%:{}",
        average, median, p95, p99
    );
}

pub fn bench_common(num_clients: usize, num_servers: usize, workload: &str) {
    // binary location. binaries include controller and worker
    let bin_dir = "./target/debug/";
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
    sleep(Duration::from_secs(30));

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
            print_performance(&result);
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
fn bench() {
    bench_common(8, 3, "read_mostly_low_contention");
    bench_common(8, 3, "read_mostly_high_contention");
    bench_common(8, 3, "write_mostly_low_contention");
    bench_common(8, 3, "write_mostly_high_contention");
}
