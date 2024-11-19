use kv_store::kvs::{KVSResult, KVSSnapshot, Operation, Pid};
use kv_store::network::{create_network_context, recv, send}; // Assuming network.rs is in the same crate
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Error, ErrorKind};
use std::path::Path;
use std::process::{Child, Command};

pub fn launch_controller(
    bin_dir: &str,
    controller_pid_for_clients: &usize,
    controller_pid_for_servers: &usize,
    num_clients: &usize,
    num_servers: &usize,
    client_pids: &Vec<usize>,
    server_pids: &Vec<usize>,
) -> std::io::Result<Child> {
    let mut command = Command::new(format!("{}/controller", bin_dir));

    command
        .arg(&(controller_pid_for_clients.to_string()))
        .arg(&(controller_pid_for_servers.to_string()))
        .arg(&(num_clients.to_string()))
        .arg(&(num_servers.to_string()));

    for client_pid in client_pids {
        command.arg(&(client_pid.to_string()));
    }

    for server_pid in server_pids {
        command.arg(&(server_pid.to_string()));
    }

    command.spawn()
}

pub fn launch_client(
    bin_dir: &str,
    self_pid: &usize,
    controller_pid: &usize,
    input_dir: &str,
    server_pids: &Vec<usize>,
    result_dir: &str,
) -> std::io::Result<Child> {
    let mut command = Command::new(format!("{}/client", bin_dir));

    command
        .arg(&(self_pid.to_string()))
        .arg(&(controller_pid.to_string()))
        .arg(input_dir)
        .arg(result_dir);

    for server_pid in server_pids {
        command.arg(&(server_pid.to_string()));
    }

    command.spawn()
}

pub fn launch_server(
    bin_dir: &str,
    self_pid: &usize,
    controller_pid: &usize,
    server_pids: &Vec<usize>,
) -> std::io::Result<Child> {
    let mut command = Command::new(format!("{}/server", bin_dir));

    command
        .arg(&(self_pid.to_string()))
        .arg(&(controller_pid.to_string()));

    for server_pid in server_pids {
        command.arg(&(server_pid.to_string()));
    }

    command.spawn()
}

pub fn read_result(file_paths: Vec<String>) -> Result<Vec<Vec<KVSResult>>, std::io::Error> {
    let mut result = Vec::new();

    for file_path in file_paths {
        result.push(Vec::new());
        // println!("{}", file_path);
        let path = Path::new(&file_path);

        let file = File::open(&path).map_err(|_| Error::new(ErrorKind::InvalidInput, ""))?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.map_err(|_| Error::new(ErrorKind::InvalidInput, ""))?;
            let mut parts = line.splitn(6, ',');
            let operation = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .to_string();
            let key = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .to_string();
            let observed_value = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .to_string();
            let new_value = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .to_string();
            let begin_time = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .parse::<u128>()
                .map_err(|_| Error::new(ErrorKind::InvalidInput, ""))?;
            let end_time = parts
                .next()
                .ok_or(Error::new(ErrorKind::InvalidInput, ""))?
                .trim()
                .parse::<u128>()
                .map_err(|_| Error::new(ErrorKind::InvalidInput, ""))?;
            result.last_mut().unwrap().push(KVSResult {
                operation: operation,
                key: key,
                observed_value: observed_value,
                new_value: new_value,
                begin_time: begin_time,
                end_time: end_time,
            });
        }
    }
    Ok(result)
}

pub fn group_result_by_key(
    result: Vec<Vec<KVSResult>>,
) -> Result<HashMap<String, Vec<Vec<KVSResult>>>, std::io::Error> {
    // Initialize the HashMap to store the grouped result
    let mut grouped_result: HashMap<String, Vec<Vec<KVSResult>>> = HashMap::new();
    for client_idx in 0..result.len() {
        let inter_vec = &result[client_idx];
        for kvs_result in inter_vec {
            if !grouped_result.contains_key(&kvs_result.key) {
                // new key
                grouped_result.insert(kvs_result.key.clone(), Vec::new());
                for _ in 0..result.len() {
                    grouped_result
                        .get_mut(&kvs_result.key)
                        .unwrap()
                        .push(Vec::new());
                }
            }
            grouped_result.get_mut(&kvs_result.key).unwrap()[client_idx].push(kvs_result.clone())
        }
    }
    // Print the grouped result
    // for (key, vec_of_vec) in &grouped_result {
    //     for inner_vec in vec_of_vec {
    //         let inner_str: Vec<String> = inner_vec
    //             .iter()
    //             .map(|vec| format!("{}", vec.begin_time))
    //             .collect();
    //         println!("{}: [{}]", key, inner_str.join(", "));
    //     }
    // }
    Ok(grouped_result)
}

pub fn get_server_snapshots(server_pids: &Vec<Pid>) -> Result<Vec<KVSSnapshot>, std::io::Error> {
    let magic_pid = 100;
    let mut ctx = create_network_context(&magic_pid).unwrap();
    let mut snapshots = Vec::new();
    for server_pid in server_pids {
        if let Err(e) = send(&mut ctx, &server_pid, &Operation::Snapshot()) {
            println!("Can not send snapshot request to server: {:?}", e);
        } else {
            match recv(&mut ctx, 1000) {
                Ok(responses) => {
                    if responses.len() != 1 {
                        return Err(Error::new(ErrorKind::InvalidInput, "Invalid response"));
                    }
                    let (_pid, operation) = responses[0].clone();
                    match operation {
                        Operation::SnapshotRes(snapshot) => snapshots.push(snapshot),
                        _ => {}
                    }
                }
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Can not get snapshot from server",
                    ));
                }
            }
        }
    }
    Ok(snapshots)
}
