use kv_store::kvs::{get_shard_id_from_key, KVSResult, Operation, Pid, ShardId, ShardInfo};
use kv_store::network::{create_network_context, recv, send, NetworkContext}; // Assuming network.rs is in the same crate
use std::env;
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind, Write};
use std::path::Path;
use std::time::{SystemTime, SystemTimeError};

fn write_result_to_file(result_filename: &str, results: Vec<KVSResult>) -> Result<(), Error> {
    let mut file = File::create(result_filename)?;

    for result in results {
        writeln!(
            &mut file,
            "{},{},{},{},{},{}",
            result.operation,
            result.key,
            result.observed_value,
            result.new_value,
            result.begin_time,
            result.end_time
        )?;
    }
    file.flush()?;
    Ok(())
}

fn get_shard_info(
    ctx: &mut NetworkContext,
    controller_pid: &Pid,
    shard_id: Option<ShardId>,
) -> Result<ShardInfo, Error> {
    // TODO: This function should send a GetShardInfo to the controller
    // wait for its reply, and returns the contained GetShardInfoRes

    match send(ctx, controller_pid, &Operation::GetShardInfo()) {
        Ok(_) => {},
        Err(x) => {
            return Err(x);
        },
    };

    match recv(ctx, 10000) {
        Ok(msg) => {
            for (_id, operation) in msg.clone() {
                match operation {
                    Operation::GetShardInfoRes(shard_info) => {
                        return Ok(shard_info);
                    }
                    _ => {},
                }
            }
            
            return Err(Error::new(ErrorKind::Other, 
                format!("Unexpected messages came through! msg={:?}", msg.clone())
            ));
        },
        Err(x) => {
            return Err(x);
        },
    }
}

fn get_timestamp() -> Result<u128, SystemTimeError> {
    let now = SystemTime::now();
    match now.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => Ok(duration.as_nanos()),
        Err(e) => Err(e),
    }
}

fn run_client(
    self_pid: Pid,
    controller_pid: Pid,
    _server_pids: Vec<Pid>,
    operations: Vec<Operation>,
) -> Result<Vec<KVSResult>, Error> {
    let mut ctx = create_network_context(&self_pid).unwrap();
    let mut shard_info = get_shard_info(&mut ctx, &controller_pid, None).unwrap();

    let mut operation_index = 0;
    let num_operations = operations.len();
    let mut results = Vec::new();
    while operation_index < num_operations {
        let operation = &operations[operation_index];
        let key;
        let mut put_value = "";
        let shard_id = match operation {
            Operation::Get(key_, _) => {
                key = key_;
                get_shard_id_from_key(&key, shard_info.locations.len())
            }
            Operation::Put(key_, value_, _) => {
                key = key_;
                put_value = value_;
                get_shard_id_from_key(&key, shard_info.locations.len())
            }
            Operation::Delete(key_, _) => {
                key = key_;
                get_shard_id_from_key(&key, shard_info.locations.len())
            }
            _ => todo!(),
        };
        let primary_server_pid = shard_info.locations[&shard_id].primary;
        let begin_time = get_timestamp().unwrap();
        if let Err(e) = send(&mut ctx, &primary_server_pid, operation) {
            println!("Client failed to send: {:?}", e);
        } else {
            match recv(&mut ctx, 1000) {
                Ok(messages) => {
                    let end_time = get_timestamp().unwrap();
                    let (_, result_operation) = &messages[0];
                    match result_operation {
                        Operation::GetRes(observed_value, _) => results.push(KVSResult {
                            operation: "get".to_string(),
                            key: key.clone(),
                            observed_value: match observed_value {
                                Some(val) => val.clone(),
                                None => "".to_string(),
                            },
                            new_value: "".to_string(),
                            begin_time: begin_time,
                            end_time: end_time,
                        }),
                        Operation::PutRes(observed_value, _) => results.push(KVSResult {
                            operation: "put".to_string(),
                            key: key.clone(),
                            observed_value: match observed_value {
                                Some(val) => val.clone(),
                                None => "".to_string(),
                            },
                            new_value: put_value.to_string(),
                            begin_time: begin_time,
                            end_time: end_time,
                        }),
                        Operation::DeleteRes(observed_value, _) => results.push(KVSResult {
                            operation: "delete".to_string(),
                            key: key.clone(),
                            observed_value: match observed_value {
                                Some(val) => val.clone(),
                                None => "".to_string(),
                            },
                            new_value: "".to_string(),
                            begin_time: begin_time,
                            end_time: end_time,
                        }),
                        _ => todo!(),
                    }
                    operation_index += 1;
                }
                Err(_) => {
                    println!("Client recv timeout, retry...");
                    // the server might have failed, so we update the shard info
                    shard_info = get_shard_info(&mut ctx, &controller_pid, None).unwrap();
                }
            }
        }
    }
    Ok(results)
}

fn parse_operations_from_file(file_path: &str) -> Result<Vec<Operation>, Error> {
    let path = Path::new(file_path);
    let file = File::open(&path)?;
    let reader = io::BufReader::new(file);
    let mut operations = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line = line?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["get", key] => {
                operations.push(Operation::Get(key.to_string(), index));
            }
            ["put", key, value] => {
                operations.push(Operation::Put(key.to_string(), value.to_string(), index));
            }
            ["delete", key] => {
                operations.push(Operation::Delete(key.to_string(), index));
            }
            _ => {
                eprintln!("Skipping invalid line {}: {:?}", index + 1, parts);
            }
        }
    }

    Ok(operations)
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 6 {
        eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
        return;
    }

    let self_pid: Pid = match args[1].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
            return;
        }
    };

    let controller_pid: Pid = match args[2].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
            return;
        }
    };

    let operation_file_dir: String = match args[3].parse() {
        Ok(path) => path,
        Err(_) => {
            eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
            return;
        }
    };

    let result_dir: String = match args[4].parse() {
        Ok(path) => path,
        Err(_) => {
            eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
            return;
        }
    };

    let server_pids: Result<Vec<Pid>, _> = args[5..].iter().map(|x| x.parse()).collect();

    let server_pids = match server_pids {
        Ok(v) => v,
        Err(_) => {
            eprintln!("Usage: client <self_pid> <controller_pid> <operation_file_dir> <result_dir> <server_pid_list>");
            return;
        }
    };

    let operation_filename = operation_file_dir + &self_pid.to_string() + ".txt";
    let operations = match parse_operations_from_file(&operation_filename) {
        Ok(ops) => ops,
        Err(e) => {
            println!("Failed to parse operations: {:?}", e);
            return;
        }
    };

    let results = run_client(self_pid, controller_pid, server_pids, operations).unwrap();
    let result_filename = result_dir + &self_pid.to_string() + ".txt";
    write_result_to_file(&result_filename, results).unwrap();
}
