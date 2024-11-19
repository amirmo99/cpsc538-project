// extern crate nanomsg;
extern crate serde_json;

// use nanomsg::{Protocol, Socket};
use crate::kvs::{Operation, Pid};
use std::io::Result;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

// Usage:
// // create network context
// let ctx = create_network_context(&self_pid).unwrap();
// // send to dst_pid an operation
// send(&mut ctx, &dst_pid, Operation::Get("x", 0));
// // recv from any sources, returns the data and src addr
// recv_from(&mut ctx, 1000);

pub struct NetworkContext {
    // sockets: HashMap<Pid, UdpSocket>
    socket: UdpSocket,
    self_pid: Pid,
}

impl NetworkContext {
    // Constructor function
    pub fn new(self_pid: &Pid) -> Result<NetworkContext> {
        let socket = UdpSocket::bind(&get_ip_port_from_pid(&self_pid))?;
        println!("create socket: {}", &get_ip_port_from_pid(&self_pid));
        Ok(NetworkContext {
            socket: socket,
            self_pid: self_pid.clone(),
        })
    }
}

impl Clone for NetworkContext {
    fn clone(&self) -> Self {
        let new_socket = self.socket.try_clone().expect("Failed to clone UdpSocket");
        NetworkContext {
            socket: new_socket,
            self_pid: self.self_pid,
        }
    }
}

fn get_ip_port_from_pid(pid: &Pid) -> String {
    format!("127.0.0.1:{}", 8000 + pid).to_string()
}

fn get_pid_from_ip_port(ip_port: &SocketAddr) -> Pid {
    (ip_port.port() - 8000) as Pid
}

pub fn create_network_context(self_pid: &Pid) -> Result<NetworkContext> {
    let ctx = NetworkContext::new(self_pid)?;
    ctx.socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    Ok(ctx)
}

pub fn send(ctx: &mut NetworkContext, dst_pid: &Pid, operation: &Operation) -> Result<usize> {
    let data = serde_json::to_string(operation).unwrap();
    // println!("send: src:{} dst:{} data:{}",  &get_ip_port_from_pid(&(ctx.self_pid)), &get_ip_port_from_pid(&dst_pid), &data);
    // println!("send: {} -> {} data:{}", &ctx.self_pid, &dst_pid, &data);
    match ctx
        .socket
        .send_to(data.as_bytes(), &get_ip_port_from_pid(&dst_pid))
    {
        Ok(_) => Ok(data.len()),
        Err(e) => Err(e),
    }
}

pub fn recv(ctx: &mut NetworkContext, timeout_ms: u64) -> Result<Vec<(Pid, Operation)>> {
    ctx.socket
        .set_read_timeout(Some(Duration::from_millis(timeout_ms)))?;

    let mut results = Vec::new();
    let start_time = Instant::now();
    let timeout = Duration::from_millis(timeout_ms as u64);

    const MAX_BUF_SIZE: usize = 1000000;
    let mut buf = [0; MAX_BUF_SIZE];
    loop {
        match ctx.socket.recv_from(&mut buf) {
            Ok((bytes_read, ip_port)) => {
                if bytes_read > 0 {
                    let data = String::from_utf8_lossy(&buf[0..bytes_read]);
                    let operation = serde_json::from_str(&data).unwrap();
                    let src_pid = get_pid_from_ip_port(&ip_port);
                    // println!("recv: {} -> {} data:{}", &src_pid, &(ctx.self_pid), &data);
                    results.push((src_pid, operation));
                }
            }
            Err(_) => {}
        }

        if !results.is_empty() {
            return Ok(results);
        }

        if start_time.elapsed() >= timeout {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "Timeout"));
        }
    }
}
