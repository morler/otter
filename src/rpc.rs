//! External control of the process tree over a JSON-RPC 2.0 TCP socket.
//!
//! This lets a tool outside the runtime drive the scheduler exactly like the
//! in-JS API does:
//!
//! - `list`          -> `[{pid, name, status}, ...]`
//! - `info` {pid}    -> process detail or `null`
//! - `count`         -> number of live processes
//! - `spawn` {name?, code} -> new pid
//! - `kill` {pid}    -> `true`/`false` (best-effort, like `killProcess`)
//! - `send` {pid, value}   -> deliver a JSON value to a mailbox
//! - `rename` {pid, name}  -> `true`/`false`
//! - `shutdown`      -> stops the control server and the runtime
//!
//! Line-delimited JSON-RPC 2.0 frames: one request per line. Requests carry a
//! numeric/string `id` and receive a matching response; notifications (no
//! `id`) get none, per the JSON-RPC 2.0 spec.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};

use crate::process::Sandbox;
use crate::scheduler::World;
/// Longest request line accepted. A longer line gets an error and the
/// connection is closed, so one client cannot make the server buffer without
/// bound.
const MAX_LINE: u64 = 1 << 20;
/// Longest a single request line may take to arrive, end to end. It also
/// bounds the idle gap between requests, so a client that stalls or trickles
/// bytes is closed instead of holding a connection slot.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Bound each response write so clients that stop reading cannot pin connection threads.
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Socket reader that gives each request line one total time budget. A plain
/// read timeout restarts on every byte, so a client trickling one byte per
/// interval could hold a connection thread forever.
pub struct DeadlineStream {
    stream: TcpStream,
    budget: std::time::Duration,
    deadline: std::time::Instant,
}

impl DeadlineStream {
    pub fn new(stream: TcpStream, budget: std::time::Duration) -> Self {
        let deadline = std::time::Instant::now() + budget;
        Self {
            stream,
            budget,
            deadline,
        }
    }

    /// Start a fresh budget for the next request line.
    pub fn restart(&mut self) {
        self.deadline = std::time::Instant::now() + self.budget;
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let now = std::time::Instant::now();
        if now >= self.deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "request deadline exceeded",
            ));
        }
        self.stream.set_read_timeout(Some(self.deadline - now))?;
        self.stream.read(buf)
    }
}
/// Bind the control listener. `port == 0` asks the OS for an ephemeral port.
pub fn bind(host: &str, port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind((host, port))
}

/// Accept connections until `shutdown` flips. Each accepted connection is
/// handled on its own detached thread, so a single idle or misbehaving client
/// (one that connects and never sends a line) can never block the accept loop
/// or the control surface for other clients. The JSON-RPC dispatch itself is
/// serialized on the shared `World` locks, so per-connection threads remain
/// race-free with the scheduler.
pub fn serve_loop(listener: TcpListener, world: Arc<World>, shutdown: Arc<AtomicBool>) {
    if let Err(e) = listener.set_nonblocking(true) {
        eprintln!("otter: failed to set RPC listener nonblocking: {e}");
        return;
    }
    // Cap live connection threads so clients cannot exhaust server resources.
    let mut live: Vec<std::thread::JoinHandle<()>> = Vec::new();
    let mut streams: Vec<TcpStream> = Vec::new();
    let max_conns = 256;
    loop {
        let mut index = 0;
        while index < live.len() {
            if live[index].is_finished() {
                let handle = live.swap_remove(index);
                let _ = handle.join();
                streams.swap_remove(index);
            } else {
                index += 1;
            }
        }
        if shutdown.load(Ordering::Relaxed) {
            for stream in &streams {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }

            break;
        }
        if live.len() >= max_conns {
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                let monitor = match stream.try_clone() {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("otter: failed to monitor rpc connection: {e}");
                        continue;
                    }
                };
                let w = world.clone();
                let s = shutdown.clone();
                match std::thread::Builder::new()
                    .name("otter-rpc-conn".to_string())
                    .spawn(move || handle_conn(stream, &w, &s))
                {
                    Ok(handle) => {
                        live.push(handle);
                        streams.push(monitor);
                    }
                    Err(e) => eprintln!("otter: failed to spawn rpc connection thread: {e}"),
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => {
                eprintln!("otter: rpc accept error: {e}");
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
    for handle in live {
        let _ = handle.join();
    }
}

fn handle_conn(mut stream: TcpStream, world: &Arc<World>, shutdown: &Arc<AtomicBool>) {
    if stream.set_write_timeout(Some(WRITE_TIMEOUT)).is_err() {
        return;
    }
    let Ok(reader_stream) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(DeadlineStream::new(reader_stream, REQUEST_TIMEOUT));
    let mut line = String::new();
    loop {
        line.clear();
        reader.get_mut().restart();
        match (&mut reader).take(MAX_LINE).read_line(&mut line) {
            Ok(0) => return, // client closed
            Ok(_) => {}
            Err(_) => return,
        }
        if !line.ends_with('\n') && line.len() as u64 >= MAX_LINE {
            let resp = json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32600, "message": "request too large" } });
            if write_all_json(&mut stream, &resp).is_err() {
                return;
            }
            return;
        }
        let req: Value = match serde_json::from_str(line.trim()) {
            Ok(v) => v,
            Err(e) => {
                let resp = json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": format!("parse error: {e}") } });
                if write_all_json(&mut stream, &resp).is_err() {
                    return;
                }
                continue;
            }
        };
        // A request without an `id` is a JSON-RPC notification and gets no
        // response, so the framing loop can keep reading without a reply to
        // consume. (A malformed payload always draws an error, id or not.)
        let is_notification = req.get("id").is_none();
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let valid_id = id.is_null() || id.is_string() || id.is_number();
        let method = req.get("method").and_then(Value::as_str);
        let params = req.get("params");
        let valid_request = req.is_object()
            && req.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
            && method.is_some()
            && valid_id
            && params.is_none_or(|value| value.is_object() || value.is_array());
        if !valid_request {
            let error_id = if valid_id { id } else { Value::Null };
            let response = json!({"jsonrpc":"2.0", "id": error_id, "error":{"code":-32600,"message":"Invalid Request"}});
            if write_all_json(&mut stream, &response).is_err() {
                return;
            }
            continue;
        }
        let method = method.unwrap();
        let params = params.cloned().unwrap_or(json!({}));
        let resp = dispatch(method, &params, world, shutdown);
        if !is_notification && write_all_json(&mut stream, &resolve(id, resp)).is_err() {
            return;
        }
        if method == "shutdown" {
            shutdown.store(true, Ordering::Relaxed);
            return;
        }
    }
}

/// `dispatch` returns either a `result` or an `error` object; `resolve` wraps
/// it in a JSON-RPC response envelope.
fn dispatch(
    method: &str,
    params: &Value,
    world: &Arc<World>,
    _shutdown: &Arc<AtomicBool>,
) -> Result<Value, Value> {
    match method {
        "list" => Ok(json!(
            crate::scheduler::list_processes(world)
                .into_iter()
                .map(|(pid, name, status)| json!({ "pid": pid, "name": name, "status": status }))
                .collect::<Vec<_>>()
        )),
        "count" => Ok(json!(crate::scheduler::process_count(world))),
        "info" => {
            let pid = params
                .get("pid")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            match crate::scheduler::process_info(world, pid) {
                Some((pid, name, status)) => {
                    Ok(json!({ "pid": pid, "name": name, "status": status }))
                }
                None => Ok(Value::Null),
            }
        }
        "spawn" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("rpc");
            let code = match params.get("code").and_then(Value::as_str) {
                Some(c) => c.to_string(),
                None => return Err(json!({ "code": -32602, "message": "missing string `code`" })),
            };
            match crate::scheduler::spawn_process(world, name, &code, Sandbox::PRIVILEGED) {
                Ok(pid) => Ok(json!({ "pid": pid })),
                Err(e) => Err(json!({ "code": -32000, "message": e })),
            }
        }
        "kill" => {
            let pid = params
                .get("pid")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            Ok(json!(crate::scheduler::kill_process(world, pid)))
        }
        "send" => {
            let pid = params
                .get("pid")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            let value = params.get("value").cloned().unwrap_or(Value::Null);
            // The value was already JSON on the wire; pass it through
            // pre-serialized so no QuickJS context is needed here.
            let json = serde_json::to_string(&value).unwrap_or_else(|_| "null".into());
            crate::scheduler::deliver_json_string(world, pid, json);
            Ok(json!(true))
        }
        "rename" => {
            let pid = params
                .get("pid")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            if crate::scheduler::rename_if_alive(world, pid, name.to_string()) {
                Ok(json!(true))
            } else {
                Ok(json!(false))
            }
        }
        "shutdown" => Ok(json!(true)),
        _ => Err(json!({ "code": -32601, "message": format!("method not found: {method}") })),
    }
}

fn resolve(id: Value, resp: Result<Value, Value>) -> Value {
    match resp {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
    }
}

fn write_all_json(stream: &mut TcpStream, value: &Value) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(value)?;
    buf.push(b'\n');
    let deadline = std::time::Instant::now() + WRITE_TIMEOUT;
    let mut remaining = &buf[..];
    while !remaining.is_empty() {
        let now = std::time::Instant::now();
        if now >= deadline {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        stream.set_write_timeout(Some(deadline - now))?;
        match stream.write(remaining) {
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => remaining = &remaining[n..],
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if std::time::Instant::now() >= deadline {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
