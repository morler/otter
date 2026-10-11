//! Tiny entry point: one QuickJS Runtime per process, multiplexed across workers.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::Parser;
use otter_rt::process::Sandbox;
use otter_rt::scheduler::{RunItem, World, worker_loop};

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[derive(Parser)]
#[command(
    name = "otter",
    about = "A BEAM-like JavaScript runtime: many isolated QuickJS processes multiplexed onto a few OS threads"
)]
struct Args {
    /// Number of worker threads that share all processes
    #[arg(long, default_value_t = default_workers())]
    workers: usize,
    /// Host for JSON-RPC. Without authentication, loopback only.
    #[arg(long, value_name = "HOST", default_value = "127.0.0.1")]
    rpc_host: String,
    /// Port for JSON-RPC control socket
    #[arg(long, value_name = "PORT")]
    rpc_port: Option<u16>,
    /// Stay alive after entry scripts finish, even without RPC.
    #[arg(long)]
    persistent: bool,
    /// Stop a process whose single synchronous job runs longer than MS milliseconds (default: no limit)
    #[arg(long, value_name = "MS")]
    max_job_ms: Option<u64>,
    /// Entry scripts; each file starts as its own process
    #[arg(required = true)]
    scripts: Vec<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let workers = args.workers.max(1);
    let world = World::with_max_job(args.max_job_ms.map(Duration::from_millis));
    let mut handles = Vec::with_capacity(workers);
    for i in 0..workers {
        let w = world.clone();
        match std::thread::Builder::new()
            .name(format!("otter-worker-{i}"))
            .spawn(move || worker_loop(w))
        {
            Ok(handle) => handles.push(handle),
            Err(e) => {
                eprintln!("otter: failed to spawn worker thread {i}: {e}");
                for _ in 0..handles.len() {
                    let _ = world.queue_tx.send(RunItem::Stop);
                }
                for handle in handles {
                    let _ = handle.join();
                }
                std::process::exit(1);
            }
        }
    }

    for path in &args.scripts {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        match std::fs::read_to_string(path) {
            Ok(source) => {
                if let Err(msg) =
                    otter_rt::scheduler::spawn_process(&world, &name, &source, Sandbox::PRIVILEGED)
                {
                    eprintln!("otter: failed to start {}: {msg}", path.display());
                    world.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                eprintln!("otter: cannot read {}: {e}", path.display());
                world.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let rpc_shutdown = Arc::new(AtomicBool::new(false));
    let rpc_handle = match args.rpc_port {
        Some(port) => {
            if !matches!(args.rpc_host.as_str(), "127.0.0.1" | "localhost" | "::1") {
                eprintln!(
                    "otter: refusing unauthenticated RPC on non-loopback host {}",
                    args.rpc_host
                );
                std::process::exit(2);
            }
            match otter_rt::rpc::bind(&args.rpc_host, port) {
                Ok(listener) => {
                    let w = world.clone();
                    let shutdown = rpc_shutdown.clone();
                    match std::thread::Builder::new()
                        .name("otter-rpc".to_string())
                        .spawn(move || otter_rt::rpc::serve_loop(listener, w, shutdown))
                    {
                        Ok(handle) => {
                            eprintln!("otter: JSON-RPC control server on {}:{port}", args.rpc_host);
                            Some(handle)
                        }
                        Err(e) => {
                            eprintln!("otter: failed to spawn RPC thread: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("otter: cannot bind RPC on {}:{port}: {e}", args.rpc_host);
                    None
                }
            }
        }
        None => None,
    };

    let keep_alive = args.persistent || rpc_handle.is_some();
    loop {
        if rpc_shutdown.load(Ordering::Relaxed) {
            break;
        }
        let mut active = world.active.lock().unwrap();
        let (guard, _) = world
            .active_cv
            .wait_timeout(active, Duration::from_millis(100))
            .unwrap();
        active = guard;
        if *active == 0 && !keep_alive {
            break;
        }
        drop(active);
    }

    rpc_shutdown.store(true, Ordering::Relaxed);
    if let Some(handle) = rpc_handle {
        let _ = handle.join();
    }
    for _ in 0..workers {
        let _ = world.queue_tx.send(RunItem::Stop);
    }
    for handle in handles {
        let _ = handle.join();
    }
    if world.failed.load(Ordering::Relaxed) > 0 {
        std::process::exit(1);
    }
}
