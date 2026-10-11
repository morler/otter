//! The scheduler: a fixed pool of worker threads multiplexes all processes.
//!
//! Processes move between workers through an unbounded run queue. Each
//! scheduling slice wakes the process (if it was parked on an empty mailbox),
//! executes one pending QuickJS job, then re-queues, parks, or finishes the
//! process based on its status.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use rquickjs::{Context, Ctx, Function, Value};

use crate::process::{self, ProcShared, Process, Sandbox, Status};

pub type Pid = u64;

pub enum RunItem {
    Process(Box<Process>),
    Stop,
}

/// How often idle workers check the timer heap for due deadlines. Timer
/// precision is bounded by this tick: `sleep(2)` may take up to ~7 ms.
const TIMER_TICK: Duration = Duration::from_millis(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimerKind {
    /// A `sleep(ms)` deadline; wakes the process from the sleeping map.
    Sleep,
    /// A `recv(timeoutMs)` deadline; rejects the stashed recv promise.
    RecvTimeout,
}

/// A pending timer, ordered so the earliest deadline pops first.
#[derive(Debug, PartialEq, Eq)]
struct TimerEntry {
    deadline: Instant,
    pid: Pid,
    kind: TimerKind,
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // BinaryHeap is a max-heap; reverse so the earliest deadline is "max".
        other.deadline.cmp(&self.deadline)
    }
}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

/// Everything workers and JS callbacks share.
pub struct World {
    pub queue_tx: Sender<RunItem>,
    pub queue_rx: Receiver<RunItem>,
    /// Processes suspended on `recv()`, keyed by pid. Holding the parked map
    /// lock is what makes "is the inbox empty?" and "park the process"
    /// atomic with respect to `send_message`.
    pub parked: Mutex<HashMap<Pid, Box<Process>>>,
    /// Processes suspended on `sleep()`, keyed by pid. Kept separate from
    /// `parked` so a `send` to a sleeping process never wakes it: `sleep` is
    /// uninterruptible, BEAM-style.
    pub sleeping: Mutex<HashMap<Pid, Box<Process>>>,
    /// Pending deadlines (a min-heap on `deadline`), serviced by idle workers
    /// every `TIMER_TICK`.
    timers: Mutex<BinaryHeap<TimerEntry>>,
    /// Inbox senders for every live process, keyed by pid.
    pub inboxes: Mutex<HashMap<Pid, mpsc::Sender<String>>>,
    /// Metadata for every live process, keyed by pid. The source of truth
    /// for liveness and for the management API (`listProcesses`,
    /// `killProcess`, `processInfo`, ...). Inserted before a process's code
    /// can run, removed in `finish`.
    pub processes: Mutex<HashMap<Pid, Arc<ProcShared>>>,
    pub next_pid: AtomicU64,
    /// Number of live processes; main waits on `active_cv` for this to hit 0.
    pub active: Mutex<usize>,
    pub active_cv: Condvar,
    pub failed: AtomicU64,
    /// Per-job time budget (`--max-job-ms`); `None` means no limit.
    pub max_job: Option<Duration>,
}

impl World {
    pub fn new() -> Arc<Self> {
        Self::with_max_job(None)
    }

    pub fn with_max_job(max_job: Option<Duration>) -> Arc<Self> {
        let (queue_tx, queue_rx) = crossbeam_channel::unbounded();
        Arc::new(World {
            queue_tx,
            queue_rx,
            parked: Mutex::new(HashMap::new()),
            sleeping: Mutex::new(HashMap::new()),
            timers: Mutex::new(BinaryHeap::new()),
            inboxes: Mutex::new(HashMap::new()),
            processes: Mutex::new(HashMap::new()),
            next_pid: AtomicU64::new(0),
            active: Mutex::new(0),
            active_cv: Condvar::new(),
            failed: AtomicU64::new(0),
            max_job,
        })
    }
}

/// Create a process from JS source, register it, and schedule it. The
/// `sandbox` argument fixes the new process's initial sandbox; callers are
/// responsible for narrowing it relative to the parent. The `spawn` JS
/// callback does this by starting from the parent's sandbox and turning off
/// the toggles the caller explicitly requested (a monotonic narrowing).
pub fn spawn_process(
    world: &Arc<World>,
    name: &str,
    source: &str,
    sandbox: Sandbox,
) -> Result<Pid, String> {
    let pid = world.next_pid.fetch_add(1, Ordering::Relaxed);
    let (inbox_tx, inbox_rx) = mpsc::channel();
    // Register the inbox BEFORE the entry script is evaluated: code that
    // runs during the eval (e.g. a child spawned synchronously) must be
    // able to deliver messages to this process already.
    world.inboxes.lock().unwrap().insert(pid, inbox_tx);
    let proc = match process::create_process(world, pid, name, source, inbox_rx, sandbox) {
        Ok(proc) => proc,
        Err(e) => {
            world.inboxes.lock().unwrap().remove(&pid);
            return Err(e);
        }
    };

    let boxed = Box::new(proc);
    let status = *boxed.shared.status.lock().unwrap();
    match status {
        // The entry script suspended before returning (e.g. top-level
        // `await recv()` with an empty inbox, or `await sleep(...)`). Park it
        // only when no job is left; otherwise it runs those jobs first.
        Status::Waiting if !boxed.rt.is_job_pending() => park(world, boxed),
        Status::Sleeping if !boxed.rt.is_job_pending() => park_sleep(world, boxed),
        _ => {
            if let Err(err) = world.queue_tx.send(RunItem::Process(boxed)) {
                if let RunItem::Process(p) = err.0 {
                    finish(world, p);
                }
            }
        }
    }
    Ok(pid)
}

/// Deliver a message to another process (JSON-serialized). Messages to
/// unknown or finished pids are silently dropped, BEAM-style.
pub fn send_message<'js>(
    world: &Arc<World>,
    cx: &Ctx<'js>,
    pid: Pid,
    value: Value<'js>,
) -> rquickjs::Result<()> {
    if value.is_undefined() || value.is_function() || value.is_symbol() {
        return Err(process::js_type_error(
            cx,
            "value is not message-serializable",
        ));
    }
    let json = match cx.json_stringify(value)? {
        Some(s) => s.to_string()?,
        None => {
            return Err(process::js_type_error(
                cx,
                "value is not message-serializable",
            ));
        }
    };

    deliver_json_string(world, pid, json);
    Ok(())
}

/// Push a pre-serialized JSON string into a process's mailbox. Buffers the
/// message *before* re-queuing the process (push-then-unpark), so a woken
/// process is guaranteed to see it. Shared with the RPC control layer, which
/// supplies already-JSON text (serialized outside a QuickJS context) instead
/// of a JS lvalue.
pub fn deliver_json_string(world: &Arc<World>, pid: Pid, json: String) {
    let tx = world.inboxes.lock().unwrap().get(&pid).cloned();
    if let Some(tx) = tx
        && tx.send(json).is_ok()
    {
        // Push-then-unpark ordering: the message is already buffered
        // before the process is re-scheduled, so the woken process is
        // guaranteed to see it.
        if let Some(p) = world.parked.lock().unwrap().remove(&pid) {
            if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                if let RunItem::Process(p) = err.0 {
                    finish(world, p);
                }
            }
        }
    }
}

/// Request termination of a live process. Best-effort and cooperative: the
/// target is marked `Status::Killed` and reaped at its next scheduling
/// boundary. Parked (`recv`) and sleeping processes are force-requeued so
/// that happens promptly; a process on the run queue or mid-slice is reaped
/// by the worker that next picks it up. Returns `false` for unknown pids
/// (already dead, or never existed).
pub fn kill_process(world: &Arc<World>, pid: Pid) -> bool {
    let Some(shared) = world.processes.lock().unwrap().get(&pid).cloned() else {
        return false;
    };
    {
        let mut st = shared.status.lock().unwrap();
        if *st != Status::Done && *st != Status::Failed {
            *st = Status::Killed;
        }
    }
    // Pull the process out of a map if it is parked/sleeping, so it gets a
    // scheduling boundary and is reaped. Processes on the queue or in a
    // worker's hands need no relocation: their next slice sees `Killed`.
    if let Some(p) = world.parked.lock().unwrap().remove(&pid) {
        if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
            if let RunItem::Process(p) = err.0 {
                finish(world, p);
            }
        }
    } else if let Some(p) = world.sleeping.lock().unwrap().remove(&pid) {
        if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
            if let RunItem::Process(p) = err.0 {
                finish(world, p);
            }
        }
    }
    true
}

/// Snapshot of every live process as `(pid, name, status)`, sorted by pid.
pub fn list_processes(world: &Arc<World>) -> Vec<(Pid, String, String)> {
    let reg = world.processes.lock().unwrap();
    let mut out: Vec<(Pid, String, String)> = reg
        .iter()
        .map(|(pid, s)| {
            (
                *pid,
                s.name.lock().unwrap().clone(),
                s.status.lock().unwrap().as_str().to_string(),
            )
        })
        .collect();
    out.sort_by_key(|(pid, _, _)| *pid);
    out
}

pub fn is_process_alive(world: &Arc<World>, pid: Pid) -> bool {
    world.processes.lock().unwrap().contains_key(&pid)
}

/// Info for one process as `(pid, name, status)`, or `None` if unknown.
pub fn process_info(world: &Arc<World>, pid: Pid) -> Option<(Pid, String, String)> {
    let reg = world.processes.lock().unwrap();
    let s = reg.get(&pid)?;
    Some((
        pid,
        s.name.lock().unwrap().clone(),
        s.status.lock().unwrap().as_str().to_string(),
    ))
}

pub fn process_count(world: &Arc<World>) -> usize {
    world.processes.lock().unwrap().len()
}

/// Rename a live process; a no-op for unknown pids.
pub fn set_process_name(world: &Arc<World>, pid: Pid, name: String) {
    if let Some(s) = world.processes.lock().unwrap().get(&pid) {
        *s.name.lock().unwrap() = name;
    }
}

pub fn rename_if_alive(world: &Arc<World>, pid: Pid, name: String) -> bool {
    let registry = world.processes.lock().unwrap();
    let Some(shared) = registry.get(&pid) else {
        return false;
    };
    *shared.name.lock().unwrap() = name;
    true
}

/// Worker entry point: pull processes off the run queue until stopped. Timers
/// are checked on every pass, so sleeping processes resume on time even when
/// the queue never goes idle.
pub fn worker_loop(world: Arc<World>) {
    loop {
        service_timers(&world);
        match world.queue_rx.recv_timeout(TIMER_TICK) {
            Ok(RunItem::Stop) => break,
            Ok(RunItem::Process(p)) => run_slice(&world, p),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

/// One scheduling slice: wake if parked, run one job, then decide whether the
/// process is done, parks, or goes back on the run queue.
fn run_slice(world: &Arc<World>, p: Box<Process>) {
    let wake_status = *p.shared.status.lock().unwrap();
    if wake_status == Status::Killed {
        // A kill landed between slices (or while queued): reap without
        // running anything.
        finish(world, p);
        return;
    }
    match wake_status {
        Status::Waiting => {
            let msg = p.shared.inbox_rx.lock().unwrap().try_recv();
            match msg {
                Ok(json) => {
                    if wake_with(&p, json).is_err() {
                        fail(world, p, "failed to deliver message");
                        return;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // Spurious wake. Park again, unless jobs are still queued.
                    if !p.rt.is_job_pending() {
                        park(world, p);
                        return;
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    fail(world, p, "mailbox disconnected");
                    return;
                }
            }
        }
        Status::Yielding => {
            if wake_unit(&p, "__otter_yield_resolve").is_err() {
                fail(world, p, "failed to resume after yield");
                return;
            }
        }
        // Woken only by its timer. Reaching a worker means jobs are queued:
        // run them, and park again once none are left.
        Status::Sleeping if !p.rt.is_job_pending() => {
            park_sleep(world, p);
            return;
        }
        _ => {}
    }

    let status = *p.shared.status.lock().unwrap();
    if status != Status::Done
        && status != Status::Failed
        && status != Status::Killed
        && p.rt.is_job_pending()
        && let Err(exc) = timed_job(&p.shared, || p.rt.execute_pending_job())
    {
        let msg = if p.shared.job_overran.swap(false, Ordering::Relaxed) {
            "job ran past the --max-job-ms budget".to_string()
        } else {
            exception_message(&exc.0)
        };
        eprintln!("[pid {}] unhandled exception: {msg}", p.shared.pid);
        p.shared.set_status_unless_killed(Status::Failed);
    }

    let status = *p.shared.status.lock().unwrap();
    match status {
        Status::Done | Status::Failed | Status::Killed => finish(world, p),
        // Jobs queued before the suspension run first; the process parks later.
        Status::Waiting | Status::Sleeping if p.rt.is_job_pending() => {
            if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                if let RunItem::Process(p) = err.0 {
                    finish(world, p);
                }
            }
        }
        Status::Waiting => park(world, p),
        Status::Sleeping => park_sleep(world, p),
        // A process that yielded goes to the back of the run queue; its
        // resolver fires the next time a worker picks it up.
        Status::Yielding => {
            if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                if let RunItem::Process(p) = err.0 {
                    finish(world, p);
                }
            }
        }
        Status::Running => {
            if p.rt.is_job_pending() {
                if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                    if let RunItem::Process(p) = err.0 {
                        finish(world, p);
                    }
                }
            } else {
                // Defensive: no waiter, no jobs, and the entry hook never
                // fired. Treat as finished so nothing leaks.
                finish(world, p);
            }
        }
    }
}

/// Run one job with the per-job clock running. The interrupt handler that
/// `create_process` installs (when `World::max_job` is set) reads this clock.
pub(crate) fn timed_job<T>(shared: &ProcShared, run: impl FnOnce() -> T) -> T {
    *shared.job_started.lock().unwrap() = Some(Instant::now());
    let out = run();
    *shared.job_started.lock().unwrap() = None;
    out
}

/// Resolve the stashed `recv()` resolver with a buffered message.
fn wake_with(p: &Process, json: String) -> rquickjs::Result<()> {
    p.ctx.with(|cx| {
        let globals = cx.globals();
        let resolve: Option<Function> = globals.get("__otter_recv_resolve")?;
        if let Some(resolve) = resolve {
            let v: Value = cx.json_parse(json)?;
            resolve.call::<_, ()>((v,))?;
            globals.set("__otter_recv_resolve", ())?;
            globals.set("__otter_recv_reject", ())?;
        }
        p.shared.set_status_unless_killed(Status::Running);
        *p.shared.deadline.lock().unwrap() = None;
        Ok(())
    })
}

/// Resume a process whose `sleep()` or `yieldNow()` finished: resolve the promise
/// stashed under `slot` so the suspended continuation becomes a pending job.
fn wake_unit(p: &Process, slot: &str) -> rquickjs::Result<()> {
    p.ctx.with(|cx| {
        let globals = cx.globals();
        let resolve: Option<Function> = globals.get(slot)?;
        if let Some(resolve) = resolve {
            resolve.call::<_, ()>(())?;
            globals.set(slot, ())?;
        }
        p.shared.set_status_unless_killed(Status::Running);
        // Clear the consumed deadline so a later plain `recv()` doesn't pick
        // it up as a stale timeout.
        *p.shared.deadline.lock().unwrap() = None;
        Ok(())
    })
}

/// Park a waiting process. The inbox is re-checked while holding the parked
/// lock so a message that races the park either wakes the process or finds it
/// already in the map.
fn park(world: &Arc<World>, p: Box<Process>) {
    let mut parked = world.parked.lock().unwrap();
    let msg = p.shared.inbox_rx.lock().unwrap().try_recv();
    match msg {
        Ok(json) => {
            drop(parked);
            if wake_with(&p, json).is_ok() {
                if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                    if let RunItem::Process(p) = err.0 {
                        finish(world, p);
                    }
                }
            } else {
                fail(world, p, "failed to deliver message");
            }
        }
        Err(TryRecvError::Empty) => {
            // A kill can land while this process is still finishing its
            // slice (after its status was last read). Re-check under the
            // parked lock so a killed process is reaped instead of parked
            // forever waiting for a message that will never wake it.
            if *p.shared.status.lock().unwrap() == Status::Killed {
                drop(parked);
                finish(world, p);
                return;
            }
            let pid = p.shared.pid;
            let deadline = *p.shared.deadline.lock().unwrap();
            parked.insert(pid, p);
            drop(parked);
            // Register the recv timeout now that the process is visible in
            // the parked map. If a message wins the race, send_message
            // removes the process first and the stale entry is skipped.
            if let Some(deadline) = deadline {
                world.timers.lock().unwrap().push(TimerEntry {
                    deadline,
                    pid,
                    kind: TimerKind::RecvTimeout,
                });
            }
        }
        Err(TryRecvError::Disconnected) => {
            drop(parked);
            finish(world, p);
        }
    }
}

/// Park a sleeping process: put it in the sleeping map and register its
/// deadline. Mirrors `park`, but a `send` never wakes a sleeper.
fn park_sleep(world: &Arc<World>, p: Box<Process>) {
    let pid = p.shared.pid;
    let deadline = *p.shared.deadline.lock().unwrap();
    {
        let mut sleeping = world.sleeping.lock().unwrap();
        // Same re-check as `park`: a kill that raced this slice must not
        // find the process parked in the sleeping map forever.
        if *p.shared.status.lock().unwrap() == Status::Killed {
            drop(sleeping);
            finish(world, p);
            return;
        }
        sleeping.insert(pid, p);
    }
    // The sleeping lock is dropped before touching the timers lock.
    if let Some(deadline) = deadline {
        world.timers.lock().unwrap().push(TimerEntry {
            deadline,
            pid,
            kind: TimerKind::Sleep,
        });
    }
}

/// Reject the stashed `recv(timeout)` resolver with a `TimeoutError` so the
/// suspended continuation throws inside the awaiting async function.
fn wake_timeout(p: &Process) -> rquickjs::Result<()> {
    p.ctx.with(|cx| {
        let globals = cx.globals();
        let reject: Option<Function> = globals.get("__otter_recv_reject")?;
        if let Some(reject) = reject {
            let err: Value = cx.eval("new TimeoutError('recv timed out')")?;
            reject.call::<_, ()>((err,))?;
            globals.set("__otter_recv_resolve", ())?;
            globals.set("__otter_recv_reject", ())?;
        }
        p.shared.set_status_unless_killed(Status::Running);
        *p.shared.deadline.lock().unwrap() = None;
        Ok(())
    })
}

/// Fire every timer whose deadline has passed. Runs on idle workers each
/// tick. Two phases keep lock ordering simple: pop the due entries under the
/// timers lock, then wake processes without holding it.
fn service_timers(world: &Arc<World>) {
    let now = Instant::now();
    let mut due = Vec::new();
    {
        let mut timers = world.timers.lock().unwrap();
        while let Some(entry) = timers.peek() {
            if entry.deadline <= now {
                due.push(timers.pop().expect("peeked entry vanished"));
            } else {
                break;
            }
        }
    }

    for entry in due {
        match entry.kind {
            TimerKind::Sleep => {
                let p = world.sleeping.lock().unwrap().remove(&entry.pid);
                if let Some(p) = p {
                    if wake_unit(&p, "__otter_sleep_resolve").is_ok() {
                        if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                            if let RunItem::Process(p) = err.0 {
                                finish(world, p);
                            }
                        }
                    } else {
                        fail(world, p, "failed to resume after sleep");
                    }
                }
                // Absent from the sleeping map: already finished; skip.
            }
            TimerKind::RecvTimeout => {
                // Only the wait that registered this timer may time out. If a
                // message woke the process and it parked again for a newer
                // recv(), this deadline is stale and must not reject it.
                let p = {
                    let mut parked = world.parked.lock().unwrap();
                    let due = parked.get(&entry.pid).is_some_and(|p| {
                        *p.shared.deadline.lock().unwrap() == Some(entry.deadline)
                    });
                    if due { parked.remove(&entry.pid) } else { None }
                };
                if let Some(p) = p {
                    if wake_timeout(&p).is_ok() {
                        if let Err(err) = world.queue_tx.send(RunItem::Process(p)) {
                            if let RunItem::Process(p) = err.0 {
                                finish(world, p);
                            }
                        }
                    } else {
                        fail(world, p, "failed to apply recv timeout");
                    }
                }
            }
        }
    }
}

fn fail(world: &Arc<World>, p: Box<Process>, msg: &str) {
    eprintln!("[pid {}] error: {msg}", p.shared.pid);
    p.shared.set_status_unless_killed(Status::Failed);
    finish(world, p);
}

/// Unregister a finished process and release its runtime.
fn finish(world: &Arc<World>, p: Box<Process>) {
    world.inboxes.lock().unwrap().remove(&p.shared.pid);
    world.processes.lock().unwrap().remove(&p.shared.pid);
    if *p.shared.status.lock().unwrap() == Status::Failed {
        world.failed.fetch_add(1, Ordering::Relaxed);
    }
    drop(p);
    let mut active = world.active.lock().unwrap();
    *active = active.saturating_sub(1);
    if *active == 0 {
        world.active_cv.notify_all();
    }
}

fn exception_message(ctx: &Context) -> String {
    ctx.with(|cx| process::describe_catch(&cx))
}
