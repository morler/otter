# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
cargo build --release                       # binary is target/release/otter
cargo run --release -- examples/ping_pong.js
cargo run --release -- --workers 1 examples/ring.js
cargo test                                  # 35 e2e + 13 rpc + 3 cli tests; the run takes under 2 s
cargo test --test e2e sleep_resumes         # one test: --test <file in tests/>, then a name filter (substring match)
cargo test --test rpc rpc_shutdown
cargo clippy --all-targets                  # clean
```

## Architecture

One process = one QuickJS `Runtime` + `Context` + mailbox (`src/process.rs`). Processes never share JS state. A fixed set of OS worker threads (`scheduler::worker_loop`) pulls `Box<Process>` values off a crossbeam run queue. A process moves between threads freely and runs one job per slice.

**Shared state.** Everything mutable lives in `World` (`src/scheduler.rs`): the run queue, the `parked` and `sleeping` maps, the timer heap, inbox senders, the `processes` registry, the `active` counter, and the `failed` counter. `main.rs` and `rpc.rs` both hold an `Arc<World>`.

**Slice (`run_slice`).** Wake the process according to its status, run exactly one pending job (`execute_pending_job`; `timed_job` times it when `World::max_job` / `--max-job-ms` is set, and an overrun fails the process), then route on the resulting `Status`: finish, park, sleep, or re-queue.

**Creation runs on the caller's thread.** `spawn_process` evaluates the child's source up to its first `await` synchronously. The thread that calls spawn does this work: a worker for JS `spawn`, or the RPC connection thread for RPC `spawn`.

**Suspension is a handshake between JS and Rust.** `recv`, `sleep`, and `yieldNow` (`process.rs::setup_globals`) store a Promise resolver in a JS global (`__otter_recv_resolve`, `__otter_sleep_resolve`, `__otter_yield_resolve`), set `ProcShared.status`, and return. The scheduler fires the stored resolver later (`wake_with`, `wake_unit`, `wake_timeout`). Renaming one of these globals means changing both files.

**Two wait maps, one timer heap.** `parked` (waiting on `recv`) is woken by `send`. `sleeping` is woken only by its timer, so `send` never interrupts `sleep`. Timers live in `World.timers`. An entry is not removed when a message arrives. `service_timers` therefore also compares the parked process's `deadline` with the entry's, so a stale `RecvTimeout` cannot reject a later `recv()`. Regression test: `recv_timeout_of_an_earlier_wait_does_not_fire_later`.

**Race rules. Keep these orderings:**
- `deliver_json_string` pushes the message into the mailbox before it removes the pid from `parked` (push, then unpark).
- `park` re-checks the inbox while it holds the `parked` lock.
- `service_timers` pops due entries under the `timers` lock, then wakes processes without holding it.

**Timers are checked before every worker wait.** `worker_loop` calls `service_timers` on each iteration before it waits for the next item, so a busy run queue cannot starve sleepers. An idle worker also wakes every `TIMER_TICK` (5 ms) to check timers. Regression test: `busy_queue_does_not_starve_sleep`.

**Status and kill.** `Status` (`process.rs`) is the state machine. `Killed` is terminal. Suspend and wake paths write through `ProcShared::set_status_unless_killed`, so a pending kill is not overwritten. `kill_process` sets `Killed` and moves a parked or sleeping process back onto the run queue, so `run_slice` reaps it. A killed process is not counted as failed.

**Exit.** `finish` increments `World.failed` for each `Failed` process. `main` exits 1 when that counter is non-zero. `main` waits on `active_cv` until `active == 0`, unless `--persistent` or `--rpc-port` keeps the runtime alive.

**Sandbox.** `Sandbox` (`process.rs`) is a set of on/off toggles that only narrow. Today there is one toggle, `can_spawn_and_kill`. A child starts from its parent's sandbox and can only narrow it. The checks live in the JS callbacks (`spawn`, `killProcess`, `restrictSandbox`), not in the scheduler.

**RPC (`src/rpc.rs`).** A second front end over the same `World`. `dispatch` calls scheduler functions directly and never touches QuickJS. `send` passes already-serialized JSON to `deliver_json_string`. Each connection gets its own thread, capped at 256 live threads. `shutdown` sets the shared `AtomicBool` that `serve_loop` polls.

## Tests

- `tests/e2e.rs`: each test builds its own `World` and worker thread, then runs real JS. Assertions are JS `throw`s inside the spawned process, not Rust asserts. A failure increments `World.failed` and writes `[pid N] error: ...` to stderr. `wait_for_completion` then fails with only the `failed == 0` check, so read stderr when an e2e test fails.
- `tests/rpc.rs`: one request per TCP connection, against an ephemeral port (`start_harness` / `teardown`).
- `docs/api.md` is the authoritative JS API reference. Don't duplicate it here.
