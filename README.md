# mockio

A single-threaded async runtime for Rust, built from scratch on Linux
**io_uring**. It includes a hand-written executor, waker and join handles, a
completion-based I/O driver, TCP and timers, and a small HTTP server that runs
on top of it.

The project started as a readiness-based reactor on `mio`/epoll (commit
`e4ad7e4`) and was rewritten onto io_uring's completion model. Both versions
were benchmarked against each other and against tokio. The results are below,
including the parts that didn't go the way I expected.

## Highlights

- **Executor from first principles.** Tasks are `Rc`-allocated futures driven
  by a custom `RawWakerVTable`, with a run queue and awaitable `JoinHandle`s.
  Nothing comes from an existing runtime.
- **Completion-based I/O on io_uring.** Accept, read, write and timeouts are
  submitted as SQEs and resumed from CQEs. Each in-flight operation lives in a
  slab, and its slab key is the SQE's `user_data`.
- **Owned-buffer API.** The kernel writes into a buffer *after* the call
  returns, so the API passes ownership in and gets it back:
  `let (n, buf) = stream.read(buf).await?`. This is the same design tokio-uring
  and glommio use, and it isn't optional for completion-based I/O.
- **Cancellation-safe.** If a future is dropped while its operation is still
  in flight, the operation is marked orphaned and its buffers stay alive until
  the kernel's completion arrives. Dropping a future can never free memory the
  kernel is about to write into.
- **Timers on the ring.** `IORING_OP_TIMEOUT` instead of a userspace timer
  heap.
- **Kernel-validated tests.** `sockaddr` decoding is tested against real
  `accept(2)` results on IPv4 and IPv6 loopback, not against a hand-built
  encoder.

## Quick start

Requires Linux 5.6+ with io_uring enabled
(`/proc/sys/kernel/io_uring_disabled` must be `0`) and Rust 1.85+
(edition 2024).

```sh
cargo run --release
```

```sh
curl -i http://127.0.0.1:7788/        # 200, serves index.html
curl -i http://127.0.0.1:7788/nope    # 404, serves 404.html
```

`/sleep` waits 7 seconds on an io_uring timer before responding. It shows
concurrency on a single thread: while a `/sleep` request is pending, other
requests are still served immediately.

```sh
curl http://127.0.0.1:7788/sleep &    # parked on a timer
curl -i http://127.0.0.1:7788/        # still answers right away
```

Run the tests:

```sh
cargo test --lib
```

## Usage

```rust
use std::time::Duration;
use mockio::{TcpListener, TcpStream};

async fn handle(stream: TcpStream) -> std::io::Result<()> {
    let buf: Box<[u8]> = vec![0u8; 1024].into_boxed_slice();

    // Ownership of `buf` goes to the kernel for the duration of the read
    // and comes back with the result.
    let (n, buf) = stream.read(buf).await?;

    mockio::timer(Duration::from_millis(10)).await;

    let (_written, _buf) = stream.write(buf[..n].into()).await?;
    Ok(())
}

async fn serve() {
    let listener = TcpListener::bind("127.0.0.1:7788".parse().unwrap());
    loop {
        let (stream, _peer) = listener.accept().await.unwrap();
        mockio::spawn(handle(stream));
    }
}

fn main() {
    let executor = mockio::executor::Executor::current();
    executor.spawn(serve());
    executor.run();
}
```

## Architecture

```
            ┌────────────────────── Executor::run ───────────────────────┐
            │                                                            │
 spawn ───▶ │  run queue ──drain──▶ poll each task                       │
            │      ▲                    │                               │
            │      │ wake               │ I/O future returns Pending    │
            │      │                    ▼                               │
            │      │             OpWaiter::new ──▶ slab insert (token)  │
            │      │                    │          SQE.user_data = token│
            │      │                    ▼                               │
            │      │            ┌── io_uring ──┐                        │
            │      │            │  SQ  ───▶ kernel ───▶  CQ │           │
            │      │            └──────────────┘          │             │
            │      │                                      ▼             │
            │      └──── pull_completions: submit_and_wait(1),          │
            │            store result in slab[token], wake task         │
            └────────────────────────────────────────────────────────────┘
```

**Executor** (`src/executor_v2.rs`). A task is an
`Rc<Task>` holding a pinned, boxed future. A `Waker` is that same `Rc` passed
through a custom vtable, and waking pushes the task back onto the run queue.
`run()` drains the queue *before* polling, because a task that spawns or wakes
during its own poll would otherwise hit an already-borrowed `RefCell`. When
the queue is empty the executor blocks in the driver until a completion
arrives.

**Driver** (`src/io_uring_driver.rs`). Every operation is an `Op` that owns
everything the kernel will touch: the read or write buffer, the
`sockaddr_storage` for accept, the `Timespec` for a timeout. The `Op` sits in
a slab for as long as it's in flight, and the boxed buffers don't move, so the
pointers inside the SQE stay valid. `OpWaiter` is the future side: it parks a
waker in the slab entry and, once the completion lands, removes the entry and
hands the buffers back to the caller.

**Runtime state is thread-local.** Everything is `Rc`/`RefCell` and `!Send`
by design. Resources such as `TcpListener` and `TcpStream` hold their own
handle to the driver instead of looking it up from thread-local storage in
`Drop`, because thread-locals can be destroyed before the values that
reference them at thread exit.

## Benchmarks

An HTTP server with identical handler logic, run on three runtimes:

- **mockio io_uring**: this version
- **mockio epoll**: the earlier `mio` reactor from this repo (commit `e4ad7e4`)
- **tokio**: `current_thread` runtime, as a reference point

**Setup.** Intel i5-10210U (4C/8T), Linux 6.12, rustc 1.98.1, `--release`,
CPU set to performance mode. The server is pinned to core 0 and the load
generator to cores 1–3. Clients run closed-loop, opening one connection per
request. The response body is served from memory. Each configuration ran 5
interleaved repetitions of 4 seconds, medians are reported, and no run was
thermally throttled.

### Throughput (requests/s)

| concurrent clients | io_uring | epoll | tokio |
|---|---|---|---|
| 1  | 22,838 | 23,854 | 21,848 |
| 4  | 49,510 | 51,502 | 47,007 |
| 16 | 54,188 | 57,764 | 53,357 |

### Per-request cost at 16 concurrent clients

| | io_uring | epoll | tokio |
|---|---|---|---|
| syscalls / request | **1.04** | 3.17 | — |
| CPU time / request | 18.1 µs | **16.9 µs** | 18.3 µs |
| task polls / request | 4.00 | **1.09** | — |
| heap allocations / request\* | 18.0 | 7.3 | **6.6** |
| p50 / p99 latency | 283 / 573 µs | 271 / 569 µs | 290 / 580 µs |

\*Includes about 2 allocations per request from the benchmark harness itself,
which is identical across all three.

### What the numbers mean

**The io_uring driver does what it was built for.** It cuts syscalls per
request about 3x at load (3.17 down to 1.04), because accept, read and write
are batched into roughly one `io_uring_enter`. At a single client it still
saves about 40% (5.02 down to 3.00).

**It doesn't translate into throughput on this workload.** All three runtimes
land within about 6% of each other, and the epoll version is marginally
fastest. Two measurements explain why:

- **Syscalls are cheap compared to the rest of the request.** A bare syscall
  measured 102 ns on this machine. Saving 2.1 syscalls per request saves about
  0.2 µs out of an 18 µs request, which is around 1%.
- **Completion I/O always pays a round trip.** On loopback under load, the
  epoll version's non-blocking `accept`, `read` and `write` almost always
  succeed on the first try, so a request finishes in about one poll. The
  io_uring version suspends and resumes on every operation (4 polls per
  request). That costs more than the syscalls it saves.

The server is CPU-bound and single-threaded, so throughput tracks
1 / CPU-per-request to within 2%. Most of that CPU time goes to the kernel's
TCP connection setup and teardown, which costs the same no matter which driver
is used. io_uring should pull ahead on workloads where the driver dominates:
keep-alive connections, real network latency (where reads actually block),
large numbers of idle connections, and disk I/O.

## Engineering notes

A few problems from building this that turned out to be more interesting than
expected:

- **Benchmark hygiene.** The first measurements had 50–190% run-to-run spread
  and appeared to show 2–3x differences between drivers. The cause was a
  browser running at 160% CPU in the background and a CPU that was thermally
  throttling (package throttle counter above 270,000). On a quiet machine,
  with per-run throttle detection, spread dropped to 3–9% and the apparent
  differences disappeared. After that I treated deterministic counters
  (syscalls and allocations per request) as the primary evidence and
  wall-clock numbers as supporting evidence.
- **Re-entrancy in a single thread.** Holding the run queue's `RefCell` borrow
  while polling caused a panic as soon as a task spawned another task. Being
  single-threaded doesn't prevent aliasing; callbacks re-entering the executor
  cause it.
- **Destructor ordering at thread exit.** `Drop` impls that reached into a
  thread-local aborted the process, because thread-locals are destroyed in an
  unspecified order. The fix was for each resource to own a handle to the
  driver.
- **Cancellation in completion-based I/O.** With readiness I/O, dropping a
  future is harmless. With completion I/O, the kernel may still be writing
  into the dropped future's buffer. Orphaned operations keep their buffers
  until the completion is reaped.

## Limitations and roadmap

- Single-threaded and Linux-only, both by design.
- `Executor::run` never returns, and there is no graceful shutdown.
- `TcpStream::write` issues a single write and doesn't loop on short writes.
- Spawning costs three allocations (boxed future, `Rc<Task>`, join state).
  It could be one.
- `submit_and_wait(1)` returns on the first completion, which limits batching
  at low concurrency.
- None of io_uring's advanced features are used yet: multishot accept,
  provided buffer rings, registered file descriptors.
- The demo server's HTTP is deliberately minimal: it prefix-matches the
  request line and closes the connection after each response.

## Project layout

```
src/
  executor_v2.rs      executor, tasks, waker vtable, JoinHandle, timer
  io_uring_driver.rs  ring, in-flight op table, OpWaiter, TcpListener/TcpStream,
                      sockaddr decoding (+ tests)
  lib.rs              public API: spawn, timer, TcpListener, TcpStream
  main.rs             demo HTTP server
index.html, 404.html  pages served by the demo
```
