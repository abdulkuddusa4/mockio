# mockio

A single-threaded async runtime for Rust, built from scratch: executor,
wakers, join handles, an I/O reactor, TCP, timers, and a small HTTP server
running on top of it.

> **The io_uring version lives on the
> [`uring` branch](https://github.com/abdulkuddusa4/mockio/tree/uring).**
> That's the current version of the project. It uses completion-based I/O on
> Linux io_uring with a cancellation-safe owned-buffer API, runs timers on the
> ring, and includes the full benchmark write-up.
>
> This branch (`master`) holds the earlier **readiness-based version built on
> mio/epoll**. The io_uring version was rewritten from it and benchmarked
> against it.

## What's on this branch

- **Executor from first principles.** Tasks are `Rc`-allocated futures driven
  by a custom `RawWakerVTable`, with a run queue and awaitable `JoinHandle`s.
  Nothing comes from an existing runtime.
- **Readiness-based I/O on epoll (via mio).** Every socket operation first
  tries the non-blocking syscall. On `WouldBlock`, the task parks its waker in
  a slab slot keyed by the socket's mio token and is woken when epoll reports
  the socket ready.
- **Borrowed-buffer API.** `stream.read(&mut buf).await` works because, in the
  readiness model, the actual read happens synchronously in your code once the
  socket is ready. The io_uring branch can't do this: the kernel writes into
  the buffer after the call returns, so it has to own the buffer.
- **Timers on a binary heap.** Deadlines go into a min-heap, and the earliest
  one becomes the `epoll_wait` timeout.
- **Safe teardown.** `TcpListener` and `TcpStream` hold their own handle to
  the reactor instead of looking it up from thread-local storage in `Drop`,
  because thread-locals can be destroyed first when the thread exits.

## Quick start

Requires Linux and Rust 1.85+ (edition 2024).

```sh
cargo run --release
```

```sh
curl -i http://127.0.0.1:7788/        # 200, serves index.html
curl -i http://127.0.0.1:7788/nope    # 404, serves 404.html
```

`/sleep` waits on a 7-second timer before responding. Other requests are still
served right away while it's pending, all on one thread.

## Usage

```rust
use std::time::{Duration, Instant};
use mockio::executor::{Executor, Timer};
use mockio::{TcpListener, TcpStream};

async fn handle(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];

    // Borrowed buffer: we wait for readiness, then the read happens here.
    let n = stream.read(&mut buf).await?;

    Timer::new(Instant::now() + Duration::from_millis(10)).await;

    stream.write_all(&buf[..n]).await
}

async fn serve() {
    let listener = TcpListener::bind("127.0.0.1:7788".parse().unwrap()).unwrap();
    loop {
        let (stream, _peer) = listener.accept().await.unwrap();
        mockio::spawn(handle(stream));
    }
}

fn main() {
    let executor = Executor::current();
    executor.spawn(serve());
    executor.run();
}
```

## How it works

Each pass of `Executor::run` does four things:

1. **Poll ready tasks.** The run queue is drained into a local list *before*
   polling. A task that spawns or wakes while it's being polled would
   otherwise hit an already-borrowed `RefCell`.
2. **Fire expired timers.** Entries are popped from the deadline heap and
   their tasks are woken.
3. **Loop again** if any of those tasks became ready.
4. **Block in epoll** until a socket becomes ready or the next timer is due,
   then wake the tasks parked on those sockets.

The reactor (`src/io_driver.rs`) keeps one slot per registered socket, holding
a read waker, a write waker and readiness flags. A socket future such as
`read` loops: try the syscall, and on `WouldBlock` park the waker and return
`Pending`.

## epoll vs io_uring

The two branches implement the same runtime on the two I/O models Linux
offers. They were benchmarked with identical HTTP handler logic; the
methodology and full analysis are in the
[`uring` branch README](https://github.com/abdulkuddusa4/mockio/tree/uring).

| at 16 concurrent clients | master (epoll) | uring |
|---|---|---|
| I/O model | readiness: "the socket is ready, now do the I/O" | completion: "here's the I/O, tell me when it's done" |
| buffer API | borrowed `&mut [u8]` | owned `Box<[u8]>`, passed in and handed back |
| timers | userspace binary heap | `IORING_OP_TIMEOUT` |
| syscalls / request | 3.17 | **1.04** |
| task polls / request | **1.09** | 4.00 |
| throughput | **57,764 req/s** | 54,188 req/s |

io_uring cuts syscalls about 3x, but on this workload the epoll version is
still slightly faster. On loopback under load, epoll's non-blocking syscalls
almost always succeed on the first try, so a request finishes in about one
poll. The io_uring version pays a suspend-and-resume round trip on every
operation, and that costs more than the syscalls it saves.

The epoll numbers were measured with the two `println!` calls removed from
the event loop (`src/executor.rs` and `src/io_driver.rs`). They're still on
this branch and cost about 12% of throughput.

## Limitations

- Single-threaded and Linux-only, both by design.
- `Executor::run` never returns, and there is no graceful shutdown.
- The reactor allocates a 1024-entry `mio::Events` buffer (about 12 KB) on
  every poll instead of reusing one.
- Spawning costs three allocations (boxed future, `Rc<Task>`, join state).
- The demo server's HTTP is deliberately minimal: it prefix-matches the
  request line and closes the connection after each response.

## Project layout

```
src/
  executor.rs   executor, tasks, waker vtable, JoinHandle, timer heap
  io_driver.rs  mio/epoll reactor, TcpListener, TcpStream
  lib.rs        public API: spawn, TcpListener, TcpStream
  main.rs       demo HTTP server
index.html, 404.html  pages served by the demo
```
