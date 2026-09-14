//! Same micro-benchmarks as `bench_core`, but on tokio's current_thread
//! runtime, to give the mockio numbers a reference point.
//!
//! Run: cargo run --release --bin bench_core_tokio

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size(), Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(new.saturating_sub(l.size()), Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn allocs() -> (usize, usize) {
    (ALLOCS.load(Relaxed), BYTES.load(Relaxed))
}

/// Identical to the mockio version so both runtimes measure the same thing.
struct Yield(bool);

impl Future for Yield {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn yield_now() -> Yield {
    Yield(false)
}

fn report(name: &str, n: usize, dt: Duration, da: usize, db: usize) {
    let per = dt.as_nanos() as f64 / n as f64;
    println!(
        "{:<34} {:>10.0} ns/op  {:>12.0} op/s  {:>6.2} allocs/op  {:>7.1} B/op",
        name,
        per,
        1e9 / per,
        da as f64 / n as f64,
        db as f64 / n as f64,
    );
}

async fn bench_spawn_complete(n: usize) {
    {
        let mut h = Vec::new();
        for _ in 0..1000 {
            h.push(tokio::spawn(async { black_box(0u64) }));
        }
        for x in h {
            black_box(x.await.unwrap());
        }
    }

    let (a0, b0) = allocs();
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        handles.push(tokio::spawn(async { black_box(0u64) }));
    }
    let spawn_dt = t0.elapsed();
    let (a1, b1) = allocs();

    for h in handles {
        black_box(h.await.unwrap());
    }
    let total_dt = t0.elapsed();
    let (a2, b2) = allocs();

    report("spawn (enqueue only)", n, spawn_dt, a1 - a0, b1 - b0);
    report("spawn + run to completion", n, total_dt, a2 - a0, b2 - b0);
}

async fn bench_yield_single(n: usize) {
    for _ in 0..1000 {
        yield_now().await;
    }

    let (a0, b0) = allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        yield_now().await;
    }
    let dt = t0.elapsed();
    let (a1, b1) = allocs();

    report("wake+reschedule (1 task/tick)", n, dt, a1 - a0, b1 - b0);
}

async fn bench_yield_batched(tasks: usize, iters: usize) {
    let (a0, b0) = allocs();
    let t0 = Instant::now();

    let mut handles = Vec::with_capacity(tasks);
    for _ in 0..tasks {
        handles.push(tokio::spawn(async move {
            for _ in 0..iters {
                yield_now().await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let dt = t0.elapsed();
    let (a1, b1) = allocs();
    let n = tasks * iters;

    report(
        &format!("wake+reschedule ({tasks} tasks/tick)"),
        n,
        dt,
        a1 - a0,
        b1 - b0,
    );
}

async fn bench_deep_chain(n: usize) {
    let (a0, b0) = allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        let h = tokio::spawn(async { black_box(1u64) });
        black_box(h.await.unwrap());
    }
    let dt = t0.elapsed();
    let (a1, b1) = allocs();
    report("spawn+await serially (chained)", n, dt, a1 - a0, b1 - b0);
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        println!(
            "\ntokio current_thread - core micro-benchmarks (single threaded)\n{}",
            "-".repeat(94)
        );

        bench_spawn_complete(200_000).await;
        bench_deep_chain(200_000).await;
        bench_yield_single(500_000).await;
        bench_yield_batched(1, 200_000).await;
        bench_yield_batched(64, 5_000).await;
        bench_yield_batched(1024, 500).await;

        println!("{}", "-".repeat(94));
    });
}
