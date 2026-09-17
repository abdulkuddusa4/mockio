// use std::future::Future;
// use std::pin::Pin;
// use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
// use std::sync::{Arc, Mutex};
// use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
// use std::thread;
// use std::time::Duration;
// pub mod executor;
// pub mod executor_v2;
// pub mod my_executor;

// // ============================================================
// //  THE FUTURE: becomes ready 200ms from now.
// //  The KEY LINE is marked ★ -- that's how it "registers a callback".
// // ============================================================
// struct Timer {
//     // shared flag: the background thread sets this true when the timer fires
//     fired: Arc<Mutex<bool>>,
//     started: bool,
// }

// impl Timer {
//     fn new() -> Self {
//         Timer {
//             fired: Arc::new(Mutex::new(false)),
//             started: false,
//         }
//     }
// }

// impl Future for Timer {
//     type Output = &'static str;

//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
//         // is the flag set? then we're done.
//         if *self.fired.lock().unwrap() {
//             return Poll::Ready("ding!");
//         }

//         // first poll only: spawn a thread that will fire the timer.
//         if !self.started {
//             self.started = true;

//             // ★★★ HERE is "future.callback_stuff" ★★★
//             // The future does NOT store the callback as a field. It GRABS the
//             // waker out of the Context it was handed, CLONES it, and moves the
//             // clone into the background thread. THAT is "registering a callback".
//             let waker: Waker = cx.waker().clone();

//             let fired = self.fired.clone();
//             thread::spawn(move || {
//                 thread::sleep(Duration::from_millis(200));
//                 *fired.lock().unwrap() = true; // set the flag
//                 waker.wake(); // ★ fire the callback: "re-poll me!"
//             });
//         }

//         // not ready yet
//         Poll::Pending
//     }
// }

// // ============================================================
// //  THE EXECUTOR: run one future, SLEEPING when it's pending
// //  instead of spinning. The waker's job is to wake us up.
// // ============================================================

// // A task-aware waker: waking pushes onto a channel the executor listens on.
// struct Signal {
//     sender: SyncSender<()>,
// }

// fn signal_waker(signal: Arc<Signal>) -> Waker {
//     let ptr = Arc::into_raw(signal) as *const ();
//     unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
// }

// static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

// unsafe fn clone_fn(ptr: *const ()) -> RawWaker {
//     Arc::increment_strong_count(ptr as *const Signal);
//     RawWaker::new(ptr, &VTABLE)
// }
// unsafe fn wake_fn(ptr: *const ()) {
//     let signal = Arc::from_raw(ptr as *const Signal);
//     let _ = signal.sender.send(()); // wake = send a ping to the executor
// }
// unsafe fn wake_by_ref_fn(ptr: *const ()) {
//     let signal = std::mem::ManuallyDrop::new(Arc::from_raw(ptr as *const Signal));
//     let _ = signal.sender.send(());
// }
// unsafe fn drop_fn(ptr: *const ()) {
//     drop(Arc::from_raw(ptr as *const Signal));
// }

// // block_on: poll the future; when Pending, BLOCK on the channel (0% CPU)
// // until the waker sends a ping, then poll again.
// fn block_on<F: Future>(fut: F) -> F::Output {
//     let (sender, receiver): (SyncSender<()>, Receiver<()>) = sync_channel(1);
//     let signal = Arc::new(Signal { sender });
//     let waker = signal_waker(signal);
//     let mut cx = Context::from_waker(&waker);

//     let mut fut = Box::pin(fut);
//     tokio::spawn(async move {});
//     loop {
//         match fut.as_mut().poll(&mut cx) {
//             Poll::Ready(v) => return v,
//             Poll::Pending => {
//                 // SLEEP here until the waker pings us. NOT a spin.
//                 println!("  [executor] pending -> sleeping until woken");
//                 receiver.recv().unwrap();
//                 println!("  [executor] woken -> polling again");
//             }
//         }
//     }
// }

// fn main() {
//     println!("starting timer future...");
//     let result = block_on(Timer::new());

//     // tokio::task::println!("result: {result}");
// }

use std::os::fd::IntoRawFd;
use std::time::{Duration, Instant};

use io_uring::types::io_uring_region_desc;
use mockio::executor::{self, timer};

use mockio::{TcpListener, TcpStream};
async fn main_task() {
    let listener = TcpListener::bind("127.0.0.1:7788".parse().unwrap());
    let mut id = 0;
    println!("listening on 127.0.0.1:7788");
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        println!("accepted connection from user - {id}");

        mockio::spawn(handle_connection(stream, id));

        id += 1;
    }
}

async fn handle_connection(mut stream: TcpStream, id: usize) {
    let mut buffer: Box<[u8]> = Box::new([0; 1024]);
    let (ln, buffer) = stream.read(buffer).await.unwrap();

    let get = b"GET / HTTP/1.1\r\n";
    let sleep = b"GET /sleep HTTP/1.1\r\n";

    let (status_line, filename) = if buffer.starts_with(get) {
        ("HTTP/1.1 200 OK", "index.html")
    } else if buffer.starts_with(sleep) {
        timer(Duration::from_secs(7)).await;
        ("HTTP/1.1 200 OK", "index.html")
    } else {
        ("HTTP/1.1 404 OK", "404.html")
    };

    let contents = std::fs::read_to_string(filename).unwrap();
    let response = "HTTP/1.1 200 OK\r\n\r\n";
    let response = format!(
        "{}\r\nContent-Length{}\r\n\r\n{}",
        status_line,
        contents.len(),
        contents
    );
    stream.write(response.as_bytes().into()).await.unwrap();
    // stream.flush().unwrap();
}
// use std::os::unix::io::{AsRawFd, RawFd};

fn main() {
    let executor = mockio::executor::Executor::current();
    // println!("executor: {:p}", &executor);

    executor.spawn(main_task());
    // println!("START {}", executor.queue().borrow().len());
    executor.run();
}
