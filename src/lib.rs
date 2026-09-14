pub mod executor;
pub mod io_driver;
pub use io_driver::{TcpListener, TcpStream};

use crate::executor::{Executor, JoinHandle};

pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let executor = Executor::current();
    executor.spawn(future)
}

pub struct BufferSlab {
    buffer: *const (),
    size: usize,
    next: usize,
}
