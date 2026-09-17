// pub mod executor;
pub mod executor_v2;
pub use executor_v2 as executor;
pub mod io_uring_driver;
use crate::executor::{Executor, JoinHandle};
pub use executor::timer;
pub use io_uring_driver::{TcpListener, TcpStream};
// pub use executor::T

pub fn spawn<T: 'static>(future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let executor = Executor::current();
    // println!("before spawn count {}", executor.queue().borrow().len());
    // println!("executor: {:p}", &executor);
    let handle = executor.spawn(future);
    // {
    //     let borrow_queue = &*executor.queue().borrow();
    //     println!("after spawn {:p}", borrow_queue);
    //     println!("after spawn count {}", borrow_queue.len());
    // }
    handle
}
