use std::{
    cell::{Cell, RefCell},
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    mem::ManuallyDrop,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Poll, RawWaker, RawWakerVTable, Waker},
    thread,
};

macro_rules! print_task_ref {
    ($label: literal) => {
        TASKS.with(|val| {
            println!("{}", $label);
            for task in val.borrow().iter() {
                print!(" {:?}", Rc::<Task>::strong_count(task),);
            }
            println!("\n******");
        });
    };
}
type SharedQueue<T> = Rc<RefCell<VecDeque<T>>>;
use std::time::Instant;

use crate::io_uring_driver::{self, OpWaiter, U_DRIVER, UDriver};

#[derive(Debug)]
pub struct TimerEntry {
    instant: Instant,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.instant.cmp(&other.instant))
    }
}

impl Eq for TimerEntry {}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.instant.cmp(&other.instant)
    }
}

pub struct Task {
    future: RefCell<Pin<Box<dyn Future<Output = ()>>>>,
    queue: SharedQueue<Rc<Task>>,
}
impl Task {
    // use slab::Slab;
    pub fn new(future: impl Future<Output = ()> + 'static, queue: SharedQueue<Rc<Task>>) -> Self {
        Self {
            future: RefCell::new(Box::pin(future)),
            queue,
        }
    }

    pub fn schedule(self: &Rc<Self>) {
        self.queue.borrow_mut().push_back(self.clone());
    }
}

static VT: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

fn make_waker(task: Rc<Task>) -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(Rc::into_raw(task) as *const (), &VT)) }
}

unsafe fn clone_fn(p: *const ()) -> RawWaker {
    Rc::increment_strong_count(p as *const Task);
    // let task = Rc::from_raw(p as *const Task);
    // TASKS.with(|val| {
    //     for task in val.borrow().iter() {
    //         print!(" {:?}", Rc::<Task>::strong_count(task),);
    //     }
    // });
    RawWaker::new(p, &VT)
}

unsafe fn wake_fn(p: *const ()) {
    Rc::from_raw(p as *const Task).schedule();
}

unsafe fn wake_by_ref_fn(p: *const ()) {
    let task = ManuallyDrop::new(Rc::from_raw(p as *const Task));
    task.schedule();
}

unsafe fn drop_fn(p: *const ()) {
    // let task = Rc::from_raw(p as *const Task);
    // TASKS.with(|val| {
    //     println!("REC COUNTS");
    //     for task in val.borrow().iter() {
    //         print!(" {:?}", Rc::<Task>::strong_count(task),);
    //     }
    //     println!("\n******");
    // });
    drop(Rc::from_raw(p as *const Task));
}
thread_local! {
    static TIMER_HEAP: Rc<RefCell<BinaryHeap<Reverse<TimerEntry>>>> = Rc::new(RefCell::new(BinaryHeap::new()));
}

pub struct Shared<T> {
    value: Option<T>,
    waker: Option<Waker>,
}
pub struct JoinHandle<T> {
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if let Some(value) = self.shared.borrow_mut().value.take() {
            Poll::Ready(value)
        } else {
            self.shared.borrow_mut().waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
#[derive(Clone)]
pub struct Executor {
    queue: SharedQueue<Rc<Task>>,
    udriver: UDriver,
}

thread_local! {
    static CURRENT_EXECUTOR: Rc<Executor> = Rc::new(Executor::new());
}
impl Executor {
    pub fn new() -> Self {
        Self {
            queue: Rc::new(RefCell::new(VecDeque::new())),
            udriver: U_DRIVER.with(|driver| driver.clone()),
        }
    }

    pub fn current() -> Rc<Executor> {
        CURRENT_EXECUTOR.with(|executor| executor.clone())
    }

    pub fn spawn<T: 'static>(&self, future: impl Future<Output = T> + 'static) -> JoinHandle<T> {
        let shared = Rc::new(RefCell::new(Shared {
            value: None,
            waker: None,
        }));
        let shared_clone = shared.clone();
        let wrapper = async move {
            let result = future.await;
            shared_clone.borrow_mut().value = Some(result);
            if let Some(waker) = shared_clone.borrow_mut().waker.take() {
                waker.wake();
            }
        };
        self._spawn(wrapper);
        JoinHandle { shared }
    }

    pub fn _spawn(&self, future: impl Future<Output = ()> + 'static) {
        let tsk = Rc::new(Task::new(future, self.queue.clone()));

        {
            self.queue.borrow_mut().push_back(tsk);
        }
    }
    pub fn queue(&self) -> &SharedQueue<Rc<Task>> {
        &self.queue
    }

    pub fn run(&self) {
        loop {
            let ready: Vec<Rc<Task>> = self.queue.borrow_mut().drain(..).collect();
            for task in ready {
                let waker = make_waker(task.clone());
                let mut cx = std::task::Context::from_waker(&waker);
                let _ = task.future.borrow_mut().as_mut().poll(&mut cx);
            }
            if !self.queue().borrow().is_empty() {
                continue;
            }

            self.udriver.pull_completions();
        }
    }
}

thread_local! {
    pub static TASKS: RefCell<Vec<Rc<Task>>> = RefCell::new(Vec::new());
}

pub async fn timer(duration: std::time::Duration) {
    let op = io_uring_driver::Op {
        kind: io_uring_driver::OpKind::Timer {
            timer_spec: Box::new(io_uring::types::Timespec::from(duration)),
        },
        waker: None,
        result: None,
        orphaned: false,
    };
    let result = OpWaiter::new(-1, op, U_DRIVER.with(Clone::clone))
        .unwrap()
        .await
        .unwrap();
}
