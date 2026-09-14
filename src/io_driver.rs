use slab::Slab;
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

#[derive(Debug)]
struct WakerSlot {
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    readable: bool,
    writable: bool,
}

impl Default for WakerSlot {
    fn default() -> Self {
        Self {
            read_waker: None,
            write_waker: None,
            readable: false,
            writable: false,
        }
    }
}

#[derive(Debug)]
struct IoDriverInner {
    pool: mio::Poll,
    waker_map: Slab<WakerSlot>,
}

#[derive(Debug, Clone)]
pub struct IoDriver(pub Rc<RefCell<IoDriverInner>>);
thread_local! {
    pub static IO_DRIVER: IoDriver = IoDriver::new();
}

impl IoDriver {
    fn new() -> Self {
        Self(Rc::new(
            IoDriverInner {
                pool: mio::Poll::new().unwrap(),
                waker_map: Default::default(),
            }
            .into(),
        ))
    }

    pub fn pull_events(&self, timeout: Option<Duration>) -> io::Result<()> {
        let mut events = mio::Events::with_capacity(1024);

        {
            println!("polling: {:?}", timeout);
            self.0.borrow_mut().pool.poll(&mut events, timeout)?;
        }

        let mut waker_list = Vec::new();
        for event in events.iter() {
            // event.isre
            let key = event.token().0;
            let mut inner = self.0.borrow_mut();
            if event.is_readable() {
                if let Some(slot) = inner.waker_map.get_mut(key) {
                    slot.readable = true;
                    if let Some(waker) = slot.read_waker.take() {
                        waker_list.push(waker);
                    }
                }
            }
            if event.is_writable() {
                if let Some(slot) = inner.waker_map.get_mut(key) {
                    slot.writable = true;
                    if let Some(waker) = slot.write_waker.take() {
                        waker_list.push(waker);
                    }
                }
            }
        }
        for w in waker_list {
            w.wake();
        }
        Ok(())
    }
}
struct WakerInjector {
    token: usize,
    read: bool,
}

impl Future for WakerInjector {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let io_driver = IO_DRIVER.with(|driver| driver.clone());
        let mut io_driver_inner_mut = io_driver.0.borrow_mut();
        let waker_slot = io_driver_inner_mut.waker_map.get_mut(self.token).unwrap();

        if self.read {
            if waker_slot.readable {
                waker_slot.readable = false;
                return Poll::Ready(());
            }
            waker_slot.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        } else {
            if waker_slot.writable {
                waker_slot.writable = false;
                return Poll::Ready(());
            }
            waker_slot.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
    }
}

pub struct TcpListener {
    inner: mio::net::TcpListener,
    token: usize,
    // Own a handle to the driver so `Drop` never has to reach into IO_DRIVER,
    // which may already be destroyed when we run at thread exit.
    driver: IoDriver,
}

impl TcpListener {
    pub fn new(mut tcp_listener: mio::net::TcpListener) -> Self {
        let driver = IO_DRIVER.with(|driver| driver.clone());
        let token = {
            let mut inner = driver.0.borrow_mut();
            let token = inner.waker_map.insert(Default::default());
            inner
                .pool
                .registry()
                .register(
                    &mut tcp_listener,
                    mio::Token(token),
                    mio::Interest::READABLE,
                )
                .unwrap();
            token
        };
        Self {
            inner: tcp_listener,
            token,
            driver,
        }
    }
    pub fn bind(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let tcp_listener = mio::net::TcpListener::bind(addr)?;
        Ok(Self::new(tcp_listener))
    }

    pub async fn accept(&self) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
        loop {
            match self.inner.accept() {
                Ok((tcp_stream, addr)) => return Ok((TcpStream::new(tcp_stream), addr)),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    WakerInjector {
                        token: self.token,
                        read: true,
                    }
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        let mut inner = self.driver.0.borrow_mut();
        let _ = inner.pool.registry().deregister(&mut self.inner);
        inner.waker_map.remove(self.token);
    }
}

pub struct TcpStream {
    inner: mio::net::TcpStream,
    token: usize,
    driver: IoDriver,
}

impl TcpStream {
    fn new(mut tcp_stream: mio::net::TcpStream) -> Self {
        let driver = IO_DRIVER.with(|driver| driver.clone());
        let token = {
            let mut inner = driver.0.borrow_mut();
            let token = inner.waker_map.insert(Default::default());
            inner
                .pool
                .registry()
                .register(
                    &mut tcp_stream,
                    mio::Token(token),
                    mio::Interest::READABLE | mio::Interest::WRITABLE,
                )
                .unwrap();
            token
        };
        Self {
            inner: tcp_stream,
            token,
            driver,
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.read(buf) {
                // Ok(0) => return Ok(0),
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    WakerInjector {
                        token: self.token,
                        read: true,
                    }
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.write(buf) {
                // Ok(0) => return Ok(0),
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    WakerInjector {
                        token: self.token,
                        read: false,
                    }
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        loop {
            match self.inner.write_all(buf) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    WakerInjector {
                        token: self.token,
                        read: false,
                    }
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let mut inner = self.driver.0.borrow_mut();
        let _ = inner.pool.registry().deregister(&mut self.inner);
        inner.waker_map.remove(self.token);
    }
}
