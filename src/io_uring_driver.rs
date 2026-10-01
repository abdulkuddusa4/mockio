use std::{
    cell::RefCell,
    fmt::Debug,
    io,
    os::fd::{AsRawFd, FromRawFd},
    rc::Rc,
    task::Waker,
};

use io_uring::types;
use slab::Slab;

use crate::io_uring_driver::OpKind::Accept;

pub enum OpKind {
    Accept {
        addr: Box<libc::sockaddr_storage>,
        addr_len: Box<libc::socklen_t>,
    },
    Read {
        buffer: Box<[u8]>,
    },
    Write {
        buffer: Box<[u8]>,
    },
    Timer {
        timer_spec: Box<io_uring::types::Timespec>,
    },
}
impl Debug for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpKind")
            .field(
                "kind",
                match &self {
                    Self::Accept { .. } => &"Accept",
                    Self::Read { .. } => &"Read",
                    Self::Write { .. } => &"Write",
                    Self::Timer { .. } => &"Timer",
                },
            )
            .finish()
    }
}

pub(crate) struct Op {
    pub kind: OpKind,
    pub waker: Option<Waker>,
    pub result: Option<i32>,
    pub orphaned: bool,
}

impl Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Op")
            .field("kind", &self.kind)
            // .field("waker", &self.waker)
            // .field("result", &self.result)
            .finish()
    }
}

impl Op {
    fn create_sq_entry(&mut self, fd: i32, token: usize) -> io_uring::squeue::Entry {
        match &mut self.kind {
            OpKind::Accept { addr, addr_len } => {
                let addr_ptr = Box::as_mut_ptr(addr) as *mut libc::sockaddr;
                let addr_len_ptr = Box::as_mut_ptr(addr_len);
                io_uring::opcode::Accept::new(types::Fd(fd), addr_ptr, addr_len_ptr)
                    .build()
                    .user_data(token as u64)
            }
            OpKind::Read { buffer } => {
                let buffer_ptr = Box::as_mut_ptr(buffer) as *mut u8;
                io_uring::opcode::Read::new(types::Fd(fd), buffer_ptr, buffer.len() as u32)
                    .build()
                    .user_data(token as u64)
            }
            OpKind::Write { buffer } => {
                let buffer_ptr = Box::as_mut_ptr(buffer) as *mut u8;
                io_uring::opcode::Write::new(types::Fd(fd), buffer_ptr, buffer.len() as u32)
                    .build()
                    .user_data(token as u64)
            }
            OpKind::Timer { timer_spec } => io_uring::opcode::Timeout::new(Box::as_ptr(timer_spec))
                .build()
                .user_data(token as u64),
        }
    }

    fn result(&self) -> Option<i32> {
        self.result
    }
}
pub struct OpResult {
    pub result: i32,
    pub kind: OpKind,
}

impl OpResult {
    pub fn new(result: i32, kind: OpKind) -> Self {
        Self { result, kind }
    }
    pub fn into_accept(self) -> (i32, Box<libc::sockaddr_storage>, Box<libc::socklen_t>) {
        match self.kind {
            OpKind::Accept { addr, addr_len } => (self.result, addr, addr_len),
            _ => panic!(
                "not an accept op. caller: {:?}",
                std::panic::Location::caller()
            ),
        }
    }

    pub fn into_read(self) -> (i32, Box<[u8]>) {
        match self.kind {
            OpKind::Read { buffer } => (self.result, buffer),
            _ => panic!(
                "not a read op. caller: {:?}",
                std::panic::Location::caller()
            ),
        }
    }

    pub fn into_write(self) -> (i32, Box<[u8]>) {
        match self.kind {
            OpKind::Write { buffer } => (self.result, buffer),
            _ => panic!(
                "not a write op. caller: {:?}",
                std::panic::Location::caller()
            ),
        }
    }
}

// #[derive(Debug)]
pub(crate) struct UDriverInner {
    pub(crate) uring: io_uring::IoUring,
    pub(crate) op_map: Slab<Op>,
}

impl Debug for UDriverInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UDriverInner")
            .field("op_map", &self.op_map)
            .finish()
    }
}

#[derive(Clone)]
pub struct UDriver(pub Rc<RefCell<UDriverInner>>);

thread_local! {
    pub static U_DRIVER: UDriver = UDriver::new();
}

impl UDriver {
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(UDriverInner {
            uring: io_uring::IoUring::new(256).unwrap(),
            op_map: Slab::new(),
        })))
    }
}

impl UDriver {
    pub fn pull_completions(&self) {
        let mut inner_driver = &self.0;

        let mut waker_list;
        inner_driver.borrow().uring.submit_and_wait(1);
        {
            let mut inner_driver = inner_driver.borrow_mut();
            let completions: Vec<_> = inner_driver.uring.completion().collect();
            waker_list = Vec::with_capacity(completions.len());

            for completion in completions {
                let token = completion.user_data();
                let orphaned = match inner_driver.op_map.get_mut(token as usize) {
                    Some(op) => {
                        if op.orphaned {
                            true
                        } else {
                            op.result = Some(completion.result());
                            if let Some(waker) = op.waker.take() {
                                waker_list.push(waker);
                            }
                            false
                        }
                    }
                    None => continue,
                };
                if orphaned {
                    inner_driver.op_map.try_remove(token as usize);
                }
            }
        }
        for waker in waker_list {
            waker.wake();
        }
    }

    // the caller must ensure that the fd is valid and the op is valid on that fd.
    pub fn submit(&self, fd: i32, mut op: Op) -> io::Result<usize> {
        let mut inner_driver = self.0.borrow_mut();

        let vac_entry = inner_driver.op_map.vacant_entry();
        let token = vac_entry.key();
        let entry = op.create_sq_entry(fd as i32, token);
        // println!("operation: {:?}", &op);
        vac_entry.insert(op);
        unsafe {
            if inner_driver.uring.submission().push(&entry).is_err() {
                if inner_driver.uring.submit().is_err() {
                    return Err(io::Error::new(io::ErrorKind::Other, "submit failed"));
                }
                if inner_driver.uring.submission().push(&entry).is_err() {
                    // println!("submit failed. caller: file: {}:{}", file!(), line!());
                    return Err(io::Error::new(io::ErrorKind::Other, "submit failed"));
                }
            }
        }
        Ok(token)
    }
}

pub struct OpWaiter {
    token: usize,
    udriver: UDriver,
}

impl Drop for OpWaiter {
    fn drop(&mut self) {
        let mut inner_driver = self.udriver.0.borrow_mut();
        let slot = inner_driver.op_map.get_mut(self.token);
        let is_result = match slot {
            Some(slot) => {
                slot.orphaned = true;
                slot.result.is_some()
            }
            None => {
                return ();
            }
        };
        if is_result {
            inner_driver.op_map.try_remove(self.token);
        }
    }
}

impl OpWaiter {
    pub fn new(fd: i32, op: Op, udriver: UDriver) -> std::io::Result<Self> {
        // println!("OpWaiter::new: submitting..");
        let token = U_DRIVER.with(|udriver| {
            udriver
                .submit(fd, op)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })?;
        Ok(Self {
            token: token,
            udriver,
        })
    }
}

impl Future for OpWaiter {
    type Output = std::io::Result<OpResult>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // println!("pooling");
        let mut inner_driver = self.udriver.0.borrow_mut();
        let Some(task_entry) = inner_driver.op_map.get_mut(self.token) else {
            panic!("file: {}:{}\nerror: impossible situation", file!(), line!());
        };
        let Some(result) = task_entry.result.take() else {
            task_entry.waker = Some(cx.waker().clone());
            return std::task::Poll::Pending;
        };

        let Some(removed_entry) = inner_driver.op_map.try_remove(self.token) else {
            return std::task::Poll::Ready(Err(io::Error::other(format!(
                "unexpected situation at file: {}:{}",
                file!(),
                line!()
            ))));
        };
        std::task::Poll::Ready(Ok(OpResult {
            result,
            kind: removed_entry.kind,
        }))
    }
}

/// Decodes a `sockaddr_storage` the kernel filled in (via `accept`) into a
/// `SocketAddr`.
///
/// # Safety
/// `addr` must point to at least `addr_len` initialized bytes written by a
/// successful accept-style syscall.
unsafe fn to_socket_addr(
    addr: *const libc::sockaddr_storage,
    addr_len: libc::socklen_t,
) -> io::Result<std::net::SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

    let family = unsafe { (*addr).ss_family } as libc::c_int;
    let len = addr_len as usize;

    match family {
        libc::AF_INET => {
            if len < size_of::<libc::sockaddr_in>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "addr_len too small for AF_INET",
                ));
            }
            let a = unsafe { &*addr.cast::<libc::sockaddr_in>() };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                // s_addr and sin_port are in network byte order.
                Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr)),
                u16::from_be(a.sin_port),
            )))
        }
        libc::AF_INET6 => {
            if len < size_of::<libc::sockaddr_in6>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "addr_len too small for AF_INET6",
                ));
            }
            let a = unsafe { &*addr.cast::<libc::sockaddr_in6>() };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                // s6_addr is already a big-endian [u8; 16].
                Ipv6Addr::from(a.sin6_addr.s6_addr),
                u16::from_be(a.sin6_port),
                u32::from_be(a.sin6_flowinfo),
                // scope_id is host byte order (RFC 3493), so no swap.
                a.sin6_scope_id,
            )))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported address family {other}"),
        )),
    }
}
pub struct TcpListener {
    udriver: UDriver,
    inner: std::net::TcpListener,
    // token: usize,
}

impl TcpListener {
    pub fn bind(addr: std::net::SocketAddr) -> Self {
        let udriver = U_DRIVER.with(|driver| driver.clone());
        let inner = std::net::TcpListener::bind(addr).unwrap();
        Self {
            udriver,
            inner,
            // token,
        }
    }

    pub async fn accept(&self) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
        let mut addr_ptr: Box<libc::sockaddr_storage> = Box::new(unsafe { std::mem::zeroed() });
        let mut addr_len =
            Box::new(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);

        let op = Op {
            kind: OpKind::Accept {
                addr: addr_ptr,
                addr_len: addr_len,
            },
            waker: None,
            result: None,
            orphaned: false,
        };
        // let token = self.udriver.submit(self.inner.as_raw_fd(), op)?;
        let op_result = OpWaiter::new(self.inner.as_raw_fd(), op, self.udriver.clone())?.await?;
        let (result, addr, addr_len) = op_result.into_accept();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(result));
        }
        Ok(unsafe {
            (
                TcpStream {
                    udriver: self.udriver.clone(),
                    inner: std::net::TcpStream::from_raw_fd(result),
                },
                to_socket_addr(Box::as_ptr(&addr), *addr_len)?,
            )
        })
    }
}

pub struct TcpStream {
    udriver: UDriver,
    inner: std::net::TcpStream,
    // token: usize,
}

impl TcpStream {
    pub async fn read(&self, buf: Box<[u8]>) -> std::io::Result<(usize, Box<[u8]>)> {
        // println!("read: start");

        // let buf_ptr = Box::as_ptr(&buf);
        // let buf_len = buf.len();
        let op = Op {
            kind: OpKind::Read { buffer: buf },
            waker: None,
            result: None,
            orphaned: false,
        };
        // println!("read wait..");
        let result = OpWaiter::new(self.inner.as_raw_fd(), op, self.udriver.clone())?.await?;
        let (result, buffer) = result.into_read();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(result));
        }
        Ok((result as usize, buffer))
    }

    pub async fn write(&self, buf: Box<[u8]>) -> std::io::Result<(usize, Box<[u8]>)> {
        let result = OpWaiter::new(
            self.inner.as_raw_fd(),
            Op {
                kind: OpKind::Write { buffer: buf },
                waker: None,
                result: None,
                orphaned: false,
            },
            self.udriver.clone(),
        )?
        .await?;
        let (result, buffer) = result.into_write();
        if result < 0 {
            return Err(std::io::Error::from_raw_os_error(result));
        }
        Ok((result as usize, buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::to_socket_addr;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::fd::AsRawFd;

    /// Accepts one connection with a raw `libc::accept` and decodes the peer
    /// address the kernel wrote. Ground truth is the client's own `local_addr`,
    /// so this validates byte order and struct layout against the kernel rather
    /// than against our own encoder.
    fn accept_and_decode(bind_to: &str) -> (SocketAddr, SocketAddr) {
        let listener = TcpListener::bind(bind_to).unwrap();
        let server_addr = listener.local_addr().unwrap();

        let client = std::thread::spawn(move || {
            let s = TcpStream::connect(server_addr).unwrap();
            let local = s.local_addr().unwrap();
            // hold the socket open until the server has accepted
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(s);
            local
        });

        let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let fd = unsafe {
            libc::accept(
                listener.as_raw_fd(),
                std::ptr::from_mut(&mut ss).cast::<libc::sockaddr>(),
                &mut len,
            )
        };
        assert!(
            fd >= 0,
            "accept failed: {}",
            std::io::Error::last_os_error()
        );

        let decoded = unsafe { to_socket_addr(&ss, len) }.unwrap();
        unsafe { libc::close(fd) };

        (decoded, client.join().unwrap())
    }

    #[test]
    fn decodes_ipv4_peer_address_from_kernel() {
        let (decoded, actual) = accept_and_decode("127.0.0.1:0");
        assert_eq!(decoded, actual, "decoded peer addr != client's real addr");
        assert!(decoded.is_ipv4());
        assert_eq!(decoded.ip().to_string(), "127.0.0.1");
        assert_ne!(decoded.port(), 0);
    }

    #[test]
    fn decodes_ipv6_peer_address_from_kernel() {
        let (decoded, actual) = accept_and_decode("[::1]:0");
        assert_eq!(decoded, actual, "decoded peer addr != client's real addr");
        assert!(decoded.is_ipv6());
        assert_eq!(decoded.ip().to_string(), "::1");
    }

    /// Port must survive the network-order swap. A byte-swap bug would turn
    /// e.g. 4660 (0x1234) into 13330 (0x3412), which this catches.
    #[test]
    fn port_byte_order_is_not_swapped() {
        let (decoded, actual) = accept_and_decode("127.0.0.1:0");
        assert_eq!(decoded.port(), actual.port());
        // sanity: ephemeral ports are >= 1024, a swapped low port would not be
        assert!(decoded.port() >= 1024, "suspicious port {}", decoded.port());
    }

    #[test]
    fn rejects_truncated_addr_len() {
        let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        ss.ss_family = libc::AF_INET as libc::sa_family_t;
        let short = (size_of::<libc::sockaddr_in>() - 1) as libc::socklen_t;
        let err = unsafe { to_socket_addr(&ss, short) }.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_unsupported_family() {
        let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        ss.ss_family = libc::AF_UNIX as libc::sa_family_t;
        let len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let err = unsafe { to_socket_addr(&ss, len) }.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("unsupported address family"));
    }
}
