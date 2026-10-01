use std::os::fd::IntoRawFd;
use std::panic::Location;
use std::pin::Pin;
use std::time::{Duration, Instant};

use io_uring::types::io_uring_region_desc;
use mockio::executor::{self, timer};

use mockio::{TcpListener, TcpStream};
async fn main_task() {
    println!("SIZE OF PIN {}", std::mem::size_of::<Pin<u32>>());
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

#[track_caller]
fn util_function() {
    println!("CALLER UTIL: {}", Location::caller())
}

#[track_caller]
async fn handle_connection(mut stream: TcpStream, id: usize) {
    println!("##LOG##: file: {}, line: {}", file!(), line!());
    util_function();

    let mut buffer: Box<[u8]> = Box::new([0; 1024]);
    let (ln, buffer) = stream.read(buffer).await.expect(&format!(
        "##LOG##: error a file: {}:{}.\ncaller: {}",
        file!(),
        line!(),
        Location::caller()
    ));

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
    println!("##LOG##: file: {}, line: {}", file!(), line!());

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

fn main() {
    println!("##LOG##: file: {}:{}", file!(), line!());

    let executor = mockio::executor::Executor::current();
    // println!("executor: {:p}", &executor);

    executor.spawn(main_task());
    // println!("START {}", executor.queue().borrow().len());
    executor.run();
}
