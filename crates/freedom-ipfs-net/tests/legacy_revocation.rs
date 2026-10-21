//! Under the legacy policy reqwest follows redirects itself, so the
//! capability is asked on every hop it follows, not only when a request
//! starts (design 0007: "every socket, HTTP attempt and redirect hop asks
//! it first"). These revoke while a server holds its 302.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_net::{ClientConfig, HttpError, Network};

/// Reads one request head from `reader`: its path, or `None` at the end of
/// the connection.
fn read_request(reader: &mut BufReader<TcpStream>) -> Option<String> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let path = line.split_whitespace().nth(1)?.to_string();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 || header == "\r\n" {
            return Some(path);
        }
    }
}

const CONFIG: ClientConfig = ClientConfig::new(Duration::from_secs(5), 10);

/// The adversarial lifecycle review's probe: a hop to another server after
/// the revoke opens no connection there.
#[test]
fn revocation_between_redirect_hops_prevents_the_second_connection() {
    let second = TcpListener::bind("127.0.0.1:0").unwrap();
    let second_addr = second.local_addr().unwrap();
    let (second_seen_tx, second_seen_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = second.accept().unwrap();
        second_seen_tx.send(()).unwrap();
        let mut reader = BufReader::new(stream);
        read_request(&mut reader);
        let _ = reader
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    });

    let first = TcpListener::bind("127.0.0.1:0").unwrap();
    let first_addr = first.local_addr().unwrap();
    let (first_seen_tx, first_seen_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        let (stream, _) = first.accept().unwrap();
        let mut reader = BufReader::new(stream);
        read_request(&mut reader);
        first_seen_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        let _ = write!(
            reader.get_mut(),
            "HTTP/1.1 302 Found\r\nLocation: http://{second_addr}/after\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
    });

    let network = Network::legacy();
    let client = network.http_client(CONFIG);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let request = runtime.spawn(async move {
        client
            .get(format!("http://{first_addr}/before"))
            .send()
            .await
    });
    first_seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    network.revoke();
    release_tx.send(()).unwrap();
    let outcome = runtime.block_on(request).unwrap();
    assert!(
        matches!(outcome, Err(HttpError::Revoked(_))),
        "a redirect hop was sent after the revoke: {:?}",
        outcome.as_ref().map(reqwest::Response::status)
    );
    assert!(second_seen_rx
        .recv_timeout(Duration::from_millis(250))
        .is_err());
}

/// A hop to the same host would reuse the pooled connection, so no new
/// socket is asked for: the redirect policy refuses it.
#[test]
fn a_hop_over_a_pooled_connection_after_the_revoke_is_refused() {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let (seen_tx, seen_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (counted, recorded) = (connections.clone(), paths.clone());
    std::thread::spawn(move || {
        let release_rx = Mutex::new(release_rx);
        for stream in server.incoming() {
            let Ok(stream) = stream else { continue };
            counted.fetch_add(1, Ordering::SeqCst);
            let mut reader = BufReader::new(stream);
            // Keep-alive: every request on the connection is answered.
            while let Some(path) = read_request(&mut reader) {
                recorded.lock().unwrap().push(path.clone());
                let response = if path == "/before" {
                    seen_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    "HTTP/1.1 302 Found\r\nLocation: /after\r\nContent-Length: 0\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                };
                if reader.get_mut().write_all(response.as_bytes()).is_err() {
                    break;
                }
            }
        }
    });

    let network = Network::legacy();
    let client = network.http_client(CONFIG);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let request =
        runtime.spawn(async move { client.get(format!("http://{addr}/before")).send().await });
    seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    network.revoke();
    release_tx.send(()).unwrap();
    let outcome = runtime.block_on(request).unwrap();
    assert!(
        matches!(outcome, Err(HttpError::Revoked(_))),
        "{:?}",
        outcome.as_ref().map(reqwest::Response::status)
    );
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(*paths.lock().unwrap(), ["/before"]);
    assert_eq!(connections.load(Ordering::SeqCst), 1);
}

/// Without a revoke, both hops are followed as before.
#[test]
fn a_live_capability_follows_both_hops() {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = server.local_addr().unwrap();
    let paths = Arc::new(Mutex::new(Vec::new()));
    let recorded = paths.clone();
    std::thread::spawn(move || {
        for stream in server.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream);
            while let Some(path) = read_request(&mut reader) {
                recorded.lock().unwrap().push(path.clone());
                let response = if path == "/before" {
                    "HTTP/1.1 302 Found\r\nLocation: /after\r\nContent-Length: 0\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                };
                if reader.get_mut().write_all(response.as_bytes()).is_err() {
                    break;
                }
            }
        }
    });
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = Network::legacy().http_client(CONFIG);
    let response = runtime
        .block_on(client.get(format!("http://{addr}/before")).send())
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(*paths.lock().unwrap(), ["/before", "/after"]);
}
