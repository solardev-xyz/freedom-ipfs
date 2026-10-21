//! `direct` ignores the environment's proxy (design 0007, deviation
//! `env-proxy`): its clients are built with `no_proxy()`, while a legacy
//! network's still go through it.
//!
//! A test binary of its own, with one test: it sets `HTTP_PROXY` for the
//! whole process.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::{Addresses, ClientConfig, Network, NetworkPolicy};
use url::Url;

/// A server recording the request line of each request it gets.
fn server() -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let recorded = lines.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let _ = reader.read_line(&mut line);
            recorded.lock().unwrap().push(line.trim().to_string());
            let _ = reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    });
    (port, lines)
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_ignores_the_environments_proxy() {
    let (proxy_port, proxied) = server();
    let (target_port, direct) = server();
    for name in ["HTTP_PROXY", "http_proxy"] {
        std::env::set_var(name, format!("http://127.0.0.1:{proxy_port}"));
    }
    for name in ["NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy"] {
        std::env::remove_var(name);
    }
    let config = ClientConfig::new(Duration::from_secs(10), 10);
    let target = format!("http://127.0.0.1:{target_port}/x");

    let policy = NetworkPolicy::direct(
        ConfiguredTargets::new(),
        Url::parse("https://dns.example/dns-query").unwrap(),
        Addresses::Unchecked,
    )
    .unwrap();
    let response = Network::new(policy)
        .unwrap()
        .http_client(config)
        .get(&target)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(*direct.lock().unwrap(), ["GET /x HTTP/1.1"]);
    assert!(proxied.lock().unwrap().is_empty());

    // A legacy network's client still goes through it, as before.
    let _ = Network::legacy()
        .http_client(config)
        .get(&target)
        .send()
        .await;
    assert_eq!(*proxied.lock().unwrap(), [format!("GET {target} HTTP/1.1")]);
}
