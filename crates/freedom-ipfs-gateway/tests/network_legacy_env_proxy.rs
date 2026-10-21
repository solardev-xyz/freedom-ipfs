//! A legacy network's clients honour the environment's proxy, as the
//! crates' clients did before the policy (design 0007: only `direct` and
//! `socks5` stop reading it, deviation `env-proxy`).
//!
//! A test binary of its own, with one test: it sets `HTTP_PROXY` for the
//! whole process.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_gateway::Network;

#[test]
fn legacy_clients_go_through_the_environments_proxy_as_before() {
    // A proxy that records the request line each client sends it.
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
            let _ = reader.get_mut().write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    let proxy = format!("http://127.0.0.1:{port}");
    for name in ["HTTP_PROXY", "http_proxy"] {
        std::env::set_var(name, &proxy);
    }
    for name in ["NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy"] {
        std::env::remove_var(name);
    }

    let timeout = Duration::from_secs(10);
    // The crates' client before the policy, verbatim.
    let before = reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .unwrap();
    let after = Network::legacy().http_client(freedom_ipfs_namesys::HTTP_CLIENT_CONFIG);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let _ = before.get("http://before.invalid/x").send().await;
        let _ = after.get("http://after.invalid/x").send().await;
    });
    assert_eq!(
        *lines.lock().unwrap(),
        [
            "GET http://before.invalid/x HTTP/1.1",
            "GET http://after.invalid/x HTTP/1.1",
        ]
    );
}
