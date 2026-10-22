//! Under `socks5` the environment's proxy variables don't apply (design
//! 0007, "Modes": every client goes through the policy's proxy and no
//! other; deviation `env-proxy`). Its own test binary: it sets the
//! process's environment.

use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{Destination, HttpServer, Reply, SocksFixture};
use freedom_ipfs_net::{ClientConfig, Network, NetworkPolicy};
use url::Url;

#[tokio::test(flavor = "multi_thread")]
async fn the_environments_proxies_are_never_used() {
    // An environment proxy that counts the connections it is offered.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let env_proxy = listener.local_addr().unwrap();
    let offered = Arc::new(AtomicUsize::new(0));
    let counted = offered.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            counted.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    for variable in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        std::env::set_var(variable, format!("http://{env_proxy}"));
    }
    std::env::remove_var("NO_PROXY");
    std::env::remove_var("no_proxy");

    let fixture = SocksFixture::start().await;
    let server = HttpServer::start(|_, _| Reply::ok("ok")).await;
    fixture.forward(
        Destination::Ip("8.8.8.8:80".parse().unwrap()),
        server.addr(),
    );
    let network = Network::new(
        NetworkPolicy::socks5(
            fixture.addr(),
            ConfiguredTargets::new(),
            Url::parse("https://dns.example/dns-query").unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let client = network.http_client(ClientConfig::new(Duration::from_secs(5), 10));
    let response = client.get("http://8.8.8.8/").send().await.unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    assert_eq!(fixture.connections(), 1);
    assert_eq!(offered.load(Ordering::SeqCst), 0);

    // The control: a legacy client reads the environment, and is offered
    // to that proxy.
    let legacy = Network::legacy().http_client(ClientConfig::new(Duration::from_secs(2), 0));
    let _ = legacy.get("http://8.8.8.8/").send().await;
    assert!(offered.load(Ordering::SeqCst) > 0);
}
