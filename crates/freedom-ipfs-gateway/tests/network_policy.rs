//! The network policy end to end through the node's builder (design 0007,
//! "Network policy", and its tests), under `socks5`:
//! - **content through the proxy:** an HTTP provider at a public address,
//!   which the proxy maps to a local trustless gateway, serves a block;
//! - **the instance:** "no socket is opened after the stop's
//!   acknowledgement", seen by the proxy and by the provider.
//!
//! The canary case is `network_policy_canary.rs`, and the light DHT's
//! `network_policy_dht.rs`.

mod policy_support;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method, StatusCode};
use cid::Cid;
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{
    GatewayCoreRequest, InstanceConfig, Network, NetworkPolicy, OnlineNode, RoutingMode,
};
use freedom_ipfs_net::socks::fixture::{Destination, HttpServer, Reply, SocksFixture};
use freedom_ipfs_net::{ClientConfig, HttpError};
use freedom_ipfs_store::SqliteBlockStore;
use policy_support::{configured, doh, node, not_found, Silent, PEER_C};

/// A trustless gateway on loopback, serving `data` as `cid`'s raw block.
async fn trustless(cid: Cid, data: &'static str) -> HttpServer {
    HttpServer::start(move |path, _| {
        if path.starts_with(&format!("/ipfs/{cid}")) {
            Reply::ok(data)
        } else {
            not_found()
        }
    })
    .await
}

/// Content loads through the proxy: an HTTP provider at a public address
/// (which the proxy maps to a local trustless gateway) serves the block,
/// reached by a `CONNECT` to that address, with `Host` naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_socks5_content_loads_through_the_proxy() {
    let data = "a block served through the proxy";
    let cid = cid_from_data(CODEC_RAW, data.as_bytes());
    let provider = trustless(cid, data).await;
    let providers = serde_json::json!({ "Providers": [
        { "Schema": "peer", "ID": PEER_C, "Addrs": ["/ip4/8.8.8.8/tcp/80/http"] },
    ]})
    .to_string();
    let router = HttpServer::start(move |path, _| {
        if path.starts_with("/routing/v1/providers/") {
            Reply::ok(&providers)
        } else {
            not_found()
        }
    })
    .await;
    let questions = Arc::new(Mutex::new(Vec::new()));
    let doh = doh(cid, questions).await;
    let fixture = SocksFixture::start().await;
    let public: SocketAddr = "8.8.8.8:80".parse().unwrap();
    fixture.forward(Destination::Ip(public), provider.addr());
    fixture.forward(Destination::Ip(router.addr()), router.addr());
    let (configured, router_url, doh_url) = configured(&router, &doh);
    let network =
        Network::new(NetworkPolicy::socks5(fixture.addr(), configured, doh_url).unwrap()).unwrap();
    let node = node(network, &router_url);
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        node.gateway_core().handle(GatewayCoreRequest::ipfs(
            cid.to_string(),
            Method::GET,
            HeaderMap::new(),
        )),
    )
    .await
    .expect("the gateway answers");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], data.as_bytes());
    let served = provider.requests();
    assert!(!served.is_empty());
    assert!(served
        .iter()
        .all(|(path, host)| path.starts_with(&format!("/ipfs/{cid}"))
            && host.as_deref() == Some("8.8.8.8")));
    assert!(fixture
        .requests()
        .iter()
        .any(|request| request.is_connect() && request.destination == Destination::Ip(public)));
}

fn wait_for(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The instance case under `socks5`: requests are held at a provider that
/// never answers, through the proxy; the instance is stopped. Once the
/// stop is acknowledged, no connection reaches the proxy or the provider,
/// and the network's clients send nothing.
#[test]
fn under_socks5_no_connection_opens_after_the_stop() {
    let harness = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let provider = Silent::start();
    let public: SocketAddr = "8.8.8.8:80".parse().unwrap();
    let (fixture, router, doh) = harness.block_on(async {
        let providers = serde_json::json!({ "Providers": [
            { "Schema": "peer", "ID": PEER_C, "Addrs": ["/ip4/8.8.8.8/tcp/80/http"] },
        ]})
        .to_string();
        let router = HttpServer::start(move |_, _| Reply::ok(&providers)).await;
        let doh = doh(
            cid_from_data(CODEC_RAW, b"unused"),
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;
        let fixture = SocksFixture::start().await;
        (fixture, router, doh)
    });
    fixture.forward(Destination::Ip(public), provider.addr);
    fixture.forward(Destination::Ip(router.addr()), router.addr());
    let (configured, router_url, doh_url) = configured(&router, &doh);
    let network =
        Network::new(NetworkPolicy::socks5(fixture.addr(), configured, doh_url).unwrap()).unwrap();
    let instance = OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
        .routing_mode(RoutingMode::Delegated)
        .delegated_routers([router_url])
        .network(network.clone())
        .build_instance(InstanceConfig {
            thread_name: "fipfs-policy".into(),
            worker_threads: 2,
            max_blocking_threads: 8,
        })
        .unwrap();
    let core = instance.gateway_core();
    for n in 0..3 {
        let cid = cid_from_data(CODEC_RAW, format!("never delivered {n}").as_bytes());
        let core = core.clone();
        drop(
            instance.spawn_request(instance.request_token(), async move {
                let response = core
                    .handle(GatewayCoreRequest::ipfs(
                        cid.to_string(),
                        Method::GET,
                        HeaderMap::new(),
                    ))
                    .await;
                let _ = to_bytes(response.into_body(), usize::MAX).await;
            }),
        );
    }
    wait_for(
        "a held provider connection",
        Duration::from_secs(20),
        || provider.accepted.load(Ordering::SeqCst) > 0,
    );

    let quiescence = instance.shutdown(Duration::from_secs(5));
    assert!(quiescence.reached, "{quiescence:?}");
    let (proxied, reached) = (
        fixture.connections(),
        provider.accepted.load(Ordering::SeqCst),
    );
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        fixture.connections(),
        proxied,
        "a connection after the stop"
    );
    assert_eq!(provider.accepted.load(Ordering::SeqCst), reached);
    // The network is revoked: its clients send nothing.
    assert!(network.capability().is_revoked());
    let result = harness.block_on(
        network
            .http_client(ClientConfig::new(Duration::from_secs(2), 0))
            .get("http://8.8.8.8/")
            .send(),
    );
    assert!(matches!(result, Err(HttpError::Revoked(_))), "{result:?}");
    assert_eq!(fixture.connections(), proxied);
}
