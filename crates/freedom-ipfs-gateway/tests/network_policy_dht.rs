//! The light DHT under the network policy, end to end through the node's
//! builder (design 0007, check 3: the gate wraps the DHT's transport, so it
//! covers Kademlia's and identify's own dials). Under `socks5`, which the
//! library allows though freedom-ipfsd refuses non-delegated routing there
//! (decision D6), its swarms reach the network through the proxy and
//! nothing else, and stop at the revoke.
//!
//! A test binary of its own: it reads the kernel's view of this process's
//! sockets, which another test running beside it would add to. Where it
//! can, it runs again in a private network of its own, whose counters see
//! every connection and datagram, however short-lived.

mod policy_support;

use std::collections::BTreeSet;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method};
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{GatewayCoreRequest, Network, NetworkPolicy, OnlineNode, RoutingMode};
use freedom_ipfs_net::socks::fixture::{Destination, HttpServer, SocksFixture};
use freedom_ipfs_net::Addresses;
use freedom_ipfs_store::SqliteBlockStore;
use policy_support::{
    configured, count_datagrams, doh, not_found, private_network, private_run_checked, Counters,
    Held, Run, Silent, Socket, Sockets, PEER_A, PEER_B, PEER_C,
};

const TEST: &str = "under_socks5_the_light_dht_dials_through_the_proxy_and_stops_on_revoke";

/// Waits up to 10 s for `done`.
async fn until(mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The light DHT under `socks5`: its swarms dial through the proxy, and
/// nothing else.
/// - A TCP bootstrap peer at a public address, which the proxy maps to a
///   local listener, is reached by a `CONNECT` to that address.
/// - One at loopback is refused by the gate, and its listener sees nothing.
/// - A QUIC one, at a public address, so only the missing transport keeps
///   it out (the `socks5` stack has no QUIC): in a private network a
///   stand-in there, which the kernel keeps local, sees no datagram;
///   elsewhere nobody listens there, and the kernel sampler would see a
///   QUIC dial's UDP socket.
/// - Once the network is revoked, a lookup opens no connection to the proxy
///   or to any listener, and sends no datagram.
/// - Throughout, the kernel sees every TCP connection of the node go to the
///   proxy and no UDP socket but the test's own; in a private network, the
///   namespace's counters see no datagram, and as many connections as the
///   proxy accepted and made.
///
/// The control, `direct` without the address table, reaches the loopback
/// TCP peer and the QUIC one, and never the proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_socks5_the_light_dht_dials_through_the_proxy_and_stops_on_revoke() {
    let private = match private_network(TEST) {
        Run::Done => return,
        Run::Private => true,
        Run::Here => false,
    };
    for proxied in [true, false] {
        let mapped = Silent::start();
        let loopback = Silent::start();
        // The QUIC peer: the control's at loopback, the proxied run's at a
        // public address, where in a private network the stand-in is.
        let quic = Arc::new(AtomicUsize::new(0));
        let quic_at = if proxied && private {
            "8.8.8.8:0"
        } else {
            "127.0.0.1:0"
        };
        let quic_addr = count_datagrams(UdpSocket::bind(quic_at).unwrap(), &quic);
        let quic_host = if proxied { "8.8.8.8" } else { "127.0.0.1" };
        let quic_peer = format!(
            "/ip4/{quic_host}/udp/{}/quic-v1/p2p/{PEER_C}",
            quic_addr.port()
        );
        let loopback_peer = format!("/ip4/127.0.0.1/tcp/{}/p2p/{PEER_B}", loopback.addr.port());
        let router = HttpServer::start(|_, _| not_found()).await;
        let doh = doh(
            cid_from_data(CODEC_RAW, b"unused"),
            Arc::new(Mutex::new(Vec::new())),
        )
        .await;
        let fixture = SocksFixture::start().await;
        let public: SocketAddr = "8.8.8.8:4001".parse().unwrap();
        fixture.forward(Destination::Ip(public), mapped.addr);
        let (configured, _, doh_url) = configured(&router, &doh);
        let (policy, peers) = if proxied {
            (
                NetworkPolicy::socks5(fixture.addr(), configured, doh_url),
                vec![
                    format!("/ip4/8.8.8.8/tcp/4001/p2p/{PEER_A}"),
                    loopback_peer,
                    quic_peer,
                ],
            )
        } else {
            (
                NetworkPolicy::direct(configured, doh_url, Addresses::Unchecked),
                vec![loopback_peer, quic_peer],
            )
        };
        // Under `socks5`, observed from before the network and the node are
        // built: what their construction opens counts too.
        let held = Held::open();
        let counted = (proxied && private).then(|| {
            Counters::assert_counting();
            Counters::read()
        });
        let sockets = (proxied && cfg!(target_os = "linux")).then(Sockets::sample);
        let network = Network::new(policy.unwrap()).unwrap();
        let node = OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
            .routing_mode(RoutingMode::LightDht)
            .dht_bootstrap_peers(peers)
            .network(network.clone())
            .build();
        let ask = |data: &'static [u8]| {
            let core = node.gateway_core();
            async move {
                tokio::time::timeout(Duration::from_secs(60), async move {
                    let response = core
                        .handle(GatewayCoreRequest::ipfs(
                            cid_from_data(CODEC_RAW, data).to_string(),
                            Method::GET,
                            HeaderMap::new(),
                        ))
                        .await;
                    let _ = to_bytes(response.into_body(), usize::MAX).await;
                })
                .await
                .expect("the gateway answers, head and body");
            }
        };

        if !proxied {
            ask(b"a block the control looks up").await;
            until(|| {
                loopback.accepted.load(Ordering::SeqCst) > 0 && quic.load(Ordering::SeqCst) > 0
            })
            .await;
            assert!(
                loopback.accepted.load(Ordering::SeqCst) > 0,
                "the control reaches the loopback TCP peer"
            );
            assert!(
                quic.load(Ordering::SeqCst) > 0,
                "the control reaches the QUIC peer"
            );
            assert_eq!(fixture.connections(), 0, "the control never uses the proxy");
            continue;
        }

        ask(b"a block the light DHT looks up").await;
        until(|| mapped.accepted.load(Ordering::SeqCst) > 0).await;
        assert!(
            mapped.accepted.load(Ordering::SeqCst) > 0,
            "the bootstrap peer was never reached through the proxy"
        );
        assert!(fixture
            .requests()
            .iter()
            .any(|request| request.is_connect() && request.destination == Destination::Ip(public)));
        assert_eq!(loopback.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(
            quic.load(Ordering::SeqCst),
            0,
            "a datagram to the QUIC peer"
        );

        network.revoke();
        let (proxied_connections, reached) = (
            fixture.connections(),
            mapped.accepted.load(Ordering::SeqCst),
        );
        ask(b"another block, after the revoke").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            fixture.connections(),
            proxied_connections,
            "a connection after the revoke"
        );
        assert_eq!(mapped.accepted.load(Ordering::SeqCst), reached);
        assert_eq!(loopback.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(
            quic.load(Ordering::SeqCst),
            0,
            "a datagram to the QUIC peer after the revoke"
        );

        // What the kernel saw.
        match sockets.map(Sockets::stop) {
            Some(seen) => {
                held.assert_seen(&seen);
                let proxy_own: BTreeSet<u16> =
                    fixture.outbound().iter().map(SocketAddr::port).collect();
                let own_udp: BTreeSet<u16> =
                    held.udp.iter().copied().chain([quic_addr.port()]).collect();
                let stray: Vec<&Socket> = seen
                    .iter()
                    .filter(|socket| match socket {
                        Socket::Tcp(local, remote) => {
                            *remote != fixture.addr().port()
                                && !proxy_own.contains(local)
                                && !held.tcp.contains(remote)
                        }
                        Socket::Udp(local) => !own_udp.contains(local),
                    })
                    .collect();
                assert!(
                    stray.is_empty(),
                    "sockets outside the proxy: {stray:?}; proxy {}",
                    fixture.addr().port()
                );
            }
            None => eprintln!("the kernel check needs Linux's /proc: skipped"),
        }
        if let Some(before) = counted {
            let sent = Counters::read().since(before);
            assert_eq!(sent.datagrams, 0, "a datagram, in the private network");
            assert_eq!(
                sent.connects,
                fixture.connections() + fixture.outbound().len(),
                "a connection outside the proxy, in the private network"
            );
            private_run_checked(sent);
        }
    }
}
