//! The network policy end to end through the node's builder, against a
//! canary (design 0007's test: "A SOCKS5 test proxy records every
//! connection, and a canary TCP and UDP listener is advertised in provider
//! records and is the target of a configured router's redirect. In
//! `socks5` mode the canary must see nothing, and the proxy everything").
//!
//! The canary is advertised and redirected to, never configured: a
//! configured router at it would be reached through the proxy, as the
//! design says. The legacy policy over the same topology is the control
//! that reaches it.
//!
//! A test binary of its own: it reads the kernel's view of this process's
//! sockets, which another test running beside it would add to.

mod policy_support;

use std::collections::BTreeSet;
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method, StatusCode};
use cid::Cid;
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{GatewayCoreRequest, Network, NetworkPolicy, OnlineNode};
use freedom_ipfs_net::socks::fixture::{Answer, Destination, HttpServer, Reply, SocksFixture};
use policy_support::{configured, doh, node, not_found, DNSLINK, PEER_A, PEER_B, PEER_C};

/// A TCP and a UDP listener that count what reaches them.
struct Canary {
    tcp: SocketAddr,
    udp: SocketAddr,
    accepted: Arc<AtomicUsize>,
    datagrams: Arc<AtomicUsize>,
}

impl Canary {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let udp = socket.local_addr().unwrap();
        let datagrams = Arc::new(AtomicUsize::new(0));
        let counted = datagrams.clone();
        std::thread::spawn(move || {
            let mut buffer = [0u8; 2048];
            while socket.recv_from(&mut buffer).is_ok() {
                counted.fetch_add(1, Ordering::SeqCst);
            }
        });
        Self {
            tcp,
            udp,
            accepted,
            datagrams,
        }
    }

    /// The TCP connections and UDP datagrams that reached it.
    fn seen(&self) -> (usize, usize) {
        (
            self.accepted.load(Ordering::SeqCst),
            self.datagrams.load(Ordering::SeqCst),
        )
    }
}

/// A delegated router on loopback: its provider records name the canary
/// (TCP, QUIC, a name that resolves to it, and an HTTP provider at it), and
/// every IPNS lookup redirects to the canary.
async fn router(cid: Cid, canary: &Canary) -> HttpServer {
    let (tcp, udp) = (canary.tcp.port(), canary.udp.port());
    let providers = serde_json::json!({ "Providers": [
        { "Schema": "peer", "ID": PEER_A, "Addrs": [
            format!("/ip4/127.0.0.1/tcp/{tcp}"),
            format!("/ip4/127.0.0.1/udp/{udp}/quic-v1"),
            format!("/dns4/canary.example/tcp/{tcp}"),
        ] },
        { "Schema": "peer", "ID": PEER_B, "Addrs": [
            format!("/ip4/127.0.0.1/tcp/{tcp}/http"),
        ] },
    ]})
    .to_string();
    HttpServer::start(move |path, _| {
        if path.starts_with(&format!("/routing/v1/providers/{cid}")) {
            Reply::ok(&providers)
        } else if path.starts_with("/routing/v1/ipns/") {
            Reply::redirect(&format!("http://127.0.0.1:{tcp}/ipns"))
        } else {
            not_found()
        }
    })
    .await
}

/// Asks the node's gateway for `cid`, for the IPNS name the router
/// redirects, and (with `dnslink`) for the DNSLink name: what an untrusted
/// network can make it fetch.
async fn requests(node: &OnlineNode, cid: Cid, dnslink: bool) -> Vec<StatusCode> {
    let core = node.gateway_core();
    let mut statuses = Vec::new();
    let mut asked = vec![
        GatewayCoreRequest::ipfs(cid.to_string(), Method::GET, HeaderMap::new()),
        GatewayCoreRequest::ipns(PEER_C, Method::GET, HeaderMap::new()),
    ];
    if dnslink {
        asked.push(GatewayCoreRequest::ipns(
            DNSLINK,
            Method::GET,
            HeaderMap::new(),
        ));
    }
    for request in asked {
        let response = tokio::time::timeout(Duration::from_secs(60), core.handle(request))
            .await
            .expect("the gateway answers");
        statuses.push(response.status());
        let _ = to_bytes(response.into_body(), usize::MAX).await;
    }
    statuses
}

/// Samples, until stopped, the remote ports of this process's TCP
/// connections whose local port isn't one of `servers`: every connection
/// something in this process opened, seen by the kernel (Linux only;
/// elsewhere it sees nothing).
struct Sockets {
    running: Arc<AtomicBool>,
    remote_ports: Arc<Mutex<BTreeSet<u16>>>,
}

impl Sockets {
    fn sample(servers: Vec<u16>) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let remote_ports = Arc::new(Mutex::new(BTreeSet::new()));
        let (going, seen) = (running.clone(), remote_ports.clone());
        std::thread::spawn(move || {
            while going.load(Ordering::SeqCst) {
                for port in connected_remote_ports(&servers) {
                    seen.lock().unwrap().insert(port);
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        Self {
            running,
            remote_ports,
        }
    }

    fn stop(self) -> BTreeSet<u16> {
        self.running.store(false, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(10));
        self.remote_ports.lock().unwrap().clone()
    }
}

/// The remote ports of this process's TCP sockets that have one and whose
/// local port isn't in `servers`.
fn connected_remote_ports(servers: &[u16]) -> Vec<u16> {
    let Ok(fds) = std::fs::read_dir("/proc/self/fd") else {
        return Vec::new();
    };
    let own: BTreeSet<String> = fds
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter_map(|target| {
            let target = target.to_string_lossy().into_owned();
            Some(
                target
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .to_string(),
            )
        })
        .collect();
    let mut ports = Vec::new();
    for table in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (Some(local), Some(remote), Some(inode)) =
                (fields.get(1), fields.get(2), fields.get(9))
            else {
                continue;
            };
            let port = |endpoint: &str| {
                endpoint
                    .rsplit(':')
                    .next()
                    .and_then(|port| u16::from_str_radix(port, 16).ok())
            };
            let (Some(local), Some(remote)) = (port(local), port(remote)) else {
                continue;
            };
            if own.contains(*inode) && remote != 0 && !servers.contains(&local) {
                ports.push(remote);
            }
        }
    }
    ports
}

/// Under `socks5`, nothing the network offers reaches the canary, and
/// everything the node sends goes through the proxy:
/// - the canary (TCP and UDP) sees nothing: its literal and QUIC addresses
///   are refused at candidate choice, its name is resolved through the
///   proxy and its loopback answer refused, its HTTP provider URL refused,
///   and the router's redirect to it refused;
/// - every connection the node opens goes to the proxy (and the kernel
///   sees no other), every `CONNECT` carries an IP literal, and every
///   connection to the router and the DoH endpoint came through the proxy;
/// - no local resolver and no DoH A or AAAA question: the only name
///   resolved is resolved by the proxy (`RESOLVE`).
///
/// The same topology under the legacy policy (the control) reaches the
/// canary, and never the proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_socks5_the_canary_sees_nothing_and_the_proxy_everything() {
    let cid = cid_from_data(CODEC_RAW, b"a block only the canary claims to have");
    for proxied in [true, false] {
        let canary = Canary::start();
        let questions = Arc::new(Mutex::new(Vec::new()));
        let router = router(cid, &canary).await;
        let doh = doh(cid, questions.clone()).await;
        let fixture = SocksFixture::start().await;
        fixture.answer("canary.example", Answer::Ip("127.0.0.1".parse().unwrap()));
        fixture.forward(Destination::Ip(router.addr()), router.addr());
        fixture.forward(Destination::Ip(doh.addr()), doh.addr());
        let (configured, router_url, doh_url) = configured(&router, &doh);
        let network = if proxied {
            Network::new(NetworkPolicy::socks5(fixture.addr(), configured, doh_url).unwrap())
                .unwrap()
        } else {
            Network::legacy()
        };
        let node = node(network, &router_url);
        let servers = vec![
            fixture.addr().port(),
            router.addr().port(),
            doh.addr().port(),
            canary.tcp.port(),
        ];
        let sockets = Sockets::sample(servers);
        // The control skips the DNSLink name: the legacy policy's DoH
        // endpoint is Cloudflare's own.
        let statuses = requests(&node, cid, proxied).await;
        // Give a late dial its chance to arrive.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let remote_ports = sockets.stop();
        let (tcp, udp) = canary.seen();
        let requests = fixture.requests();

        if !proxied {
            assert!(tcp > 0, "the control reaches the canary: {statuses:?}");
            assert!(udp > 0, "the control reaches the UDP canary: {statuses:?}");
            assert_eq!(fixture.connections(), 0, "the control never uses the proxy");
            continue;
        }

        assert_eq!((tcp, udp), (0, 0), "the canary was reached: {statuses:?}");
        // The router answered (providers, and the IPNS redirect), and the
        // DoH endpoint was asked for the DNSLink name.
        let paths: Vec<String> = router
            .requests()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert!(
            paths
                .iter()
                .any(|path| path.starts_with("/routing/v1/providers/"))
                && paths
                    .iter()
                    .any(|path| path.starts_with("/routing/v1/ipns/")),
            "{paths:?}"
        );
        // Every connection the router and the DoH endpoint saw came through
        // the proxy: one `CONNECT` each (each server closes after one
        // request).
        let connects: Vec<Destination> = requests
            .iter()
            .filter(|request| request.is_connect())
            .map(|request| request.destination.clone())
            .collect();
        let to = |addr: SocketAddr| {
            connects
                .iter()
                .filter(|destination| **destination == Destination::Ip(addr))
                .count()
        };
        assert_eq!(to(router.addr()), router.requests().len());
        assert_eq!(to(doh.addr()), doh.requests().len());
        assert!(
            connects
                .iter()
                .all(|destination| matches!(destination, Destination::Ip(_))),
            "{connects:?}"
        );
        assert_eq!(
            connects.len(),
            router.requests().len() + doh.requests().len(),
            "{connects:?}"
        );
        // The only name resolved, by the proxy: the provider's.
        let resolved: BTreeSet<String> = requests
            .iter()
            .filter(|request| request.is_resolve())
            .map(|request| match &request.destination {
                Destination::Name(name, _) => name.clone(),
                Destination::Ip(addr) => addr.to_string(),
            })
            .collect();
        assert_eq!(
            resolved,
            BTreeSet::from(["canary.example".to_string()]),
            "{requests:?}"
        );
        let asked = questions.lock().unwrap().clone();
        assert!(
            asked.iter().all(|question| question.ends_with("/TXT")),
            "a DoH question other than TXT: {asked:?}"
        );
        assert!(
            asked.contains(&format!("_dnslink.{DNSLINK}/TXT")),
            "{asked:?}"
        );
        // What the kernel saw: the node's connections went to the proxy, and
        // the proxy's own to the router and the DoH endpoint.
        let allowed = BTreeSet::from([
            fixture.addr().port(),
            router.addr().port(),
            doh.addr().port(),
        ]);
        let other: BTreeSet<u16> = remote_ports.difference(&allowed).copied().collect();
        assert!(
            other.is_empty(),
            "connections to {other:?}; allowed {allowed:?}; canary {}",
            canary.tcp.port()
        );
    }
}
