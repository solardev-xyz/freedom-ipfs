//! The network policy end to end through the node's builder, against
//! canaries (design 0007's test: "A SOCKS5 test proxy records every
//! connection, and a canary TCP and UDP listener is advertised in provider
//! records and is the target of a configured router's redirect. In
//! `socks5` mode the canary must see nothing, and the proxy everything").
//!
//! The canary is advertised and redirected to, never configured: a
//! configured router at it would be reached through the proxy, as the
//! design says. Each way of reaching it is its own request, so each is
//! tried whatever the others do: a literal and QUIC, a name, an HTTP
//! provider, and the router's IPNS redirect, which has a canary of its own.
//!
//! The control is `direct` without the address table, over the same
//! topology: it reaches every canary, and never the proxy. Everything stays
//! on loopback: the DoH endpoint, which DNSLink asks first for every IPNS
//! name, is the topology's own.
//!
//! A test binary of its own: it reads the kernel's view of this process's
//! sockets, which another test running beside it would add to.

mod policy_support;

use std::collections::BTreeSet;
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method, StatusCode};
use cid::Cid;
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{GatewayCoreRequest, Network, NetworkPolicy, OnlineNode};
use freedom_ipfs_net::socks::fixture::{Answer, Destination, HttpServer, Reply, SocksFixture};
use freedom_ipfs_net::Addresses;
use policy_support::{configured, doh, node, not_found, DNSLINK, PEER_A, PEER_B, PEER_C};

/// A TCP listener that counts its connections, and, when `udp`, a UDP
/// socket on another port that counts its datagrams.
struct Canary {
    tcp: SocketAddr,
    udp: Option<SocketAddr>,
    accepted: Arc<AtomicUsize>,
    datagrams: Arc<AtomicUsize>,
}

impl Canary {
    fn start(udp: bool) -> Self {
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
        let datagrams = Arc::new(AtomicUsize::new(0));
        let udp = udp.then(|| count_datagrams(UdpSocket::bind("127.0.0.1:0").unwrap(), &datagrams));
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

/// Counts the datagrams `socket` receives, from now on.
fn count_datagrams(socket: UdpSocket, count: &Arc<AtomicUsize>) -> SocketAddr {
    let addr = socket.local_addr().unwrap();
    let counted = count.clone();
    std::thread::spawn(move || {
        let mut buffer = [0u8; 2048];
        while socket.recv_from(&mut buffer).is_ok() {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });
    addr
}

/// The datagrams sent to a TCP server's port, over UDP: a direct datagram
/// to the router or the DoH endpoint lands here.
fn udp_beside(server: &HttpServer) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let socket = UdpSocket::bind(server.addr()).expect("the server's port is free over UDP");
    count_datagrams(socket, &count);
    count
}

/// The blocks asked for, one per way the canary is advertised, and the
/// IPNS name the router redirects. Each run has its own, so nothing one run
/// cached (a name's failure, a provider's) decides another's.
struct Blocks {
    literal: Cid,
    name: Cid,
    http: Cid,
    ipns: &'static str,
}

impl Blocks {
    fn new(run: &str, ipns: &'static str) -> Self {
        let block = |what: &str| cid_from_data(CODEC_RAW, format!("{run}: {what}").as_bytes());
        Self {
            literal: block("claimed at the canary's literals"),
            name: block("claimed at the canary's name"),
            http: block("claimed at the canary's HTTP provider"),
            ipns,
        }
    }
}

/// A delegated router on loopback. Each block's provider record names the
/// canary one way: TCP and QUIC literals, a name that resolves to it, or
/// an HTTP provider. Every IPNS lookup redirects to the redirect canary.
async fn router(blocks: &Blocks, canary: &Canary, redirect: &Canary) -> HttpServer {
    let (tcp, udp) = (canary.tcp.port(), canary.udp.unwrap().port());
    let record = |peer: &str, addrs: Vec<String>| {
        serde_json::json!({ "Providers": [{ "Schema": "peer", "ID": peer, "Addrs": addrs }] })
            .to_string()
    };
    let answers = [
        (
            blocks.literal,
            record(
                PEER_A,
                vec![
                    format!("/ip4/127.0.0.1/tcp/{tcp}"),
                    format!("/ip4/127.0.0.1/udp/{udp}/quic-v1"),
                ],
            ),
        ),
        (
            blocks.name,
            record(PEER_B, vec![format!("/dns4/canary.example/tcp/{tcp}")]),
        ),
        (
            blocks.http,
            record(PEER_C, vec![format!("/ip4/127.0.0.1/tcp/{tcp}/http")]),
        ),
    ];
    let redirect = redirect.tcp.port();
    HttpServer::start(move |path, _| {
        for (cid, record) in &answers {
            if path.starts_with(&format!("/routing/v1/providers/{cid}")) {
                return Reply::ok(record);
            }
        }
        if path.starts_with("/routing/v1/ipns/") {
            Reply::redirect(&format!("http://127.0.0.1:{redirect}/ipns"))
        } else {
            not_found()
        }
    })
    .await
}

/// Asks the node's gateway for the IPNS name the router redirects, on its
/// own, so nothing else delays its redirect; then, all at once, for each
/// block and for the DNSLink name. Each answer's head and body within a
/// bound.
async fn requests(node: &OnlineNode, blocks: &Blocks) -> Vec<StatusCode> {
    let core = node.gateway_core();
    let ask = |request: GatewayCoreRequest| {
        let core = core.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(60), async move {
                let response = core.handle(request).await;
                let status = response.status();
                let _ = to_bytes(response.into_body(), usize::MAX).await;
                status
            })
            .await
            .expect("the gateway answers, head and body")
        }
    };
    let mut statuses = vec![
        ask(GatewayCoreRequest::ipns(
            blocks.ipns,
            Method::GET,
            HeaderMap::new(),
        ))
        .await,
    ];
    let rest = [
        GatewayCoreRequest::ipfs(blocks.literal.to_string(), Method::GET, HeaderMap::new()),
        GatewayCoreRequest::ipfs(blocks.name.to_string(), Method::GET, HeaderMap::new()),
        GatewayCoreRequest::ipfs(blocks.http.to_string(), Method::GET, HeaderMap::new()),
        GatewayCoreRequest::ipns(DNSLINK, Method::GET, HeaderMap::new()),
    ];
    statuses.extend(futures::future::join_all(rest.into_iter().map(ask)).await);
    statuses
}

/// A client socket of this process, as the kernel lists it: its local and
/// remote ports.
type Tuple = (u16, u16);

/// Samples, until stopped, every TCP client socket of this process (IPv4
/// and IPv6): those whose local port isn't one this process listens on.
/// Linux only.
struct Sockets {
    running: Arc<AtomicBool>,
    seen: Arc<Mutex<BTreeSet<Tuple>>>,
    thread: std::thread::JoinHandle<()>,
}

impl Sockets {
    /// Starts sampling, once a first full scan has worked.
    fn sample() -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let seen = Arc::new(Mutex::new(BTreeSet::new()));
        let (going, recorded) = (running.clone(), seen.clone());
        let (ready, is_ready) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let mut first = true;
            while going.load(Ordering::SeqCst) {
                let scan = client_sockets().expect("the kernel's socket tables are readable");
                recorded.lock().unwrap().extend(scan);
                if std::mem::take(&mut first) {
                    let _ = ready.send(());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        is_ready
            .recv_timeout(Duration::from_secs(5))
            .expect("the sampler's first scan");
        Self {
            running,
            seen,
            thread,
        }
    }

    fn stop(self) -> BTreeSet<Tuple> {
        self.running.store(false, Ordering::SeqCst);
        self.thread.join().expect("the sampler ends");
        self.seen.lock().unwrap().clone()
    }
}

/// This process's TCP client sockets now: those of its sockets (by inode,
/// from `/proc/self/fd`) in `/proc/self/net/tcp` and `tcp6` with a remote
/// port, whose local port isn't one it listens on. An error if the tables
/// can't be read.
fn client_sockets() -> std::io::Result<Vec<Tuple>> {
    let own: BTreeSet<String> = std::fs::read_dir("/proc/self/fd")?
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
    let port = |endpoint: &str| {
        endpoint
            .rsplit(':')
            .next()
            .and_then(|port| u16::from_str_radix(port, 16).ok())
    };
    let mut rows = Vec::new();
    for table in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        let text = std::fs::read_to_string(table)?;
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (Some(local), Some(remote), Some(state), Some(inode)) =
                (fields.get(1), fields.get(2), fields.get(3), fields.get(9))
            else {
                continue;
            };
            if !own.contains(*inode) {
                continue;
            }
            if let (Some(local), Some(remote)) = (port(local), port(remote)) {
                rows.push((local, remote, *state == "0A"));
            }
        }
    }
    let listening: BTreeSet<u16> = rows
        .iter()
        .filter(|(_, _, listens)| *listens)
        .map(|(local, _, _)| *local)
        .collect();
    Ok(rows
        .into_iter()
        .filter(|(local, remote, listens)| !listens && *remote != 0 && !listening.contains(local))
        .map(|(local, remote, _)| (local, remote))
        .collect())
}

/// Two client connections held for as long as it lives, over IPv4 and
/// IPv6: what the sampler must see, or it is reading nothing.
struct Held {
    ports: Vec<u16>,
    _streams: Vec<TcpStream>,
    _listeners: Vec<TcpListener>,
}

impl Held {
    fn open() -> Self {
        let mut ports = Vec::new();
        let mut streams = Vec::new();
        let mut listeners = Vec::new();
        for bind in ["127.0.0.1:0", "[::1]:0"] {
            let listener = TcpListener::bind(bind).expect("a loopback listener, IPv4 and IPv6");
            let addr = listener.local_addr().unwrap();
            streams.push(TcpStream::connect(addr).unwrap());
            streams.push(listener.accept().unwrap().0);
            ports.push(addr.port());
            listeners.push(listener);
        }
        Self {
            ports,
            _streams: streams,
            _listeners: listeners,
        }
    }
}

/// Under `socks5`, nothing the network offers reaches a canary, and
/// everything the node sends goes through the proxy:
/// - the canary (TCP and UDP) and the redirect canary see nothing: the
///   literal and QUIC addresses are refused at candidate choice, the name
///   is resolved through the proxy and its loopback answer refused, the
///   HTTP provider URL is refused, and the router's IPNS redirect is
///   refused;
/// - the router and the DoH endpoint saw exactly the connections the proxy
///   made to them, and no datagram; every `CONNECT` carries an IP literal;
/// - the kernel saw every TCP connection of the node go to the proxy, and
///   the proxy's own to the router and the DoH endpoint (told apart by
///   their local ports); the held IPv4 and IPv6 connections show it reads;
/// - no local resolver and no DoH A or AAAA question: the only name
///   resolved is the provider's, by the proxy.
///
/// The control, `direct` without the address table over the same
/// topology, reaches the canary over TCP and UDP and the redirect canary,
/// and never the proxy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn under_socks5_the_canary_sees_nothing_and_the_proxy_everything() {
    for proxied in [true, false] {
        let blocks = if proxied {
            Blocks::new("socks5", PEER_C)
        } else {
            Blocks::new("direct", PEER_B)
        };
        let canary = Canary::start(true);
        let redirect = Canary::start(false);
        let questions = Arc::new(Mutex::new(Vec::new()));
        let router = router(&blocks, &canary, &redirect).await;
        let doh = doh(blocks.literal, questions.clone()).await;
        let (router_udp, doh_udp) = (udp_beside(&router), udp_beside(&doh));
        let fixture = SocksFixture::start().await;
        fixture.answer("canary.example", Answer::Ip("127.0.0.1".parse().unwrap()));
        fixture.forward(Destination::Ip(router.addr()), router.addr());
        fixture.forward(Destination::Ip(doh.addr()), doh.addr());
        let (configured, router_url, doh_url) = configured(&router, &doh);
        let policy = if proxied {
            NetworkPolicy::socks5(fixture.addr(), configured, doh_url)
        } else {
            NetworkPolicy::direct(configured, doh_url, Addresses::Unchecked)
        };
        let node = node(Network::new(policy.unwrap()).unwrap(), &router_url);
        let held = Held::open();
        let sockets = cfg!(target_os = "linux").then(Sockets::sample);
        let statuses = requests(&node, &blocks).await;
        // A late dial has its chance to arrive.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let tuples = sockets.map(Sockets::stop);
        let ((tcp, udp), (redirected, _)) = (canary.seen(), redirect.seen());

        if !proxied {
            assert!(tcp > 0, "the control reaches the canary: {statuses:?}");
            assert!(udp > 0, "the control reaches the UDP canary: {statuses:?}");
            assert!(
                redirected > 0,
                "the control follows the redirect: {statuses:?}, the router saw {:?}",
                router.requests()
            );
            assert_eq!(fixture.connections(), 0, "the control never uses the proxy");
            continue;
        }

        assert_eq!(
            (tcp, udp, redirected),
            (0, 0, 0),
            "a canary was reached: {statuses:?}"
        );
        let requests = fixture.requests();
        // The router answered every provider lookup and the IPNS lookup, and
        // the DoH endpoint was asked for the DNSLink name.
        let paths: Vec<String> = router
            .requests()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        for cid in [blocks.literal, blocks.name, blocks.http] {
            assert!(
                paths
                    .iter()
                    .any(|path| path.starts_with(&format!("/routing/v1/providers/{cid}"))),
                "{paths:?}"
            );
        }
        assert!(
            paths
                .iter()
                .any(|path| path.starts_with("/routing/v1/ipns/")),
            "{paths:?}"
        );
        // Every connection the router and the DoH endpoint accepted came
        // through the proxy, and no datagram reached either.
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
        assert_eq!(router.connections(), to(router.addr()), "{connects:?}");
        assert_eq!(doh.connections(), to(doh.addr()), "{connects:?}");
        assert_eq!(
            router_udp.load(Ordering::SeqCst),
            0,
            "a datagram to the router"
        );
        assert_eq!(
            doh_udp.load(Ordering::SeqCst),
            0,
            "a datagram to the DoH endpoint"
        );
        assert_eq!(
            connects.len(),
            to(router.addr()) + to(doh.addr()),
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
        // What the kernel saw.
        match tuples {
            Some(tuples) => {
                let remote: BTreeSet<u16> = tuples.iter().map(|(_, remote)| *remote).collect();
                for port in &held.ports {
                    assert!(
                        remote.contains(port),
                        "the sampler missed a held connection"
                    );
                }
                let proxy_own: BTreeSet<u16> =
                    fixture.outbound().iter().map(SocketAddr::port).collect();
                let stray: Vec<&Tuple> = tuples
                    .iter()
                    .filter(|(local, remote)| {
                        *remote != fixture.addr().port()
                            && !proxy_own.contains(local)
                            && !held.ports.contains(remote)
                    })
                    .collect();
                assert!(
                    stray.is_empty(),
                    "connections outside the proxy: {stray:?}; proxy {}",
                    fixture.addr().port()
                );
            }
            None => eprintln!("the kernel check needs Linux's /proc: skipped"),
        }
    }
}
