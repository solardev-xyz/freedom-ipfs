//! What the network policy's end-to-end tests share:
//! - a delegated router's and a DoH endpoint's stand-ins on loopback, a
//!   provider that never answers, and a node built through the builder;
//! - the kernel's view of this process's sockets, sampled ([`Sockets`]),
//!   with sockets it must see ([`Held`]);
//! - a private network for a test ([`private_network`]): every address is
//!   local to it, and its counters ([`Counters`]) see every connection and
//!   datagram, however short-lived.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use freedom_ipfs_gateway::{Network, OnlineNode, RoutingMode};
use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{HttpServer, Reply};
use freedom_ipfs_store::SqliteBlockStore;
use reqwest::Url;

pub const PEER_A: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
pub const PEER_B: &str = "12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA";
pub const PEER_C: &str = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
pub const PEER_D: &str = "12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3";
pub const DNSLINK: &str = "site.example";

/// A 404.
pub fn not_found() -> Reply {
    Reply {
        status: 404,
        location: None,
        body: String::new(),
    }
}

/// A DoH endpoint (JSON) on loopback: `_dnslink.site.example` answers with
/// `cid`, `canary.example`'s A question with `127.0.0.1`; every other
/// question has no answer. It records each question as `name/type`.
pub async fn doh(cid: Cid, questions: Arc<Mutex<Vec<String>>>) -> HttpServer {
    HttpServer::start(move |path, _| {
        let query = path.split_once('?').map_or("", |(_, query)| query);
        let mut name = String::new();
        let mut kind = String::new();
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("name", value)) => name = value.to_string(),
                Some(("type", value)) => kind = value.to_string(),
                _ => {}
            }
        }
        questions.lock().unwrap().push(format!("{name}/{kind}"));
        let owner = format!("_dnslink.{DNSLINK}");
        let body = if name.trim_end_matches('.') == owner && matches!(kind.as_str(), "TXT" | "16") {
            serde_json::json!({ "Status": 0, "Answer": [
                { "name": owner, "type": 16, "TTL": 60,
                  "data": format!("\"dnslink=/ipfs/{cid}\"") },
            ]})
        } else if name.trim_end_matches('.') == "canary.example"
            && matches!(kind.as_str(), "A" | "1")
        {
            serde_json::json!({ "Status": 0, "Answer": [
                { "name": "canary.example", "type": 1, "TTL": 60, "data": "127.0.0.1" },
            ]})
        } else {
            serde_json::json!({ "Status": 3 })
        };
        Reply::ok(&body.to_string())
    })
    .await
}

/// The node's routers and DoH endpoint, configured by IP literal.
pub fn configured(router: &HttpServer, doh: &HttpServer) -> (ConfiguredTargets, String, Url) {
    let router_url = format!("http://{}/routing/v1", router.addr());
    let doh_url = Url::parse(&format!("http://{}/dns-query", doh.addr())).unwrap();
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse(&router_url).unwrap())
        .unwrap();
    (configured, router_url, doh_url)
}

pub fn node(network: Network, router_url: &str) -> OnlineNode {
    OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
        .routing_mode(RoutingMode::Delegated)
        .delegated_routers([router_url.to_string()])
        .network(network)
        .build()
}

/// A provider that accepts and never answers, counting its connections.
pub struct Silent {
    pub addr: SocketAddr,
    pub accepted: Arc<AtomicUsize>,
}

impl Silent {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        let held = Arc::new(Mutex::new(Vec::new()));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counted.fetch_add(1, Ordering::SeqCst);
                if let Ok(stream) = stream {
                    held.lock().unwrap().push(stream);
                }
            }
        });
        Self { addr, accepted }
    }
}

/// Counts the datagrams `socket` receives, from now on.
pub fn count_datagrams(socket: UdpSocket, count: &Arc<AtomicUsize>) -> SocketAddr {
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

/// A socket of this process, as the kernel lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Socket {
    /// A TCP client socket: its local and remote ports.
    Tcp(u16, u16),
    /// A UDP socket, bound or connected: its local port.
    Udp(u16),
}

/// Samples, until stopped, every TCP client socket and every UDP socket of
/// this process, IPv4 and IPv6, every 2 ms. Linux only.
///
/// A socket opened and closed between two samples isn't seen: what
/// [`private_network`]'s counters are for.
pub struct Sockets {
    running: Arc<AtomicBool>,
    seen: Arc<Mutex<BTreeSet<Socket>>>,
    thread: std::thread::JoinHandle<()>,
}

impl Sockets {
    /// Starts sampling, once a first full scan has worked.
    pub fn sample() -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let seen = Arc::new(Mutex::new(BTreeSet::new()));
        let (going, recorded) = (running.clone(), seen.clone());
        let (ready, is_ready) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let mut first = true;
            while going.load(Ordering::SeqCst) {
                let scan = own_sockets().expect("the kernel's socket tables are readable");
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

    pub fn stop(self) -> BTreeSet<Socket> {
        self.running.store(false, Ordering::SeqCst);
        self.thread.join().expect("the sampler ends");
        self.seen.lock().unwrap().clone()
    }
}

/// This process's sockets now (by inode, from `/proc/self/fd`):
/// - in `/proc/self/net/tcp` and `tcp6`, those with a remote port whose
///   local port isn't one it listens on;
/// - in `/proc/self/net/udp` and `udp6`, every one.
///
/// An error if a table can't be read.
fn own_sockets() -> std::io::Result<Vec<Socket>> {
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
    // (local port, remote port, listening), this process's own only.
    let rows = |tables: [&str; 2]| -> std::io::Result<Vec<(u16, u16, bool)>> {
        let mut rows = Vec::new();
        for table in tables {
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
        Ok(rows)
    };
    let tcp = rows(["/proc/self/net/tcp", "/proc/self/net/tcp6"])?;
    let udp = rows(["/proc/self/net/udp", "/proc/self/net/udp6"])?;
    let listening: BTreeSet<u16> = tcp
        .iter()
        .filter(|(_, _, listens)| *listens)
        .map(|(local, _, _)| *local)
        .collect();
    Ok(tcp
        .into_iter()
        .filter(|(local, remote, listens)| !listens && *remote != 0 && !listening.contains(local))
        .map(|(local, remote, _)| Socket::Tcp(local, remote))
        .chain(udp.into_iter().map(|(local, _, _)| Socket::Udp(local)))
        .collect())
}

/// Sockets held for as long as it lives, over IPv4 and IPv6: two TCP
/// client connections and two UDP sockets, what the sampler must see, or
/// it is reading nothing.
pub struct Held {
    /// The TCP connections' remote ports.
    pub tcp: Vec<u16>,
    /// The UDP sockets' local ports.
    pub udp: Vec<u16>,
    _streams: Vec<TcpStream>,
    _listeners: Vec<TcpListener>,
    _sockets: Vec<UdpSocket>,
}

impl Held {
    pub fn open() -> Self {
        let mut held = Self {
            tcp: Vec::new(),
            udp: Vec::new(),
            _streams: Vec::new(),
            _listeners: Vec::new(),
            _sockets: Vec::new(),
        };
        for bind in ["127.0.0.1:0", "[::1]:0"] {
            let listener = TcpListener::bind(bind).expect("a loopback listener, IPv4 and IPv6");
            let addr = listener.local_addr().unwrap();
            held._streams.push(TcpStream::connect(addr).unwrap());
            held._streams.push(listener.accept().unwrap().0);
            held.tcp.push(addr.port());
            held._listeners.push(listener);
            let socket = UdpSocket::bind(bind).expect("a loopback UDP socket, IPv4 and IPv6");
            held.udp.push(socket.local_addr().unwrap().port());
            held._sockets.push(socket);
        }
        held
    }

    /// Fails unless the sampler saw every held socket.
    pub fn assert_seen(&self, seen: &BTreeSet<Socket>) {
        for port in &self.tcp {
            assert!(
                seen.iter()
                    .any(|socket| matches!(socket, Socket::Tcp(_, remote) if remote == port)),
                "the sampler missed a held TCP connection"
            );
        }
        for port in &self.udp {
            assert!(
                seen.contains(&Socket::Udp(*port)),
                "the sampler missed a held UDP socket"
            );
        }
    }
}

/// Marks a test's run in its private network.
const PRIVATE: &str = "FREEDOM_IPFS_TEST_PRIVATE_NETWORK";

/// Set where the private network must run (CI's Linux job): a test that
/// can't enter one fails, rather than running in place.
const REQUIRE: &str = "FREEDOM_IPFS_REQUIRE_PRIVATE_NET";

/// What a run in the private network prints once its checks have passed
/// ([`private_run_checked`]): its parent requires it, and repeats it where
/// the test log shows it.
const CHECKED: &str = "checked in a private network:";

/// The private network: loopback up, and every IPv4 and IPv6 address local
/// to it, so a datagram or a connection to any address stays on this host
/// and is counted.
const SETUP: &str = "ip link set lo up \
    && ip route add local 0.0.0.0/0 dev lo \
    && ip -6 route add local ::/0 dev lo";

/// Where a test runs ([`private_network`]).
pub enum Run {
    /// It ran again in a private network of its own, and passed.
    Done,
    /// This is that run: [`Counters`] are this test's alone.
    Private,
    /// Here, as no private network is available, and the run doesn't
    /// require one: the counters mean nothing, and the skip is reported
    /// where the test log shows it.
    Here,
}

/// Runs `test` (this binary's, by its exact name) again in a private
/// network of its own: a user and network namespace (`unshare -rn`) where
/// every address is local (`SETUP`), and nothing else runs. Its outcome is
/// this run's, and it must have reached [`private_run_checked`]; the line
/// that prints is repeated on this process's stderr, uncaptured, so the
/// test log shows the private run happened.
///
/// Where namespaces aren't available (not Linux, no `unshare` or `ip`,
/// unprivileged user namespaces refused): with
/// `FREEDOM_IPFS_REQUIRE_PRIVATE_NET` set, the test fails; without it, it
/// runs here, with the kernel sampler only, and the skip is written to
/// stderr, uncaptured, so the test log shows it even when the test passes.
pub fn private_network(test: &str) -> Run {
    if std::env::var_os(PRIVATE).is_some() {
        return Run::Private;
    }
    let here = |why: String| {
        assert!(
            std::env::var_os(REQUIRE).is_none(),
            "{REQUIRE} is set, and {test} can't enter a private network: {why}"
        );
        // libtest captures `eprintln!`, not a write to stderr itself.
        let _ = writeln!(
            std::io::stderr(),
            "{test}: SKIPPED its private network ({why}); ran in place, with the kernel \
             sampler only, which a socket opened and closed between two samples can pass"
        );
        Run::Here
    };
    if !cfg!(target_os = "linux") {
        return here("private networks need Linux".into());
    }
    let unshare = || {
        let mut command = Command::new("unshare");
        command.args(["--user", "--map-root-user", "--net", "sh", "-c"]);
        command
    };
    match unshare().arg(SETUP).output() {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            return here(format!(
                "`unshare -rn` and `ip` failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
        Err(error) => return here(format!("no `unshare`: {error}")),
    }
    let output = unshare()
        .arg(format!("{SETUP} && exec \"$0\" \"$@\""))
        .arg(std::env::current_exe().expect("the test binary's path"))
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(PRIVATE, "1")
        .output()
        .expect("the test runs in its private network");
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let checked = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.find(CHECKED).map(|at| line[at..].to_string()));
    let (true, true, Some(checked)) = (
        output.status.success(),
        report.contains("1 passed"),
        checked,
    ) else {
        panic!(
            "the test, in its private network: {}\n{report}",
            output.status
        );
    };
    let _ = writeln!(std::io::stderr(), "{test}: {checked}");
    Run::Done
}

/// Reports, from a run in the private network, that its checks have
/// passed there, with what its counters saw.
pub fn private_run_checked(sent: Counters) {
    println!(
        "{CHECKED} {} datagrams, {} connections",
        sent.datagrams, sent.connects
    );
}

/// What a network namespace has sent since it began: TCP connections
/// opened (`Tcp: ActiveOpens`) and UDP datagrams, IPv4 and IPv6 (`Udp:
/// OutDatagrams`, `Udp6OutDatagrams`). In a private network, every one the
/// test sent, however short-lived its socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counters {
    pub connects: usize,
    pub datagrams: usize,
}

impl Counters {
    pub fn read() -> Self {
        let snmp = std::fs::read_to_string("/proc/self/net/snmp").expect("/proc/net/snmp");
        let field = |protocol: &str, name: &str| -> usize {
            let prefix = format!("{protocol}:");
            let mut lines = snmp.lines().filter(|line| line.starts_with(&prefix));
            let (Some(names), Some(values)) = (lines.next(), lines.next()) else {
                panic!("no {protocol} counters");
            };
            let index = names
                .split_whitespace()
                .position(|field| field == name)
                .unwrap_or_else(|| panic!("no {protocol} {name}"));
            values
                .split_whitespace()
                .nth(index)
                .unwrap()
                .parse()
                .unwrap()
        };
        let snmp6 = std::fs::read_to_string("/proc/self/net/snmp6").expect("/proc/net/snmp6");
        let udp6: usize = snmp6
            .lines()
            .find_map(|line| line.strip_prefix("Udp6OutDatagrams"))
            .expect("Udp6OutDatagrams")
            .trim()
            .parse()
            .unwrap();
        Self {
            connects: field("Tcp", "ActiveOpens"),
            datagrams: field("Udp", "OutDatagrams") + udp6,
        }
    }

    /// What was sent since `before`.
    pub fn since(self, before: Self) -> Self {
        Self {
            connects: self.connects - before.connects,
            datagrams: self.datagrams - before.datagrams,
        }
    }

    /// Fails unless the counters see a datagram to a documentation address,
    /// over IPv4 and over IPv6, and a connection to one: the private network
    /// keeps them local, and counts them.
    pub fn assert_counting() {
        let before = Self::read();
        for (bind, to) in [("0.0.0.0:0", "192.0.2.1:9"), ("[::]:0", "[2001:db8::1]:9")] {
            let socket = UdpSocket::bind(bind).unwrap();
            socket
                .send_to(b"counted", to)
                .expect("the private network takes a datagram to any address");
        }
        let _ = TcpStream::connect("192.0.2.1:9");
        assert_eq!(
            Self::read().since(before),
            Self {
                connects: 1,
                datagrams: 2,
            },
            "the private network's counters count"
        );
    }
}
