//! One DNS configuration for a `direct` network's own lookups (design 0007,
//! "The resolver contract"): the gate's HTTP resolver and the libp2p
//! transports' DNS layer (`/dns*`, `/dnsaddr`, WebSocket names) ask the
//! same servers. The system's resolv.conf is read once, and the built-in
//! fallback is taken, and reported, only when it can't be read or parsed.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, SocketAddrV6, TcpListener};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_net::gate::{parse_url, ConfiguredTargets, Nat64State};
use freedom_ipfs_net::transport::{SwarmTransport, Transports};
use freedom_ipfs_net::{
    Addresses, ClientConfig, DnsConfig, DnsSource, Nat64Handle, Network, NetworkPolicy,
};
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
use hickory_resolver::proto::op::{Message, MessageType, ResponseCode};
use hickory_resolver::proto::rr::rdata::{A, TXT};
use hickory_resolver::proto::rr::{RData, Record, RecordType};
use libp2p::core::transport::{DialOpts, PortUse, TransportError};
use libp2p::core::Endpoint;
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, Transport};
use url::Url;

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
/// A name only the fixture server answers: Cloudflare's resolvers, the
/// built-in fallback, answer it with NXDOMAIN, as every `.example` name.
const NAME: &str = "system-dns.example";
const DNSADDR: &str = "peers.system-dns.example";

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

fn multiaddr(text: &str) -> Multiaddr {
    text.parse()
        .unwrap_or_else(|err| panic!("{text} parses: {err}"))
}

/// A DNS server on a local UDP socket: it answers from a table, NXDOMAIN
/// otherwise, and records every query.
struct Dns {
    addr: SocketAddr,
    queries: Arc<Mutex<Vec<(String, RecordType)>>>,
}

impl Dns {
    fn start(answers: HashMap<(String, RecordType), Vec<RData>>) -> Self {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let recorded = queries.clone();
        std::thread::spawn(move || {
            let mut buffer = [0u8; 1500];
            while let Ok((len, from)) = socket.recv_from(&mut buffer) {
                let Ok(request) = Message::from_vec(&buffer[..len]) else {
                    continue;
                };
                let Some(query) = request.queries().first().cloned() else {
                    continue;
                };
                let name = query.name().to_ascii().trim_end_matches('.').to_string();
                recorded
                    .lock()
                    .unwrap()
                    .push((name.clone(), query.query_type()));
                let mut response = Message::new();
                response
                    .set_id(request.id())
                    .set_message_type(MessageType::Response)
                    .set_recursion_desired(true)
                    .set_recursion_available(true)
                    .add_query(query.clone());
                match answers.get(&(name, query.query_type())) {
                    Some(records) => {
                        for rdata in records {
                            response.add_answer(Record::from_rdata(
                                query.name().clone(),
                                60,
                                rdata.clone(),
                            ));
                        }
                    }
                    None => {
                        response.set_response_code(ResponseCode::NXDomain);
                    }
                }
                let _ = socket.send_to(&response.to_vec().unwrap(), from);
            }
        });
        Self { addr, queries }
    }

    fn config(&self) -> ResolverConfig {
        ResolverConfig::from_parts(
            None,
            Vec::new(),
            NameServerConfigGroup::from_ips_clear(&[self.addr.ip()], self.addr.port(), true),
        )
    }

    fn queries(&self) -> Vec<(String, RecordType)> {
        self.queries.lock().unwrap().clone()
    }

    fn asked(&self, name: &str, kind: RecordType) -> bool {
        self.queries
            .lock()
            .unwrap()
            .iter()
            .any(|(asked, asked_kind)| asked == name && *asked_kind == kind)
    }
}

/// A listener that counts the connections it accepts; as an HTTP server,
/// it answers each request with 200.
struct Listener {
    port: u16,
    accepted: Arc<AtomicUsize>,
}

impl Listener {
    fn start(http: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                counted.fetch_add(1, Ordering::SeqCst);
                if !http {
                    continue;
                }
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                            break;
                        }
                    }
                    let _ = stream.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    );
                });
            }
        });
        Self { port, accepted }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

async fn wait_until(limit: Duration, done: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    done()
}

/// Dials `addr` through `transport`, for at most `limit`.
async fn dial_through(transport: &mut SwarmTransport, addr: &Multiaddr, limit: Duration) {
    let opts = DialOpts {
        role: Endpoint::Dialer,
        port_use: PortUse::New,
    };
    match transport.dial(addr.clone(), opts) {
        Err(TransportError::Other(_)) | Err(_) => {}
        Ok(dial) => {
            let _ = tokio::time::timeout(limit, dial).await;
        }
    }
}

/// The network's one configuration, a fixture server whose answers the
/// fallback doesn't give, serves an HTTP request through the gate's
/// resolver and every transport's lookups: a `/dns4` dial through the DHT's
/// stack, a `/dns4/../ws` dial through Bitswap's WebSocket, and a
/// `/dnsaddr` expansion. The transports are `Transports::new`'s alone: no
/// test override of their DNS.
#[tokio::test(flavor = "multi_thread")]
async fn one_dns_configuration_serves_http_and_every_transport() {
    let http = Listener::start(true);
    let canary = Listener::start(false);
    let dns = Dns::start(HashMap::from([
        (
            (NAME.to_string(), RecordType::A),
            vec![RData::A(A::new(127, 0, 0, 1))],
        ),
        (
            (format!("_dnsaddr.{DNSADDR}"), RecordType::TXT),
            vec![RData::TXT(TXT::new(vec![format!(
                "dnsaddr=/dns4/{NAME}/tcp/{}/p2p/{PEER}",
                canary.port
            )]))],
        ),
    ]));
    // The HTTP server's name and port, and the canary's address and port,
    // are configured, so the checked address policy lets loopback through
    // to them alone.
    let mut configured = ConfiguredTargets::new();
    for url in [
        format!("http://{NAME}:{}/", http.port),
        format!("http://127.0.0.1:{}/", canary.port),
    ] {
        configured.add_url(&parse_url(&url).unwrap()).unwrap();
    }
    let network = Network::with_dns(
        NetworkPolicy::direct(
            configured,
            doh(),
            Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE)),
        )
        .unwrap(),
        DnsConfig::custom(dns.config(), ResolverOpts::default()),
    )
    .unwrap();
    assert_eq!(
        network.dns().map(DnsConfig::source),
        Some(&DnsSource::Custom)
    );

    let client = network.http_client(ClientConfig::new(Duration::from_secs(5), 0));
    let status = client
        .get(format!("http://{NAME}:{}/", http.port))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status.as_u16(), 200);
    assert_eq!(http.accepted(), 1);
    assert!(dns.asked(NAME, RecordType::A));

    let keypair = Keypair::generate_ed25519();
    let transports = Transports::new(&network);
    let port = canary.port;
    for (stack, addr) in [
        ("dht", format!("/dns4/{NAME}/tcp/{port}/p2p/{PEER}")),
        ("bitswap", format!("/dns4/{NAME}/tcp/{port}/ws/p2p/{PEER}")),
        ("dht", format!("/dnsaddr/{DNSADDR}/p2p/{PEER}")),
    ] {
        let mut transport = match stack {
            "dht" => transports.dht(&keypair).unwrap(),
            _ => transports.bitswap(&keypair).unwrap(),
        };
        let before = canary.accepted();
        dial_through(&mut transport, &multiaddr(&addr), Duration::from_secs(3)).await;
        assert!(
            wait_until(Duration::from_secs(2), || canary.accepted() > before).await,
            "{stack}: {addr} didn't reach the canary"
        );
    }
    assert!(dns.asked(&format!("_dnsaddr.{DNSADDR}"), RecordType::TXT));
}

/// Every name is looked up exactly as written, over HTTP and through
/// `/dns4`: a single label and two, with a configuration whose search list
/// and domain would expand both first (its ndots is 5). A name that
/// doesn't exist as written isn't tried under the search domain either,
/// though the server would answer it there. The fixture server sees the
/// names as written and nothing under the search domain.
#[tokio::test(flavor = "multi_thread")]
async fn names_are_looked_up_exactly_as_written() {
    let http = Listener::start(true);
    let canary = Listener::start(false);
    let loopback = || vec![RData::A(A::new(127, 0, 0, 1))];
    let dns = Dns::start(HashMap::from([
        (("intranet".to_string(), RecordType::A), loopback()),
        (("two.labels".to_string(), RecordType::A), loopback()),
        (
            ("intranet.corp.example".to_string(), RecordType::A),
            loopback(),
        ),
        (
            ("two.labels.corp.example".to_string(), RecordType::A),
            loopback(),
        ),
        (
            ("missing.corp.example".to_string(), RecordType::A),
            loopback(),
        ),
    ]));
    let corp = hickory_resolver::Name::from_ascii("corp.example.").unwrap();
    let config = ResolverConfig::from_parts(
        Some(corp.clone()),
        vec![corp],
        dns.config().name_servers().to_vec(),
    );
    let mut options = ResolverOpts::default();
    options.ndots = 5;
    let dns_config = DnsConfig::custom(config, options);
    assert!(dns_config.config().search().is_empty());
    assert_eq!(dns_config.config().domain(), None);
    assert_eq!(dns_config.options().ndots, 0);

    let mut configured = ConfiguredTargets::new();
    for url in [
        format!("http://intranet:{}/", http.port),
        format!("http://two.labels:{}/", http.port),
        format!("http://missing:{}/", http.port),
        format!("http://127.0.0.1:{}/", canary.port),
    ] {
        configured.add_url(&parse_url(&url).unwrap()).unwrap();
    }
    let network = Network::with_dns(
        NetworkPolicy::direct(
            configured,
            doh(),
            Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE)),
        )
        .unwrap(),
        dns_config,
    )
    .unwrap();
    let client = network.http_client(ClientConfig::new(Duration::from_secs(5), 0));
    let keypair = Keypair::generate_ed25519();
    let transports = Transports::new(&network);
    for name in ["intranet", "two.labels"] {
        let status = client
            .get(format!("http://{name}:{}/", http.port))
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status.as_u16(), 200, "{name}");
        let mut transport = transports.dht(&keypair).unwrap();
        let before = canary.accepted();
        let addr = multiaddr(&format!("/dns4/{name}/tcp/{}/p2p/{PEER}", canary.port));
        dial_through(&mut transport, &addr, Duration::from_secs(3)).await;
        assert!(
            wait_until(Duration::from_secs(2), || canary.accepted() > before).await,
            "{name}"
        );
    }
    let served = http.accepted();
    assert!(client
        .get(format!("http://missing:{}/", http.port))
        .send()
        .await
        .is_err());
    assert_eq!(http.accepted(), served);
    let mut transport = transports.dht(&keypair).unwrap();
    let before = canary.accepted();
    let addr = multiaddr(&format!("/dns4/missing/tcp/{}/p2p/{PEER}", canary.port));
    dial_through(&mut transport, &addr, Duration::from_secs(3)).await;
    assert!(!wait_until(Duration::from_millis(500), || canary.accepted() > before).await);

    let queries = dns.queries();
    assert!(dns.asked("intranet", RecordType::A) && dns.asked("two.labels", RecordType::A));
    assert!(dns.asked("missing", RecordType::A));
    assert!(
        queries
            .iter()
            .all(|(name, _)| ["intranet", "two.labels", "missing"].contains(&name.as_str())),
        "{queries:?}"
    );
}

/// The legacy policy's network reads no configuration: its crates resolve
/// as they did before.
#[test]
fn the_legacy_policy_reads_no_dns_configuration() {
    assert!(Network::legacy().dns().is_none());
    let dns = Dns::start(HashMap::new());
    let legacy = Network::with_dns(
        NetworkPolicy::legacy(),
        DnsConfig::custom(dns.config(), ResolverOpts::default()),
    )
    .unwrap();
    assert!(legacy.dns().is_none());
}

/// resolv.conf files, read as `DnsConfig::system` reads /etc/resolv.conf.
#[cfg(unix)]
mod resolv_conf {
    use std::path::PathBuf;

    use freedom_ipfs_net::DnsFallback;

    use super::*;

    /// A directory of its own under the system's temporary one, removed
    /// when dropped.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "freedom-ipfs-net-{name}-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn file(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn servers(config: &ResolverConfig) -> Vec<SocketAddr> {
        let mut servers: Vec<SocketAddr> = config
            .name_servers()
            .iter()
            .map(|server| server.socket_addr)
            .collect();
        servers.dedup();
        servers
    }

    /// A resolv.conf that reads and parses is the system's: its
    /// nameservers and its options, nothing of the fallback's.
    #[test]
    fn a_resolv_conf_that_parses_is_used() {
        let scratch = Scratch::new("parses");
        let path = scratch.file(
            "resolv.conf",
            "# a comment\nnameserver 192.0.2.53\nnameserver 2001:db8::53\n\
             search example.com\noptions edns0 trust-ad timeout:3 attempts:4\n",
        );
        let dns = DnsConfig::from_resolv_conf(&path);
        assert_eq!(dns.source(), &DnsSource::System);
        assert_eq!(
            servers(dns.config()),
            [
                "192.0.2.53:53".parse::<SocketAddr>().unwrap(),
                "[2001:db8::53]:53".parse().unwrap(),
            ]
        );
        assert_eq!(dns.options().timeout, Duration::from_secs(3));
        assert_eq!(dns.options().attempts, 4);
        // Names are looked up as written: the file's search list is
        // dropped, and ndots is 0.
        assert!(dns.config().search().is_empty());
        assert_eq!(dns.config().domain(), None);
        assert_eq!(dns.options().ndots, 0);
    }

    /// The loopback interface's name and index here: `lo` on Linux, `lo0`
    /// on macOS and the BSDs.
    fn loopback() -> (&'static str, u32) {
        for name in ["lo", "lo0"] {
            let c_name = std::ffi::CString::new(name).unwrap();
            // SAFETY: a NUL-terminated string that outlives the call.
            let index = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
            if index != 0 {
                return (name, index);
            }
        }
        panic!("no loopback interface named lo or lo0");
    }

    /// The resolv.conf files users have, as their systems write them
    /// (`tests/fixtures/resolv-conf/`), are the system's configuration, not
    /// the fallback: each one's nameservers, a link-local one with its
    /// zone. None has a line the strict parser refuses: comments,
    /// `search .`, `options edns0 trust-ad`, a zoned link-local
    /// nameserver, OpenBSD's `lookup` and `family`, and glibc's `sortlist`
    /// all parse. Their search lists and ndots are dropped: names are
    /// looked up as written.
    ///
    /// NetworkManager's and macOS's files name their link-local server's
    /// interface (`%wlp2s0`, `%en0`), which this host may not have: the
    /// test reads them with that name replaced by this host's loopback
    /// interface, and expects its index as the server's scope.
    #[test]
    fn the_resolv_conf_files_users_have_are_used() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("resolv-conf");
        let scratch = Scratch::new("fixtures");
        let (loopback, index) = loopback();
        let link_local =
            |zone| SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 53, 0, zone));
        let at = |ip: &str| SocketAddr::new(ip.parse().unwrap(), 53);
        for (file, zone, nameservers) in [
            ("ubuntu-systemd-resolved-stub", None, vec![at("127.0.0.53")]),
            (
                "ubuntu-systemd-resolved-stub-no-search",
                None,
                vec![at("127.0.0.53")],
            ),
            (
                "systemd-resolved-uplink",
                None,
                vec![at("192.168.1.1"), at("fd00::1"), at("192.168.1.2")],
            ),
            (
                "networkmanager",
                Some("wlp2s0"),
                vec![at("192.168.1.1"), link_local(index)],
            ),
            ("fedora-systemd-resolved-stub", None, vec![at("127.0.0.53")]),
            (
                "debian-resolvconf",
                None,
                vec![at("192.168.1.1"), at("2001:db8::1")],
            ),
            ("debian-dhclient", None, vec![at("192.168.1.1")]),
            ("alpine-udhcpc", None, vec![at("192.168.1.1")]),
            (
                "docker-default-bridge",
                None,
                vec![at("8.8.8.8"), at("8.8.4.4")],
            ),
            ("docker-user-network", None, vec![at("127.0.0.11")]),
            ("kubernetes-pod", None, vec![at("10.96.0.10")]),
            ("wsl", None, vec![at("172.29.112.1")]),
            ("wsl-dns-tunneling", None, vec![at("10.255.255.254")]),
            (
                "macos",
                Some("en0"),
                vec![at("192.168.1.1"), link_local(index)],
            ),
            ("freebsd-resolvconf", None, vec![at("192.168.1.1")]),
            ("openbsd", None, vec![at("192.168.1.1")]),
            (
                "glibc-options",
                None,
                vec![at("192.168.1.1"), at("192.168.1.2")],
            ),
        ] {
            let path = match zone {
                None => dir.join(file),
                Some(zone) => {
                    let contents = std::fs::read_to_string(dir.join(file)).unwrap();
                    let named = format!("%{zone}");
                    assert!(contents.contains(&named), "{file} names {zone}");
                    scratch.file(file, &contents.replace(&named, &format!("%{loopback}")))
                }
            };
            let dns = DnsConfig::from_resolv_conf(&path);
            assert_eq!(dns.source(), &DnsSource::System, "{file}");
            assert_eq!(servers(dns.config()), nameservers, "{file}");
            assert!(dns.config().search().is_empty(), "{file}");
            assert_eq!(dns.options().ndots, 0, "{file}");
        }
    }

    /// A nameserver's zone is kept on both its servers (UDP and TCP): a
    /// numeric one as it is, a named one as its interface's index. A name
    /// no interface has here can't be used: the fallback, reported as
    /// unparseable, with the zone named. Only the first three nameservers
    /// count: a fourth, its zone, or a fourth that doesn't parse at all, is
    /// ignored, and the lines after it are still read.
    #[test]
    fn a_nameservers_zone_is_kept_or_reported() {
        let scratch = Scratch::new("zones");
        let (loopback, index) = loopback();
        for (file, line, scope) in [
            ("numeric", "nameserver fe80::1%3".to_string(), 3),
            ("named", format!("nameserver fe80::1%{loopback}"), index),
        ] {
            let path = scratch.file(file, &format!("nameserver 192.0.2.53\n{line}\n"));
            let dns = DnsConfig::from_resolv_conf(&path);
            assert_eq!(dns.source(), &DnsSource::System, "{file}");
            let zoned: Vec<u32> = dns
                .config()
                .name_servers()
                .iter()
                .filter_map(|server| match server.socket_addr {
                    SocketAddr::V6(addr) => Some(addr.scope_id()),
                    SocketAddr::V4(_) => None,
                })
                .collect();
            assert_eq!(zoned, [scope, scope], "{file}: UDP and TCP");
        }
        // Only the first three nameservers are used, as glibc's, musl's and
        // the BSDs' resolvers use them: a usable fourth one is ignored, and
        // so is a fourth one's zone that names no interface.
        let first_three = [
            "192.0.2.1:53".parse::<SocketAddr>().unwrap(),
            SocketAddr::V6(SocketAddrV6::new("fe80::1".parse().unwrap(), 53, 0, 3)),
            "192.0.2.2:53".parse().unwrap(),
        ];
        // A malformed fourth is ignored too, and a line after it is still
        // read: its options apply.
        for (file, fourth) in [
            ("fourth", "fe80::2%4"),
            ("fourth-unknown", "fe80::2%nosuchif0"),
            ("fourth-malformed", "not-an-address"),
        ] {
            let path = scratch.file(
                file,
                &format!(
                    "nameserver 192.0.2.1\nnameserver fe80::1%3\nnameserver 192.0.2.2\n\
                     nameserver {fourth}\noptions timeout:3 attempts:4\n"
                ),
            );
            let dns = DnsConfig::from_resolv_conf(&path);
            assert_eq!(dns.source(), &DnsSource::System, "{file}");
            assert_eq!(servers(dns.config()), first_three, "{file}");
            assert_eq!(dns.options().timeout, Duration::from_secs(3), "{file}");
            assert_eq!(dns.options().attempts, 4, "{file}");
        }
        // A malformed nameserver among the first three still fails the
        // file, as any malformed line does: the fallback, reported.
        let path = scratch.file(
            "third-malformed",
            "nameserver 192.0.2.1\nnameserver 192.0.2.2\nnameserver not-an-address\n\
             nameserver 192.0.2.3\n",
        );
        assert!(matches!(
            DnsConfig::from_resolv_conf(&path).source(),
            DnsSource::Fallback(DnsFallback::Unparseable(_))
        ));
        let path = scratch.file("unknown", "nameserver fe80::1%nosuchif0\n");
        let dns = DnsConfig::from_resolv_conf(&path);
        match dns.source() {
            DnsSource::Fallback(DnsFallback::Unparseable(why)) => {
                assert!(why.contains("nosuchif0"), "{why}");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            servers(dns.config()),
            servers(&ResolverConfig::cloudflare())
        );
    }

    /// A resolv.conf that is readable but malformed, or names no
    /// nameserver, takes the fallback, Cloudflare's resolvers, and says
    /// so, with the parser's reason; so does one that can't be read. A
    /// network built over it reports the same.
    #[test]
    fn a_malformed_or_unreadable_resolv_conf_takes_the_fallback_and_says_why() {
        let scratch = Scratch::new("malformed");
        let cloudflare = servers(&ResolverConfig::cloudflare());
        for (name, contents, reason) in [
            ("bad-address", "nameserver not-an-address\n", "invalid IP"),
            (
                "bad-option",
                "nameserver 192.0.2.53\noptions ndots:many\n",
                "invalid value of some option",
            ),
            (
                "unknown-directive",
                "nameserver 192.0.2.53\nresolver 192.0.2.54\n",
                "is not recognized",
            ),
            ("no-nameserver", "search example.com\n", "no nameservers"),
        ] {
            let path = scratch.file(name, contents);
            let dns = DnsConfig::from_resolv_conf(&path);
            match dns.source() {
                DnsSource::Fallback(DnsFallback::Unparseable(why)) => {
                    assert!(why.contains(reason), "{name}: {why}");
                }
                other => panic!("{name}: {other:?}"),
            }
            assert_eq!(servers(dns.config()), cloudflare, "{name}");
            assert!(dns.source().to_string().starts_with("Cloudflare's resolvers, the fallback: the system's DNS configuration couldn't be parsed"));

            let network = Network::with_dns(
                NetworkPolicy::direct(ConfiguredTargets::new(), doh(), Addresses::Unchecked)
                    .unwrap(),
                dns,
            )
            .unwrap();
            assert!(
                matches!(
                    network.dns().map(DnsConfig::source),
                    Some(DnsSource::Fallback(DnsFallback::Unparseable(_)))
                ),
                "{name}"
            );
        }

        for path in [scratch.0.join("missing"), scratch.0.clone()] {
            let dns = DnsConfig::from_resolv_conf(&path);
            assert!(
                matches!(
                    dns.source(),
                    DnsSource::Fallback(DnsFallback::Unreadable(_))
                ),
                "{}: {:?}",
                path.display(),
                dns.source()
            );
            assert_eq!(servers(dns.config()), cloudflare);
        }
    }
}
