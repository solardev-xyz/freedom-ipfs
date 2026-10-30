//! One DNS configuration for a `direct` network's own lookups (design 0007,
//! "The resolver contract"): the gate's HTTP resolver and the libp2p
//! transports' DNS layer (`/dns*`, `/dnsaddr`, WebSocket names) ask the
//! same servers. The system's resolv.conf is read once, and the built-in
//! fallback is taken, and reported, only when it can't be read or parsed.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
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
    }

    /// The resolv.conf files users have, as their systems write them
    /// (`tests/fixtures/resolv-conf/`), are the system's configuration, not
    /// the fallback: each one's nameservers, and its options where it sets
    /// any. None has a line the strict parser refuses: comments, `search .`,
    /// `options edns0 trust-ad`, a zoned link-local nameserver, OpenBSD's
    /// `lookup` and `family`, and glibc's `sortlist` all parse.
    #[test]
    fn the_resolv_conf_files_users_have_are_used() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("resolv-conf");
        for (file, nameservers, ndots) in [
            ("ubuntu-systemd-resolved-stub", &["127.0.0.53"][..], 1),
            ("ubuntu-systemd-resolved-stub-no-search", &["127.0.0.53"], 1),
            (
                "systemd-resolved-uplink",
                &["192.168.1.1", "fd00::1", "192.168.1.2"],
                1,
            ),
            ("networkmanager", &["192.168.1.1", "fe80::1"], 1),
            ("fedora-systemd-resolved-stub", &["127.0.0.53"], 1),
            ("debian-resolvconf", &["192.168.1.1", "2001:db8::1"], 1),
            ("debian-dhclient", &["192.168.1.1"], 1),
            ("alpine-udhcpc", &["192.168.1.1"], 1),
            ("docker-default-bridge", &["8.8.8.8", "8.8.4.4"], 1),
            ("docker-user-network", &["127.0.0.11"], 0),
            ("kubernetes-pod", &["10.96.0.10"], 5),
            ("wsl", &["172.29.112.1"], 1),
            ("wsl-dns-tunneling", &["10.255.255.254"], 1),
            ("macos", &["192.168.1.1", "fe80::1"], 1),
            ("freebsd-resolvconf", &["192.168.1.1"], 1),
            ("openbsd", &["192.168.1.1"], 1),
            ("glibc-options", &["192.168.1.1", "192.168.1.2"], 1),
        ] {
            let dns = DnsConfig::from_resolv_conf(&dir.join(file));
            assert_eq!(dns.source(), &DnsSource::System, "{file}");
            let expected: Vec<SocketAddr> = nameservers
                .iter()
                .map(|ip| SocketAddr::new(ip.parse().unwrap(), 53))
                .collect();
            assert_eq!(servers(dns.config()), expected, "{file}");
            assert_eq!(dns.options().ndots, ndots, "{file}");
        }
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
