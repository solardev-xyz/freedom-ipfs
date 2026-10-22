//! The gate in the libp2p transports (design 0007, "The gate", check 3),
//! layer by layer over a transport that counts its dials, and through the
//! swarms' own transport stacks against canary listeners and a local DNS
//! server that records every query. Each refusal has a control that does
//! dial: the legacy policy, or `direct` without the address table.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use freedom_ipfs_net::gate::{ConfiguredTargets, Nat64State};
use freedom_ipfs_net::transport::{GatedError, Transports};
use freedom_ipfs_net::{
    Addresses, DialPolicy, DialRefusal, Nat64Handle, Network, NetworkCapability, NetworkPolicy,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use hickory_resolver::config::{NameServerConfigGroup, ResolverConfig};
use hickory_resolver::proto::op::{Message, MessageType, ResponseCode};
use hickory_resolver::proto::rr::rdata::{A, TXT};
use hickory_resolver::proto::rr::{RData, Record, RecordType};
use libp2p::core::transport::{DialOpts, ListenerId, PortUse, TransportError, TransportEvent};
use libp2p::core::Endpoint;
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, Transport};
use url::Url;

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
const ONION3: &str = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";

fn opts() -> DialOpts {
    DialOpts {
        role: Endpoint::Dialer,
        port_use: PortUse::New,
    }
}

fn multiaddr(text: &str) -> Multiaddr {
    text.parse()
        .unwrap_or_else(|err| panic!("{text} parses: {err}"))
}

/// A transport that records every address it is asked to dial and
/// "connects" at once, or, when held, only once released; it records when
/// a held dial is dropped.
#[derive(Clone, Default)]
struct Counting {
    dialed: Arc<Mutex<Vec<Multiaddr>>>,
    hold: Option<Arc<tokio::sync::Notify>>,
    dropped: Arc<AtomicBool>,
}

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Counting {
    fn dialed(&self) -> Vec<String> {
        self.dialed
            .lock()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect()
    }
}

impl Transport for Counting {
    type Output = ();
    type Error = io::Error;
    type ListenerUpgrade = futures::future::Pending<Result<(), io::Error>>;
    type Dial = BoxFuture<'static, Result<(), io::Error>>;

    fn listen_on(
        &mut self,
        _: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<io::Error>> {
        Err(TransportError::MultiaddrNotSupported(addr))
    }

    fn remove_listener(&mut self, _: ListenerId) -> bool {
        false
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        _: DialOpts,
    ) -> Result<Self::Dial, TransportError<io::Error>> {
        self.dialed.lock().unwrap().push(addr);
        let hold = self.hold.clone();
        let guard = SetOnDrop(self.dropped.clone());
        Ok(async move {
            let _guard = guard;
            if let Some(hold) = hold {
                hold.notified().await;
            }
            Ok(())
        }
        .boxed())
    }

    fn poll(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, io::Error>> {
        Poll::Pending
    }
}

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

fn unchecked() -> Network {
    Network::new(
        NetworkPolicy::direct(ConfiguredTargets::new(), doh(), Addresses::Unchecked).unwrap(),
    )
    .unwrap()
}

fn checked_with(configured: ConfiguredTargets) -> Network {
    Network::with_lookup(
        NetworkPolicy::direct(
            configured,
            doh(),
            Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE)),
        )
        .unwrap(),
        None,
    )
}

fn checked() -> Network {
    checked_with(ConfiguredTargets::new())
}

fn refused<T, E: std::fmt::Debug>(
    result: Result<T, TransportError<GatedError<E>>>,
) -> GatedError<E> {
    match result {
        Err(TransportError::Other(err)) => err,
        Err(other) => panic!("not refused by the gate: {other:?}"),
        Ok(_) => panic!("not refused: the dial went on"),
    }
}

#[tokio::test]
async fn the_inner_gate_refuses_an_embedded_private_address_before_any_socket() {
    for (network, refuses) in [(checked(), true), (unchecked(), false)] {
        let counting = Counting::default();
        let mut gated =
            freedom_ipfs_net::transport::Gated::inner(counting.clone(), network.dial_policy());
        let embedded = multiaddr("/ip6/64:ff9b::a9fe:a9fe/tcp/80");
        let public = multiaddr("/ip6/64:ff9b::808:808/tcp/80");
        let result = gated.dial(embedded.clone(), opts());
        if refuses {
            assert!(matches!(
                refused(result),
                GatedError::Refused(DialRefusal::Address(_))
            ));
        } else {
            result.unwrap().await.unwrap();
        }
        gated.dial(public.clone(), opts()).unwrap().await.unwrap();
        let expected: Vec<String> = if refuses {
            vec![public.to_string()]
        } else {
            vec![embedded.to_string(), public.to_string()]
        };
        assert_eq!(counting.dialed(), expected);
    }
}

#[tokio::test]
async fn the_inner_gate_exempts_a_configured_address_and_port() {
    let mut configured = ConfiguredTargets::new();
    configured.add_endpoint("127.0.0.1:4001".parse().unwrap());
    let counting = Counting::default();
    let mut gated = freedom_ipfs_net::transport::Gated::inner(
        counting.clone(),
        checked_with(configured).dial_policy(),
    );
    gated
        .dial(multiaddr("/ip4/127.0.0.1/tcp/4001"), opts())
        .unwrap()
        .await
        .unwrap();
    assert!(gated
        .dial(multiaddr("/ip4/127.0.0.1/tcp/4002"), opts())
        .is_err());
    assert_eq!(counting.dialed(), ["/ip4/127.0.0.1/tcp/4001"]);
}

#[tokio::test]
async fn the_outer_gate_refuses_what_isnt_a_network_target_and_onions() {
    let refused_targets = [
        "/ip6zone/eth0/ip6/fe80::1/tcp/4001".to_string(),
        "/unix/sock".to_string(),
        format!("/onion3/{ONION3}:4001"),
        "/dns/x.onion/tcp/80".to_string(),
        "/ip4/8.8.8.8/dns/x.onion/tcp/80".to_string(),
        "/ip4/8.8.8.8/ip4/127.0.0.1/tcp/8080".to_string(),
    ];
    let counting = Counting::default();
    let network = unchecked();
    let mut gated = freedom_ipfs_net::transport::Gated::outer(
        counting.clone(),
        network.capability().clone(),
        network.dial_policy(),
    );
    for text in &refused_targets {
        assert!(
            matches!(
                refused(gated.dial(multiaddr(text), opts())),
                GatedError::Refused(_)
            ),
            "{text}"
        );
    }
    assert!(counting.dialed().is_empty());
    assert_eq!(network.capability().permits(), 0);

    // The legacy policy's outer gate asks the capability only.
    let counting = Counting::default();
    let mut gated = freedom_ipfs_net::transport::Gated::outer(
        counting.clone(),
        NetworkCapability::new(),
        DialPolicy::legacy(),
    );
    for text in &refused_targets {
        gated.dial(multiaddr(text), opts()).unwrap().await.unwrap();
    }
    assert_eq!(counting.dialed().len(), refused_targets.len());
}

#[tokio::test]
async fn a_revoked_capability_refuses_every_later_dial() {
    for policy in [DialPolicy::legacy(), unchecked().dial_policy()] {
        let counting = Counting::default();
        let capability = NetworkCapability::new();
        let mut gated =
            freedom_ipfs_net::transport::Gated::outer(counting.clone(), capability.clone(), policy);
        gated
            .dial(multiaddr("/ip4/8.8.8.8/tcp/4001"), opts())
            .unwrap()
            .await
            .unwrap();
        capability.revoke();
        assert!(matches!(
            refused(gated.dial(multiaddr("/ip4/8.8.8.8/tcp/4002"), opts())),
            GatedError::Revoked(_)
        ));
        assert_eq!(counting.dialed(), ["/ip4/8.8.8.8/tcp/4001"]);
    }
}

/// Revoked while a dial is in flight: the dial is dropped before it
/// completes, and the capability's quiescence comes only after that.
#[tokio::test]
async fn a_dial_in_flight_is_dropped_at_the_revoke_before_quiescence() {
    let hold = Arc::new(tokio::sync::Notify::new());
    let counting = Counting {
        hold: Some(hold.clone()),
        ..Counting::default()
    };
    let capability = NetworkCapability::new();
    let mut gated = freedom_ipfs_net::transport::Gated::outer(
        counting.clone(),
        capability.clone(),
        DialPolicy::legacy(),
    );
    let dial = tokio::spawn(
        gated
            .dial(multiaddr("/ip4/8.8.8.8/tcp/4001"), opts())
            .unwrap(),
    );
    tokio::task::yield_now().await;
    assert_eq!(capability.permits(), 1);
    assert!(!counting.dropped.load(Ordering::SeqCst));
    capability.revoke();
    tokio::time::timeout(Duration::from_secs(2), capability.quiesced())
        .await
        .expect("quiescence once the dial is dropped");
    assert!(counting.dropped.load(Ordering::SeqCst));
    hold.notify_waiters();
    assert!(matches!(dial.await.unwrap(), Err(GatedError::Revoked(_))));
}

/// A DNS server on a local UDP socket: it records every query, and answers
/// from a table (or not at all, when it holds).
struct Dns {
    addr: SocketAddr,
    queries: Arc<Mutex<Vec<(String, RecordType)>>>,
    answered: Arc<AtomicUsize>,
}

impl Dns {
    fn start(answers: HashMap<(String, RecordType), Vec<RData>>, answer: bool) -> Self {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let answered = Arc::new(AtomicUsize::new(0));
        let (recorded, counted) = (queries.clone(), answered.clone());
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
                if !answer {
                    continue;
                }
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
                counted.fetch_add(1, Ordering::SeqCst);
                let _ = socket.send_to(&response.to_vec().unwrap(), from);
            }
        });
        Self {
            addr,
            queries,
            answered,
        }
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
}

fn txt(text: &str) -> RData {
    RData::TXT(TXT::new(vec![text.to_string()]))
}

fn a(ip: &str) -> RData {
    RData::A(A(ip.parse().unwrap()))
}

/// A `/dnsaddr` expanded under the gate (check 2): every multiaddr its TXT
/// records yield is classified before anything else, so a refused shape is
/// never dialed and a hidden onion is never resolved.
#[tokio::test]
async fn a_dnsaddr_is_expanded_under_the_gate() {
    let dns = Dns::start(
        HashMap::from([(
            ("_dnsaddr.peers.example".to_string(), RecordType::TXT),
            // The good record first: records are tried last-in first-out,
            // so every refused one would be dialed before it.
            vec![
                txt(&format!("dnsaddr=/ip4/8.8.8.8/tcp/4001/p2p/{PEER}")),
                txt(&format!(
                    "dnsaddr=/ip4/8.8.8.8/ip4/127.0.0.1/tcp/1/p2p/{PEER}"
                )),
                txt(&format!("dnsaddr=/dns4/x.onion/tcp/1/p2p/{PEER}")),
                txt(&format!(
                    "dnsaddr=/ip4/127.0.0.1/tcp/443/tls/sni/x.example/ws/p2p/{PEER}"
                )),
            ],
        )]),
        true,
    );
    let network = unchecked();
    let resolver = hickory_resolver::TokioResolver::builder_with_config(
        dns.config(),
        hickory_resolver::name_server::TokioConnectionProvider::default(),
    )
    .build();
    let counting = Counting::default();
    let mut gated = freedom_ipfs_net::transport::Gated::outer_expanding_dnsaddr(
        counting.clone(),
        network.capability().clone(),
        network.dial_policy(),
        resolver,
    );
    gated
        .dial(
            multiaddr(&format!("/dnsaddr/peers.example/p2p/{PEER}")),
            opts(),
        )
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        counting.dialed(),
        [format!("/ip4/8.8.8.8/tcp/4001/p2p/{PEER}")]
    );
    assert_eq!(
        dns.queries(),
        [("_dnsaddr.peers.example".to_string(), RecordType::TXT)]
    );
}

/// A canary: a TCP listener that counts the connections it accepts.
struct Canary {
    port: u16,
    accepted: Arc<AtomicUsize>,
}

impl Canary {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
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

/// Dials `addr` through a stack, for at most `limit`.
async fn dial_through(
    transport: &mut freedom_ipfs_net::transport::SwarmTransport,
    addr: &Multiaddr,
    limit: Duration,
) -> Option<Result<(), String>> {
    match transport.dial(addr.clone(), opts()) {
        Err(TransportError::Other(err)) => Some(Err(err.to_string())),
        Err(err) => Some(Err(err.to_string())),
        Ok(dial) => tokio::time::timeout(limit, dial)
            .await
            .ok()
            .map(|result| result.map(drop).map_err(|err| err.to_string())),
    }
}

/// Loopback, a literal or a name resolved by the DNS layer, never reaches a
/// canary through either swarm's stack with checked addresses; without the
/// address table (and under the legacy policy) it does.
#[tokio::test(flavor = "multi_thread")]
async fn loopback_never_reaches_a_canary_with_checked_addresses() {
    let keypair = Keypair::generate_ed25519();
    for (name, network, reaches) in [
        ("checked", checked(), false),
        ("unchecked", unchecked(), true),
        ("legacy", Network::legacy(), true),
    ] {
        let transports = Transports::new(&network);
        let canary = Canary::start();
        let port = canary.port;
        for (stack, addrs) in [
            (
                "dht",
                vec![
                    format!("/ip4/127.0.0.1/tcp/{port}/p2p/{PEER}"),
                    format!("/dns4/localhost/tcp/{port}/p2p/{PEER}"),
                ],
            ),
            (
                "bitswap",
                vec![
                    format!("/ip4/127.0.0.1/tcp/{port}/p2p/{PEER}"),
                    format!("/dns4/localhost/tcp/{port}/ws/p2p/{PEER}"),
                ],
            ),
        ] {
            for text in addrs {
                let mut transport = match stack {
                    "dht" => transports.dht(&keypair).unwrap(),
                    _ => transports.bitswap(&keypair).unwrap(),
                };
                let before = canary.accepted();
                let _ =
                    dial_through(&mut transport, &multiaddr(&text), Duration::from_secs(3)).await;
                let reached =
                    wait_until(Duration::from_millis(500), || canary.accepted() > before).await;
                assert_eq!(reached, reaches, "{name}, {stack}: {text}");
            }
        }
    }
}

/// Revoked: a later dial through the DHT's stack refuses before any DNS
/// query starts. The control, before the revoke, does query.
#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_capability_starts_no_dns_query() {
    let keypair = Keypair::generate_ed25519();
    for network in [Network::legacy(), unchecked()] {
        let dns = Dns::start(HashMap::new(), true);
        let mut transport = Transports::new(&network)
            .with_dns(dns.config())
            .dht(&keypair)
            .unwrap();
        let name = multiaddr(&format!("/dns4/name.example/tcp/4001/p2p/{PEER}"));
        let _ = dial_through(&mut transport, &name, Duration::from_secs(3)).await;
        assert!(!dns.queries().is_empty(), "the control asks");
        let before = dns.queries().len();
        network.revoke();
        let result = dial_through(&mut transport, &name, Duration::from_secs(3)).await;
        assert!(
            matches!(&result, Some(Err(err)) if err.contains("revoked")),
            "{result:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(dns.queries().len(), before, "a query after the revoke");
    }
}

/// Revoked while the DNS layer waits for an answer: the dial is dropped at
/// once, the capability's quiescence comes only after that, and nothing is
/// connected when an answer would have come.
#[tokio::test(flavor = "multi_thread")]
async fn a_dial_held_in_dns_is_dropped_at_the_revoke() {
    let keypair = Keypair::generate_ed25519();
    for network in [Network::legacy(), unchecked()] {
        let dns = Dns::start(HashMap::new(), false);
        let mut transport = Transports::new(&network)
            .with_dns(dns.config())
            .dht(&keypair)
            .unwrap();
        let dial = transport
            .dial(
                multiaddr(&format!("/dns4/held.example/tcp/4001/p2p/{PEER}")),
                opts(),
            )
            .unwrap();
        let dial = tokio::spawn(dial);
        assert!(wait_until(Duration::from_secs(2), || !dns.queries().is_empty()).await);
        assert_eq!(network.capability().permits(), 1);
        network.revoke();
        tokio::time::timeout(Duration::from_secs(2), network.capability().quiesced())
            .await
            .expect("quiescence once the held dial is dropped");
        let result = tokio::time::timeout(Duration::from_secs(2), dial)
            .await
            .expect("the dial ends")
            .unwrap();
        assert!(
            matches!(&result, Err(err) if err.to_string().contains("revoked")),
            "{:?}",
            result.map(drop)
        );
        assert_eq!(dns.answered.load(Ordering::SeqCst), 0);
    }
}

/// The design's hidden target: a public address first, a name after it.
/// libp2p's DNS layer would resolve every DNS component; the outer gate
/// refuses the shape before it, so no query goes out. The legacy control
/// does resolve it. (An onion name there is never asked for by hickory,
/// which answers `.onion` itself, so the control uses an ordinary name.)
#[tokio::test(flavor = "multi_thread")]
async fn a_hidden_name_is_never_resolved_by_the_dns_layer() {
    let keypair = Keypair::generate_ed25519();
    let hidden = multiaddr(&format!(
        "/ip4/8.8.8.8/dns4/hidden.example/tcp/80/p2p/{PEER}"
    ));
    for (network, queried) in [(unchecked(), false), (Network::legacy(), true)] {
        let dns = Dns::start(HashMap::new(), true);
        let mut transport = Transports::new(&network)
            .with_dns(dns.config())
            .dht(&keypair)
            .unwrap();
        let _ = dial_through(&mut transport, &hidden, Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            dns.queries()
                .iter()
                .any(|(name, _)| name == "hidden.example"),
            queried,
            "{:?}",
            dns.queries()
        );
    }
}

/// A name resolved by the DNS layer to a private address: refused below
/// the resolver with checked addresses; dialed without them.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_resolved_to_loopback_is_refused_below_the_dns_layer() {
    let keypair = Keypair::generate_ed25519();
    let canary = Canary::start();
    let dns = Dns::start(
        HashMap::from([(
            ("private.example".to_string(), RecordType::A),
            vec![a("127.0.0.1")],
        )]),
        true,
    );
    let addr = multiaddr(&format!(
        "/dns4/private.example/tcp/{}/p2p/{PEER}",
        canary.port
    ));
    for (network, reaches) in [(checked(), false), (unchecked(), true)] {
        let mut transport = Transports::new(&network)
            .with_dns(dns.config())
            .dht(&keypair)
            .unwrap();
        let before = canary.accepted();
        let _ = dial_through(&mut transport, &addr, Duration::from_secs(3)).await;
        let reached = wait_until(Duration::from_millis(500), || canary.accepted() > before).await;
        assert_eq!(reached, reaches);
    }
    assert!(dns
        .queries()
        .contains(&("private.example".to_string(), RecordType::A)));
}

/// A configured name's answer is dialed as one for that dial only
/// (`dial_configured_answer`): the inner gate exempts that address and
/// port while the dial is asked for, and nothing else, then or later.
#[tokio::test]
async fn a_configured_answer_is_exempt_for_its_own_dial_only() {
    let counting = Counting::default();
    let mut gated =
        freedom_ipfs_net::transport::Gated::inner(counting.clone(), checked().dial_policy());
    let answer = multiaddr(&format!("/ip4/127.0.0.1/tcp/4001/p2p/{PEER}"));
    let other_port = multiaddr("/ip4/127.0.0.1/tcp/4002");
    // Outside the call: refused.
    assert!(gated.dial(answer.clone(), opts()).is_err());
    // Inside it: that address and port only.
    let (exempt, other) = freedom_ipfs_net::transport::dial_configured_answer(&answer, || {
        (
            gated.dial(answer.clone(), opts()).is_ok(),
            gated.dial(other_port.clone(), opts()).is_ok(),
        )
    });
    assert!(exempt);
    assert!(!other);
    // And after it, refused again.
    assert!(gated.dial(answer.clone(), opts()).is_err());
    assert_eq!(counting.dialed(), [answer.to_string()]);
    // Its shape is still checked inside the call.
    let hidden = multiaddr("/ip4/127.0.0.1/ip4/10.0.0.1/tcp/4001");
    let refused_shape = freedom_ipfs_net::transport::dial_configured_answer(&hidden, || {
        gated.dial(hidden.clone(), opts()).is_ok()
    });
    assert!(!refused_shape);
}

/// The exemption ends with its call even when the dial panics (#48 review,
/// both reports' minor finding): unwinding puts the thread's previous state
/// back, so a later dial of the same address on that thread is untrusted.
#[test]
fn a_configured_answer_exemption_is_restored_when_the_dial_panics() {
    let counting = Counting::default();
    let mut gated =
        freedom_ipfs_net::transport::Gated::inner(counting.clone(), checked().dial_policy());
    let answer = multiaddr(&format!("/ip4/127.0.0.1/tcp/4001/p2p/{PEER}"));

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        freedom_ipfs_net::transport::dial_configured_answer(&answer, || {
            panic!("the synchronous swarm dial panicked")
        });
    }));
    assert!(unwound.is_err());

    assert!(
        gated.dial(answer, opts()).is_err(),
        "the panicking call left its configured-address exemption installed"
    );
    assert!(counting.dialed().is_empty());
}

/// A call inside another restores the outer one's exemption when it ends,
/// by returning or by unwinding, and the outer one's end restores none.
#[test]
fn a_nested_configured_answer_restores_the_outer_one() {
    let counting = Counting::default();
    let mut gated =
        freedom_ipfs_net::transport::Gated::inner(counting.clone(), checked().dial_policy());
    let outer = multiaddr(&format!("/ip4/127.0.0.1/tcp/4001/p2p/{PEER}"));
    let inner = multiaddr(&format!("/ip4/127.0.0.1/tcp/4002/p2p/{PEER}"));
    for panics in [false, true] {
        let (outer_after_inner, inner_after_inner) =
            freedom_ipfs_net::transport::dial_configured_answer(&outer, || {
                let nested = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    freedom_ipfs_net::transport::dial_configured_answer(&inner, || {
                        assert!(gated.dial(inner.clone(), opts()).is_ok());
                        assert!(gated.dial(outer.clone(), opts()).is_err());
                        if panics {
                            panic!("the nested dial panicked");
                        }
                    })
                }));
                assert_eq!(nested.is_err(), panics);
                (
                    gated.dial(outer.clone(), opts()).is_ok(),
                    gated.dial(inner.clone(), opts()).is_ok(),
                )
            });
        assert!(outer_after_inner, "panics: {panics}");
        assert!(!inner_after_inner, "panics: {panics}");
        assert!(
            gated.dial(outer.clone(), opts()).is_err(),
            "panics: {panics}"
        );
        assert!(
            gated.dial(inner.clone(), opts()).is_err(),
            "panics: {panics}"
        );
    }
    assert_eq!(
        counting.dialed(),
        [
            inner.to_string(),
            outer.to_string(),
            inner.to_string(),
            outer.to_string()
        ]
    );
}
