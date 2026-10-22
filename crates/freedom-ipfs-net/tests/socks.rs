//! `socks5` through a test proxy (design 0007, "Modes", `socks5`; its
//! tests: "A SOCKS fixture with `RESOLVE`", "Failures", "The onion route"),
//! through the swarms' own transport stacks. The proxy records every
//! connection and request, and forwards a `CONNECT` only to the local
//! listener a test mapped its destination to. Each refusal has a control
//! that goes through.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{
    Answer, ClientHelloCatcher, Destination, Request, SocksFixture,
};
use freedom_ipfs_net::transport::{SwarmTransport, Transports};
use freedom_ipfs_net::{
    ClientConfig, DialRefusal, Network, NetworkError, NetworkPolicy, TransportPolicy,
};
use libp2p::core::transport::{DialOpts, PortUse, TransportError};
use libp2p::core::Endpoint;
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, Transport};
use url::Url;

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
const ONION3: &str = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";
const CONFIG: ClientConfig = ClientConfig::new(Duration::from_secs(5), 10);

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

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

/// A `socks5` network through `fixture`, configured with `configured`.
fn socks5(fixture: &SocksFixture, configured: ConfiguredTargets) -> Network {
    Network::with_lookup(
        NetworkPolicy::socks5(fixture.addr(), configured, doh()).unwrap(),
        None,
    )
}

/// Both swarms' stacks for `network`.
fn stacks(network: &Network) -> [(&'static str, SwarmTransport); 2] {
    let keypair = Keypair::generate_ed25519();
    let transports = Transports::new(network);
    [
        ("dht", transports.dht(&keypair).unwrap()),
        ("bitswap", transports.bitswap(&keypair).unwrap()),
    ]
}

/// A listener that counts the connections it accepts, and drops them.
struct Canary {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
}

impl Canary {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        Self { addr, accepted }
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

/// Dials `addr` through `transport`: `Err` with the error's text when it
/// was refused or failed, `Ok` when it connected (and upgraded), or `None`
/// when it was still going after `limit`.
async fn dial_through(
    transport: &mut SwarmTransport,
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

fn connects(requests: &[Request]) -> Vec<Destination> {
    requests
        .iter()
        .filter(|request| request.is_connect())
        .map(|request| request.destination.clone())
        .collect()
}

fn resolves(requests: &[Request]) -> Vec<String> {
    requests
        .iter()
        .filter(|request| request.is_resolve())
        .map(|request| match &request.destination {
            Destination::Name(name, _) => name.clone(),
            Destination::Ip(addr) => addr.to_string(),
        })
        .collect()
}

fn public(port: u16) -> SocketAddr {
    SocketAddr::new("8.8.8.8".parse().unwrap(), port)
}

/// A name that looks acceptable but resolves to a loopback or private
/// address (DNS64's embedding of loopback included) is refused, and no
/// `CONNECT` is sent; a name resolving to a public address (the control)
/// is connected at that address, and reaches its listener.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_is_checked_before_anything_connects_to_it() {
    for (answer, reaches) in [
        ("127.0.0.1", false),
        ("10.0.0.1", false),
        ("::ffff:127.0.0.1", false),
        ("64:ff9b::7f00:1", false),
        ("8.8.8.8", true),
    ] {
        for stack in 0..2 {
            let fixture = SocksFixture::start().await;
            let canary = Canary::start();
            fixture.answer("peer.example", Answer::Ip(answer.parse().unwrap()));
            fixture.forward(Destination::Ip(public(4001)), canary.addr);
            let network = socks5(&fixture, ConfiguredTargets::new());
            let (name, mut transport) = stacks(&network).into_iter().nth(stack).unwrap();
            let addr = multiaddr(&format!("/dns4/peer.example/tcp/4001/p2p/{PEER}"));
            let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
            let requests = fixture.requests();
            assert_eq!(resolves(&requests), ["peer.example"], "{answer} {name}");
            if reaches {
                assert_eq!(connects(&requests), [Destination::Ip(public(4001))]);
                assert!(wait_until(Duration::from_secs(2), || canary.accepted() > 0).await);
            } else {
                assert_eq!(connects(&requests), [], "{answer} {name}");
                assert_eq!(canary.accepted(), 0);
            }
        }
    }
}

/// An empty answer (domain-typed, `0.0.0.0`, `::`), a failed one, an
/// unknown name, and a proxy that refuses `RESOLVE` (`0x07`): the attempt
/// fails, and no `CONNECT` is sent, with the name or otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn no_usable_answer_fails_and_connects_nothing() {
    for answer in [
        Some(Answer::Domain("other.example".into())),
        Some(Answer::Ip("0.0.0.0".parse().unwrap())),
        Some(Answer::Ip("::".parse().unwrap())),
        Some(Answer::Reply(4)),
        None,
    ] {
        for refuse_resolve in [false, true] {
            let fixture = SocksFixture::start().await;
            if let Some(answer) = answer.clone() {
                fixture.answer("peer.example", answer);
            }
            if refuse_resolve {
                fixture.refuse_resolve();
            }
            let network = socks5(&fixture, ConfiguredTargets::new());
            for (name, mut transport) in stacks(&network) {
                let addr = multiaddr(&format!("/dns4/peer.example/tcp/4001/p2p/{PEER}"));
                let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
                assert!(
                    matches!(result, Some(Err(_))),
                    "{answer:?} {refuse_resolve} {name}"
                );
            }
            let requests = fixture.requests();
            assert_eq!(resolves(&requests).len(), 2, "{answer:?}");
            assert_eq!(connects(&requests), [], "{answer:?} {refuse_resolve}");
        }
    }
}

/// An answer that changes from a public address to loopback between two
/// dials: the first `CONNECT`s to the public address it checked; the
/// second is refused, and never `CONNECT`s to loopback.
#[tokio::test(flavor = "multi_thread")]
async fn each_connect_goes_to_the_address_checked_for_it() {
    let fixture = SocksFixture::start().await;
    let canary = Canary::start();
    fixture.forward(Destination::Ip(public(4001)), canary.addr);
    fixture.forward(
        Destination::Ip("127.0.0.1:4001".parse().unwrap()),
        canary.addr,
    );
    let network = socks5(&fixture, ConfiguredTargets::new());
    let [(_, mut transport), _] = stacks(&network);
    let addr = multiaddr(&format!("/dns4/peer.example/tcp/4001/p2p/{PEER}"));
    fixture.answer("peer.example", Answer::Ip("8.8.8.8".parse().unwrap()));
    let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    fixture.answer("peer.example", Answer::Ip("127.0.0.1".parse().unwrap()));
    let second = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    assert!(matches!(second, Some(Err(_))));
    let requests = fixture.requests();
    assert_eq!(resolves(&requests), ["peer.example", "peer.example"]);
    assert_eq!(connects(&requests), [Destination::Ip(public(4001))]);
    assert!(wait_until(Duration::from_secs(2), || canary.accepted() == 1).await);
}

/// Every `CONNECT` carries an IP literal (an IPv6 one as an IPv6 address),
/// except an onion's, which carries its name and is never resolved.
#[tokio::test(flavor = "multi_thread")]
async fn every_connect_carries_an_ip_literal_but_an_onions() {
    let fixture = SocksFixture::start().await;
    fixture.answer("peer.example", Answer::Ip("8.8.4.4".parse().unwrap()));
    fixture.answer(
        "peer6.example",
        Answer::Ip("2001:4860:4860::8844".parse().unwrap()),
    );
    let network = socks5(&fixture, ConfiguredTargets::new());
    let [(_, mut transport), _] = stacks(&network);
    for addr in [
        "/ip4/8.8.8.8/tcp/4001".to_string(),
        "/ip6/2001:4860:4860::8888/tcp/4001".to_string(),
        "/dns4/peer.example/tcp/4001".to_string(),
        "/dns6/peer6.example/tcp/4001".to_string(),
        format!("/onion3/{ONION3}:4001"),
        format!("/dns/{ONION3}.onion/tcp/4002"),
        format!("/dns/www.{ONION3}.onion/tcp/4003"),
    ] {
        let addr = multiaddr(&format!("{addr}/p2p/{PEER}"));
        let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    }
    let requests = fixture.requests();
    assert_eq!(resolves(&requests), ["peer.example", "peer6.example"]);
    assert_eq!(
        connects(&requests),
        [
            Destination::Ip(public(4001)),
            Destination::Ip("[2001:4860:4860::8888]:4001".parse().unwrap()),
            Destination::Ip("8.8.4.4:4001".parse().unwrap()),
            Destination::Ip("[2001:4860:4860::8844]:4001".parse().unwrap()),
            Destination::Name(format!("{ONION3}.onion"), 4001),
            Destination::Name(format!("{ONION3}.onion"), 4002),
            Destination::Name(format!("www.{ONION3}.onion"), 4003),
        ]
    );
}

/// An onion is reached through the proxy, by name: the route reaches the
/// listener the proxy maps it to.
#[tokio::test(flavor = "multi_thread")]
async fn an_onion_is_reached_by_name_through_the_proxy() {
    let fixture = SocksFixture::start().await;
    let canary = Canary::start();
    fixture.forward(
        Destination::Name(format!("{ONION3}.onion"), 4001),
        canary.addr,
    );
    let network = socks5(&fixture, ConfiguredTargets::new());
    for (_, mut transport) in stacks(&network) {
        let addr = multiaddr(&format!("/onion3/{ONION3}:4001/p2p/{PEER}"));
        let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    }
    assert!(wait_until(Duration::from_secs(2), || canary.accepted() == 2).await);
    assert_eq!(resolves(&fixture.requests()), Vec::<String>::new());
}

/// The onion route takes only a well-formed v3 address over TCP or
/// WebSocket: a v2 `/onion`, `onion`, `foo.onion`, a `/dnsaddr` onion and
/// an onion over QUIC are refused before any connection to the proxy.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_onion_takes_no_route() {
    let fixture = SocksFixture::start().await;
    let network = socks5(&fixture, ConfiguredTargets::new());
    for (name, mut transport) in stacks(&network) {
        for addr in [
            "/onion/aaimaq4ygg2iegci:80".to_string(),
            "/dns/onion/tcp/80".to_string(),
            "/dns/foo.onion/tcp/80".to_string(),
            format!("/dnsaddr/{ONION3}.onion"),
            format!("/dns/{ONION3}.onion/udp/4001/quic-v1"),
        ] {
            let addr = multiaddr(&format!("{addr}/p2p/{PEER}"));
            let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
            assert!(
                matches!(&result, Some(Err(err)) if err.contains("network policy")),
                "{addr} {name}: {result:?}"
            );
        }
    }
    assert_eq!(fixture.connections(), 0);
}

/// What the proxy can't carry is refused before any connection to it: a
/// `/dnsaddr` (expanded before a dial, never by a local lookup), QUIC, a
/// zoned IPv6 address and a Unix socket.
#[tokio::test(flavor = "multi_thread")]
async fn what_the_proxy_cant_carry_is_refused_before_it() {
    let fixture = SocksFixture::start().await;
    let network = socks5(&fixture, ConfiguredTargets::new());
    for (name, mut transport) in stacks(&network) {
        for addr in [
            format!("/dnsaddr/peers.example/p2p/{PEER}"),
            format!("/ip4/8.8.8.8/udp/4001/quic-v1/p2p/{PEER}"),
            format!("/ip6zone/eth0/ip6/fe80::1/tcp/4001/p2p/{PEER}"),
            "/unix/tmp%2Fsocket".to_string(),
        ] {
            let addr = multiaddr(&addr);
            let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
            assert!(matches!(result, Some(Err(_))), "{addr} {name}: {result:?}");
        }
    }
    assert_eq!(fixture.connections(), 0);
}

/// A proxy that is down fails the dial, and nothing falls back to a direct
/// socket: the target, a configured address that passes the gate, is a
/// canary that sees nothing. With the proxy up (the control), the same dial
/// reaches it through the proxy.
#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_that_is_down_fails_closed() {
    for up in [false, true] {
        let fixture = SocksFixture::start().await;
        let canary = Canary::start();
        let mut configured = ConfiguredTargets::new();
        configured.add_endpoint(canary.addr);
        fixture.forward(Destination::Ip(canary.addr), canary.addr);
        let network = socks5(&fixture, configured);
        let _running = if up {
            Some(fixture)
        } else {
            fixture.stop();
            None
        };
        for (name, mut transport) in stacks(&network) {
            let addr = multiaddr(&format!(
                "/ip4/127.0.0.1/tcp/{}/p2p/{PEER}",
                canary.addr.port()
            ));
            let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
            if !up {
                assert!(matches!(result, Some(Err(_))), "{name}: {result:?}");
            }
        }
        let reached = wait_until(Duration::from_secs(if up { 2 } else { 1 }), || {
            canary.accepted() == 2
        })
        .await;
        assert_eq!(reached, up);
        if !up {
            assert_eq!(canary.accepted(), 0);
        }
    }
}

/// The proxy's own endpoint is the transport, never a target through it:
/// a provider's address at the proxy's port, or a name answered with it,
/// is untrusted loopback, refused before anything is sent for it.
#[tokio::test(flavor = "multi_thread")]
async fn the_proxys_own_address_is_no_exemption() {
    let fixture = SocksFixture::start().await;
    let proxy = fixture.addr();
    fixture.answer("proxy.example", Answer::Ip(proxy.ip()));
    let network = socks5(&fixture, ConfiguredTargets::new());
    assert!(!network
        .dial_policy()
        .is_configured(&multiaddr(&format!("/ip4/127.0.0.1/tcp/{}", proxy.port()))));
    for (name, mut transport) in stacks(&network) {
        for addr in [
            format!("/ip4/127.0.0.1/tcp/{}/p2p/{PEER}", proxy.port()),
            format!("/dns4/proxy.example/tcp/{}/p2p/{PEER}", proxy.port()),
        ] {
            let addr = multiaddr(&addr);
            let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
            assert!(
                matches!(&result, Some(Err(err)) if err.contains("network policy")),
                "{addr} {name}: {result:?}"
            );
        }
    }
    let requests = fixture.requests();
    assert_eq!(connects(&requests), []);
    assert_eq!(resolves(&requests), ["proxy.example", "proxy.example"]);
}

/// A configured name and port keep their exemption through `RESOLVE`: a
/// loopback answer is connected to. The same name on another port is
/// untrusted, and its answer refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_configured_name_keeps_its_exemption_through_resolve() {
    let fixture = SocksFixture::start().await;
    let canary = Canary::start();
    let port = canary.addr.port();
    fixture.answer("router.example", Answer::Ip("127.0.0.1".parse().unwrap()));
    fixture.forward(Destination::Ip(canary.addr), canary.addr);
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse(&format!("http://router.example:{port}/")).unwrap())
        .unwrap();
    let network = socks5(&fixture, configured);
    let [(_, mut transport), _] = stacks(&network);
    let other = dial_through(
        &mut transport,
        &multiaddr(&format!("/dns4/router.example/tcp/{}/p2p/{PEER}", port + 1)),
        Duration::from_secs(2),
    )
    .await;
    assert!(matches!(&other, Some(Err(err)) if err.contains("network policy")));
    assert_eq!(connects(&fixture.requests()), []);
    let _ = dial_through(
        &mut transport,
        &multiaddr(&format!("/dns4/router.example/tcp/{port}/p2p/{PEER}")),
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(
        connects(&fixture.requests()),
        [Destination::Ip(canary.addr)]
    );
    assert!(wait_until(Duration::from_secs(2), || canary.accepted() == 1).await);
}

/// A revoked network opens no connection to the proxy; and a dial waiting
/// for its `RESOLVE` answer is dropped at the revoke, quiescence comes, and
/// no `CONNECT` follows.
#[tokio::test(flavor = "multi_thread")]
async fn the_revoke_stops_every_request_through_the_proxy() {
    let fixture = SocksFixture::start().await;
    fixture.answer("held.example", Answer::Hang);
    let network = socks5(&fixture, ConfiguredTargets::new());
    let [(_, mut transport), _] = stacks(&network);
    let dial = transport
        .dial(
            multiaddr(&format!("/dns4/held.example/tcp/4001/p2p/{PEER}")),
            opts(),
        )
        .unwrap();
    let dial = tokio::spawn(dial);
    assert!(wait_until(Duration::from_secs(2), || !fixture.requests().is_empty()).await);
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
    let connections = fixture.connections();
    for (_, mut transport) in stacks(&network) {
        let result = dial_through(
            &mut transport,
            &multiaddr(&format!("/ip4/8.8.8.8/tcp/4001/p2p/{PEER}")),
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(&result, Some(Err(err)) if err.contains("revoked")));
    }
    assert_eq!(fixture.connections(), connections);
    assert_eq!(connects(&fixture.requests()), []);
}

/// The greeting offers one method, no authentication, and a proxy that
/// accepts only another fails the dial: nothing is requested. (Whether each
/// instance authenticates with credentials of its own is design 0007's open
/// question 11.)
#[tokio::test(flavor = "multi_thread")]
async fn the_greeting_offers_one_method_and_never_falls_back() {
    let fixture = SocksFixture::start().await;
    let network = socks5(&fixture, ConfiguredTargets::new());
    let [(_, mut transport), _] = stacks(&network);
    let addr = multiaddr(&format!("/ip4/8.8.8.8/tcp/4001/p2p/{PEER}"));
    let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    let requests = fixture.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].methods, [0x00]);
    assert_eq!(requests[0].credentials, None);

    fixture.accept_methods(&[0x02]);
    let result = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    assert!(
        matches!(&result, Some(Err(err)) if err.contains("authentication method")),
        "{result:?}"
    );
    assert_eq!(fixture.requests().len(), 1);
    assert_eq!(fixture.connections(), 2);
}

/// A WSS address keeps its name for TLS: the name is resolved through the
/// proxy, the `CONNECT` goes to the checked address, and the ClientHello
/// that arrives through it names the host (SNI).
#[tokio::test(flavor = "multi_thread")]
async fn wss_keeps_the_name_for_tls() {
    let fixture = SocksFixture::start().await;
    let hello = ClientHelloCatcher::start().await;
    fixture.answer("ws.example", Answer::Ip("8.8.8.8".parse().unwrap()));
    fixture.forward(Destination::Ip(public(443)), hello.addr());
    let network = socks5(&fixture, ConfiguredTargets::new());
    let [_, (_, mut transport)] = stacks(&network);
    let addr = multiaddr(&format!("/dns4/ws.example/tcp/443/wss/p2p/{PEER}"));
    let _ = dial_through(&mut transport, &addr, Duration::from_secs(2)).await;
    assert!(wait_until(Duration::from_secs(2), || !hello.names().is_empty()).await);
    assert_eq!(hello.names(), [Some("ws.example".to_string())]);
    let requests = fixture.requests();
    assert_eq!(resolves(&requests), ["ws.example"]);
    assert_eq!(connects(&requests), [Destination::Ip(public(443))]);
}

/// `socks5` policies: the proxy is an IP literal and a port, the DoH
/// endpoint is a configured target, and the proxy's own endpoint isn't. A
/// network of one has its HTTP clients and its transports.
#[test]
fn a_socks5_policy_names_its_proxy() {
    for proxy in ["0.0.0.0:9150", "[::]:9150", "127.0.0.1:0"] {
        assert!(matches!(
            NetworkPolicy::socks5(proxy.parse().unwrap(), ConfiguredTargets::new(), doh()),
            Err(NetworkError::ProxyEndpoint)
        ));
    }
    let policy = NetworkPolicy::socks5(
        "127.0.0.1:9150".parse().unwrap(),
        ConfiguredTargets::new(),
        doh(),
    )
    .unwrap();
    assert_eq!(policy.doh(), Some(doh().as_str()));
    let configured = policy.configured().unwrap();
    assert!(configured.contains(
        &freedom_ipfs_net::gate::Target::Name("dns.example".into()),
        443
    ));
    assert!(!configured.contains(
        &freedom_ipfs_net::gate::Target::Ip("127.0.0.1".parse().unwrap()),
        9150
    ));
    let network = Network::new(policy).unwrap();
    assert_eq!(network.transport_policy(), TransportPolicy::SOCKS5);
    assert_eq!(
        network.http_client(CONFIG).error().map(ToString::to_string),
        None
    );
    // An onion endpoint can be configured: socks5 has a route to it.
    let mut onion = ConfiguredTargets::new();
    onion
        .add_url(&Url::parse(&format!("http://{ONION3}.onion/routing/v1")).unwrap())
        .unwrap();
    assert!(NetworkPolicy::socks5("127.0.0.1:9150".parse().unwrap(), onion, doh()).is_ok());
}

/// Candidate choice under `socks5`: QUIC is refused before the cut; the
/// onion route admits a well-formed v3 address over TCP, WebSocket or HTTP
/// (an HTTP provider's multiaddr, or its URL) and nothing else. Under
/// `direct` (the control) every onion is refused as `direct` refuses them.
#[tokio::test]
async fn the_dial_policy_knows_the_proxy() {
    let fixture = SocksFixture::start().await;
    let policy = socks5(&fixture, ConfiguredTargets::new()).dial_policy();
    let direct = Network::with_lookup(
        NetworkPolicy::direct(
            ConfiguredTargets::new(),
            doh(),
            freedom_ipfs_net::Addresses::Unchecked,
        )
        .unwrap(),
        None,
    )
    .dial_policy();
    assert!(policy.transports().proxied());
    assert!(!direct.transports().proxied());
    assert_eq!(
        policy.check_now(&multiaddr("/ip4/8.8.8.8/udp/4001/quic-v1")),
        Err(DialRefusal::Transport)
    );
    for admitted in [
        format!("/onion3/{ONION3}:4001"),
        format!("/dns/{ONION3}.onion/tcp/4001"),
        format!("/dns4/www.{ONION3}.onion/tcp/443/wss"),
        format!("/dns6/{ONION3}.onion/tcp/80/ws"),
        format!("/dns/{ONION3}.onion/tcp/443/https"),
        format!("/dns/{ONION3}.onion/http"),
    ] {
        let addr = multiaddr(&format!("{admitted}/p2p/{PEER}"));
        assert_eq!(policy.check_now(&addr), Ok(()), "{admitted}");
        assert_eq!(
            direct.check_now(&addr),
            Err(DialRefusal::Onion),
            "{admitted}"
        );
    }
    for refused in [
        "/onion/aaimaq4ygg2iegci:80".to_string(),
        "/dns/foo.onion/tcp/80".to_string(),
        format!("/dnsaddr/{ONION3}.onion"),
        format!("/dns/{ONION3}.onion/udp/4001/quic-v1"),
        "/dns/foo.onion/tcp/443/https".to_string(),
    ] {
        let addr = multiaddr(&refused);
        assert_eq!(
            policy.check_now(&addr),
            Err(DialRefusal::OnionRoute),
            "{refused}"
        );
        assert_eq!(
            direct.check_now(&addr),
            Err(DialRefusal::Onion),
            "{refused}"
        );
    }
    let onion_url = Url::parse(&format!("http://{ONION3}.onion/")).unwrap();
    assert_eq!(policy.check_url_now(&onion_url), Ok(()));
    assert_eq!(direct.check_url_now(&onion_url), Err(DialRefusal::Onion));
    let malformed = Url::parse("http://foo.onion/").unwrap();
    assert_eq!(
        policy.check_url_now(&malformed),
        Err(DialRefusal::OnionRoute)
    );
    // Literals pass the address table, with no NAT64 prefix learned.
    assert!(policy
        .check_now(&multiaddr("/ip4/10.0.0.1/tcp/4001"))
        .is_err());
    assert_eq!(
        policy.check_now(&multiaddr("/ip6/64:ff9b::808:808/tcp/4001")),
        Ok(())
    );
    assert!(policy
        .check_now(&multiaddr("/ip6/64:ff9b::7f00:1/tcp/4001"))
        .is_err());
    assert_eq!(
        policy.check_now(&multiaddr("/ip6/2002:808:808::/tcp/4001")),
        Ok(())
    );
}
