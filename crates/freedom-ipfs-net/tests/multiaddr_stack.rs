//! A dialed multiaddr's whole protocol stack is validated (design 0007,
//! "The gate"), not only its first component: libp2p-tcp 0.44 dials the
//! *last* IP and TCP pair of an address, and libp2p-dns resolves every DNS
//! component wherever it is. So a public address first must not hide the
//! address the transport actually reads, or an onion behind it.
//!
//! That a refused hidden-onion shape causes no DNS or TXT query, and that
//! `/dnsaddr` results are judged by the same shapes, are retrieval's tests
//! (Bitswap's address expansion: `tests/network_expansion.rs` and its unit
//! tests); the transports' own check comes with them.

use std::net::IpAddr;
use std::time::Duration;

use freedom_ipfs_net::gate::{
    check_address, classify_dialed, classify_multiaddr, parse_url, ConfiguredTargets, DialTarget,
    Nat64State, Refusal, Shape, Target,
};
use libp2p_core::transport::{DialOpts, PortUse};
use libp2p_core::{Endpoint, Transport};
use multiaddr::{Multiaddr, Protocol};

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
const ONION3: &str = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";
const CERTHASH: &str = "uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g";

fn multiaddr(text: &str) -> Multiaddr {
    text.parse()
        .unwrap_or_else(|err| panic!("{text} parses: {err}"))
}

fn ip(text: &str) -> Target {
    Target::Ip(text.parse::<IpAddr>().unwrap())
}

fn name(text: &str) -> Target {
    Target::Name(text.into())
}

#[test]
fn the_shapes_the_node_dials_are_accepted() {
    use Shape::*;
    let onion = Target::Onion(format!("{ONION3}.onion"));
    let cases: Vec<(String, Target, Option<u16>, Shape)> = vec![
        // TCP, alone and with each WebSocket suffix.
        (
            "/ip4/8.8.8.8/tcp/4001".into(),
            ip("8.8.8.8"),
            Some(4001),
            Tcp,
        ),
        (
            "/ip6/2a00:1::1/tcp/4001".into(),
            ip("2a00:1::1"),
            Some(4001),
            Tcp,
        ),
        (
            "/dns/example.com/tcp/4001".into(),
            name("example.com"),
            Some(4001),
            Tcp,
        ),
        ("/ip4/8.8.8.8/tcp/80/ws".into(), ip("8.8.8.8"), Some(80), Ws),
        (
            "/dns4/example.com/tcp/443/wss".into(),
            name("example.com"),
            Some(443),
            Wss,
        ),
        (
            "/dns6/example.com/tcp/443/tls/ws".into(),
            name("example.com"),
            Some(443),
            Wss,
        ),
        // HTTP providers, with and without `/tcp`: without it, the port is
        // 80 for HTTP and 443 for HTTPS.
        (
            "/ip4/8.8.8.8/tcp/8080/http".into(),
            ip("8.8.8.8"),
            Some(8080),
            Http,
        ),
        ("/ip4/8.8.8.8/http".into(), ip("8.8.8.8"), Some(80), Http),
        (
            "/dns/provider.example/tcp/8443/https".into(),
            name("provider.example"),
            Some(8443),
            Https,
        ),
        (
            "/dns/provider.example/https".into(),
            name("provider.example"),
            Some(443),
            Https,
        ),
        (
            "/dns/provider.example/tcp/8443/tls/http".into(),
            name("provider.example"),
            Some(8443),
            Https,
        ),
        (
            "/dns/provider.example/tls/http".into(),
            name("provider.example"),
            Some(443),
            Https,
        ),
        // QUIC v1, `/dnsaddr`, onions.
        (
            "/ip6/2a00:1::1/udp/4001/quic-v1".into(),
            ip("2a00:1::1"),
            Some(4001),
            QuicV1,
        ),
        (
            "/dns/example.com/udp/4001/quic-v1".into(),
            name("example.com"),
            Some(4001),
            QuicV1,
        ),
        (
            "/dnsaddr/bootstrap.libp2p.io".into(),
            name("bootstrap.libp2p.io"),
            None,
            Dnsaddr,
        ),
        (
            format!("/onion3/{ONION3}:1234"),
            onion.clone(),
            Some(1234),
            Onion,
        ),
        (
            "/onion/aaimaq4ygg2iegci:80".into(),
            Target::Onion("aaimaq4ygg2iegci.onion".into()),
            Some(80),
            Onion,
        ),
    ];
    for (text, target, port, shape) in cases {
        let expected = Ok(DialTarget {
            target: target.clone(),
            port,
            shape,
        });
        assert_eq!(classify_dialed(&multiaddr(&text)), expected, "{text}");
        assert_eq!(classify_multiaddr(&multiaddr(&text)), Ok(target), "{text}");
        // Each may end in one `/p2p/<peer>`.
        let with_peer = format!("{text}/p2p/{PEER}");
        assert_eq!(
            classify_dialed(&multiaddr(&with_peer)),
            expected,
            "{with_peer}"
        );
    }
}

#[test]
fn a_websockets_path_doesnt_change_its_shape() {
    // `Protocol::Ws(path)` and `Wss(path)` with any path are `/ws` and
    // `/wss`; their text is `/x-parity-ws/<path>` and `/x-parity-wss/<path>`.
    let base = || multiaddr("/ip4/8.8.8.8/tcp/443");
    let ws = || Protocol::Ws("/some/path".into());
    let wss = || Protocol::Wss("/some/path".into());
    for (addr, shape) in [
        (base().with(ws()), Some(Shape::Ws)),
        (base().with(wss()), Some(Shape::Wss)),
        (base().with(Protocol::Tls).with(ws()), Some(Shape::Wss)),
        // Two TLS layers.
        (base().with(Protocol::Tls).with(wss()), None),
    ] {
        let text = addr.to_string();
        assert!(text.contains("/x-parity-ws"), "{text}");
        let expected = match shape {
            Some(shape) => Ok(DialTarget {
                target: ip("8.8.8.8"),
                port: Some(443),
                shape,
            }),
            None => Err(Refusal::Multiaddr),
        };
        assert_eq!(classify_dialed(&addr), expected, "{text}");
        assert_eq!(classify_dialed(&multiaddr(&text)), expected, "{text}");
    }
}

#[test]
fn an_http_providers_effective_port_is_what_the_exemption_compares() {
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&parse_url("https://provider.example/").unwrap())
        .unwrap();
    configured
        .add_url(&parse_url("http://router.example:8080/").unwrap())
        .unwrap();
    for (text, exempt) in [
        ("/dns/provider.example/https", true),
        ("/dns/provider.example/tls/http", true),
        ("/dns/provider.example/tcp/443/https", true),
        ("/dns/provider.example/http", false),
        ("/dns/provider.example/tcp/8443/https", false),
        ("/dns/router.example/tcp/8080/http", true),
        ("/dns/router.example/http", false),
        ("/dns/router.example/tcp/8080", true),
        // A `/dnsaddr` reaches no port of its own.
        ("/dnsaddr/provider.example", false),
    ] {
        let dial = classify_dialed(&multiaddr(text)).unwrap();
        assert_eq!(configured.contains_dial(&dial), exempt, "{text}");
    }
}

#[test]
fn everything_else_is_refused_wherever_it_appears() {
    for text in [
        // A second host component, before or after the first.
        "/ip4/8.8.8.8/ip4/127.0.0.1/tcp/8080",
        "/ip4/8.8.8.8/tcp/9/ip4/127.0.0.1/tcp/8080",
        "/ip6/2606:4700:4700::1111/ip4/127.0.0.1/tcp/8080",
        "/ip4/8.8.8.8/tcp/80/ip4/127.0.0.1/tcp/80/ws",
        "/dns/example.com/dns/localhost/tcp/80",
        "/ip4/8.8.8.8/dns/localhost/http",
        // An onion behind a public address.
        "/ip4/8.8.8.8/dns/secret.ONION./tcp/80",
        "/ip4/8.8.8.8/dnsaddr/secret.onion/tcp/80",
        // Components no shape has, anywhere.
        "/ip4/8.8.8.8/ipcidr/32/ip4/127.0.0.1/tcp/8080",
        "/ip4/8.8.8.8/p2p-circuit/p2p-circuit/ip4/127.0.0.1/tcp/8080",
        "/ip4/8.8.8.8/ip6zone/eth0/ip4/127.0.0.1/tcp/8080",
        "/ip4/8.8.8.8/ip6zone/eth0/ip6/fe80::1/tcp/80",
        "/ip6zone/eth0/ip6/fe80::1/tcp/80",
        "/ip4/8.8.8.8/tcp/4001/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/p2p-circuit",
        "/ip4/8.8.8.8/udp/4001/quic",
        "/ip4/8.8.8.8/udp/4001/quic-v1/webtransport",
        "/ip4/8.8.8.8/udp/4001/webtransport/certhash/uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g",
        "/ip4/8.8.8.8/udp/4001/webrtc-direct/certhash/uEiDDq4_xNyDorZBH3TlGazyJdOWSwvo4PUo5YHFMrvDE8g",
        "/ip4/8.8.8.8/udp/4001",
        "/ip4/8.8.8.8",
        "/dns/example.com",
        "/unix/sock",
        "/memory/1234",
        "/ip4/8.8.8.8/tcp/80/unix/sock",
        "/tcp/80",
        "/http",
        "/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
        // `/sni`, for now: the pinned WebSocket transport only sees TLS
        // right before `/ws`, and an HTTP provider's URL would lose TLS.
        "/ip4/8.8.8.8/tcp/4002/tls/sni/8-8-8-8.k51qzi5uqu5dl.libp2p.direct/ws",
        "/ip4/8.8.8.8/tcp/443/tls/sni/provider.example/http",
        "/dns/provider.example/tls/sni/provider.example/http",
        "/ip4/8.8.8.8/tcp/443/sni/x.example/ws",
        "/ip4/8.8.8.8/tcp/443/tls/sni/x.example",
        // Suffixes outside the list, more than one, or an extra trailing
        // component.
        "/ip4/8.8.8.8/tcp/443/tls",
        "/ip4/8.8.8.8/tls/ws",
        "/ip4/8.8.8.8/ws",
        "/ip4/8.8.8.8/tcp/443/noise",
        "/ip4/8.8.8.8/tcp/443/ws/ws",
        "/ip4/8.8.8.8/tcp/443/tls/wss",
        "/ip4/8.8.8.8/tcp/443/tls/tls/ws",
        "/ip4/8.8.8.8/tcp/443/http/ws",
        "/ip4/8.8.8.8/tcp/443/ws/http",
        "/ip4/8.8.8.8/tcp/443/https/http",
        "/ip4/8.8.8.8/http/tcp/80",
        "/ip4/8.8.8.8/udp/4001/quic-v1/ws",
        "/ip4/8.8.8.8/udp/4001/quic-v1/tcp/1",
        "/ip4/8.8.8.8/tcp/80/ws/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/ws",
        // `/dnsaddr` and onions take nothing after them but `/p2p`.
        "/dnsaddr/bootstrap.libp2p.io/tcp/4001",
        "/dnsaddr/bootstrap.libp2p.io/ws",
        "/dnsaddr/bootstrap.libp2p.io/http",
        "/dnsaddr/a.example/dnsaddr/b.example",
        "/dnsaddr/bootstrap.libp2p.io/ip4/127.0.0.1/tcp/80",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234/tcp/80",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234/http",
        // At most one `/p2p`, and only last.
        "/ip4/8.8.8.8/tcp/4001/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
        "/ip4/8.8.8.8/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/tcp/4001",
        "/dnsaddr/x.example/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
    ] {
        assert_eq!(
            classify_dialed(&multiaddr(text)),
            Err(Refusal::Multiaddr),
            "{text}"
        );
    }
    assert_eq!(
        classify_multiaddr(&Multiaddr::empty()),
        Err(Refusal::Multiaddr)
    );
    // The constants are spelled as multiaddr parses them.
    let _ = multiaddr(&format!("/onion3/{ONION3}:1"));
    let _ = multiaddr(&format!(
        "/ip4/1.2.3.4/udp/1/webtransport/certhash/{CERTHASH}"
    ));
}

/// The reviewer's differential, turned around: each of these addresses
/// puts a public address first, and libp2p-tcp, reading from the end,
/// connects to the loopback listener after it. The gate refuses every one,
/// before any transport sees it.
#[tokio::test]
async fn a_public_prefix_cant_hide_the_address_libp2p_tcp_dials() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    for prefix in [
        "/ip4/8.8.8.8",
        "/ip4/8.8.8.8/tcp/9",
        "/ip6/2606:4700:4700::1111",
        "/ip4/8.8.8.8/ipcidr/32",
        "/ip4/8.8.8.8/p2p-circuit/p2p-circuit",
        "/ip4/8.8.8.8/ip6zone/eth0",
    ] {
        let text = format!("{prefix}/ip4/127.0.0.1/tcp/{port}");
        let addr = multiaddr(&text);
        assert_eq!(classify_multiaddr(&addr), Err(Refusal::Multiaddr), "{text}");

        // Why: the transport would connect to the loopback listener.
        let mut transport = libp2p_tcp::tokio::Transport::default();
        let dial = transport
            .dial(
                addr,
                DialOpts {
                    role: Endpoint::Dialer,
                    port_use: PortUse::New,
                },
            )
            .unwrap();
        let stream = tokio::time::timeout(Duration::from_secs(5), dial)
            .await
            .expect("the dial ends")
            .expect("libp2p-tcp connects");
        let (_socket, peer) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("the listener is reached")
            .unwrap();
        assert!(peer.ip().is_loopback(), "{text}");
        drop(stream);
    }
    // The address the transport reads, on its own, is refused by the
    // address policy too.
    assert!(check_address("127.0.0.1".parse().unwrap(), &Nat64State::NONE).is_err());
}
