//! A dialed multiaddr's whole protocol stack is validated (design 0007,
//! "The gate"), not only its first component: libp2p-tcp 0.44 dials the
//! *last* IP and TCP pair of an address, and libp2p-dns resolves every DNS
//! component wherever it is. So a public address first must not hide the
//! address the transport actually reads, or an onion behind it.

use std::net::IpAddr;
use std::time::Duration;

use freedom_ipfs_net::gate::{check_address, classify_multiaddr, Nat64State, Refusal, Target};
use libp2p_core::transport::{DialOpts, PortUse};
use libp2p_core::{Endpoint, Transport};
use multiaddr::Multiaddr;

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
    let cases = [
        // TCP, alone and with each WebSocket suffix.
        ("/ip4/8.8.8.8/tcp/4001", ip("8.8.8.8")),
        ("/ip6/2a00:1::1/tcp/4001", ip("2a00:1::1")),
        ("/dns/example.com/tcp/4001", name("example.com")),
        ("/ip4/8.8.8.8/tcp/80/ws", ip("8.8.8.8")),
        ("/dns4/example.com/tcp/443/wss", name("example.com")),
        ("/dns6/example.com/tcp/443/tls/ws", name("example.com")),
        // AutoTLS: the `/sni` name is the TLS server's, never resolved, so
        // the target is the address before it.
        (
            "/ip4/8.8.8.8/tcp/4002/tls/sni/8-8-8-8.k51qzi5uqu5dl.libp2p.direct/ws",
            ip("8.8.8.8"),
        ),
        ("/ip4/8.8.8.8/tcp/4002/tls/sni/x.onion/ws", ip("8.8.8.8")),
        // HTTP providers.
        ("/ip4/8.8.8.8/tcp/80/http", ip("8.8.8.8")),
        (
            "/dns/provider.example/tcp/443/https",
            name("provider.example"),
        ),
        (
            "/dns/provider.example/tcp/443/tls/http",
            name("provider.example"),
        ),
        (
            "/ip4/8.8.8.8/tcp/443/tls/sni/provider.example/http",
            ip("8.8.8.8"),
        ),
        // QUIC v1, `/dnsaddr`, onions.
        ("/ip6/2a00:1::1/udp/4001/quic-v1", ip("2a00:1::1")),
        ("/dns/example.com/udp/4001/quic-v1", name("example.com")),
        ("/dnsaddr/bootstrap.libp2p.io", name("bootstrap.libp2p.io")),
        (
            &format!("/onion3/{ONION3}:1234"),
            Target::Onion(format!("{ONION3}.onion")),
        ),
        (
            "/onion/aaimaq4ygg2iegci:80",
            Target::Onion("aaimaq4ygg2iegci.onion".into()),
        ),
    ];
    for (text, target) in cases {
        assert_eq!(
            classify_multiaddr(&multiaddr(text)),
            Ok(target.clone()),
            "{text}"
        );
        // Each may end in one `/p2p/<peer>`.
        let with_peer = format!("{text}/p2p/{PEER}");
        assert_eq!(
            classify_multiaddr(&multiaddr(&with_peer)),
            Ok(target),
            "{with_peer}"
        );
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
        "/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
        // Suffixes outside the list, or more than one of them.
        "/ip4/8.8.8.8/tcp/443/tls",
        "/ip4/8.8.8.8/tcp/443/noise",
        "/ip4/8.8.8.8/tcp/443/ws/ws",
        "/ip4/8.8.8.8/tcp/443/tls/wss",
        "/ip4/8.8.8.8/tcp/443/sni/x.example/ws",
        "/ip4/8.8.8.8/tcp/443/tls/sni/x.example",
        "/ip4/8.8.8.8/tcp/443/tls/sni/a.example/sni/b.example/ws",
        "/ip4/8.8.8.8/tcp/443/http/ws",
        "/ip4/8.8.8.8/tcp/443/ws/http",
        // `/dnsaddr` and onions take no transport after them.
        "/dnsaddr/bootstrap.libp2p.io/tcp/4001",
        "/dnsaddr/bootstrap.libp2p.io/ws",
        "/dnsaddr/a.example/dnsaddr/b.example",
        "/dnsaddr/bootstrap.libp2p.io/ip4/127.0.0.1/tcp/80",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234/tcp/80",
        // At most one `/p2p`, and only last.
        "/ip4/8.8.8.8/tcp/4001/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
        "/ip4/8.8.8.8/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG/tcp/4001",
    ] {
        assert_eq!(
            classify_multiaddr(&multiaddr(text)),
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
