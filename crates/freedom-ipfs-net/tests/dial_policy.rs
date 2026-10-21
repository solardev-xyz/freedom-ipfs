//! The gate where candidates are chosen and addresses expanded (design
//! 0007, "The gate", checks 1 and 2, and *Candidate choice knows the
//! transports*), and the HTTP provider URLs built from parsed multiaddrs.

use std::net::IpAddr;
use std::time::Duration;

use freedom_ipfs_net::gate::{
    classify_dialed, classify_url, parse_url, AddressRefusal, ConfiguredTargets, Nat64State,
    Refusal, Shape, UrlTarget,
};
use freedom_ipfs_net::{
    Addresses, DialPolicy, DialRefusal, Nat64Handle, Network, NetworkPolicy, TransportPolicy,
};
use multiaddr::Multiaddr;
use tokio::time::Instant;
use url::Url;

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";

fn multiaddr(text: &str) -> Multiaddr {
    text.parse()
        .unwrap_or_else(|err| panic!("{text} parses: {err}"))
}

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

fn configured(urls: &[&str]) -> ConfiguredTargets {
    let mut configured = ConfiguredTargets::new();
    for url in urls {
        configured.add_url(&parse_url(url).unwrap()).unwrap();
    }
    configured
}

fn unchecked() -> DialPolicy {
    Network::new(NetworkPolicy::direct(configured(&[]), doh(), Addresses::Unchecked).unwrap())
        .unwrap()
        .dial_policy()
}

fn checked_with(urls: &[&str], handle: Nat64Handle) -> DialPolicy {
    Network::with_lookup(
        NetworkPolicy::direct(configured(urls), doh(), Addresses::Checked(handle)).unwrap(),
        None,
    )
    .dial_policy()
}

fn checked(urls: &[&str]) -> DialPolicy {
    checked_with(urls, Nat64Handle::fixed(Nat64State::NONE))
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

/// Addresses no shape accepts, or that hide an onion or a second host.
const REFUSED_SHAPES: &[&str] = &[
    "/ip4/8.8.8.8/ip4/127.0.0.1/tcp/8080",
    "/ip4/8.8.8.8/dns/x.onion/tcp/80",
    "/ip6zone/eth0/ip6/fe80::1/tcp/4001",
    "/unix/sock",
    "/ip4/8.8.8.8/tcp/4001/p2p-circuit",
    "/dns4/example.com/tcp/443/tls/sni/example.com/ws",
    "/dns4/example.com/tcp/443/tls/sni/example.com/http",
];

#[tokio::test]
async fn the_legacy_policy_has_no_gate() {
    let policy = DialPolicy::legacy();
    assert!(!policy.is_gated());
    assert_eq!(policy.transports(), TransportPolicy::ALL);
    assert_eq!(Network::legacy().transport_policy(), TransportPolicy::ALL);
    for text in REFUSED_SHAPES
        .iter()
        .chain(&["/ip4/127.0.0.1/tcp/4001", "/dns4/x.onion/tcp/80"])
    {
        let addr = multiaddr(text);
        assert_eq!(policy.check_now(&addr), Ok(()), "{text}");
        assert_eq!(policy.check(&addr, soon()).await, Ok(()), "{text}");
    }
    let onion = Url::parse("http://x.onion/").unwrap();
    assert_eq!(policy.check_url(&onion, soon()).await, Ok(()));
}

#[tokio::test]
async fn unchecked_refuses_shapes_and_onions_but_no_address() {
    let policy = unchecked();
    assert!(policy.is_gated());
    for text in REFUSED_SHAPES {
        assert!(
            matches!(
                policy.check_now(&multiaddr(text)),
                Err(DialRefusal::Target(Refusal::Multiaddr))
            ),
            "{text}"
        );
    }
    for text in [
        "/dns4/x.onion/tcp/80",
        "/dnsaddr/peers.onion",
        "/onion3/vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd:1234",
    ] {
        assert_eq!(
            policy.check_now(&multiaddr(text)),
            Err(DialRefusal::Onion),
            "{text}"
        );
    }
    // No address table yet: a private literal passes, as a name does.
    for text in [
        "/ip4/127.0.0.1/tcp/4001",
        "/ip4/10.0.0.1/udp/4001/quic-v1",
        "/dns4/example.com/tcp/443/wss",
        "/dnsaddr/peers.example",
    ] {
        let text = format!("{text}/p2p/{PEER}");
        assert_eq!(policy.check_now(&multiaddr(&text)), Ok(()), "{text}");
    }
    let answer: IpAddr = "127.0.0.1".parse().unwrap();
    assert_eq!(
        policy
            .check_answer(&multiaddr("/dns4/a.example/tcp/1"), answer, soon())
            .await,
        Ok(())
    );
    for (url, refused) in [
        ("http://x.onion/", Some(DialRefusal::Onion)),
        ("http://127.0.0.1:8080/", None),
        ("https://example.com/", None),
    ] {
        let url = Url::parse(url).unwrap();
        assert_eq!(policy.check_url_now(&url).err(), refused, "{url}");
    }
}

#[tokio::test]
async fn checked_refuses_untrusted_addresses_and_exempts_configured_ones() {
    let policy = checked(&["http://127.0.0.1:5001/routing/v1", "https://router.lan/"]);
    for text in [
        "/ip4/127.0.0.1/tcp/4001",
        "/ip4/169.254.169.254/tcp/80/http",
        "/ip6/::ffff:169.254.169.254/tcp/80",
        "/ip6/64:ff9b::7f00:1/tcp/80",
        "/ip6/64:ff9b::a9fe:a9fe/tcp/80",
    ] {
        assert!(
            matches!(
                policy.check_now(&multiaddr(text)),
                Err(DialRefusal::Address(_))
            ),
            "{text}"
        );
    }
    for text in [
        "/ip4/8.8.8.8/tcp/4001",
        "/ip6/64:ff9b::808:808/tcp/80",
        "/dns4/example.com/tcp/4001",
    ] {
        assert_eq!(policy.check_now(&multiaddr(text)), Ok(()), "{text}");
    }
    // A configured target, by exact host and port: an HTTP provider at the
    // router's host and port passes; the same host on another port doesn't.
    assert_eq!(
        policy.check_now(&multiaddr("/ip4/127.0.0.1/tcp/5001/http")),
        Ok(())
    );
    assert!(policy
        .check_now(&multiaddr("/ip4/127.0.0.1/tcp/5002/http"))
        .is_err());

    // A/AAAA answers: refused unless the name and port are configured.
    let private: IpAddr = "192.168.1.5".parse().unwrap();
    let dns64: IpAddr = "64:ff9b::7f00:1".parse().unwrap();
    let public: IpAddr = "8.8.8.8".parse().unwrap();
    let untrusted = multiaddr("/dns4/private.example/tcp/4001");
    for ip in [private, dns64] {
        assert!(matches!(
            policy.check_answer(&untrusted, ip, soon()).await,
            Err(DialRefusal::Address(_))
        ));
    }
    assert_eq!(
        policy.check_answer(&untrusted, public, soon()).await,
        Ok(())
    );
    let router = multiaddr("/dns4/router.lan/https");
    assert_eq!(policy.check_answer(&router, private, soon()).await, Ok(()));
    let other_port = multiaddr("/dns4/router.lan/tcp/8443/https");
    assert!(policy
        .check_answer(&other_port, private, soon())
        .await
        .is_err());

    // Provider URLs: literals are checked, names are left to the resolver.
    for (url, passes) in [
        ("http://127.0.0.1:5001/", true),
        ("http://127.0.0.1:5002/", false),
        ("http://[::1]/", false),
        ("http://8.8.8.8/", true),
        ("https://private.example/", true),
        ("http://x.onion/", false),
    ] {
        let parsed = Url::parse(url).unwrap();
        assert_eq!(
            policy.check_url(&parsed, soon()).await.is_ok(),
            passes,
            "{url}"
        );
        assert_eq!(policy.check_url_now(&parsed).is_ok(), passes, "{url}");
    }
}

#[tokio::test]
async fn a_tcp_only_policy_refuses_quic() {
    for policy in [
        DialPolicy::legacy().with_transports(TransportPolicy::TCP_ONLY),
        unchecked().with_transports(TransportPolicy::TCP_ONLY),
    ] {
        assert!(!policy.transports().quic());
        assert!(!policy.transports().carries(Shape::QuicV1));
        assert!(policy.transports().carries(Shape::Wss));
        let quic = multiaddr("/ip4/8.8.8.8/udp/4001/quic-v1");
        if policy.is_gated() {
            assert_eq!(policy.check_now(&quic), Err(DialRefusal::Transport));
        }
        assert_eq!(
            policy.check_now(&multiaddr("/ip4/8.8.8.8/tcp/4001")),
            Ok(())
        );
    }
}

#[tokio::test]
async fn an_ipv6_address_waits_for_nat64_discovery_until_the_deadline() {
    let addr = multiaddr("/ip6/2a00:1:2:3:a9:fea9:fe00:0/tcp/4001");
    let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
    let policy = checked_with(&[], handle);
    assert_eq!(
        policy.check_now(&addr),
        Err(DialRefusal::Address(AddressRefusal::Nat64Pending))
    );
    // Past the deadline: still pending, refused.
    let short = Instant::now() + Duration::from_millis(30);
    assert_eq!(
        policy.check(&addr, short).await,
        Err(DialRefusal::Address(AddressRefusal::Nat64Pending))
    );
    // Discovery learns the /64 whose embedding is 169.254.169.254: refused
    // as that address, once known.
    let waiting = {
        let policy = policy.clone();
        let addr = addr.clone();
        tokio::spawn(async move { policy.check(&addr, soon()).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!waiting.is_finished());
    let prefix =
        freedom_ipfs_net::gate::Nat64Prefix::new("2a00:1:2:3::".parse().unwrap(), 64).unwrap();
    publisher.publish(Nat64State::known([prefix]));
    assert!(matches!(
        waiting.await.unwrap(),
        Err(DialRefusal::Address(AddressRefusal::Embedded { .. }))
    ));
}

/// The HTTP provider URL is built from the gate's own reading of the
/// multiaddr: the scheme, host and port `classify_url` reads back from it
/// are the shape, target and port `classify_dialed` judged.
#[test]
fn a_providers_url_is_the_target_the_gate_judged() {
    for (text, url) in [
        ("/dns/https/http", "http://https/"),
        ("/dns/http/https", "https://http/"),
        ("/dns/tls/http", "http://tls/"),
        ("/dns/example.com/tls/http", "https://example.com/"),
        (
            "/dns4/Example.COM./tcp/8080/http",
            "http://example.com:8080/",
        ),
        ("/dns6/example.com/tcp/443/https", "https://example.com/"),
        ("/dns/example.com/tcp/80/https", "https://example.com:80/"),
        ("/ip4/8.8.8.8/http", "http://8.8.8.8/"),
        (
            "/ip6/2001:db8::1/tcp/8080/http",
            "http://[2001:db8::1]:8080/",
        ),
        ("/ip6/2001:db8::1/https", "https://[2001:db8::1]/"),
        ("/ip6/::ffff:8.8.8.8/tcp/80/http", "http://8.8.8.8/"),
        ("/dns/x.onion/http", "http://x.onion/"),
    ] {
        for text in [text.to_string(), format!("{text}/p2p/{PEER}")] {
            let dial = classify_dialed(&multiaddr(&text)).unwrap();
            let built = dial.http_url().unwrap();
            assert_eq!(built.as_str(), url, "{text}");
            assert_eq!(
                classify_url(&built).unwrap(),
                UrlTarget {
                    secure: dial.shape == Shape::Https,
                    target: dial.target.clone(),
                    port: dial.port.unwrap(),
                },
                "{text}"
            );
        }
    }
    // Not an HTTP provider's shape: no URL.
    for text in [
        "/ip4/8.8.8.8/tcp/4001",
        "/dns4/example.com/tcp/443/wss",
        "/ip4/8.8.8.8/udp/4001/quic-v1",
        "/dnsaddr/example.com",
    ] {
        assert_eq!(classify_dialed(&multiaddr(text)).unwrap().http_url(), None);
    }
}
