//! Stream isolation under `socks5` (design 0007, "Stream isolation", and its
//! test): each network sends SOCKS5 credentials of its own (RFC 1929), so
//! the proxy keeps its streams on circuits of their own.
//! - Two networks send different credentials, and every stream of one
//!   (`RESOLVE`, the libp2p transport's `CONNECT`, the HTTP clients'
//!   `CONNECT`, every redirect hop's and the onion client's) sends the same
//!   ones, in the SOCKS5 handshake only, never in a request header.
//! - Every greeting offers username/password alone: a proxy that takes only
//!   no authentication fails every dial, and nothing falls back.
//! - The operating system's randomness failing fails the network.
//! - The credentials are never shown: not in a `Debug`, nor in an error.
//!
//! The test proxy prefers no authentication when a greeting offers both, so
//! a client that also offered it would be seen sending none.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{Answer, Destination, Request, SocksFixture};
use freedom_ipfs_net::socks::RandomFailed;
use freedom_ipfs_net::transport::{SwarmTransport, Transports};
use freedom_ipfs_net::{ClientConfig, Network, NetworkError, NetworkPolicy};
use libp2p::core::transport::{DialOpts, PortUse, TransportError};
use libp2p::core::Endpoint;
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, Transport};
use url::Url;

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
const ONION3: &str = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";
const CONFIG: ClientConfig = ClientConfig::new(Duration::from_secs(5), 10);
const USERNAME: &[u8] = b"freedom-ipfs";

fn onion() -> String {
    format!("{ONION3}.onion")
}

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

/// A `socks5` network through `fixture`, with `configured` as its
/// configured targets.
fn socks5(fixture: &SocksFixture, configured: ConfiguredTargets) -> Network {
    Network::new(NetworkPolicy::socks5(fixture.addr(), configured, doh()).unwrap()).unwrap()
}

fn multiaddr(text: &str) -> Multiaddr {
    format!("{text}/p2p/{PEER}").parse().unwrap()
}

/// The DHT's stack for `network`.
fn dht(network: &Network) -> SwarmTransport {
    Transports::new(network)
        .dht(&Keypair::generate_ed25519())
        .unwrap()
}

/// Dials `addr` through `transport`, for up to 2 s: the error's text if
/// it failed.
async fn dial(transport: &mut SwarmTransport, addr: &Multiaddr) -> Result<(), String> {
    let opts = DialOpts {
        role: Endpoint::Dialer,
        port_use: PortUse::New,
    };
    match transport.dial(addr.clone(), opts) {
        Err(TransportError::Other(err)) => Err(err.to_string()),
        Err(err) => Err(err.to_string()),
        Ok(dial) => match tokio::time::timeout(Duration::from_secs(2), dial).await {
            Ok(result) => result.map(drop).map_err(|err| err.to_string()),
            Err(_) => Err("still dialing".into()),
        },
    }
}

/// An HTTP server on loopback that answers each request with `respond`'s
/// reply for its path, raw, and keeps each request's head as received.
struct Server {
    addr: SocketAddr,
    heads: Arc<Mutex<Vec<String>>>,
}

impl Server {
    fn start(respond: fn(&str) -> String) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let heads = Arc::new(Mutex::new(Vec::new()));
        let kept = heads.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                    head.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&head).into_owned();
                let path = head.split(' ').nth(1).unwrap_or("/").to_string();
                kept.lock().unwrap().push(head);
                let _ = stream.write_all(respond(&path).as_bytes());
            }
        });
        Server { addr, heads }
    }

    fn heads(&self) -> Vec<String> {
        self.heads.lock().unwrap().clone()
    }
}

/// `/hop` redirects to `clear.example`'s `/end`; anything else answers.
fn respond(path: &str) -> String {
    if path == "/hop" {
        "HTTP/1.1 302 Found\r\nLocation: http://clear.example/end\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()
    } else {
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".into()
    }
}

/// A listener that accepts and drops, for the libp2p dials' destinations.
fn sink() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || for _ in listener.incoming() {});
    addr
}

/// Every request's credentials, which each must have.
fn credentials(requests: &[Request]) -> Vec<(Vec<u8>, Vec<u8>)> {
    requests
        .iter()
        .map(|request| {
            assert_eq!(request.methods, [0x02], "{request:?}");
            request.credentials.clone().expect("credentials")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_stream_carries_its_networks_credentials_and_networks_differ() {
    let fixture = SocksFixture::start().await;
    let server = Server::start(respond);
    let configured_server = Server::start(respond);
    // The clearnet literal and name, the configured target, the onion, and
    // the libp2p dials' destinations.
    fixture.forward(Destination::Ip("8.8.8.8:80".parse().unwrap()), server.addr);
    fixture.answer("clear.example", Answer::Ip("8.8.4.4".parse().unwrap()));
    fixture.forward(Destination::Ip("8.8.4.4:80".parse().unwrap()), server.addr);
    fixture.forward(
        Destination::Ip("9.9.9.9:80".parse().unwrap()),
        configured_server.addr,
    );
    fixture.forward(Destination::Name(onion(), 80), server.addr);
    fixture.forward(Destination::Ip("8.8.8.8:4001".parse().unwrap()), sink());
    fixture.answer("peer.example", Answer::Ip("1.1.1.1".parse().unwrap()));
    fixture.forward(Destination::Ip("1.1.1.1:4002".parse().unwrap()), sink());
    fixture.forward(Destination::Name(onion(), 4003), sink());
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse("http://9.9.9.9/").unwrap())
        .unwrap();

    let a = socks5(&fixture, configured.clone());
    let client = a.http_client(CONFIG);
    // An untrusted literal that redirects to a name: RESOLVE, then the hop.
    let response = client.get("http://8.8.8.8/hop").send().await.unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    // The configured target's client, and the onion client.
    let response = client
        .get("http://9.9.9.9/configured")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    let response = client
        .get(format!("http://{}/onion", onion()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    // The libp2p transport: a literal, a name (RESOLVE), an onion.
    let mut transport = dht(&a);
    for addr in [
        "/ip4/8.8.8.8/tcp/4001",
        "/dns4/peer.example/tcp/4002",
        &format!("/dns/{}/tcp/4003", onion()),
    ] {
        let _ = dial(&mut transport, &multiaddr(addr)).await;
    }
    let of_a = fixture.requests();
    let kinds: Vec<(u8, Destination)> = of_a
        .iter()
        .map(|request| (request.command, request.destination.clone()))
        .collect();
    assert_eq!(
        kinds,
        [
            (0x01, Destination::Ip("8.8.8.8:80".parse().unwrap())),
            (0xF0, Destination::Name("clear.example".into(), 0)),
            (0x01, Destination::Ip("8.8.4.4:80".parse().unwrap())),
            (0x01, Destination::Ip("9.9.9.9:80".parse().unwrap())),
            (0x01, Destination::Name(onion(), 80)),
            (0x01, Destination::Ip("8.8.8.8:4001".parse().unwrap())),
            (0xF0, Destination::Name("peer.example".into(), 0)),
            (0x01, Destination::Ip("1.1.1.1:4002".parse().unwrap())),
            (0x01, Destination::Name(onion(), 4003)),
        ]
    );
    let credentials_a = credentials(&of_a);
    let (username, password) = credentials_a[0].clone();
    assert_eq!(username, USERNAME);
    assert_eq!(password.len(), 32);
    assert!(credentials_a
        .iter()
        .all(|each| *each == (username.clone(), password.clone())));
    // In the handshake only: no request carries them as a header.
    let password_text = String::from_utf8(password.clone()).unwrap();
    let heads = [server.heads(), configured_server.heads()].concat();
    assert_eq!(heads.len(), 4, "{heads:?}");
    for head in &heads {
        let lower = head.to_ascii_lowercase();
        assert!(!lower.contains("authorization"), "{head}");
        assert!(!head.contains(&password_text), "{head}");
    }
    // Every greeting offered username/password alone.
    assert!(fixture.greetings().iter().all(|methods| *methods == [0x02]));

    // Another network: credentials of its own, the same on its every stream.
    let b = socks5(&fixture, configured);
    let response = b
        .http_client(CONFIG)
        .get("http://8.8.8.8/b")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    let _ = dial(&mut dht(&b), &multiaddr("/dns4/peer.example/tcp/4002")).await;
    let credentials_b = credentials(&fixture.requests()[of_a.len()..]);
    assert_eq!(credentials_b.len(), 3);
    let (username_b, password_b) = credentials_b[0].clone();
    assert_eq!(username_b, USERNAME);
    assert_ne!(password_b, password);
    assert!(credentials_b
        .iter()
        .all(|each| *each == (username_b.clone(), password_b.clone())));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_that_takes_only_no_authentication_fails_every_dial() {
    let fixture = SocksFixture::start().await;
    fixture.accept_methods(&[0x00]);
    fixture.answer("clear.example", Answer::Ip("8.8.4.4".parse().unwrap()));
    let network = socks5(&fixture, ConfiguredTargets::new());
    let client = network.http_client(CONFIG);
    // HTTP: a literal (CONNECT), a name (RESOLVE), an onion.
    for url in [
        "http://8.8.8.8/".to_string(),
        "http://clear.example/".to_string(),
        format!("http://{}/", onion()),
    ] {
        let result = client.get(url.as_str()).send().await;
        assert!(result.is_err(), "{url}: {result:?}");
    }
    // libp2p: a literal (CONNECT), a name (RESOLVE), an onion.
    let mut transport = dht(&network);
    for addr in [
        "/ip4/8.8.8.8/tcp/4001".to_string(),
        "/dns4/clear.example/tcp/4002".to_string(),
        format!("/dns/{}/tcp/4003", onion()),
    ] {
        let result = dial(&mut transport, &multiaddr(&addr)).await;
        assert!(
            matches!(&result, Err(err) if err.contains("authentication method")),
            "{addr}: {result:?}"
        );
    }
    // Nothing was requested, and each greeting offered username/password
    // alone: none fell back.
    assert_eq!(fixture.requests(), []);
    let greetings = fixture.greetings();
    assert_eq!(greetings.len(), 6, "{greetings:?}");
    assert!(
        greetings.iter().all(|methods| *methods == [0x02]),
        "{greetings:?}"
    );
}

#[test]
fn the_operating_systems_randomness_failing_fails_the_network() {
    fn failing(_: &mut [u8]) -> Result<(), RandomFailed> {
        Err(RandomFailed)
    }
    let policy = NetworkPolicy::socks5(
        "127.0.0.1:9150".parse().unwrap(),
        ConfiguredTargets::new(),
        doh(),
    )
    .unwrap();
    assert!(matches!(
        Network::with_lookup_and_random(policy.clone(), None, failing),
        Err(NetworkError::Credentials)
    ));
    // The control: the same policy with working randomness.
    assert!(Network::with_lookup_and_random(policy, None, |bytes| {
        bytes.fill(7);
        Ok(())
    })
    .is_ok());
    // `direct` needs no credentials.
    let direct = NetworkPolicy::direct(
        ConfiguredTargets::new(),
        doh(),
        freedom_ipfs_net::Addresses::Unchecked,
    )
    .unwrap();
    assert!(Network::with_lookup_and_random(direct, None, failing).is_ok());
}

/// Every error's text, and its sources'.
fn chain(error: &dyn std::error::Error) -> String {
    let mut text = format!("{error} / {error:?}");
    let mut source = error.source();
    while let Some(next) = source {
        text.push_str(&format!(" / {next} / {next:?}"));
        source = next.source();
    }
    text
}

#[tokio::test(flavor = "multi_thread")]
async fn the_credentials_are_never_shown() {
    let fixture = SocksFixture::start().await;
    fixture.forward(
        Destination::Ip("8.8.8.8:80".parse().unwrap()),
        Server::start(respond).addr,
    );
    let network = socks5(&fixture, ConfiguredTargets::new());
    let client = network.http_client(CONFIG);
    client.get("http://8.8.8.8/").send().await.unwrap();
    let (username, password) = fixture.requests()[0].credentials.clone().unwrap();
    let (username, password) = (
        String::from_utf8(username).unwrap(),
        String::from_utf8(password).unwrap(),
    );
    let mut shown = vec![
        format!("{network:?}"),
        format!("{network:#?}"),
        format!("{client:?}"),
        format!("{:?}", network.policy()),
        format!("{:?}", network.dial_policy()),
    ];
    // Failures: the method refused, a request refused, the proxy gone.
    fixture.accept_methods(&[0x00]);
    let refused = client.get("http://8.8.8.8/").send().await.unwrap_err();
    shown.push(chain(&refused));
    let mut transport = dht(&network);
    shown.push(
        dial(&mut transport, &multiaddr("/ip4/8.8.8.8/tcp/4001"))
            .await
            .unwrap_err(),
    );
    fixture.accept_methods(&[0x02]);
    let unmapped = client.get("http://8.8.8.8:81/").send().await.unwrap_err();
    shown.push(chain(&unmapped));
    shown.push(
        dial(&mut transport, &multiaddr("/ip4/8.8.8.8/tcp/4009"))
            .await
            .unwrap_err(),
    );
    shown.push(NetworkError::Credentials.to_string());
    for text in &shown {
        assert!(!text.contains(&password), "{text}");
        assert!(!text.contains(&username), "{text}");
    }
    // A proxy that's gone: its network's credentials are never sent, and
    // no error shows the username either.
    let closed = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let gone =
        Network::new(NetworkPolicy::socks5(closed, ConfiguredTargets::new(), doh()).unwrap())
            .unwrap();
    let unreachable = gone
        .http_client(CONFIG)
        .get("http://8.8.8.8/")
        .send()
        .await
        .unwrap_err();
    let failed = dial(&mut dht(&gone), &multiaddr("/ip4/8.8.8.8/tcp/4001"))
        .await
        .unwrap_err();
    for text in [chain(&unreachable), failed, format!("{gone:?}")] {
        assert!(!text.contains(&username), "{text}");
    }
}
