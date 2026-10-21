//! Bitswap's address expansion and candidate choice under a `direct`
//! network policy (design 0007, "The gate", checks 1 and 2), through the
//! retriever, against a local DoH server and canary listeners:
//! - the DoH endpoint is the policy's;
//! - a hidden onion makes no DNS or TXT query;
//! - a `/dnsaddr` that expands to loopback, or to a refused shape, and a
//!   name that resolves to a private address, never reach a socket, while
//!   the same expansions without the address table (or with a dialable
//!   shape) do: the control that proves the dial would happen.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use freedom_ipfs_net::gate::{ConfiguredTargets, Nat64State};
use freedom_ipfs_net::{Addresses, Nat64Handle, Network, NetworkPolicy};
use freedom_ipfs_retrieval::{HttpRetriever, HTTP_PROVIDER_CLIENT_CONFIG};
use freedom_ipfs_routing::{DelegatedRoutingClient, Provider, ProviderRoutingClient};
use freedom_ipfs_store::{CachedProviderRecord, SqliteBlockStore};
use libp2p::identity::Keypair;
use libp2p::PeerId;
use multihash::Multihash;
use url::Url;

/// A DoH server answering from a table of `(name, type)` to answer data,
/// recording every query. An answer `CNAME <target>` is an alias record.
struct Doh {
    port: u16,
    queries: Arc<Mutex<Vec<(String, String)>>>,
}

impl Doh {
    fn start(answers: HashMap<(String, String), Vec<String>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let recorded = queries.clone();
        let answers = Arc::new(answers);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (recorded, answers) = (recorded.clone(), answers.clone());
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        return;
                    }
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                            break;
                        }
                    }
                    let target = line.split_whitespace().nth(1).unwrap_or("");
                    let query = target.split_once('?').map_or("", |(_, query)| query);
                    let mut name = String::new();
                    let mut kind = String::new();
                    for pair in query.split('&') {
                        match pair.split_once('=') {
                            Some(("name", value)) => name = value.to_string(),
                            Some(("type", value)) => kind = value.to_string(),
                            _ => {}
                        }
                    }
                    recorded.lock().unwrap().push((name.clone(), kind.clone()));
                    let record_type = match kind.as_str() {
                        "TXT" => 16,
                        "1" => 1,
                        "28" => 28,
                        _ => 0,
                    };
                    let answer = answers
                        .get(&(name, kind))
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|data| match data.strip_prefix("CNAME ") {
                            Some(target) => {
                                serde_json::json!({ "type": 5, "TTL": 60, "data": target })
                            }
                            None => {
                                serde_json::json!({ "type": record_type, "TTL": 60, "data": data })
                            }
                        })
                        .collect::<Vec<_>>();
                    let body = serde_json::json!({ "Status": 0, "Answer": answer }).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/dns-json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = reader.get_mut().write_all(response.as_bytes());
                });
            }
        });
        Self { port, queries }
    }

    fn url(&self) -> Url {
        Url::parse(&format!("http://127.0.0.1:{}/dns-query", self.port)).unwrap()
    }

    fn queries(&self) -> Vec<(String, String)> {
        self.queries.lock().unwrap().clone()
    }
}

/// A listener that counts the connections it accepts, and drops them.
struct Canary {
    port: u16,
    accepted: Arc<AtomicUsize>,
}

impl Canary {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
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

fn network(doh: &Doh, addresses: Addresses) -> Network {
    Network::new(NetworkPolicy::direct(ConfiguredTargets::new(), doh.url(), addresses).unwrap())
        .unwrap()
}

fn checked() -> Addresses {
    Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE))
}

fn peer() -> PeerId {
    Keypair::generate_ed25519().public().to_peer_id()
}

fn cid() -> Cid {
    Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[7; 32]).unwrap())
}

/// Starts a fetch from `addrs` (one provider, `peer`) through a retriever of
/// `network`; the caller aborts it once it has seen enough.
fn fetch(network: &Network, peer: PeerId, addrs: Vec<String>) -> tokio::task::JoinHandle<()> {
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let providers = vec![Provider::from_parts(Some(peer.to_string()), addrs).unwrap()];
    tokio::spawn(async move {
        let _ = retriever.fetch_from_providers(&cid(), &providers).await;
    })
}

/// Waits until `done` holds, at most `limit`.
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

#[tokio::test(flavor = "multi_thread")]
async fn the_doh_endpoint_comes_from_the_policy() {
    let doh = Doh::start(HashMap::new());
    let peer = peer();
    let fetch = fetch(
        &network(&doh, Addresses::Unchecked),
        peer,
        vec![
            format!("/dnsaddr/peers.example/p2p/{peer}"),
            format!("/dns4/name.example/tcp/4001/p2p/{peer}"),
        ],
    );
    let expected = [
        ("_dnsaddr.peers.example".to_string(), "TXT".to_string()),
        ("name.example".to_string(), "1".to_string()),
    ];
    let seen = wait_until(Duration::from_secs(10), || {
        let queries = doh.queries();
        expected.iter().all(|query| queries.contains(query))
    })
    .await;
    fetch.abort();
    assert!(seen, "{:?}", doh.queries());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hidden_onion_makes_no_dns_or_txt_query() {
    let doh = Doh::start(HashMap::new());
    let peer = peer();
    let network = network(&doh, Addresses::Unchecked);
    let fetch = fetch(
        &network,
        peer,
        vec![
            format!("/ip4/8.8.8.8/dns/x.onion/tcp/4001/p2p/{peer}"),
            format!("/ip4/8.8.8.8/dnsaddr/x.onion/p2p/{peer}"),
            format!("/dns4/x.onion/tcp/4001/p2p/{peer}"),
            format!("/dnsaddr/x.onion/p2p/{peer}"),
        ],
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), fetch).await;
    assert_eq!(doh.queries(), Vec::<(String, String)>::new());
}

/// A `/dnsaddr` whose TXT record is a loopback address: refused before a
/// socket opens with checked addresses, dialed without them.
#[tokio::test(flavor = "multi_thread")]
async fn a_dnsaddr_that_expands_to_loopback_is_refused_before_a_socket() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([(
        ("_dnsaddr.peers.example".to_string(), "TXT".to_string()),
        vec![format!(
            "\"dnsaddr=/ip4/127.0.0.1/tcp/{}/p2p/{peer}\"",
            canary.port
        )],
    )]));
    let addrs = vec![format!("/dnsaddr/peers.example/p2p/{peer}")];

    let refused = fetch(&network(&doh, checked()), peer, addrs.clone());
    let queried = wait_until(Duration::from_secs(10), || !doh.queries().is_empty()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    refused.abort();
    assert!(queried, "the TXT record was asked for");
    assert_eq!(canary.accepted(), 0, "the loopback record was dialed");

    // The control: without the address table, the same record is dialed.
    let dialed = fetch(&network(&doh, Addresses::Unchecked), peer, addrs);
    let reached = wait_until(Duration::from_secs(10), || canary.accepted() > 0).await;
    dialed.abort();
    assert!(reached, "the control never dialed the record");
}

/// TXT records in shapes the gate refuses, though their transport would
/// reach the canary: a second host (libp2p's TCP transport dials the last
/// address) and `/sni`.
#[tokio::test(flavor = "multi_thread")]
async fn a_dnsaddr_answer_with_a_refused_shape_never_connects() {
    let canary = Canary::start();
    let peer = peer();
    let port = canary.port;
    let doh = Doh::start(HashMap::from([(
        ("_dnsaddr.peers.example".to_string(), "TXT".to_string()),
        vec![
            format!("\"dnsaddr=/ip4/8.8.8.8/ip4/127.0.0.1/tcp/{port}/p2p/{peer}\""),
            format!("\"dnsaddr=/ip4/127.0.0.1/tcp/{port}/tls/sni/x.example/ws/p2p/{peer}\""),
        ],
    )]));
    let fetch = fetch(
        &network(&doh, Addresses::Unchecked),
        peer,
        vec![format!("/dnsaddr/peers.example/p2p/{peer}")],
    );
    let queried = wait_until(Duration::from_secs(10), || !doh.queries().is_empty()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    fetch.abort();
    assert!(queried);
    assert_eq!(canary.accepted(), 0, "a refused shape was dialed");
}

/// A name whose A answer is loopback: refused before a socket opens with
/// checked addresses, dialed without them.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_that_resolves_to_a_private_address_is_refused_before_a_socket() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([
        (
            ("private.example".to_string(), "1".to_string()),
            vec!["127.0.0.1".to_string()],
        ),
        (
            ("private.example".to_string(), "28".to_string()),
            vec!["64:ff9b::7f00:1".to_string()],
        ),
    ]));
    let addrs = vec![format!(
        "/dns4/private.example/tcp/{}/p2p/{peer}",
        canary.port
    )];

    let refused = fetch(&network(&doh, checked()), peer, addrs.clone());
    let queried = wait_until(Duration::from_secs(10), || doh.queries().len() >= 2).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    refused.abort();
    assert!(queried, "{:?}", doh.queries());
    assert_eq!(canary.accepted(), 0, "the private answer was dialed");

    let dialed = fetch(&network(&doh, Addresses::Unchecked), peer, addrs);
    let reached = wait_until(Duration::from_secs(10), || canary.accepted() > 0).await;
    dialed.abort();
    assert!(reached, "the control never dialed the answer");
}

/// A delegated router answering every lookup with `body`.
fn router(body: String) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let body = body.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = reader.get_mut().write_all(response.as_bytes());
            });
        }
    });
    port
}

/// Providers come in through the policy wherever they come from: a
/// delegated router's answer and the store's provider cache, as well as a
/// caller's list. A loopback provider is never dialed with checked
/// addresses; the controls, without the address table, dial it.
#[tokio::test(flavor = "multi_thread")]
async fn providers_from_routing_and_from_the_cache_are_admitted_too() {
    let canary = Canary::start();
    let peer = peer();
    let addr = format!("/ip4/127.0.0.1/tcp/{}/p2p/{peer}", canary.port);
    let doh = Doh::start(HashMap::new());
    let router_port = router(format!(
        r#"{{"Providers":[{{"Schema":"peer","ID":"{peer}","Addrs":["{addr}"]}}]}}"#
    ));
    let router_url = format!("http://127.0.0.1:{router_port}");
    let network_for = |addresses: Addresses| {
        let mut configured = ConfiguredTargets::new();
        configured
            .add_url(&freedom_ipfs_net::gate::parse_url(&router_url).unwrap())
            .unwrap();
        Network::new(NetworkPolicy::direct(configured, doh.url(), addresses).unwrap()).unwrap()
    };

    for (addresses, dialed) in [(checked(), false), (Addresses::Unchecked, true)] {
        let network = network_for(addresses);
        // From the router.
        let routing = DelegatedRoutingClient::with_endpoints_and_client(
            [router_url.clone()],
            network.http_client(freedom_ipfs_routing::HTTP_CLIENT_CONFIG),
        );
        let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(routing, store)
            .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
        let before = canary.accepted();
        let fetch = tokio::spawn(async move {
            let _ = retriever.fetch_block(&cid()).await;
        });
        let reached = wait_until(Duration::from_secs(if dialed { 10 } else { 3 }), || {
            canary.accepted() > before
        })
        .await;
        fetch.abort();
        assert_eq!(reached, dialed, "from the router");

        // From the store's provider cache.
        let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
        store
            .put_provider_records(
                &cid(),
                &[CachedProviderRecord {
                    id: Some(peer.to_string()),
                    addrs: vec![addr.clone()],
                }],
                Duration::from_secs(60),
            )
            .unwrap();
        let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
            .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
        let before = canary.accepted();
        let fetch = tokio::spawn(async move {
            let _ = retriever.fetch_block(&cid()).await;
        });
        let reached = wait_until(Duration::from_secs(if dialed { 10 } else { 3 }), || {
            canary.accepted() > before
        })
        .await;
        fetch.abort();
        assert_eq!(reached, dialed, "from the cache");
    }
}

/// A configured name and port keep their exemption through expansion
/// (#47 review, 47-M3): the configured name's loopback answer is dialed with
/// checked addresses, as without them.
#[tokio::test(flavor = "multi_thread")]
async fn a_configured_names_answer_is_dialed_with_checked_addresses() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([(
        ("configured.example".to_string(), "1".to_string()),
        vec!["127.0.0.1".to_string()],
    )]));
    let addrs = vec![format!(
        "/dns4/configured.example/tcp/{}/p2p/{peer}",
        canary.port
    )];
    let control = fetch(&network(&doh, Addresses::Unchecked), peer, addrs.clone());
    let reached = wait_until(Duration::from_secs(10), || canary.accepted() > 0).await;
    control.abort();
    assert!(reached, "the control never dialed the answer");

    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse(&format!("http://configured.example:{}/", canary.port)).unwrap())
        .unwrap();
    let gated =
        Network::new(NetworkPolicy::direct(configured, doh.url(), checked()).unwrap()).unwrap();
    let before = canary.accepted();
    let allowed = fetch(&gated, peer, addrs);
    let reached = wait_until(Duration::from_secs(10), || canary.accepted() > before).await;
    allowed.abort();
    assert!(reached, "the configured name's answer was refused");
}

/// Once the network is revoked, checks 1 and 2 admit nothing (#47 review,
/// 47-M2): a fresh retriever of the revoked network never dials, where the
/// same fetch before the revoke did.
#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_network_admits_no_candidate() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::new());
    let network = network(&doh, Addresses::Unchecked);
    let addrs = vec![format!("/ip4/127.0.0.1/tcp/{}/p2p/{peer}", canary.port)];
    let control = fetch(&network, peer, addrs.clone());
    let reached = wait_until(Duration::from_secs(10), || canary.accepted() > 0).await;
    control.abort();
    assert!(reached, "the control never dialed");

    network.revoke();
    let check = network.dial_policy().check_now(&addrs[0].parse().unwrap());
    assert!(
        matches!(check, Err(freedom_ipfs_net::DialRefusal::Revoked(_))),
        "{check:?}"
    );
    let before = canary.accepted();
    let refused = fetch(&network, peer, addrs);
    let reached = wait_until(Duration::from_secs(3), || canary.accepted() > before).await;
    refused.abort();
    assert!(!reached, "a revoked network's candidate was dialed");
}

fn dns4_provider(name: &str, port: u16, peer: PeerId) -> String {
    format!("/dns4/{name}/tcp/{port}/p2p/{peer}")
}

fn dnsaddr_provider(name: &str, _: u16, peer: PeerId) -> String {
    format!("/dnsaddr/{name}/p2p/{peer}")
}

/// An alias is an expansion result (#47 adversarial review, 47-A1): its
/// target is classified before it is followed, for A and TXT lookups alike.
/// An onion alias is refused with no query for it, and nothing is dialed;
/// an ordinary alias, the control, still resolves and is dialed.
#[tokio::test(flavor = "multi_thread")]
async fn an_alias_is_classified_before_it_is_followed() {
    for lookup in ["A", "TXT"] {
        let canary = Canary::start();
        let peer = peer();
        let port = canary.port;
        let record = |target: &str| {
            (
                (target.to_string(), "1".to_string()),
                vec!["127.0.0.1".to_string()],
            )
        };
        let txt = |name: &str| {
            (
                (name.to_string(), "TXT".to_string()),
                vec![format!("\"dnsaddr=/ip4/127.0.0.1/tcp/{port}/p2p/{peer}\"")],
            )
        };
        let (provider, alias_query, onion_query, answers) = match lookup {
            "A" => (
                dns4_provider as fn(&str, u16, PeerId) -> String,
                "1",
                ("secret.onion", "1"),
                HashMap::from([
                    (
                        ("hidden.example".to_string(), "1".to_string()),
                        vec!["CNAME secret.onion.".to_string()],
                    ),
                    (
                        ("plain.example".to_string(), "1".to_string()),
                        vec!["CNAME target.example.".to_string()],
                    ),
                    record("target.example"),
                    record("secret.onion"),
                ]),
            ),
            _ => (
                dnsaddr_provider as fn(&str, u16, PeerId) -> String,
                "TXT",
                ("_dnsaddr.secret.onion", "TXT"),
                HashMap::from([
                    (
                        ("_dnsaddr.hidden.example".to_string(), "TXT".to_string()),
                        vec!["CNAME _dnsaddr.secret.onion.".to_string()],
                    ),
                    (
                        ("_dnsaddr.plain.example".to_string(), "TXT".to_string()),
                        vec!["CNAME _dnsaddr.target.example.".to_string()],
                    ),
                    txt("_dnsaddr.target.example"),
                    txt("_dnsaddr.secret.onion"),
                ]),
            ),
        };
        let doh = Doh::start(answers);
        let network = network(&doh, Addresses::Unchecked);

        let control = fetch(&network, peer, vec![provider("plain.example", port, peer)]);
        let reached = wait_until(Duration::from_secs(10), || canary.accepted() > 0).await;
        control.abort();
        assert!(
            reached,
            "{lookup}: the ordinary alias's answer wasn't dialed"
        );
        assert!(doh.queries().iter().any(|(_, kind)| kind == alias_query));

        let before = canary.accepted();
        let hidden = fetch(&network, peer, vec![provider("hidden.example", port, peer)]);
        let queried = wait_until(Duration::from_secs(5), || {
            doh.queries()
                .iter()
                .any(|(name, _)| name.contains("hidden.example"))
        })
        .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        hidden.abort();
        assert!(queried, "{lookup}: the alias itself was asked for");
        assert!(
            !doh.queries()
                .contains(&(onion_query.0.to_string(), onion_query.1.to_string())),
            "{lookup}: the onion alias was queried: {:?}",
            doh.queries()
        );
        assert_eq!(
            canary.accepted(),
            before,
            "{lookup}: the onion alias's answer was dialed"
        );
    }
}
