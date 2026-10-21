//! DoH answers under a gated policy (#47 review, round 2: 47-R2-M1, the
//! adversarial owner finding, and 47-R2-m3): a record counts only if the
//! queried name, or an alias of it the gate admitted, owns it; every alias
//! is classified before anything it owns is taken, inline answers
//! included; and a name is classified as written, never trimmed into
//! another. Each case has a control that resolves and dials.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use freedom_ipfs_namesys::CloudflareDohResolver;
use freedom_ipfs_net::gate::{classify_host, Refusal};
use freedom_ipfs_net::gate::{ConfiguredTargets, Nat64State};
use freedom_ipfs_net::{Addresses, Nat64Handle, Network, NetworkPolicy};
use freedom_ipfs_retrieval::{HttpRetriever, HTTP_PROVIDER_CLIENT_CONFIG};
use freedom_ipfs_routing::{Provider, ProviderRoutingClient};
use freedom_ipfs_store::SqliteBlockStore;
use libp2p::identity::Keypair;
use libp2p::PeerId;
use multihash::Multihash;
use serde_json::Value;
use url::Url;

type Answers = HashMap<(String, String), Vec<Value>>;

struct Doh {
    port: u16,
    queries: Arc<Mutex<Vec<(String, String)>>>,
}

impl Doh {
    fn start(answers: Answers) -> Self {
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
                    let answer = answers.get(&(name, kind)).cloned().unwrap_or_default();
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

    fn queried(&self, name: &str, kind: &str) -> bool {
        self.queries
            .lock()
            .unwrap()
            .contains(&(name.to_string(), kind.to_string()))
    }
}

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
}

fn network(doh: &Doh, addresses: Addresses) -> Network {
    Network::new(NetworkPolicy::direct(ConfiguredTargets::new(), doh.url(), addresses).unwrap())
        .unwrap()
}

fn peer() -> PeerId {
    Keypair::generate_ed25519().public().to_peer_id()
}

fn cid() -> Cid {
    Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[47; 32]).unwrap())
}

fn fetch(network: &Network, peer: PeerId, addr: String) -> tokio::task::JoinHandle<()> {
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let providers = vec![Provider::from_parts(Some(peer.to_string()), vec![addr]).unwrap()];
    tokio::spawn(async move {
        let _ = retriever.fetch_from_providers(&cid(), &providers).await;
    })
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

fn cname(name: &str) -> Value {
    serde_json::json!({ "type": 5, "TTL": 60, "data": name })
}

fn a(ip: &str) -> Value {
    serde_json::json!({ "type": 1, "TTL": 60, "data": ip })
}

fn txt(value: String) -> Value {
    serde_json::json!({ "type": 16, "TTL": 60, "data": format!("\"{value}\"") })
}

fn owned_cname(owner: &str, name: &str) -> Value {
    serde_json::json!({ "name": owner, "type": 5, "TTL": 60, "data": name })
}

fn owned_a(owner: &str, ip: &str) -> Value {
    serde_json::json!({ "name": owner, "type": 1, "TTL": 60, "data": ip })
}

async fn control_dials(doh: &Doh, canary: &Canary, peer: PeerId) -> usize {
    let net = network(doh, Addresses::Unchecked);
    let request = fetch(
        &net,
        peer,
        format!("/dns4/control.example/tcp/{}/p2p/{peer}", canary.port),
    );
    assert!(
        wait_until(Duration::from_secs(10), || canary
            .accepted
            .load(Ordering::SeqCst)
            > 0)
        .await
    );
    request.abort();
    canary.accepted.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bundled_onion_alias_and_its_a_record_are_never_dialed() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([
        (
            ("control.example".into(), "1".into()),
            vec![owned_a("control.example.", "127.0.0.1")],
        ),
        (
            ("hidden.example".into(), "1".into()),
            vec![
                owned_cname("hidden.example.", "secret.onion."),
                owned_a("secret.onion.", "127.0.0.1"),
            ],
        ),
    ]));
    let before = control_dials(&doh, &canary, peer).await;
    let net = network(&doh, Addresses::Unchecked);
    let hidden = fetch(
        &net,
        peer,
        format!("/dns4/hidden.example/tcp/{}/p2p/{peer}", canary.port),
    );
    let dialed = wait_until(Duration::from_secs(5), || {
        canary.accepted.load(Ordering::SeqCst) > before
    })
    .await;
    hidden.abort();
    assert!(
        !dialed,
        "a bundled A record bypassed classification of its onion CNAME owner"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_a_record_owned_by_an_unrelated_name_is_ignored() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([
        (
            ("control.example".into(), "1".into()),
            vec![owned_a("control.example.", "127.0.0.1")],
        ),
        (
            ("ordinary.example".into(), "1".into()),
            vec![owned_a("secret.onion.", "127.0.0.1")],
        ),
    ]));
    let before = control_dials(&doh, &canary, peer).await;
    let net = network(&doh, Addresses::Unchecked);
    let hidden = fetch(
        &net,
        peer,
        format!("/dns4/ordinary.example/tcp/{}/p2p/{peer}", canary.port),
    );
    let dialed = wait_until(Duration::from_secs(5), || {
        canary.accepted.load(Ordering::SeqCst) > before
    })
    .await;
    hidden.abort();
    assert!(
        !dialed,
        "an A record for an unrelated onion owner was treated as the queried name's answer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gated_lookup_never_trims_an_empty_label_away() {
    let doh = Doh::start(HashMap::new());
    let net = network(&doh, Addresses::Unchecked);
    let resolver = CloudflareDohResolver::with_client(
        doh.url().to_string(),
        net.http_client(HTTP_PROVIDER_CLIENT_CONFIG),
    );
    resolver.ip_lookup("control-dot.example.").await.unwrap();
    assert!(doh.queried("control-dot.example", "1"));
    assert_eq!(
        classify_host("invalid-dot.example.."),
        Err(Refusal::InvalidHost)
    );
    let _ = resolver.ip_lookup("invalid-dot.example..").await;
    assert!(
        !doh.queried("invalid-dot.example", "1"),
        "admit_name stripped every terminal dot before calling the gate classifier"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configured_exemption_never_moves_to_another_owners_record() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([
        (
            ("control.example".into(), "1".into()),
            vec![owned_a("control.example.", "127.0.0.1")],
        ),
        (
            ("configured.example".into(), "1".into()),
            vec![owned_a("secret.onion.", "127.0.0.1")],
        ),
    ]));
    let mut configured = ConfiguredTargets::new();
    for host in ["control.example", "configured.example"] {
        configured
            .add_url(&Url::parse(&format!("http://{host}:{}", canary.port)).unwrap())
            .unwrap();
    }
    let net = Network::new(
        NetworkPolicy::direct(
            configured,
            doh.url(),
            Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE)),
        )
        .unwrap(),
    )
    .unwrap();
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(net.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let get = |host: &str| {
        let providers = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec![format!("/dns4/{host}/tcp/{}/p2p/{peer}", canary.port)],
        )
        .unwrap()];
        let retriever = retriever.clone();
        tokio::spawn(async move {
            let _ = retriever.fetch_from_providers(&cid(), &providers).await;
        })
    };
    let control = get("control.example");
    assert!(
        wait_until(Duration::from_secs(10), || canary
            .accepted
            .load(Ordering::SeqCst)
            > 0)
        .await
    );
    control.abort();
    let before = canary.accepted.load(Ordering::SeqCst);
    let target = get("configured.example");
    let dialed = wait_until(Duration::from_secs(5), || {
        canary.accepted.load(Ordering::SeqCst) > before
    })
    .await;
    target.abort();
    assert!(
        !dialed,
        "the configured name's exemption was applied to an A record owned by secret.onion"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_onion_alias_is_classified_before_its_a_record() {
    let canary = Canary::start();
    let peer = peer();
    let doh = Doh::start(HashMap::from([
        (
            ("control.example".into(), "1".into()),
            vec![cname("public-alias.example."), a("127.0.0.1")],
        ),
        (
            ("hidden.example".into(), "1".into()),
            vec![
                serde_json::json!({"name":"hidden.example.","type":5,"TTL":60,"data":"secret.onion."}),
                serde_json::json!({"name":"secret.onion.","type":1,"TTL":60,"data":"127.0.0.1"}),
            ],
        ),
    ]));
    let net = network(&doh, Addresses::Unchecked);
    let control = fetch(
        &net,
        peer,
        format!("/dns4/control.example/tcp/{}/p2p/{peer}", canary.port),
    );
    assert!(
        wait_until(Duration::from_secs(8), || canary
            .accepted
            .load(Ordering::SeqCst)
            > 0)
        .await
    );
    control.abort();
    let before = canary.accepted.load(Ordering::SeqCst);
    let hidden = fetch(
        &net,
        peer,
        format!("/dns4/hidden.example/tcp/{}/p2p/{peer}", canary.port),
    );
    let connected = wait_until(Duration::from_secs(3), || {
        canary.accepted.load(Ordering::SeqCst) > before
    })
    .await;
    hidden.abort();
    assert!(
        !connected,
        "inline final A caused the onion CNAME to bypass classification"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_onion_alias_is_classified_before_its_txt_record() {
    use freedom_ipfs_namesys::{CloudflareDohResolver, DnsTxtResolver, HTTP_CLIENT_CONFIG};
    let doh = Doh::start(HashMap::from([(
        ("_dnslink.hidden.example".into(), "TXT".into()),
        vec![
            cname("_dnslink.secret.onion."),
            txt("dnslink=/ipfs/test".into()),
        ],
    )]));
    let net = network(&doh, Addresses::Unchecked);
    let resolver =
        CloudflareDohResolver::with_client(doh.url().as_str(), net.http_client(HTTP_CLIENT_CONFIG));
    let result = resolver.txt_lookup("_dnslink.hidden.example").await;
    assert!(
        result.is_err(),
        "inline TXT caused the onion CNAME to bypass classification: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alias_with_an_empty_label_is_never_queried() {
    use freedom_ipfs_namesys::{CloudflareDohResolver, HTTP_CLIENT_CONFIG};
    let doh = Doh::start(HashMap::from([
        (
            ("root.example".into(), "1".into()),
            vec![cname("empty-label.example..")],
        ),
        (
            ("empty-label.example".into(), "1".into()),
            vec![a("8.8.8.8")],
        ),
    ]));
    let net = network(&doh, Addresses::Unchecked);
    let resolver =
        CloudflareDohResolver::with_client(doh.url().as_str(), net.http_client(HTTP_CLIENT_CONFIG));
    let result = resolver.ip_lookup("root.example").await;
    assert!(
        !doh.queried("empty-label.example", "1") && result.is_err(),
        "invalid alias was queried after trimming: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_onion_alias_is_classified_before_its_aaaa_record() {
    use freedom_ipfs_namesys::{CloudflareDohResolver, HTTP_CLIENT_CONFIG};
    let doh = Doh::start(HashMap::from([(
        ("hidden6.example".into(), "28".into()),
        vec![
            cname("secret.onion."),
            serde_json::json!({"type":28,"TTL":60,"data":"2a00:1::1"}),
        ],
    )]));
    let net = network(&doh, Addresses::Unchecked);
    let resolver =
        CloudflareDohResolver::with_client(doh.url().as_str(), net.http_client(HTTP_CLIENT_CONFIG));
    let result = resolver.ip_lookup("hidden6.example").await;
    assert!(
        result.is_err(),
        "inline AAAA bypassed onion classification: {result:?}"
    );
}

fn direct_resolver(doh: &Doh) -> CloudflareDohResolver {
    let net = network(doh, Addresses::Unchecked);
    CloudflareDohResolver::with_client(
        doh.url().to_string(),
        net.http_client(HTTP_PROVIDER_CLIENT_CONFIG),
    )
}

fn owned_txt(owner: &str, value: &str) -> Value {
    serde_json::json!({ "name": owner, "type": 16, "TTL": 60, "data": format!("\"{value}\"") })
}

/// Aliases in a loop, or two for one name (#47 adversarial review round 3,
/// 47-R3-A1): a lookup whose chain revisits a name has no last name, and a
/// name with two aliases is refused rather than one chosen by the answers'
/// order; for A and for the TXT records `/dnsaddr` reads. Each has a
/// finite chain that resolves as its control.
#[tokio::test(flavor = "multi_thread")]
async fn an_alias_loop_has_no_last_name() {
    let doh = Doh::start(HashMap::from([
        (
            ("control-case.example".into(), "1".into()),
            vec![
                owned_cname("CONTROL-CASE.EXAMPLE.", "Alias.Example."),
                owned_a("ALIAS.EXAMPLE", "8.8.8.8"),
            ],
        ),
        (
            ("loop.example".into(), "1".into()),
            vec![
                owned_cname("loop.example.", "alias.example."),
                owned_cname("alias.example.", "LOOP.EXAMPLE."),
                owned_a("loop.example.", "8.8.4.4"),
            ],
        ),
    ]));
    let resolver = direct_resolver(&doh);
    let control = resolver.ip_lookup("Control-Case.Example.").await.unwrap();
    assert_eq!(control, ["8.8.8.8".parse::<std::net::IpAddr>().unwrap()]);

    let result = resolver.ip_lookup("loop.example").await;
    assert!(
        result.as_ref().is_err() || result.as_ref().is_ok_and(Vec::is_empty),
        "a cyclic CNAME graph was treated as having a terminal owner: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_aliases_for_one_name_are_refused() {
    let doh = Doh::start(HashMap::from([
        (
            ("control.example".into(), "1".into()),
            vec![
                owned_cname("control.example.", "safe.example."),
                owned_a("safe.example.", "8.8.8.8"),
            ],
        ),
        (
            ("branch.example".into(), "1".into()),
            vec![
                owned_cname("branch.example.", "safe.example."),
                owned_cname("branch.example.", "secret.onion."),
                owned_a("safe.example.", "8.8.4.4"),
            ],
        ),
    ]));
    let resolver = direct_resolver(&doh);
    let control = resolver.ip_lookup("control.example").await.unwrap();
    assert_eq!(control, ["8.8.8.8".parse::<std::net::IpAddr>().unwrap()]);

    let result = resolver.ip_lookup("branch.example").await;
    assert!(
        result.as_ref().is_err() || result.as_ref().is_ok_and(Vec::is_empty),
        "the first of two CNAMEs won by answer order and the other alias was never classified: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alias_loop_cannot_feed_a_dnsaddr_record() {
    use freedom_ipfs_namesys::DnsTxtResolver;
    let doh = Doh::start(HashMap::from([
        (
            ("_dnsaddr.control.example".into(), "TXT".into()),
            vec![
                owned_cname("_DNSADDR.CONTROL.EXAMPLE.", "_dnsaddr.alias.example."),
                owned_txt("_DNSADDR.ALIAS.EXAMPLE", "dnsaddr=/ip4/8.8.8.8/tcp/4001"),
            ],
        ),
        (
            ("_dnsaddr.loop.example".into(), "TXT".into()),
            vec![
                owned_cname("_dnsaddr.loop.example.", "_dnsaddr.alias.example."),
                owned_cname("_dnsaddr.alias.example.", "_DNSADDR.LOOP.EXAMPLE."),
                owned_txt("_dnsaddr.loop.example.", "dnsaddr=/ip4/127.0.0.1/tcp/4001"),
            ],
        ),
    ]));
    let resolver = direct_resolver(&doh);
    let control = resolver
        .txt_lookup("_dnsaddr.control.example")
        .await
        .unwrap();
    assert_eq!(control, ["dnsaddr=/ip4/8.8.8.8/tcp/4001"]);

    let result = resolver.txt_lookup("_dnsaddr.loop.example").await;
    assert!(
        result.as_ref().is_err() || result.as_ref().is_ok_and(Vec::is_empty),
        "a cyclic TXT CNAME graph supplied a /dnsaddr record: {result:?}"
    );
}
