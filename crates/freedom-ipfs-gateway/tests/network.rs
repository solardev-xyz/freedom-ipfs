//! A node's network (design 0007, "Network policy", upstream item 3): its
//! HTTP clients come from one constructor, every request asks the network's
//! capability, and an instance's teardown revokes it before it cancels
//! anything.

use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method};
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{
    GatewayCoreRequest, InstanceConfig, Network, NetworkCapability, OnlineNode, RoutingMode,
};
use freedom_ipfs_namesys::{
    CloudflareDohResolver, DelegatedIpnsResolver, DnsTxtResolver, IpnsResolver, NamesysError,
};
use freedom_ipfs_net::{ClientConfig, HttpClient, HttpError};
use freedom_ipfs_retrieval::HttpRetriever;
use freedom_ipfs_routing::{DelegatedRoutingClient, Provider, ProviderRoutingClient, RoutingError};
use freedom_ipfs_store::SqliteBlockStore;
use reqwest::Url;

/// A request a server saw: its path, and its `Referer`.
type Seen = (String, Option<String>);

const PEER: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";

/// A server on its own threads, outside every runtime. `/hop/<n>` answers
/// a 302 to `/hop/<n + 1>` until `n` reaches its limit, then 200; anything
/// else is 404. It counts connections and records each request's path and
/// `Referer`.
struct Server {
    port: u16,
    connections: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    fn start(limit: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (counted, recorded) = (connections.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                counted.fetch_add(1, Ordering::SeqCst);
                let recorded = recorded.clone();
                std::thread::spawn(move || answer(stream, limit, &recorded));
            }
        });
        Self {
            port,
            connections,
            requests,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<Seen> {
        self.requests.lock().unwrap().clone()
    }
}

fn answer(stream: TcpStream, limit: usize, recorded: &Mutex<Vec<Seen>>) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
    let mut referer = None;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("referer") {
                referer = Some(value.trim().to_string());
            }
        }
    }
    recorded.lock().unwrap().push((path.clone(), referer));
    let response = match path.strip_prefix("/hop/").and_then(|n| n.parse::<usize>().ok()) {
        Some(n) if n < limit => format!(
            "HTTP/1.1 302 Found\r\nLocation: /hop/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            n + 1
        ),
        Some(_) => "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone".into(),
        None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
    };
    let _ = reader.get_mut().write_all(response.as_bytes());
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// What a request came to: a status, reqwest's too-many-redirects error,
/// or another error.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Status(u16),
    TooManyRedirects,
    Error,
}

fn outcome<E>(result: Result<reqwest::Response, E>, is_redirect: impl Fn(&E) -> bool) -> Outcome {
    match result {
        Ok(response) => Outcome::Status(response.status().as_u16()),
        Err(err) if is_redirect(&err) => Outcome::TooManyRedirects,
        Err(_) => Outcome::Error,
    }
}

/// The client each crate built before the policy, verbatim:
/// `timeout_http_client` in namesys and routing, and in retrieval with
/// `Policy::none()`.
fn before(timeout: Duration, provider: bool) -> reqwest::Client {
    let builder = reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout);
    let builder = if provider {
        builder.redirect(reqwest::redirect::Policy::none())
    } else {
        builder
    };
    builder.build().unwrap()
}

#[test]
fn legacy_clients_keep_each_crates_timeout_and_redirects() {
    let crates = [
        (
            "namesys",
            freedom_ipfs_namesys::HTTP_CLIENT_CONFIG,
            Duration::from_secs(10),
            false,
        ),
        (
            "routing",
            freedom_ipfs_routing::HTTP_CLIENT_CONFIG,
            Duration::from_secs(10),
            false,
        ),
        (
            "retrieval",
            freedom_ipfs_retrieval::HTTP_PROVIDER_CLIENT_CONFIG,
            Duration::from_secs(20),
            true,
        ),
    ];
    let runtime = runtime();
    for (name, config, timeout, provider) in crates {
        assert_eq!(config.timeout, timeout, "{name}");
        assert_eq!(
            config.max_redirects,
            if provider {
                0
            } else {
                ClientConfig::REQWEST_DEFAULT_REDIRECTS
            },
            "{name}"
        );
        // An endless chain, and one of exactly 10 redirects: the same
        // outcome, the same requests and the same `Referer`s as before.
        for limit in [usize::MAX, 10] {
            let old = Server::start(limit);
            let new = Server::start(limit);
            let (was, is) = runtime.block_on(async {
                let was = outcome(
                    before(timeout, provider)
                        .get(old.url("/hop/0"))
                        .send()
                        .await,
                    reqwest::Error::is_redirect,
                );
                let is = outcome(
                    Network::legacy()
                        .http_client(config)
                        .get(new.url("/hop/0"))
                        .send()
                        .await,
                    |err: &HttpError| matches!(err, HttpError::Http(err) if err.is_redirect()),
                );
                (was, is)
            });
            assert_eq!(is, was, "{name}, limit {limit}");
            let strip = |requests: Vec<Seen>, port: u16| {
                requests
                    .into_iter()
                    .map(|(path, referer)| {
                        (path, referer.map(|r| r.replace(&port.to_string(), "PORT")))
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                strip(new.requests(), new.port),
                strip(old.requests(), old.port),
                "{name}, limit {limit}"
            );
            let expected = match (provider, limit) {
                (true, _) => (Outcome::Status(302), 1),
                (false, 10) => (Outcome::Status(200), 11),
                (false, _) => (Outcome::TooManyRedirects, 11),
            };
            assert_eq!(
                (is, new.requests().len()),
                expected,
                "{name}, limit {limit}"
            );
        }
    }
}

fn revoked(config: ClientConfig) -> HttpClient {
    let network = Network::legacy();
    let client = network.http_client(config);
    network.revoke();
    client
}

fn is_revoked_namesys(result: Result<impl std::fmt::Debug, NamesysError>) -> bool {
    matches!(result, Err(NamesysError::Network(HttpError::Revoked(_))))
}

/// Each of the five request sites sends through its network's client: a
/// live one reaches the server, and a revoked one sends nothing.
#[test]
fn every_request_site_asks_the_capability() {
    let runtime = runtime();
    let namesys = freedom_ipfs_namesys::HTTP_CLIENT_CONFIG;
    let routing = freedom_ipfs_routing::HTTP_CLIENT_CONFIG;
    let provider = freedom_ipfs_retrieval::HTTP_PROVIDER_CLIENT_CONFIG;
    let cid = cid_from_data(CODEC_RAW, b"a block");

    // DoH: TXT (DNSLink, `/dnsaddr`), then A and AAAA.
    let server = Server::start(0);
    let doh = CloudflareDohResolver::with_client(server.url("/dns-query"), revoked(namesys));
    assert!(is_revoked_namesys(
        runtime.block_on(doh.txt_lookup("example.com"))
    ));
    assert!(is_revoked_namesys(
        runtime.block_on(doh.ip_lookup("example.com"))
    ));
    assert_eq!(server.connections(), 0, "DoH");
    let doh = CloudflareDohResolver::with_client(
        server.url("/dns-query"),
        Network::legacy().http_client(namesys),
    );
    let _ = runtime.block_on(doh.txt_lookup("example.com"));
    let _ = runtime.block_on(doh.ip_lookup("example.com"));
    assert!(server.requests().len() >= 2, "DoH, live");

    // Delegated IPNS.
    let server = Server::start(0);
    let ipns = DelegatedIpnsResolver::with_endpoints_and_client([server.url("")], revoked(namesys));
    assert!(is_revoked_namesys(
        runtime.block_on(ipns.resolve_ipns(PEER))
    ));
    assert_eq!(server.connections(), 0, "IPNS");
    let ipns = DelegatedIpnsResolver::with_endpoints_and_client(
        [server.url("")],
        Network::legacy().http_client(namesys),
    );
    let _ = runtime.block_on(ipns.resolve_ipns(PEER));
    assert_eq!(server.requests().len(), 1, "IPNS, live");

    // Delegated routing.
    let server = Server::start(0);
    let router =
        DelegatedRoutingClient::with_endpoints_and_client([server.url("")], revoked(routing));
    assert!(matches!(
        runtime.block_on(router.providers(&cid)),
        Err(RoutingError::Network(HttpError::Revoked(_)))
    ));
    assert_eq!(server.connections(), 0, "routing");
    let router = DelegatedRoutingClient::with_endpoints_and_client(
        [server.url("")],
        Network::legacy().http_client(routing),
    );
    let _ = runtime.block_on(router.providers(&cid));
    assert_eq!(server.requests().len(), 1, "routing, live");

    // HTTP providers.
    let server = Server::start(0);
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let source = Provider {
        id: Some(PEER.into()),
        addrs: Vec::new(),
        http_urls: vec![Url::parse(&server.url("/")).unwrap()],
    };
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store.clone())
        .with_http_client(revoked(provider));
    let result =
        runtime.block_on(retriever.fetch_from_providers(&cid, std::slice::from_ref(&source)));
    // The retriever falls back to Bitswap, which has no provider either.
    assert!(result.is_err());
    assert_eq!(server.connections(), 0, "HTTP provider");
    // A store of its own: the failed attempt marked the provider bad in the
    // first one.
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(Network::legacy().http_client(provider));
    let _ = runtime.block_on(retriever.fetch_from_providers(&cid, &[source]));
    assert!(!server.requests().is_empty(), "HTTP provider, live");
}

/// Gateway requests the node can only answer from the network: a block
/// nobody has (a provider lookup) and an IPNS name (a record lookup), both
/// through the delegated router.
fn request_through_the_router(runtime: &tokio::runtime::Runtime, node: &OnlineNode) {
    let cid = cid_from_data(CODEC_RAW, b"nobody has this block");
    let core = node.gateway_core();
    runtime.block_on(async move {
        for request in [
            GatewayCoreRequest::ipfs(cid.to_string(), Method::GET, HeaderMap::new()),
            GatewayCoreRequest::ipns(PEER, Method::GET, HeaderMap::new()),
        ] {
            let response = tokio::time::timeout(Duration::from_secs(30), core.handle(request))
                .await
                .expect("the gateway answers");
            let _ = to_bytes(response.into_body(), usize::MAX).await;
        }
    });
}

#[test]
fn a_nodes_clients_belong_to_its_network() {
    let runtime = runtime();
    let node = |router: &Server, network: Network| {
        OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
            .routing_mode(RoutingMode::Delegated)
            .delegated_routers([router.url("/routing/v1")])
            .network(network)
            .build()
    };

    let router = Server::start(0);
    let network = Network::legacy();
    let live = node(&router, network.clone());
    request_through_the_router(&runtime, &live);
    let paths: Vec<String> = router
        .requests()
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    assert!(
        paths
            .iter()
            .any(|path| path.starts_with("/routing/v1/providers/"))
            && paths
                .iter()
                .any(|path| path.starts_with("/routing/v1/ipns/")),
        "a live network's node asks its router for providers and records: {paths:?}"
    );
    assert!(!live.network().capability().is_revoked());

    let router = Server::start(0);
    let network = Network::legacy();
    let revoked = node(&router, network.clone());
    network.revoke();
    assert!(revoked.network().capability().is_revoked());
    request_through_the_router(&runtime, &revoked);
    assert_eq!(
        router.connections(),
        0,
        "a revoked network's node sends nothing"
    );
}

/// Records whether the capability was revoked at the moment the instance's
/// root token woke it: `CancellationToken::cancel` wakes its waiters
/// synchronously, so this sees the order of revoke and cancel exactly.
struct RevokedAtCancel {
    capability: NetworkCapability,
    seen: Mutex<Option<bool>>,
}

impl Wake for RevokedAtCancel {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut seen = self.seen.lock().unwrap();
        if seen.is_none() {
            *seen = Some(self.capability.is_revoked());
        }
    }
}

fn offline_instance() -> freedom_ipfs_gateway::Instance {
    OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
        .routing_mode(RoutingMode::Offline)
        .network(Network::legacy())
        .build_instance(InstanceConfig {
            thread_name: "fipfs-network".into(),
            worker_threads: 1,
            max_blocking_threads: 4,
        })
        .unwrap()
}

#[test]
fn a_teardown_revokes_the_capability_before_it_cancels() {
    let instance = offline_instance();
    let capability = instance.network_capability();
    assert!(!capability.is_revoked());
    let observer = Arc::new(RevokedAtCancel {
        capability: capability.clone(),
        seen: Mutex::new(None),
    });
    let mut cancelled = Box::pin(
        instance
            .context()
            .cancellation_token()
            .clone()
            .cancelled_owned(),
    );
    let waker = Waker::from(observer.clone());
    assert!(cancelled
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());

    let quiescence = instance.shutdown(Duration::from_secs(5));
    assert!(quiescence.reached, "{quiescence:?}");
    assert_eq!(
        *observer.seen.lock().unwrap(),
        Some(true),
        "the capability was revoked when the root token was cancelled"
    );
    assert!(capability.is_revoked());
    assert_eq!(
        cancelled.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(())
    );
}

#[test]
fn a_host_can_revoke_first_and_an_abandoned_instance_is_revoked_too() {
    // Revoked by the host before the teardown (the service's step 2): the
    // instance's clients send nothing from then on, and the teardown still
    // completes.
    let instance = offline_instance();
    let capability = instance.network_capability();
    capability.revoke();
    assert!(instance.node().network().capability().is_revoked());
    // With no attempt in flight, the revocation is its own quiescence.
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(1), capability.quiesced()).await
        })
        .expect("quiescent at once");
    assert!(instance.shutdown(Duration::from_secs(5)).reached);

    // Dropped without a teardown: revoked, then cancelled, as a teardown.
    let instance = offline_instance();
    let capability = instance.network_capability();
    let token = instance.context().cancellation_token().clone();
    drop(instance);
    assert!(capability.is_revoked());
    assert!(token.is_cancelled());
}
