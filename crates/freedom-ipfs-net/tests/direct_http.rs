//! `direct` mode for HTTP (design 0007, "The gate", check 4, and "The
//! resolver contract"): no environment proxy, the policy's own hop loop,
//! onions refused before any socket, the capability asked on every hop,
//! and, with checked addresses, the address policy for every untrusted
//! target while configured ones are exempt by exact host and port.

use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use freedom_ipfs_net::gate::{parse_url, AddressRefusal, ConfiguredTargets, Nat64State, Refusal};
use freedom_ipfs_net::resolve::{LookupError, LookupFuture, NameLookup};
use freedom_ipfs_net::{
    Addresses, ClientConfig, HttpClient, HttpError, Nat64Handle, Network, NetworkPolicy, Refused,
};
use url::Url;

/// What a server saw of a request: its path and headers.
#[derive(Clone, Debug)]
struct Seen {
    path: String,
    headers: Vec<(String, String)>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// How a server answers a path: a status, with a `Location` for a redirect.
type Handler = Arc<dyn Fn(&str) -> (u16, Option<Vec<u8>>) + Send + Sync>;

/// A server on its own threads, outside every runtime, that counts
/// connections and records requests.
struct Server {
    port: u16,
    connections: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    fn start(handler: impl Fn(&str) -> (u16, Option<Vec<u8>>) + Send + Sync + 'static) -> Self {
        Self::start_slow(Duration::ZERO, handler)
    }

    fn start_slow(
        delay: Duration,
        handler: impl Fn(&str) -> (u16, Option<Vec<u8>>) + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handler: Handler = Arc::new(handler);
        let (counted, recorded) = (connections.clone(), seen.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                counted.fetch_add(1, Ordering::SeqCst);
                let (recorded, handler) = (recorded.clone(), handler.clone());
                std::thread::spawn(move || answer(stream, delay, &handler, &recorded));
            }
        });
        Self {
            port,
            connections,
            seen,
        }
    }

    /// A server that answers every path with 200.
    fn ok() -> Self {
        Self::start(|_| (200, None))
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port)
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn answer(stream: TcpStream, delay: Duration, handler: &Handler, recorded: &Mutex<Vec<Seen>>) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("").to_string();
    let mut headers = Vec::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).is_err() || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }
    recorded.lock().unwrap().push(Seen {
        path: path.clone(),
        headers,
    });
    std::thread::sleep(delay);
    let (status, location) = handler(&path);
    let mut response = format!("HTTP/1.1 {status} X\r\n").into_bytes();
    if let Some(location) = location {
        response.extend_from_slice(b"Location: ");
        response.extend_from_slice(&location);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"Content-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = reader.get_mut().write_all(&response);
}

/// A redirect to `location`.
fn redirect(location: impl Into<String>) -> (u16, Option<Vec<u8>>) {
    (302, Some(location.into().into_bytes()))
}

const ROUTING: ClientConfig = ClientConfig::new(Duration::from_secs(10), 10);
const PROVIDER: ClientConfig = ClientConfig::new(Duration::from_secs(20), 0);

fn configured(urls: &[String]) -> ConfiguredTargets {
    let mut configured = ConfiguredTargets::new();
    for url in urls {
        configured.add_url(&parse_url(url).unwrap()).unwrap();
    }
    configured
}

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

fn unchecked(urls: &[String]) -> Network {
    Network::new(NetworkPolicy::direct(configured(urls), doh(), Addresses::Unchecked).unwrap())
        .unwrap()
}

/// A lookup that answers from a table: deterministic names for the gate's
/// resolver.
#[derive(Debug)]
struct Table(Vec<(&'static str, Vec<IpAddr>)>);

impl NameLookup for Table {
    fn lookup(&self, name: &str) -> LookupFuture {
        let answer = self
            .0
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, ips)| ips.clone())
            .ok_or_else(|| LookupError("NXDOMAIN".into()));
        Box::pin(async move { answer })
    }
}

fn loopback() -> Vec<IpAddr> {
    vec!["127.0.0.1".parse().unwrap()]
}

fn checked_with(urls: &[String], handle: Nat64Handle) -> Network {
    let policy =
        NetworkPolicy::direct(configured(urls), doh(), Addresses::Checked(handle)).unwrap();
    let table = Table(vec![
        ("router.test", loopback()),
        ("private.test", loopback()),
        ("other.test", loopback()),
        ("empty.test", Vec::new()),
    ]);
    Network::with_lookup(policy, Some(Arc::new(table)))
}

fn checked(urls: &[String]) -> Network {
    checked_with(urls, Nat64Handle::fixed(Nat64State::NONE))
}

async fn get(client: &HttpClient, url: &str) -> Result<u16, HttpError> {
    client
        .get(url)
        .send()
        .await
        .map(|response| response.status().as_u16())
}

fn is_refused(result: &Result<u16, HttpError>, refused: impl Fn(&Refused) -> bool) -> bool {
    matches!(result, Err(HttpError::Refused(r)) if refused(r))
}

#[tokio::test(flavor = "multi_thread")]
async fn unchecked_reaches_a_private_target_but_never_an_onion_or_another_scheme() {
    let target = Server::ok();
    let to_target = target.url("/next");
    let router = Server::start(move |path| match path {
        "/private" => redirect(to_target.clone()),
        "/onion" => redirect("http://abcdefgh.onion/"),
        "/ftp" => redirect("ftp://127.0.0.1/"),
        "/file" => redirect("file:///etc/hosts"),
        _ => (404, None),
    });
    let client = unchecked(&[]).http_client(ROUTING);
    // The address table doesn't apply yet: loopback is reached.
    assert_eq!(get(&client, &router.url("/private")).await.unwrap(), 200);
    assert_eq!(target.seen().len(), 1);
    // An onion and another scheme are refused before anything is sent.
    assert!(is_refused(
        &get(&client, &router.url("/onion")).await,
        |r| matches!(r, Refused::Onion)
    ));
    for path in ["/ftp", "/file"] {
        assert!(is_refused(
            &get(&client, &router.url(path)).await,
            |r| matches!(r, Refused::Target(Refusal::Scheme))
        ));
    }
    assert!(is_refused(
        &get(&client, "http://abcdefgh.onion/").await,
        |r| matches!(r, Refused::Onion)
    ));
    assert_eq!(target.seen().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn checked_exempts_configured_targets_by_host_and_port_and_refuses_the_rest() {
    let other = Server::ok();
    let to_other_literal = other.url("/");
    let to_other_name = format!("http://router.test:{}/", other.port);
    let to_private_name = format!("http://private.test:{}/", other.port);
    let router = Server::start(move |path| match path {
        "/literal" => redirect(to_other_literal.clone()),
        "/same-name-other-port" => redirect(to_other_name.clone()),
        "/private-name" => redirect(to_private_name.clone()),
        _ => (200, None),
    });
    let router_literal = router.url("/");
    let router_name = format!("http://router.test:{}/", router.port);
    let client = checked(&[router_literal, router_name.clone()]).http_client(ROUTING);

    // The configured router is reached, by literal and by name, though
    // both are loopback.
    assert_eq!(get(&client, &router.url("/")).await.unwrap(), 200);
    assert_eq!(get(&client, &router_name).await.unwrap(), 200);
    // An untrusted literal is refused in the loop, before any socket.
    assert!(is_refused(
        &get(&client, &router.url("/literal")).await,
        |r| matches!(r, Refused::Address(AddressRefusal::Block { .. }))
    ));
    // The same name on another port is untrusted: its resolver refuses
    // loopback, so nothing connects.
    assert!(get(
        &client,
        &format!("http://router.test:{}/same-name-other-port", router.port)
    )
    .await
    .is_err());
    // A name that resolves to a private address.
    assert!(get(
        &client,
        &format!("http://router.test:{}/private-name", router.port)
    )
    .await
    .is_err());
    assert!(
        get(&client, &format!("http://private.test:{}/", other.port))
            .await
            .is_err()
    );
    assert_eq!(other.connections(), 0);

    // Once configured too, the other server is reached.
    let both = checked(&[router.url("/"), other.url("/")]).http_client(ROUTING);
    assert_eq!(get(&both, &router.url("/literal")).await.unwrap(), 200);
    assert_eq!(other.seen().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_literal_is_refused_before_any_socket_in_checked_mode() {
    let client = checked(&[]).http_client(PROVIDER);
    for url in [
        "http://10.0.0.1:9/",
        "http://169.254.169.254/latest",
        "http://[::ffff:a9fe:a9fe]/",
        "http://0x7f.1:9/",
        "http://[64:ff9b::7f00:1]/",
    ] {
        assert!(
            is_refused(&get(&client, url).await, |r| matches!(
                r,
                Refused::Address(_)
            )),
            "{url}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_answer_is_an_error_and_nothing_connects() {
    let server = Server::ok();
    let client = checked(&[]).http_client(ROUTING);
    assert!(get(&client, &format!("http://empty.test:{}/", server.port))
        .await
        .is_err());
    assert!(
        get(&client, &format!("http://unknown.test:{}/", server.port))
            .await
            .is_err()
    );
    assert_eq!(server.connections(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn redirect_hops_carry_no_credentials_to_another_authority_and_never_a_referer() {
    // The router's address, for the other server's redirect back to it.
    let back: Arc<Mutex<String>> = Arc::default();
    let back_in_other = back.clone();
    let other = Server::start(move |path| match path {
        "/back" => redirect(format!("{}/landed", back_in_other.lock().unwrap())),
        // Within the other authority, after leaving the first.
        "/hop-a" => redirect("/hop-b"),
        _ => (200, None),
    });
    let (other_port, other_url, other_back, other_hop) = (
        other.port,
        other.url("/elsewhere"),
        other.url("/back"),
        other.url("/hop-a"),
    );
    let router = Server::start(move |path| match path {
        "/" => redirect("/same"),
        "/same" => redirect(other_url.clone()),
        "/host" => redirect(format!("http://localhost:{other_port}/host-changed")),
        "/bounce" => redirect(other_back.clone()),
        "/away" => redirect(other_hop.clone()),
        _ => (200, None),
    });
    *back.lock().unwrap() = format!("http://127.0.0.1:{}", router.port);
    let client = unchecked(&[]).http_client(ROUTING);
    let send = |path: String| {
        client
            .get(path)
            .header("authorization", "Basic c2VjcmV0")
            .header("cookie", "session=1")
            .header("proxy-authorization", "Basic cHJveHk=")
            .header("accept", "application/json")
            .send()
    };
    for path in ["/", "/host", "/bounce", "/away"] {
        assert_eq!(
            send(router.url(path)).await.unwrap().status(),
            200,
            "{path}"
        );
    }
    let at_router = router.seen();
    let credentials = |seen: &Seen| {
        ["authorization", "cookie", "proxy-authorization"]
            .iter()
            .filter(|name| seen.header(name).is_some())
            .count()
    };
    // Same authority: the credentials stay.
    let same = at_router.iter().find(|seen| seen.path == "/same").unwrap();
    assert_eq!(credentials(same), 3);
    // Another port, and another host: dropped; the operation's other
    // headers stay.
    for seen in other.seen() {
        assert_eq!(credentials(&seen), 0, "{}", seen.path);
        assert_eq!(seen.header("accept"), Some("application/json"));
    }
    // Including a hop within the other authority, once they were dropped.
    assert!(other.seen().iter().any(|seen| seen.path == "/hop-b"));
    assert_eq!(other.seen().len(), 5);
    // Back at the first authority after leaving it: still dropped.
    let landed = at_router
        .iter()
        .find(|seen| seen.path == "/landed")
        .unwrap();
    assert_eq!(credentials(landed), 0);
    // No hop sends `Referer`.
    for seen in at_router.iter().chain(other.seen().iter()) {
        assert_eq!(seen.header("referer"), None, "{}", seen.path);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_budget_per_operation() {
    let hop = |path: &str| -> usize { path.trim_start_matches("/hop/").parse().unwrap_or(0) };
    // Exactly 10 redirects, then 200; and an endless chain.
    let ten = Server::start(move |path| match hop(path) {
        n if n < 10 => redirect(format!("/hop/{}", n + 1)),
        _ => (200, None),
    });
    let endless = Server::start(move |path| redirect(format!("/hop/{}", hop(path) + 1)));
    for network in [unchecked(&[]), checked(&[ten.url("/"), endless.url("/")])] {
        let client = network.http_client(ROUTING);
        let before = ten.seen().len();
        assert_eq!(get(&client, &ten.url("/hop/0")).await.unwrap(), 200);
        assert_eq!(ten.seen().len() - before, 11);
        // The 11th redirect ends an endless chain.
        let before = endless.seen().len();
        assert!(matches!(
            get(&client, &endless.url("/hop/0")).await,
            Err(HttpError::TooManyRedirects)
        ));
        assert_eq!(endless.seen().len() - before, 11);
        // A budget of 0 (the provider client): the redirect is the response.
        let provider = network.http_client(PROVIDER);
        let before = endless.seen().len();
        assert_eq!(get(&provider, &endless.url("/hop/0")).await.unwrap(), 302);
        assert_eq!(endless.seen().len() - before, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_deadline_per_operation() {
    // Each hop takes 400 ms, the operation's deadline is 1 s: the chain
    // ends with reqwest's timeout before its fourth hop answers.
    let slow = Server::start_slow(Duration::from_millis(400), |path| {
        let n: usize = path.trim_start_matches("/hop/").parse().unwrap_or(0);
        redirect(format!("/hop/{}", n + 1))
    });
    let client = unchecked(&[]).http_client(ClientConfig::new(Duration::from_secs(1), 10));
    let result = client.get(slow.url("/hop/0")).send().await;
    assert!(
        matches!(&result, Err(HttpError::Http(err)) if err.is_timeout()),
        "{result:?}"
    );
    assert!(slow.seen().len() <= 3, "{} hops", slow.seen().len());
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_mid_chain_stops_the_next_hop() {
    for checked_addresses in [false, true] {
        let next = Server::ok();
        // The network is known once the servers are, to configure them.
        let slot: Arc<Mutex<Option<Network>>> = Arc::default();
        let (in_server, to_next) = (slot.clone(), next.url("/"));
        let first = Server::start(move |_| {
            // The capability is revoked while the first hop is answered.
            if let Some(network) = in_server.lock().unwrap().as_ref() {
                network.revoke();
            }
            redirect(to_next.clone())
        });
        let network = if checked_addresses {
            checked(&[first.url("/"), next.url("/")])
        } else {
            unchecked(&[])
        };
        *slot.lock().unwrap() = Some(network.clone());
        let client = network.http_client(ROUTING);
        let result = get(&client, &first.url("/")).await;
        assert!(matches!(result, Err(HttpError::Revoked(_))), "{result:?}");
        assert_eq!(first.seen().len(), 1);
        assert_eq!(next.connections(), 0);
        // Every later request is refused before it starts.
        assert!(matches!(
            get(&client, &first.url("/")).await,
            Err(HttpError::Revoked(_))
        ));
        assert_eq!(first.seen().len(), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_location_is_read_as_a_configured_endpoint_is() {
    let target = Server::ok();
    let port = target.port;
    let router = Server::start(move |path| match path {
        "/userinfo" => redirect(format!("http://user@127.0.0.1:{port}/")),
        "/percent" => redirect(format!("http://127.0.0.%31:{port}/")),
        "/unicode" => (
            302,
            Some(format!("http://b\u{fc}cher.example:{port}/").into_bytes()),
        ),
        "/backslash" => redirect(format!("\\\\127.0.0.1:{port}/")),
        _ => (404, None),
    });
    let client = unchecked(&[]).http_client(ROUTING);
    for path in ["/userinfo", "/percent", "/unicode", "/backslash"] {
        assert!(
            matches!(
                get(&client, &router.url(path)).await,
                Err(HttpError::Refused(_))
            ),
            "{path}"
        );
    }
    assert_eq!(target.connections(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ipv6_literal_waits_for_nat64_discovery() {
    let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
    let client = checked_with(&[], handle).http_client(PROVIDER);
    let request = tokio::spawn(async move { get(&client, "http://[2a00:1::1]:9/").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!request.is_finished(), "the hop waits for discovery");
    publisher.publish(Nat64State::Unknown(Vec::new()));
    let result = request.await.unwrap();
    assert!(
        is_refused(&result, |r| matches!(
            r,
            Refused::Address(AddressRefusal::Nat64Unknown)
        )),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_systems_resolver_reaches_a_configured_name() {
    // The real resolver (hickory, over the system's configuration):
    // `localhost`, configured, reaches the loopback server; on another,
    // unconfigured port it is refused before any socket.
    let server = Server::ok();
    let other = Server::ok();
    let configured_url = format!("http://localhost:{}/", server.port);
    let network = Network::new(
        NetworkPolicy::direct(
            configured(std::slice::from_ref(&configured_url)),
            doh(),
            Addresses::Checked(Nat64Handle::fixed(Nat64State::NONE)),
        )
        .unwrap(),
    )
    .unwrap();
    let client = network.http_client(ROUTING);
    assert_eq!(get(&client, &configured_url).await.unwrap(), 200);
    assert!(get(&client, &format!("http://localhost:{}/", other.port))
        .await
        .is_err());
    assert_eq!(other.connections(), 0);
}
