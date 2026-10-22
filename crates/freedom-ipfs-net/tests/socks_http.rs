//! `socks5` over HTTP (design 0007, "Modes", `socks5`, "The onion route",
//! and its tests: "A SOCKS fixture with `RESOLVE`", "The onion route",
//! "Redirect headers"), through a network's own HTTP clients against the
//! test proxy, which records every connection and request and forwards a
//! `CONNECT` only to the local server its destination is mapped to. Each
//! refusal has a control that goes through.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{
    Answer, ClientHelloCatcher, Destination, HttpServer, Reply, Request, SocksFixture,
};
use freedom_ipfs_net::{Addresses, ClientConfig, HttpError, Network, NetworkPolicy, Refused};
use url::Url;

const ONION3: &str = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";
const CONFIG: ClientConfig = ClientConfig::new(Duration::from_secs(5), 10);

fn doh() -> Url {
    Url::parse("https://dns.example/dns-query").unwrap()
}

fn onion() -> String {
    format!("{ONION3}.onion")
}

/// A `socks5` network through `fixture`, configured with `configured`.
fn socks5(fixture: &SocksFixture, configured: ConfiguredTargets) -> Network {
    Network::new(NetworkPolicy::socks5(fixture.addr(), configured, doh()).unwrap()).unwrap()
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

fn ip(addr: &str) -> Destination {
    Destination::Ip(addr.parse().unwrap())
}

fn name(host: &str, port: u16) -> Destination {
    Destination::Name(host.to_string(), port)
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

/// Every `CONNECT` carries an IP literal, an IPv6 one as an IPv6 address
/// (hyper-util 0.1.20 sent `"[2001:…]"` as a domain), for names resolved
/// through the proxy and for literal URLs alike; the request's `Host` keeps
/// the URL's host.
#[tokio::test(flavor = "multi_thread")]
async fn every_connect_carries_an_ip_literal_ipv6_included() {
    let fixture = SocksFixture::start().await;
    let server = HttpServer::start(|_, _| Reply::ok("ok")).await;
    fixture.answer("v4.example", Answer::Ip("8.8.8.8".parse().unwrap()));
    fixture.answer(
        "v6.example",
        Answer::Ip("2001:4860:4860::8888".parse().unwrap()),
    );
    for addr in [
        "8.8.8.8:80",
        "[2001:4860:4860::8888]:80",
        "8.8.4.4:80",
        "[2001:4860:4860::8844]:80",
    ] {
        fixture.forward(ip(addr), server.addr());
    }
    let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
    for url in [
        "http://v4.example/",
        "http://v6.example/",
        "http://8.8.4.4/",
        "http://[2001:4860:4860::8844]/",
    ] {
        let response = client.get(url).send().await.unwrap();
        assert_eq!(response.text().await.unwrap(), "ok", "{url}");
    }
    let requests = fixture.requests();
    assert_eq!(resolves(&requests), ["v4.example", "v6.example"]);
    assert_eq!(
        connects(&requests),
        [
            ip("8.8.8.8:80"),
            ip("[2001:4860:4860::8888]:80"),
            ip("8.8.4.4:80"),
            ip("[2001:4860:4860::8844]:80"),
        ]
    );
    let hosts = server
        .requests()
        .into_iter()
        .map(|(_, host)| host.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        hosts,
        [
            "v4.example",
            "v6.example",
            "8.8.4.4",
            "[2001:4860:4860::8844]"
        ]
    );
}

/// TLS's server name is the URL's name, not the address it was resolved to.
#[tokio::test(flavor = "multi_thread")]
async fn tls_names_the_host_through_the_proxy() {
    let fixture = SocksFixture::start().await;
    let hello = ClientHelloCatcher::start().await;
    fixture.answer("tls.example", Answer::Ip("8.8.8.8".parse().unwrap()));
    fixture.forward(ip("8.8.8.8:443"), hello.addr());
    let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
    let _ = client.get("https://tls.example/").send().await;
    assert!(wait_until(Duration::from_secs(2), || !hello.names().is_empty()).await);
    assert_eq!(hello.names(), [Some("tls.example".to_string())]);
    assert_eq!(connects(&fixture.requests()), [ip("8.8.8.8:443")]);
}

/// A name that looks acceptable but resolves to a loopback or private
/// address (DNS64's embedding of loopback included) is refused, and no
/// `CONNECT` is sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_private_answer_is_refused_and_nothing_connects() {
    for answer in [
        "127.0.0.1",
        "10.0.0.1",
        "::ffff:127.0.0.1",
        "64:ff9b::7f00:1",
    ] {
        let fixture = SocksFixture::start().await;
        let server = HttpServer::start(|_, _| Reply::ok("ok")).await;
        fixture.answer("peer.example", Answer::Ip(answer.parse().unwrap()));
        fixture.forward(ip("127.0.0.1:80"), server.addr());
        let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
        let result = client.get("http://peer.example/").send().await;
        assert!(result.is_err(), "{answer}");
        let requests = fixture.requests();
        assert_eq!(resolves(&requests), ["peer.example"], "{answer}");
        assert_eq!(connects(&requests), [], "{answer}");
    }
}

/// reqwest's trap: a resolver that succeeds with no address makes its
/// SOCKS connector hand the proxy the name. An empty answer (domain-typed,
/// `0.0.0.0`, `::`), a failure, an unknown name and a proxy without
/// `RESOLVE` all fail the request, and no `CONNECT` is sent at all.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_answer_never_becomes_a_connect_by_name() {
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
            let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
            let result = client.get("http://peer.example/").send().await;
            assert!(result.is_err(), "{answer:?} {refuse_resolve}");
            assert_eq!(
                connects(&fixture.requests()),
                [],
                "{answer:?} {refuse_resolve}"
            );
        }
    }
}

/// An untrusted IP literal is checked by the hop loop itself: refused
/// before anything reaches the proxy. The proxy's own endpoint is no
/// exemption.
#[tokio::test(flavor = "multi_thread")]
async fn a_literal_is_checked_before_the_proxy_sees_it() {
    let fixture = SocksFixture::start().await;
    let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
    for url in [
        "http://10.0.0.1/".to_string(),
        "http://[::1]/".to_string(),
        format!("http://127.0.0.1:{}/", fixture.addr().port()),
    ] {
        let result = client.get(&url).send().await;
        assert!(
            matches!(result, Err(HttpError::Refused(Refused::Address(_)))),
            "{url}: {result:?}"
        );
    }
    assert_eq!(fixture.connections(), 0);
}

/// An onion URL takes the onion route: one `CONNECT` with the onion's name,
/// no `RESOLVE`, and the request's `Host` is the onion's.
#[tokio::test(flavor = "multi_thread")]
async fn an_onion_url_is_connected_by_name_without_resolve() {
    let fixture = SocksFixture::start().await;
    let server = HttpServer::start(|_, _| Reply::ok("onion")).await;
    fixture.forward(name(&onion(), 80), server.addr());
    let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);
    let response = client
        .get(format!("http://{}/x", onion()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "onion");
    let requests = fixture.requests();
    assert_eq!(connects(&requests), [name(&onion(), 80)]);
    assert_eq!(resolves(&requests), Vec::<String>::new());
    assert_eq!(server.requests(), [("/x".to_string(), Some(onion()))]);
    // A malformed onion takes no route: nothing reaches the proxy.
    let result = client.get("http://foo.onion/").send().await;
    assert!(
        matches!(result, Err(HttpError::Refused(Refused::OnionRoute))),
        "{result:?}"
    );
    assert_eq!(fixture.connections(), 1);
}

/// A server for redirect chains: on the clearnet name and on the onion,
/// `/to-onion/<n>` and `/to-clear/<n>` redirect to `/…/<n - 1>` on the other
/// route, and `/0` answers.
fn chain_server() -> impl Fn(&str, Option<&str>) -> Reply + Send + Sync + 'static {
    |path, _| {
        let mut parts = path.trim_start_matches('/').split('/');
        let (Some(kind), Some(n)) = (parts.next(), parts.next()) else {
            return Reply::ok("end");
        };
        let n: u32 = n.parse().unwrap();
        if n == 0 {
            return Reply::ok("end");
        }
        match kind {
            "to-onion" => Reply::redirect(&format!("http://{}/to-clear/{}", onion(), n - 1)),
            _ => Reply::redirect(&format!("http://clear.example/to-onion/{}", n - 1)),
        }
    }
}

/// Redirects re-select the route per hop: an onion redirecting to a
/// clearnet name goes through `RESOLVE`, the gate and an IP `CONNECT`; a
/// clearnet URL redirecting to an onion goes to the onion route.
#[tokio::test(flavor = "multi_thread")]
async fn redirects_reselect_the_route_per_hop() {
    let fixture = SocksFixture::start().await;
    let server = HttpServer::start(chain_server()).await;
    fixture.answer("clear.example", Answer::Ip("8.8.8.8".parse().unwrap()));
    fixture.forward(ip("8.8.8.8:80"), server.addr());
    fixture.forward(name(&onion(), 80), server.addr());
    let client = socks5(&fixture, ConfiguredTargets::new()).http_client(CONFIG);

    // Onion, then clearnet.
    let response = client
        .get(format!("http://{}/to-clear/1", onion()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "end");
    let requests = fixture.requests();
    assert_eq!(connects(&requests), [name(&onion(), 80), ip("8.8.8.8:80")]);
    assert_eq!(resolves(&requests), ["clear.example"]);

    // Clearnet, then onion.
    let response = client
        .get("http://clear.example/to-onion/1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "end");
    assert_eq!(
        connects(&fixture.requests())[2..],
        [ip("8.8.8.8:80"), name(&onion(), 80)]
    );
    let hosts = server
        .requests()
        .into_iter()
        .map(|(_, host)| host.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        hosts,
        [
            onion(),
            "clear.example".into(),
            "clear.example".into(),
            onion()
        ]
    );
}

/// Under `direct` (the control) a redirect to an onion is refused before
/// any socket.
#[tokio::test(flavor = "multi_thread")]
async fn under_direct_a_redirect_to_an_onion_is_refused() {
    let server = HttpServer::start(chain_server()).await;
    let direct = Network::new(
        NetworkPolicy::direct(ConfiguredTargets::new(), doh(), Addresses::Unchecked).unwrap(),
    )
    .unwrap();
    let client = direct.http_client(CONFIG);
    let result = client
        .get(format!("http://{}/to-onion/1", server.addr()))
        .send()
        .await;
    assert!(
        matches!(result, Err(HttpError::Refused(Refused::Onion))),
        "{result:?}"
    );
    assert_eq!(server.requests().len(), 1);
}

/// One hop budget covers the whole chain, as it moves between the ordinary
/// and onion routes: past it the request fails, and no further hop is
/// sent; with one more hop of budget (the control) it completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_chain_across_routes_shares_one_budget() {
    for (budget, completes) in [(2, false), (3, true)] {
        let fixture = SocksFixture::start().await;
        let server = HttpServer::start(chain_server()).await;
        fixture.answer("clear.example", Answer::Ip("8.8.8.8".parse().unwrap()));
        fixture.forward(ip("8.8.8.8:80"), server.addr());
        fixture.forward(name(&onion(), 80), server.addr());
        let client = socks5(&fixture, ConfiguredTargets::new())
            .http_client(ClientConfig::new(Duration::from_secs(5), budget));
        let result = client.get("http://clear.example/to-onion/3").send().await;
        if completes {
            assert_eq!(result.unwrap().text().await.unwrap(), "end");
        } else {
            assert!(
                matches!(result, Err(HttpError::TooManyRedirects)),
                "{result:?}"
            );
        }
        assert_eq!(server.requests().len(), budget + 1, "{budget}");
        assert_eq!(connects(&fixture.requests()).len(), budget + 1, "{budget}");
    }
}

/// A configured name and port are exempt from the address policy, and
/// reached through the proxy all the same: a loopback answer is connected
/// to. The same name on another port is untrusted, and refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_configured_name_is_exempt_by_host_and_port_through_the_proxy() {
    let fixture = SocksFixture::start().await;
    let server = HttpServer::start(|_, _| Reply::ok("router")).await;
    let port = server.addr().port();
    fixture.answer("router.example", Answer::Ip("127.0.0.1".parse().unwrap()));
    fixture.forward(Destination::Ip(server.addr()), server.addr());
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse(&format!("http://router.example:{port}/")).unwrap())
        .unwrap();
    let client = socks5(&fixture, configured).http_client(CONFIG);
    let other = client
        .get(format!("http://router.example:{}/", port + 1))
        .send()
        .await;
    assert!(other.is_err());
    assert_eq!(connects(&fixture.requests()), []);
    let response = client
        .get(format!("http://router.example:{port}/"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "router");
    assert_eq!(
        connects(&fixture.requests()),
        [Destination::Ip(server.addr())]
    );
}

/// A proxy that is down fails the request, and nothing falls back to a
/// direct socket: the target, a configured address that passes the gate,
/// is a canary that sees nothing. With the proxy up (the control), the
/// request reaches it through the proxy.
#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_that_is_down_fails_closed() {
    for up in [false, true] {
        let fixture = SocksFixture::start().await;
        let canary = Canary::start();
        let mut configured = ConfiguredTargets::new();
        configured.add_endpoint(canary.addr);
        fixture.forward(Destination::Ip(canary.addr), canary.addr);
        let client = socks5(&fixture, configured).http_client(CONFIG);
        let _running = if up {
            Some(fixture)
        } else {
            fixture.stop();
            None
        };
        let result = client.get(format!("http://{}/", canary.addr)).send().await;
        assert!(result.is_err());
        let reached = wait_until(Duration::from_millis(if up { 2000 } else { 500 }), || {
            canary.accepted() > 0
        })
        .await;
        assert_eq!(reached, up);
    }
}

/// A request waiting for its `RESOLVE` answer is dropped at the revoke:
/// quiescence comes, the request ends revoked, and no `CONNECT` follows.
#[tokio::test(flavor = "multi_thread")]
async fn the_revoke_drops_a_request_held_in_resolve() {
    let fixture = SocksFixture::start().await;
    fixture.answer("held.example", Answer::Hang);
    let network = socks5(&fixture, ConfiguredTargets::new());
    let client = network.http_client(CONFIG);
    let request = tokio::spawn(async move { client.get("http://held.example/").send().await });
    assert!(wait_until(Duration::from_secs(2), || !fixture.requests().is_empty()).await);
    network.revoke();
    tokio::time::timeout(Duration::from_secs(2), network.capability().quiesced())
        .await
        .expect("quiescence once the held request is dropped");
    let result = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .expect("the request ends")
        .unwrap();
    assert!(matches!(result, Err(HttpError::Revoked(_))), "{result:?}");
    assert_eq!(connects(&fixture.requests()), []);
}
