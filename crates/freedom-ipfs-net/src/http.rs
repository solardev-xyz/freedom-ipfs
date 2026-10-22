//! The crates' HTTP clients, built by one constructor (design 0007,
//! upstream item 3.1 and 3.2).
//!
//! Every client the node uses (DoH, delegated IPNS, delegated routing, HTTP
//! providers) is an [`HttpClient`] built by [`Network::http_client`] for its
//! [`ClientConfig`]. Each request asks the network's capability first, and a
//! client whose configuration reqwest refused fails its requests with that
//! error instead of panicking.
//!
//! The capability is asked at every boundary where a connection or a
//! request could start (design 0007: "every socket, HTTP attempt and
//! redirect hop asks it first"):
//! - **each request,** in [`HttpRequest::send`];
//! - **each redirect hop:** under the legacy policy in the client's redirect
//!   policy, so a hop over a pooled connection is refused too; under
//!   `direct` in the policy's own hop loop;
//! - **each new connection,** in a connector layer that runs before
//!   reqwest's resolver: a revoked client starts no DNS lookup and opens no
//!   socket, whatever asked for the connection. The connection is made
//!   under a permit of the capability ([`NetworkCapability::admit`]), so a
//!   revoke while it resolves or connects drops it, and the capability's
//!   quiescence waits until it has been dropped.
//!
//! A request sent over a connection already in the pool opens no new
//! connection: revoking stops new ones, and the teardown closes the open
//! ones.
//!
//! Under `direct` the policy follows redirects itself (design 0007, "The
//! gate", check 4): every client is built with `Policy::none()` and no
//! proxy, and each request runs [the hop loop](HttpRequest::send).
//!
//! Under `socks5` (cargo feature `socks`) the same hop loop runs, and every
//! client goes through the policy's proxy and no other: the environment's
//! proxy variables don't apply.
//! - **Ordinary names** go to clients with `socks5://`, whose resolver is the
//!   gate's over Tor's `RESOLVE`: the answer is checked (the untrusted
//!   client's) and the proxy is handed the checked address. The resolver
//!   never answers an empty success, so no name reaches the proxy that way.
//! - **Onions** go to the onion client, with `socks5h://`, so the proxy
//!   gets the name. It takes only a [`crate::gate::OnionUrl`], and checks
//!   its classification again before it sends.
//!
//! [`Network::http_client`]: crate::policy::Network::http_client

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use reqwest::header::{
    HeaderName, HeaderValue, AUTHORIZATION, COOKIE, HOST, LOCATION, PROXY_AUTHORIZATION, REFERER,
};
use reqwest::redirect::Policy;
use reqwest::StatusCode;
use serde::Serialize;
use tower_layer::Layer;
use tower_service::Service;
use url::Url;

use crate::capability::{NetworkCapability, Revoked};
#[cfg(feature = "socks")]
use crate::gate::{check_address, Nat64State, OnionUrl};
use crate::gate::{classify_url, parse_url, AddressRefusal, Refusal, Target};
use crate::policy::{Addresses, Mode, Network, NetworkError};
use crate::resolve::{check_waiting, GateResolver};

/// How a client times out, and how many redirects it follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// The connect timeout, and the whole request's, its redirects
    /// included.
    pub timeout: Duration,
    /// The redirects a request follows: 0 returns the 3xx itself.
    pub max_redirects: usize,
}

impl ClientConfig {
    /// reqwest's own default: up to 10 redirects, failing on the 11th.
    pub const REQWEST_DEFAULT_REDIRECTS: usize = 10;

    pub const fn new(timeout: Duration, max_redirects: usize) -> Self {
        Self {
            timeout,
            max_redirects,
        }
    }
}

/// The reqwest clients a policy builds for one config: the one constructor.
#[derive(Debug)]
enum Transport {
    /// Exactly what each crate built before the policy: `connect_timeout`
    /// and `timeout` set to the config's, reqwest's default redirect policy
    /// for 10 redirects, `Policy::none()` for 0, and nothing else (so the
    /// system proxy and reqwest's resolver apply).
    Legacy(Result<reqwest::Client, NetworkError>),
    /// `direct`: no proxy, no redirect followed by reqwest, no `Referer`,
    /// no cookie store. The hop loop picks one client per hop.
    Direct {
        /// Every target's under unchecked addresses; an untrusted target's
        /// under checked addresses, whose resolver applies the address
        /// policy.
        untrusted: Result<reqwest::Client, NetworkError>,
        /// A configured target's, under checked addresses: its resolver
        /// takes answers as they come. `None` under unchecked addresses.
        configured: Option<Result<reqwest::Client, NetworkError>>,
    },
    /// `socks5`: `direct`'s clients, through the proxy. The hop loop picks
    /// one client per hop.
    #[cfg(feature = "socks")]
    Socks5 {
        /// An untrusted target's: `socks5://`, its names through the gate's
        /// resolver over `RESOLVE`, which checks every answer.
        untrusted: Result<reqwest::Client, NetworkError>,
        /// A configured target's: the same, taking answers as they come.
        configured: Result<reqwest::Client, NetworkError>,
        /// An onion's: `socks5h://`, so the proxy gets the name.
        onion: Result<OnionClient, NetworkError>,
    },
    /// A mode this version can't build clients for.
    Unavailable(NetworkError),
}

/// The onion route's HTTP client (design 0007, "The onion route"): it takes
/// only an [`OnionUrl`], and checks the URL's classification again before
/// it sends, since reqwest's connector can't see the destination and
/// `socks5h://` skips the resolver. Anything else is refused before it
/// sends.
#[cfg(feature = "socks")]
#[derive(Debug)]
struct OnionClient(reqwest::Client);

#[cfg(feature = "socks")]
impl OnionClient {
    /// The URL's host must still be a well-formed onion, spelled exactly
    /// as classified: the host the proxy receives is the host checked.
    fn get(&self, url: &OnionUrl) -> Result<reqwest::RequestBuilder, HttpError> {
        if url.host().is_none() {
            return Err(Refused::OnionRoute.into());
        }
        Ok(self.0.get(url.as_url().clone()))
    }
}

/// The onion client's resolver, which reqwest never asks under
/// `socks5h://`: if it ever did, it would get an error, never an address.
#[cfg(feature = "socks")]
#[derive(Debug)]
struct NoLookup;

#[cfg(feature = "socks")]
impl reqwest::dns::Resolve for NoLookup {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async {
            Err(Box::new(crate::resolve::ResolveRefused::Onion) as Box<dyn StdError + Send + Sync>)
        })
    }
}

fn build(builder: reqwest::ClientBuilder) -> Result<reqwest::Client, NetworkError> {
    builder
        .build()
        .map_err(|err| NetworkError::ClientBuild(Arc::new(err)))
}

/// What each crate built before the policy, behind the capability: the
/// same timeouts, and the same redirect behaviour (none for 0; for
/// `max_redirects`, reqwest's `limited`, so the same "too many redirects"
/// on the hop past it), as a custom policy that also refuses every hop once
/// the capability is revoked. Every new connection goes through
/// [`CapabilityLayer`] first.
fn legacy_builder(config: ClientConfig, capability: &NetworkCapability) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .connect_timeout(config.timeout)
        .timeout(config.timeout)
        .connector_layer(CapabilityLayer(capability.clone()));
    match config.max_redirects {
        0 => builder.redirect(Policy::none()),
        max => {
            let capability = capability.clone();
            builder.redirect(Policy::custom(move |attempt| {
                if capability.is_revoked() {
                    attempt.error(Revoked)
                } else if attempt.previous().len() > max {
                    // As reqwest's `limited`: the first URL is the request's
                    // own, not a redirect.
                    attempt.error(TooManyRedirects)
                } else {
                    attempt.follow()
                }
            }))
        }
    }
}

/// reqwest's own redirect-limit error, which it doesn't export: the same
/// text.
#[derive(Debug)]
struct TooManyRedirects;

impl fmt::Display for TooManyRedirects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("too many redirects")
    }
}

impl StdError for TooManyRedirects {}

type BoxError = Box<dyn StdError + Send + Sync>;

/// A connector layer that asks the capability before each new connection,
/// before reqwest's resolver or proxy connection: once revoked, the
/// connection fails at once, and nothing is resolved or dialed. A
/// connection in progress holds a permit of the capability, and is dropped
/// when the capability is revoked, its resolution or connect included.
#[derive(Clone, Debug)]
struct CapabilityLayer(NetworkCapability);

impl<S> Layer<S> for CapabilityLayer {
    type Service = CapabilityConnector<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CapabilityConnector {
            inner,
            capability: self.0.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct CapabilityConnector<S> {
    inner: S,
    capability: NetworkCapability,
}

impl<S, R> Service<R> for CapabilityConnector<S>
where
    S: Service<R>,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    /// The connection is made under a permit of the capability: refused at
    /// once if it is revoked, and otherwise dropped (its resolution or
    /// connect aborted) as soon as it is, so the capability's quiescence
    /// waits for it.
    fn call(&mut self, request: R) -> Self::Future {
        let permit = match self.capability.admit() {
            Ok(permit) => permit,
            Err(revoked) => return Box::pin(async move { Err(Box::new(revoked) as BoxError) }),
        };
        let connecting = self.inner.call(request);
        Box::pin(async move {
            match permit.run(connecting).await {
                Ok(connected) => connected.map_err(Into::into),
                Err(revoked) => Err(Box::new(revoked) as BoxError),
            }
        })
    }
}

/// Whether `err`, or an error it wraps, is the capability's refusal: from
/// the redirect policy or the connector layer.
fn is_revoked(err: &reqwest::Error) -> bool {
    let mut source = err.source();
    while let Some(err) = source {
        if err.is::<Revoked>() {
            return true;
        }
        source = err.source();
    }
    false
}

/// A `direct` client: behind the capability's connector layer, as every
/// client is.
fn direct_builder(config: ClientConfig, capability: &NetworkCapability) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(config.timeout)
        .timeout(config.timeout)
        .connector_layer(CapabilityLayer(capability.clone()))
        .no_proxy()
        .redirect(Policy::none())
        .referer(false)
}

fn transport(network: &Network, config: ClientConfig) -> Transport {
    match network.policy().mode() {
        Mode::Legacy => Transport::Legacy(build(legacy_builder(config, network.capability()))),
        Mode::Direct(direct) => match (&direct.addresses, network.lookup()) {
            (Addresses::Unchecked, _) => Transport::Direct {
                untrusted: build(direct_builder(config, network.capability())),
                configured: None,
            },
            (Addresses::Checked(handle), Some(lookup)) => {
                let resolver = |addresses| {
                    Arc::new(GateResolver::new(
                        lookup.clone(),
                        network.capability().clone(),
                        addresses,
                    ))
                };
                Transport::Direct {
                    untrusted: build(
                        direct_builder(config, network.capability())
                            .dns_resolver(resolver(Some(handle.clone()))),
                    ),
                    configured: Some(build(
                        direct_builder(config, network.capability()).dns_resolver(resolver(None)),
                    )),
                }
            }
            (Addresses::Checked(_), None) => Transport::Unavailable(NetworkError::Unsupported(
                "checked addresses without a resolver",
            )),
        },
        #[cfg(feature = "socks")]
        Mode::Socks5(socks5) => socks5_transport(network, socks5, config),
        #[cfg(not(feature = "socks"))]
        Mode::Socks5(_) => Transport::Unavailable(NetworkError::Unsupported("socks5")),
    }
}

/// `socks5`'s clients: `direct`'s, with the policy's proxy as their only
/// one.
#[cfg(feature = "socks")]
fn socks5_transport(
    network: &Network,
    socks5: &crate::policy::Socks5,
    config: ClientConfig,
) -> Transport {
    let Some(client) = network.socks() else {
        return Transport::Unavailable(NetworkError::Unsupported("socks5 without its client"));
    };
    let lookup: Arc<dyn crate::resolve::NameLookup> =
        Arc::new(crate::socks::SocksLookup(client.clone()));
    let resolver = |addresses| {
        Arc::new(GateResolver::new(
            lookup.clone(),
            network.capability().clone(),
            addresses,
        ))
    };
    let through = |scheme: &str| {
        reqwest::Proxy::all(format!("{scheme}://{}", socks5.proxy))
            .map(|proxy| direct_builder(config, network.capability()).proxy(proxy))
            .map_err(|err| NetworkError::ClientBuild(Arc::new(err)))
    };
    // socks5 learns no NAT64 prefix: only the fixed forms apply.
    let untrusted = crate::policy::Nat64Handle::fixed(Nat64State::known([]));
    Transport::Socks5 {
        untrusted: through("socks5")
            .and_then(|builder| build(builder.dns_resolver(resolver(Some(untrusted))))),
        configured: through("socks5")
            .and_then(|builder| build(builder.dns_resolver(resolver(None)))),
        onion: through("socks5h")
            .and_then(|builder| build(builder.dns_resolver(Arc::new(NoLookup))))
            .map(OnionClient),
    }
}

/// An HTTP client of a [`Network`]: reqwest's, behind the network's
/// capability and, under `direct`, its hop loop. Clones share the clients
/// and their connection pools.
///
/// One pointer wide, as the `reqwest::Client` it replaces: the crates embed
/// it in values whose size matters (the mobile node's).
#[derive(Clone, Debug)]
pub struct HttpClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    network: Network,
    config: ClientConfig,
    transport: Transport,
}

impl HttpClient {
    pub(crate) fn new(network: Network, config: ClientConfig) -> Self {
        let transport = transport(&network, config);
        Self {
            inner: Arc::new(ClientInner {
                network,
                config,
                transport,
            }),
        }
    }

    /// A client of a [`Network::legacy`] network of its own: what a crate's
    /// `Default` builds when no network is given.
    ///
    /// **Its capability belongs to no node:** nothing can revoke it, so an
    /// instance's teardown doesn't stop its requests. A node's clients come
    /// from its network ([`Network::http_client`]); the call sites that
    /// still build one of these in a node's paths are listed in
    /// freedom-ipfs-gateway's `tests/network_outside_clients.rs`.
    pub fn legacy(config: ClientConfig) -> Self {
        Network::legacy().http_client(config)
    }

    pub fn config(&self) -> ClientConfig {
        self.inner.config
    }

    /// The capability every request and hop asks.
    pub fn capability(&self) -> &NetworkCapability {
        self.inner.network.capability()
    }

    /// The network the client belongs to: its policy and its capability.
    pub fn network(&self) -> &Network {
        &self.inner.network
    }

    /// Why reqwest refused to build the client, if it did: every request
    /// then fails with [`HttpError::Unavailable`].
    pub fn error(&self) -> Option<&NetworkError> {
        match &self.inner.transport {
            Transport::Legacy(client) => client.as_ref().err(),
            Transport::Direct {
                untrusted,
                configured,
            } => untrusted
                .as_ref()
                .err()
                .or_else(|| configured.as_ref().and_then(|client| client.as_ref().err())),
            #[cfg(feature = "socks")]
            Transport::Socks5 {
                untrusted,
                configured,
                onion,
            } => untrusted
                .as_ref()
                .err()
                .or_else(|| configured.as_ref().err())
                .or_else(|| onion.as_ref().err()),
            Transport::Unavailable(err) => Some(err),
        }
    }

    /// A GET request for `url`, as reqwest's `Client::get` starts one.
    ///
    /// Under `direct`, the URL's text is read as [`parse_url`] reads a
    /// configured endpoint before reqwest sees it: `http` or `https`, and no
    /// userinfo, which reqwest would turn into an `Authorization` header. A
    /// refused URL fails the request with [`HttpError::Refused`], and
    /// nothing is sent.
    pub fn get(&self, url: impl AsRef<str>) -> HttpRequest {
        let raw = url.as_ref();
        let builder = match &self.inner.transport {
            Transport::Legacy(client) => client
                .as_ref()
                .map(|client| client.get(raw))
                .map_err(|err| HttpError::Unavailable(err.clone())),
            Transport::Direct {
                untrusted: client, ..
            } => match parse_url(raw) {
                Err(refusal) => Err(Refused::Target(refusal).into()),
                Ok(url) => client
                    .as_ref()
                    .map(|client| client.get(url))
                    .map_err(|err| HttpError::Unavailable(err.clone())),
            },
            // The request is only built here: the hop loop sends each hop
            // through the client its target takes.
            #[cfg(feature = "socks")]
            Transport::Socks5 {
                untrusted: client, ..
            } => match parse_url(raw) {
                Err(refusal) => Err(Refused::Target(refusal).into()),
                Ok(url) => client
                    .as_ref()
                    .map(|client| client.get(url))
                    .map_err(|err| HttpError::Unavailable(err.clone())),
            },
            Transport::Unavailable(err) => Err(HttpError::Unavailable(err.clone())),
        };
        HttpRequest {
            builder,
            client: self.clone(),
        }
    }
}

/// A request being built, as reqwest's `RequestBuilder`.
#[derive(Debug)]
pub struct HttpRequest {
    builder: Result<reqwest::RequestBuilder, HttpError>,
    client: HttpClient,
}

impl HttpRequest {
    /// Adds a header, as `RequestBuilder::header` does.
    pub fn header<K, V>(self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        Self {
            builder: self.builder.map(|builder| builder.header(key, value)),
            client: self.client,
        }
    }

    /// Adds a query string, as `RequestBuilder::query` does.
    pub fn query<T: Serialize + ?Sized>(self, query: &T) -> Self {
        Self {
            builder: self.builder.map(|builder| builder.query(query)),
            client: self.client,
        }
    }

    /// Sends the request, once the capability allows it: refused with
    /// [`HttpError::Revoked`] after the network is revoked, before anything
    /// is resolved or connected.
    ///
    /// Under `direct`, the policy follows redirects itself, one budget
    /// (the config's `max_redirects`) and one deadline (its `timeout`) for
    /// the whole operation. For each hop:
    /// 1. the capability is asked again;
    /// 2. the target is classified: `http` or `https` only, no userinfo, and
    ///    an onion is refused before any socket. Under checked addresses, a
    ///    configured target (by exact host and port) goes to the configured
    ///    client, and an untrusted IP literal must pass the address policy
    ///    here, since it never reaches a resolver;
    /// 3. the hop is a fresh request, from the operation's own fixed
    ///    headers: once a hop changes the scheme, host or port,
    ///    `Authorization`, `Cookie` and `Proxy-Authorization` are dropped,
    ///    and no hop sends `Referer`;
    /// 4. a 301, 302, 303, 307 or 308 with a `Location` is followed, while
    ///    the budget lasts: past it, [`HttpError::TooManyRedirects`]. With a
    ///    budget of 0 the redirect is the response. The `Location` must be
    ///    visible ASCII with nothing the WHATWG rules would strip, and an
    ///    absolute or network-path one is read as [`parse_url`] reads a
    ///    configured endpoint.
    pub async fn send(self) -> Result<reqwest::Response, HttpError> {
        let inner = self.client.inner.as_ref();
        inner.network.capability().check()?;
        let builder = self.builder?;
        match &inner.transport {
            Transport::Legacy(_) => builder.send().await.map_err(from_reqwest),
            Transport::Direct { .. } => follow(inner, builder.build()?).await,
            #[cfg(feature = "socks")]
            Transport::Socks5 { .. } => follow(inner, builder.build()?).await,
            Transport::Unavailable(err) => Err(HttpError::Unavailable(err.clone())),
        }
    }
}

/// reqwest's error, or the capability's refusal it carries: a refused hop
/// or connection is [`HttpError::Revoked`].
fn from_reqwest(err: reqwest::Error) -> HttpError {
    if is_revoked(&err) {
        HttpError::Revoked(Revoked)
    } else {
        HttpError::Http(err)
    }
}

/// The redirect statuses reqwest follows, and so the hop loop does.
fn is_followed(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

/// The hop loop (see [`HttpRequest::send`]).
async fn follow(
    inner: &ClientInner,
    request: reqwest::Request,
) -> Result<reqwest::Response, HttpError> {
    // A timeout too long to be a deadline (`Duration::MAX`) is none.
    let deadline = Instant::now().checked_add(inner.config.timeout);
    let mut fixed = request.headers().clone();
    // No hop sends a `Referer`, the caller's own included, and each hop's
    // `Host` is its own target's, which reqwest writes.
    fixed.remove(REFERER);
    fixed.remove(HOST);
    let mut url = request.url().clone();
    let mut credentials = true;
    let mut hops = 0;
    loop {
        inner.network.capability().check()?;
        let route = admit(inner, &url, deadline).await?;
        let mut headers = fixed.clone();
        if !credentials {
            for name in [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION] {
                headers.remove(name);
            }
        }
        let mut hop = route.get(&url)?.headers(headers);
        if let Some(deadline) = deadline {
            hop = hop.timeout(deadline.saturating_duration_since(Instant::now()));
        }
        let response = hop.send().await.map_err(from_reqwest)?;
        if inner.config.max_redirects == 0 || !is_followed(response.status()) {
            return Ok(response);
        }
        let Some(location) = response.headers().get(LOCATION) else {
            return Ok(response);
        };
        if hops == inner.config.max_redirects {
            return Err(HttpError::TooManyRedirects);
        }
        let next = next_url(&url, location)?;
        if !same_authority(&url, &next) {
            credentials = false;
        }
        hops += 1;
        url = next;
    }
}

/// The client a hop goes through: an ordinary one, or the onion route's
/// with the hop's onion URL.
enum Route<'a> {
    Client(&'a reqwest::Client),
    #[cfg(feature = "socks")]
    Onion(&'a OnionClient, OnionUrl),
}

impl Route<'_> {
    fn get(&self, url: &Url) -> Result<reqwest::RequestBuilder, HttpError> {
        match self {
            Self::Client(client) => Ok(client.get(url.clone())),
            #[cfg(feature = "socks")]
            Self::Onion(client, onion) => client.get(onion),
        }
    }
}

/// The route for a hop to `url`, once its target is admitted: under
/// `direct` or `socks5`, chosen again for every hop.
async fn admit<'a>(
    inner: &'a ClientInner,
    url: &Url,
    deadline: Option<Instant>,
) -> Result<Route<'a>, HttpError> {
    let target = classify_url(url).map_err(Refused::Target)?;
    #[cfg(feature = "socks")]
    if let (
        Transport::Socks5 {
            untrusted,
            configured,
            onion,
        },
        Mode::Socks5(socks5),
    ) = (&inner.transport, inner.network.policy().mode())
    {
        if target.target.is_onion() {
            let url = OnionUrl::new(url).ok_or(Refused::OnionRoute)?;
            let onion = onion
                .as_ref()
                .map_err(|err| HttpError::Unavailable(err.clone()))?;
            return Ok(Route::Onion(onion, url));
        }
        let client = if socks5.configured.contains(&target.target, target.port) {
            configured
        } else {
            // An IP literal never needs the resolver: it is checked here.
            if let Target::Ip(ip) = target.target {
                check_address(ip, &Nat64State::known([])).map_err(Refused::Address)?;
            }
            untrusted
        };
        return client
            .as_ref()
            .map(Route::Client)
            .map_err(|err| HttpError::Unavailable(err.clone()));
    }
    if target.target.is_onion() {
        return Err(Refused::Onion.into());
    }
    let Transport::Direct {
        untrusted,
        configured,
    } = &inner.transport
    else {
        return Err(HttpError::Unavailable(NetworkError::Unsupported("direct")));
    };
    let client = match (inner.network.policy().mode(), configured) {
        (Mode::Direct(direct), Some(configured_client)) => {
            let Addresses::Checked(handle) = &direct.addresses else {
                return Err(HttpError::Unavailable(NetworkError::Unsupported("direct")));
            };
            if direct.configured.contains(&target.target, target.port) {
                configured_client
            } else {
                if let Target::Ip(ip) = target.target {
                    let mut handle = handle.clone();
                    let check = check_waiting(ip, &mut handle);
                    let checked = match deadline {
                        Some(deadline) => tokio::time::timeout_at(deadline.into(), check)
                            .await
                            .map_err(|_| HttpError::Timeout)?,
                        None => check.await,
                    };
                    checked.map_err(Refused::Address)?;
                    // Revoked while it waited: nothing is sent.
                    inner.network.capability().check()?;
                }
                untrusted
            }
        }
        _ => untrusted,
    };
    client
        .as_ref()
        .map(Route::Client)
        .map_err(|err| HttpError::Unavailable(err.clone()))
}

/// The URL a `Location` points to, from `base`.
fn next_url(base: &Url, location: &HeaderValue) -> Result<Url, HttpError> {
    let raw = location.to_str().map_err(|_| Refused::Location)?;
    let stripped = |c: char| c <= ' ';
    if raw.is_empty()
        || raw.bytes().any(|b| matches!(b, b'\t' | b'\n' | b'\r'))
        || raw.starts_with(stripped)
        || raw.ends_with(stripped)
    {
        return Err(Refused::Location.into());
    }
    let next = if has_scheme(raw) {
        parse_url(raw)
    } else if raw.starts_with("//") {
        parse_url(&format!("{}:{raw}", base.scheme()))
    } else if raw.starts_with(['/', '\\']) && raw[1..].starts_with(['/', '\\']) {
        // A network path spelled with a backslash: refused rather than read
        // as the WHATWG rules would.
        Err(Refusal::InvalidUrl)
    } else {
        base.join(raw).map_err(|_| Refusal::InvalidUrl)
    };
    next.map_err(|refusal| Refused::Target(refusal).into())
}

/// Whether `raw` starts with a URL scheme and its colon.
fn has_scheme(raw: &str) -> bool {
    raw.find(':').is_some_and(|colon| {
        let scheme = &raw[..colon];
        scheme
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic())
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    })
}

fn same_authority(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

/// Why a request failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HttpError {
    /// reqwest's own error, as before the policy.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// The network's capability is revoked: nothing more was sent.
    #[error(transparent)]
    Revoked(#[from] Revoked),
    /// The client couldn't be built: nothing was sent.
    #[error(transparent)]
    Unavailable(NetworkError),
    /// The policy refused a hop's target: nothing was sent to it.
    #[error("refused by the network policy: {0}")]
    Refused(#[from] Refused),
    /// The redirect budget ran out.
    #[error("too many redirects")]
    TooManyRedirects,
    /// The operation's deadline passed before the hop was sent, while its
    /// IP literal waited for NAT64 discovery: a timeout, which the gateway
    /// answers with 504, as it does reqwest's own.
    #[error("operation timed out: the deadline passed while NAT64 discovery was pending")]
    Timeout,
}

impl HttpError {
    /// Whether the operation timed out: its deadline passed, here or in
    /// reqwest.
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout => true,
            Self::Http(err) => err.is_timeout(),
            _ => false,
        }
    }
}

/// Why the policy refused a hop. Each text names the rule, never the target.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Refused {
    #[error("{0}")]
    Target(Refusal),
    #[error("an onion can't be reached in direct mode")]
    Onion,
    /// An onion the onion route doesn't take: not a well-formed v3 address.
    #[error("the onion route takes only a v3 onion address")]
    OnionRoute,
    #[error("{0}")]
    Address(AddressRefusal),
    #[error("a redirect's Location can't be read")]
    Location,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One of the client constructors.
    type Builder = fn(ClientConfig, &NetworkCapability) -> reqwest::ClientBuilder;

    /// A resolver that counts the names it is asked for.
    #[derive(Clone, Default)]
    struct Counting(Arc<std::sync::atomic::AtomicUsize>);

    impl reqwest::dns::Resolve for Counting {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async {
                let addrs: reqwest::dns::Addrs =
                    Box::new(std::iter::once("127.0.0.1:9".parse().unwrap()));
                Ok(addrs)
            })
        }
    }

    /// The connector layer on its own, under a client asked directly (not
    /// through `HttpRequest::send`'s own check): once revoked, a new
    /// connection fails before the resolver is asked or a socket opened.
    #[tokio::test]
    async fn a_revoked_capability_opens_no_connection_and_resolves_nothing() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let builders: [(&str, Builder); 2] =
            [("legacy", legacy_builder), ("direct", direct_builder)];
        for ((config, followed), (kind, builder)) in [
            (ClientConfig::new(Duration::from_secs(5), 10), "followed"),
            (ClientConfig::new(Duration::from_secs(5), 0), "unfollowed"),
        ]
        .into_iter()
        .flat_map(|config| builders.into_iter().map(move |builder| (config, builder)))
        {
            let name = format!("{kind}, {followed}");
            let capability = NetworkCapability::new();
            let resolver = Counting::default();
            let client = builder(config, &capability)
                .no_proxy()
                .dns_resolver(Arc::new(resolver.clone()))
                .build()
                .unwrap();
            capability.revoke();
            for url in [
                format!("http://counted.test:{port}/"),
                format!("http://127.0.0.1:{port}/"),
            ] {
                let err = client.get(url).send().await.unwrap_err();
                assert!(is_revoked(&err), "{name}: {err:?}");
            }
            assert_eq!(
                resolver.0.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "{name}"
            );
            assert!(
                listener.accept().is_err(),
                "{name}: a connection was opened"
            );
        }
    }

    /// A resolver held until released: it reports the moment it is asked,
    /// then answers `address` once `release` is notified (the #45 review's
    /// probes, round 2).
    #[derive(Clone)]
    struct HeldResolver {
        address: std::net::SocketAddr,
        asked: std::sync::mpsc::Sender<()>,
        release: Arc<tokio::sync::Notify>,
    }

    impl reqwest::dns::Resolve for HeldResolver {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            self.asked.send(()).unwrap();
            let (release, address) = (self.release.clone(), self.address);
            Box::pin(async move {
                release.notified().await;
                Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs)
            })
        }
    }

    /// Revoked while the connector's resolution is pending, after every
    /// check passed: the resolution's answer is never connected to, and the
    /// capability's quiescence comes once the attempt has been dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revocation_while_resolution_is_held_prevents_the_later_connect() {
        for redirects in [10, 0] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (asked, was_asked) = std::sync::mpsc::channel();
            let release = Arc::new(tokio::sync::Notify::new());
            let capability = NetworkCapability::new();
            let config = ClientConfig::new(Duration::from_secs(5), redirects);
            let reqwest = legacy_builder(config, &capability)
                .no_proxy()
                .dns_resolver(Arc::new(HeldResolver {
                    address,
                    asked,
                    release: release.clone(),
                }))
                .build()
                .unwrap();
            let request = tokio::spawn(async move {
                reqwest
                    .get(format!("http://held.test:{}/", address.port()))
                    .send()
                    .await
                    .map_err(from_reqwest)
            });
            tokio::task::spawn_blocking(move || {
                was_asked.recv_timeout(Duration::from_secs(2)).unwrap()
            })
            .await
            .unwrap();
            assert_eq!(capability.permits(), 1);
            capability.revoke();
            tokio::time::timeout(Duration::from_secs(2), capability.quiesced())
                .await
                .expect("the held attempt is dropped");
            release.notify_one();
            let accepted =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;
            assert!(
                accepted.is_err(),
                "{redirects}: a socket was opened after revocation by an attempt that passed the layer"
            );
            let result = request.await.unwrap();
            assert!(
                matches!(result, Err(HttpError::Revoked(_))),
                "{redirects}: {result:?}"
            );
        }
    }

    /// A connector whose connect is held until released: it records
    /// whether it connected, and when it is dropped.
    #[derive(Clone)]
    struct HeldConnect {
        release: Arc<tokio::sync::Notify>,
        connected: Arc<std::sync::atomic::AtomicBool>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    struct SetOnDrop(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl Service<()> for HeldConnect {
        type Response = ();
        type Error = BoxError;
        type Future = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, (): ()) -> Self::Future {
            let held = self.clone();
            Box::pin(async move {
                let _dropped = SetOnDrop(held.dropped.clone());
                held.release.notified().await;
                held.connected
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
        }
    }

    /// The same, with the connect held rather than the resolution, through
    /// the layer alone: revoked while connecting, the connect is dropped
    /// before it completes, quiescence comes only after that drop, and a
    /// release afterwards connects nothing.
    #[tokio::test]
    async fn revocation_while_the_connect_is_held_drops_it_before_quiescence() {
        let capability = NetworkCapability::new();
        let held = HeldConnect {
            release: Arc::new(tokio::sync::Notify::new()),
            connected: Default::default(),
            dropped: Default::default(),
        };
        let mut connector = CapabilityLayer(capability.clone()).layer(held.clone());
        let connecting = tokio::spawn(connector.call(()));
        tokio::task::yield_now().await;
        assert_eq!(capability.permits(), 1);
        assert!(!held.dropped.load(std::sync::atomic::Ordering::SeqCst));

        capability.revoke();
        tokio::time::timeout(Duration::from_secs(2), capability.quiesced())
            .await
            .expect("quiescence once the work is dropped");
        assert!(
            held.dropped.load(std::sync::atomic::Ordering::SeqCst),
            "quiescence before the connect was dropped"
        );
        held.release.notify_one();
        let err = connecting.await.unwrap().unwrap_err();
        assert!(err.is::<Revoked>(), "{err:?}");
        assert!(!held.connected.load(std::sync::atomic::Ordering::SeqCst));
        // And a connection asked for now is refused before the connector.
        let err = connector.call(()).await.unwrap_err();
        assert!(err.is::<Revoked>(), "{err:?}");
        assert_eq!(capability.permits(), 0);
    }

    /// The hop loop's own wait for NAT64 discovery, for an IP literal:
    /// revoked meanwhile, the hop is refused as the wait ends, before
    /// anything is dispatched, even over a connection already in a pool
    /// (#46 review, 46-M1).
    #[tokio::test]
    async fn a_literal_admitted_after_a_revoke_is_refused_before_dispatch() {
        use crate::gate::{ConfiguredTargets, Nat64State};
        use crate::policy::{Nat64Handle, NetworkPolicy};
        let (publisher, handle) = Nat64Handle::channel(Nat64State::Pending);
        let policy = NetworkPolicy::direct(
            ConfiguredTargets::new(),
            Url::parse("https://dns.example/dns-query").unwrap(),
            Addresses::Checked(handle),
        )
        .unwrap();
        let network = Network::with_lookup(policy, Some(crate::resolve::system_lookup()));
        let client = network.http_client(ClientConfig::new(Duration::from_secs(5), 0));
        let url = Url::parse("http://[2a00:1::1]:9/").unwrap();
        let inner = client.inner.clone();
        let admitting = tokio::spawn(async move { admit(&inner, &url, None).await.map(drop) });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!admitting.is_finished(), "the literal waits for discovery");
        network.revoke();
        publisher.publish(Nat64State::NONE);
        let result = admitting.await.unwrap();
        assert!(matches!(result, Err(HttpError::Revoked(_))), "{result:?}");
    }

    /// The onion client checks its URL's classification again before it
    /// sends (design 0007, "The onion route"): handed an ordinary name, or
    /// a malformed onion, it refuses, and nothing reaches the proxy. A
    /// well-formed onion (the control) is sent to the proxy by name.
    #[cfg(feature = "test-util")]
    #[tokio::test]
    async fn the_onion_client_refuses_anything_but_an_onion() {
        use crate::gate::{ConfiguredTargets, OnionUrl};
        use crate::policy::NetworkPolicy;
        use crate::socks::fixture::{Destination, SocksFixture};
        let fixture = SocksFixture::start().await;
        let network = Network::new(
            NetworkPolicy::socks5(
                fixture.addr(),
                ConfiguredTargets::new(),
                Url::parse("https://dns.example/dns-query").unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let client = network.http_client(ClientConfig::new(Duration::from_secs(2), 0));
        let Transport::Socks5 { onion, .. } = &client.inner.transport else {
            panic!("a socks5 client");
        };
        let onion = onion.as_ref().unwrap();
        let v3 = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion";
        for refused in [
            "http://example.com/".to_string(),
            "http://127.0.0.1/".to_string(),
            "http://foo.onion/".to_string(),
            // A well-formed onion, spelled otherwise than classified. (A
            // `Url` holds an http host lowercase already.)
            format!("http://{v3}./"),
        ] {
            let url = OnionUrl::unchecked(Url::parse(&refused).unwrap());
            assert!(
                matches!(
                    onion.get(&url),
                    Err(HttpError::Refused(Refused::OnionRoute))
                ),
                "{refused}"
            );
        }
        assert_eq!(fixture.connections(), 0);
        let url = OnionUrl::new(&Url::parse(&format!("http://{v3}/")).unwrap()).unwrap();
        let _ = onion.get(&url).unwrap().send().await;
        let requests = fixture.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].destination,
            Destination::Name(v3.to_string(), 80)
        );
    }

    #[test]
    fn a_client_is_one_pointer_wide() {
        assert_eq!(
            std::mem::size_of::<HttpClient>(),
            std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn locations_are_read_as_a_configured_endpoint_is() {
        let base = Url::parse("http://router.example:8080/a/b").unwrap();
        let next = |raw: &str| {
            next_url(&base, &HeaderValue::from_str(raw).unwrap()).map(|url| url.to_string())
        };
        assert_eq!(next("c").unwrap(), "http://router.example:8080/a/c");
        assert_eq!(next("/c?x=1").unwrap(), "http://router.example:8080/c?x=1");
        assert_eq!(next("//other.example/c").unwrap(), "http://other.example/c");
        assert_eq!(
            next("HTTPS://other.example/c").unwrap(),
            "https://other.example/c"
        );
        for refused in [
            "ftp://other.example/",
            "file:///etc/hosts",
            "javascript:alert(1)",
            "http://user@other.example/",
            "//user@other.example/",
            "http://ex%61mple.com/",
            "\\\\other.example/c",
            "/\\other.example/c",
            "http:other.example",
            " /c",
            "/c ",
        ] {
            assert!(next(refused).is_err(), "{refused:?}");
        }
        let bytes = HeaderValue::from_bytes(b"/r\xc3\xa9").unwrap();
        assert!(next_url(&base, &bytes).is_err());
        let tab = HeaderValue::from_bytes(b"/a\tb").unwrap();
        assert!(next_url(&base, &tab).is_err());
    }

    #[test]
    fn credentials_go_only_where_the_authority_stays() {
        let url = |text: &str| Url::parse(text).unwrap();
        assert!(same_authority(
            &url("http://a.example/x"),
            &url("http://a.example:80/y")
        ));
        assert!(!same_authority(
            &url("http://a.example/"),
            &url("https://a.example/")
        ));
        assert!(!same_authority(
            &url("http://a.example/"),
            &url("http://b.example/")
        ));
        assert!(!same_authority(
            &url("http://a.example/"),
            &url("http://a.example:81/")
        ));
    }

    #[test]
    fn its_texts_never_say_timeout() {
        for err in [
            HttpError::TooManyRedirects,
            HttpError::Refused(Refused::Onion),
            HttpError::Refused(Refused::Location),
            HttpError::Refused(Refused::Target(Refusal::Scheme)),
            HttpError::Refused(Refused::Address(AddressRefusal::Nat64Pending)),
            HttpError::Refused(Refused::Address(AddressRefusal::NotGlobalUnicast)),
            HttpError::Revoked(Revoked),
        ] {
            let text = err.to_string().to_ascii_lowercase();
            assert!(
                !text.contains("timeout") && !text.contains("timed out"),
                "{text}"
            );
        }
    }
}
