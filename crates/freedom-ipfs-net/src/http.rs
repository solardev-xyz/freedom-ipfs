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
//! - **each redirect hop reqwest follows,** in the client's redirect policy,
//!   so a hop over a pooled connection is refused too;
//! - **each new connection,** in a connector layer that runs before
//!   reqwest's resolver: a revoked client starts no DNS lookup and opens no
//!   socket, whatever asked for the connection.
//!
//! [`Network::http_client`]: crate::policy::Network::http_client

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue};
use reqwest::redirect::Policy;
use reqwest::IntoUrl;
use serde::Serialize;
use tower_layer::Layer;
use tower_service::Service;

use crate::capability::{NetworkCapability, Revoked};
use crate::policy::{Mode, Network, NetworkError, NetworkPolicy};

/// How a client times out, and how many redirects it follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// The connect timeout, and the whole request's.
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

/// The one constructor: the reqwest client for `config` under `policy`.
///
/// Under [`Mode::Legacy`] it is exactly what each crate built before:
/// `connect_timeout` and `timeout` set to `config.timeout`, reqwest's default
/// redirect policy for 10 redirects, `Policy::none()` for 0, and nothing else
/// (so the system proxy and reqwest's resolver apply).
pub(crate) fn build_client(
    config: ClientConfig,
    policy: &NetworkPolicy,
    capability: &NetworkCapability,
) -> Result<reqwest::Client, NetworkError> {
    let builder = match policy.mode() {
        Mode::Legacy => legacy_builder(config, capability),
        Mode::Direct(_) => return Err(NetworkError::Unsupported("direct")),
        Mode::Socks5(_) => return Err(NetworkError::Unsupported("socks5")),
    };
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
/// connection fails at once, and nothing is resolved or dialed.
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

    fn call(&mut self, request: R) -> Self::Future {
        if let Err(revoked) = self.capability.check() {
            return Box::pin(async move { Err(Box::new(revoked) as BoxError) });
        }
        let connecting = self.inner.call(request);
        Box::pin(async move { connecting.await.map_err(Into::into) })
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

/// An HTTP client of a [`Network`]: reqwest's, behind the network's
/// capability. Clones share the client and its connection pool.
///
/// One pointer wide, as the `reqwest::Client` it replaces: the crates embed
/// it in values whose size matters (the mobile node's).
#[derive(Clone, Debug)]
pub struct HttpClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    client: Result<reqwest::Client, NetworkError>,
    capability: NetworkCapability,
    config: ClientConfig,
}

impl HttpClient {
    pub(crate) fn new(
        client: Result<reqwest::Client, NetworkError>,
        capability: NetworkCapability,
        config: ClientConfig,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                client,
                capability,
                config,
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

    /// The capability every request asks.
    pub fn capability(&self) -> &NetworkCapability {
        &self.inner.capability
    }

    /// Why reqwest refused to build the client, if it did: every request
    /// then fails with [`HttpError::Unavailable`].
    pub fn error(&self) -> Option<&NetworkError> {
        self.inner.client.as_ref().err()
    }

    /// A GET request for `url`, as reqwest's `Client::get` starts one.
    pub fn get<U: IntoUrl>(&self, url: U) -> HttpRequest {
        let builder = match &self.inner.client {
            Ok(client) => Ok(client.get(url)),
            Err(err) => Err(HttpError::Unavailable(err.clone())),
        };
        HttpRequest {
            builder,
            capability: self.inner.capability.clone(),
        }
    }
}

/// A request being built, as reqwest's `RequestBuilder`.
#[derive(Debug)]
pub struct HttpRequest {
    builder: Result<reqwest::RequestBuilder, HttpError>,
    capability: NetworkCapability,
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
            capability: self.capability,
        }
    }

    /// Adds a query string, as `RequestBuilder::query` does.
    pub fn query<T: Serialize + ?Sized>(self, query: &T) -> Self {
        Self {
            builder: self.builder.map(|builder| builder.query(query)),
            capability: self.capability,
        }
    }

    /// Sends the request, once the capability allows it: refused with
    /// [`HttpError::Revoked`] after the network is revoked, before anything
    /// is resolved or connected.
    ///
    /// A redirect hop or a new connection that meets a revoked capability
    /// fails the same way.
    pub async fn send(self) -> Result<reqwest::Response, HttpError> {
        self.capability.check()?;
        match self.builder?.send().await {
            Ok(response) => Ok(response),
            Err(err) if is_revoked(&err) => Err(HttpError::Revoked(Revoked)),
            Err(err) => Err(err.into()),
        }
    }
}

/// Why a request failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HttpError {
    /// reqwest's own error, as before the policy.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// The network's capability is revoked: nothing was sent.
    #[error(transparent)]
    Revoked(#[from] Revoked),
    /// The client couldn't be built: nothing was sent.
    #[error(transparent)]
    Unavailable(NetworkError),
}

#[cfg(test)]
mod tests {
    use super::*;

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
        for (config, name) in [
            (ClientConfig::new(Duration::from_secs(5), 10), "followed"),
            (ClientConfig::new(Duration::from_secs(5), 0), "unfollowed"),
        ] {
            let capability = NetworkCapability::new();
            let resolver = Counting::default();
            let client = legacy_builder(config, &capability)
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

    #[test]
    fn a_client_is_one_pointer_wide() {
        assert_eq!(
            std::mem::size_of::<HttpClient>(),
            std::mem::size_of::<usize>()
        );
    }
}
