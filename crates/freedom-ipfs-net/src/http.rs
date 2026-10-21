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
//!   socket, whatever asked for the connection. The connection is made
//!   under a permit of the capability ([`NetworkCapability::admit`]), so a
//!   revoke while it resolves or connects drops it, and the capability's
//!   quiescence waits until it has been dropped.
//!
//! A request sent over a connection already in the pool opens no new
//! connection: revoking stops new ones, and the teardown closes the open
//! ones.
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
            let client = HttpClient::new(Ok(reqwest), capability.clone(), config);

            let request = tokio::spawn(async move {
                client
                    .get(format!("http://held.test:{}/", address.port()))
                    .send()
                    .await
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

    #[test]
    fn a_client_is_one_pointer_wide() {
        assert_eq!(
            std::mem::size_of::<HttpClient>(),
            std::mem::size_of::<usize>()
        );
    }
}
