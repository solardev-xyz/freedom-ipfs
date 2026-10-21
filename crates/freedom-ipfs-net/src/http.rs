//! The crates' HTTP clients, built by one constructor (design 0007,
//! upstream item 3.1 and 3.2).
//!
//! Every client the node uses (DoH, delegated IPNS, delegated routing, HTTP
//! providers) is an [`HttpClient`] built by [`Network::http_client`] for its
//! [`ClientConfig`]. Each request asks the network's capability first, and a
//! client whose configuration reqwest refused fails its requests with that
//! error instead of panicking.
//!
//! Under `direct` the policy follows redirects itself (design 0007, "The
//! gate", check 4): every client is built with `Policy::none()` and no
//! proxy, and each request runs [the hop loop](HttpRequest::send).
//!
//! [`Network::http_client`]: crate::policy::Network::http_client

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{
    HeaderName, HeaderValue, AUTHORIZATION, COOKIE, LOCATION, PROXY_AUTHORIZATION,
};
use reqwest::redirect::Policy;
use reqwest::{IntoUrl, StatusCode};
use serde::Serialize;
use url::Url;

use crate::capability::{NetworkCapability, Revoked};
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
    /// A mode this version can't build clients for.
    Unavailable(NetworkError),
}

fn build(builder: reqwest::ClientBuilder) -> Result<reqwest::Client, NetworkError> {
    builder
        .build()
        .map_err(|err| NetworkError::ClientBuild(Arc::new(err)))
}

fn legacy_builder(config: ClientConfig) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .connect_timeout(config.timeout)
        .timeout(config.timeout);
    match config.max_redirects {
        0 => builder.redirect(Policy::none()),
        ClientConfig::REQWEST_DEFAULT_REDIRECTS => builder,
        hops => builder.redirect(Policy::limited(hops)),
    }
}

fn direct_builder(config: ClientConfig) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(config.timeout)
        .timeout(config.timeout)
        .no_proxy()
        .redirect(Policy::none())
        .referer(false)
}

fn transport(network: &Network, config: ClientConfig) -> Transport {
    match network.policy().mode() {
        Mode::Legacy => Transport::Legacy(build(legacy_builder(config))),
        Mode::Direct(direct) => match (&direct.addresses, network.lookup()) {
            (Addresses::Unchecked, _) => Transport::Direct {
                untrusted: build(direct_builder(config)),
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
                        direct_builder(config).dns_resolver(resolver(Some(handle.clone()))),
                    ),
                    configured: Some(build(direct_builder(config).dns_resolver(resolver(None)))),
                }
            }
            (Addresses::Checked(_), None) => Transport::Unavailable(NetworkError::Unsupported(
                "checked addresses without a resolver",
            )),
        },
        Mode::Socks5(_) => Transport::Unavailable(NetworkError::Unsupported("socks5")),
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

    /// A client of a [`Network::legacy`] network of its own, whose capability
    /// nobody else holds: what a crate's `Default` builds when no network is
    /// given.
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
            Transport::Unavailable(err) => Some(err),
        }
    }

    /// A GET request for `url`, as reqwest's `Client::get` starts one.
    pub fn get<U: IntoUrl>(&self, url: U) -> HttpRequest {
        let client = match &self.inner.transport {
            Transport::Legacy(client)
            | Transport::Direct {
                untrusted: client, ..
            } => client.as_ref().map_err(|err| err.clone()),
            Transport::Unavailable(err) => Err(err.clone()),
        };
        HttpRequest {
            builder: client
                .map(|client| client.get(url))
                .map_err(HttpError::Unavailable),
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
            Transport::Legacy(_) => Ok(builder.send().await?),
            Transport::Direct { .. } => follow(inner, builder.build()?).await,
            Transport::Unavailable(err) => Err(HttpError::Unavailable(err.clone())),
        }
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
    let deadline = Instant::now() + inner.config.timeout;
    let fixed = request.headers().clone();
    let mut url = request.url().clone();
    let mut credentials = true;
    let mut hops = 0;
    loop {
        inner.network.capability().check()?;
        let client = admit(inner, &url, deadline).await?;
        let mut headers = fixed.clone();
        if !credentials {
            for name in [AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION] {
                headers.remove(name);
            }
        }
        let response = client
            .get(url.clone())
            .headers(headers)
            .timeout(deadline.saturating_duration_since(Instant::now()))
            .send()
            .await?;
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

/// The client for a hop to `url`, once its target is admitted.
async fn admit<'a>(
    inner: &'a ClientInner,
    url: &Url,
    deadline: Instant,
) -> Result<&'a reqwest::Client, HttpError> {
    let target = classify_url(url).map_err(Refused::Target)?;
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
                    match tokio::time::timeout_at(deadline.into(), check).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(refusal)) => return Err(Refused::Address(refusal).into()),
                        Err(_) => return Err(Refused::Address(AddressRefusal::Nat64Pending).into()),
                    }
                }
                untrusted
            }
        }
        _ => untrusted,
    };
    client
        .as_ref()
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
}

/// Why the policy refused a hop. Each text names the rule, never the target.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Refused {
    #[error("{0}")]
    Target(Refusal),
    #[error("an onion can't be reached in direct mode")]
    Onion,
    #[error("{0}")]
    Address(AddressRefusal),
    #[error("a redirect's Location can't be read")]
    Location,
}

#[cfg(test)]
mod tests {
    use super::*;

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
