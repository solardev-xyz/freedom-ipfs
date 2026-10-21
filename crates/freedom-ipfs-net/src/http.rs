//! The crates' HTTP clients, built by one constructor (design 0007,
//! upstream item 3.1 and 3.2).
//!
//! Every client the node uses (DoH, delegated IPNS, delegated routing, HTTP
//! providers) is an [`HttpClient`] built by [`Network::http_client`] for its
//! [`ClientConfig`]. Each request asks the network's capability first, and a
//! client whose configuration reqwest refused fails its requests with that
//! error instead of panicking.
//!
//! [`Network::http_client`]: crate::policy::Network::http_client

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{HeaderName, HeaderValue};
use reqwest::redirect::Policy;
use reqwest::IntoUrl;
use serde::Serialize;

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
) -> Result<reqwest::Client, NetworkError> {
    let builder = match policy.mode() {
        Mode::Legacy => legacy_builder(config),
        Mode::Direct(_) => return Err(NetworkError::Unsupported("direct")),
        Mode::Socks5(_) => return Err(NetworkError::Unsupported("socks5")),
    };
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

    /// A client of a [`Network::legacy`] network of its own, whose capability
    /// nobody else holds: what a crate's `Default` builds when no network is
    /// given.
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
    pub async fn send(self) -> Result<reqwest::Response, HttpError> {
        self.capability.check()?;
        Ok(self.builder?.send().await?)
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

    #[test]
    fn a_client_is_one_pointer_wide() {
        assert_eq!(
            std::mem::size_of::<HttpClient>(),
            std::mem::size_of::<usize>()
        );
    }
}
