//! Configured targets (design 0007, "The policy").

use std::collections::HashSet;
use std::net::SocketAddr;

use url::Url;

use super::classify::{canonical, classify_url, DialTarget, Refusal, Target};

/// The targets configuration names: the delegated routers and the DoH
/// endpoint in `ipfs.start`, and the proxy's endpoint. They are exempt from
/// the address policy by exact host and port, including when one is local.
/// Everything learned from the network is untrusted, a redirect from a
/// configured endpoint included, unless it reaches a configured host and
/// port itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfiguredTargets {
    targets: HashSet<(Target, u16)>,
}

impl ConfiguredTargets {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an endpoint URL's host and port (its scheme's default unless it
    /// names one). It must be `http` or `https`, with no userinfo: a
    /// configured endpoint carries no credentials.
    pub fn add_url(&mut self, url: &Url) -> Result<(), Refusal> {
        let classified = classify_url(url)?;
        self.targets.insert((classified.target, classified.port));
        Ok(())
    }

    /// Adds a socket address, such as the proxy's.
    pub fn add_endpoint(&mut self, endpoint: SocketAddr) {
        self.targets
            .insert((Target::Ip(canonical(endpoint.ip())), endpoint.port()));
    }

    /// Whether `target` at `port` is configured: the same name, onion or
    /// address (an IPv4-mapped address is its IPv4 address), and the same
    /// port.
    pub fn contains(&self, target: &Target, port: u16) -> bool {
        self.targets.contains(&(target.clone(), port))
    }

    /// Whether a dialed multiaddr's target is configured, at the port it
    /// reaches: an HTTP provider without `/tcp` at its effective port (80,
    /// or 443 for HTTPS). A `/dnsaddr`, which reaches no port of its own,
    /// never is.
    pub fn contains_dial(&self, dial: &DialTarget) -> bool {
        dial.port
            .is_some_and(|port| self.contains(&dial.target, port))
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}
