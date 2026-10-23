//! Configured targets (design 0007, "The policy").

use std::collections::HashSet;
use std::net::SocketAddr;

use url::Url;

use super::classify::{canonical, classify_url, DialTarget, Refusal, Target};

/// The targets configuration names: the delegated routers and the DoH
/// endpoint in `ipfs.start`. They are exempt from the address policy by
/// exact host and port, including when one is local. Everything learned
/// from the network is untrusted, a redirect from a configured endpoint
/// included, unless it reaches a configured host and port itself.
///
/// A SOCKS proxy's own endpoint isn't a target: the transport connects to
/// it, and every target goes through it. So a target at the proxy's
/// address, reached through the proxy, is untrusted like any other.
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

    /// Adds a socket address: an endpoint configured by address and port.
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

    /// Whether any configured target is an onion.
    pub fn has_onion(&self) -> bool {
        self.targets.iter().any(|(target, _)| target.is_onion())
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}
