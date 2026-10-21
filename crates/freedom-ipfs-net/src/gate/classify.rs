//! One classification, before anything else (design 0007, "The gate").

use std::net::{IpAddr, Ipv6Addr};

use data_encoding::BASE32_NOPAD;
use multiaddr::{Multiaddr, Protocol};
use url::{Host, Url};

/// What a target is, decided once, before any DNS or TXT expansion.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    /// An onion name, lowercase and without a trailing dot: the last label
    /// is `onion`, or it came from a `/onion3` or `/onion` component. Only
    /// a proxy resolves it.
    Onion(String),
    /// An IP literal, by its binary value. An IPv4-mapped IPv6 address is
    /// its IPv4 address.
    Ip(IpAddr),
    /// Any other name: ASCII, lowercase, without a trailing dot, with no
    /// empty label.
    Name(String),
}

impl Target {
    pub fn is_onion(&self) -> bool {
        matches!(self, Self::Onion(_))
    }
}

/// An `http` or `https` URL's target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UrlTarget {
    /// `https`.
    pub secure: bool,
    pub target: Target,
    /// The URL's port, or its scheme's default.
    pub port: u16,
}

/// Why a target can't be classified. Each text names the rule, never the
/// target, so a refusal can be logged as is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Refusal {
    #[error("the host is empty")]
    EmptyHost,
    #[error("the host isn't ASCII: names must already be in punycode")]
    NonAscii,
    #[error("the host isn't a valid name or IP literal")]
    InvalidHost,
    #[error("the URL can't be parsed")]
    InvalidUrl,
    #[error("only http and https URLs are accepted")]
    Scheme,
    #[error("a URL with userinfo is refused")]
    Userinfo,
    #[error(
        "a dialed multiaddr must start with /ip4, /ip6, /dns, /dns4, /dns6, /dnsaddr, /onion3 or /onion"
    )]
    Multiaddr,
}

/// Classifies a host: a URL's host, a `/dns*` component, or any name or
/// literal from configuration or from the network.
///
/// - `[…]` and bare text with a `:` are IPv6 literals, and must parse as
///   one: a zone (`%eth0`) is refused.
/// - Everything else is parsed by the WHATWG host rules, so `0x7f.1`,
///   `2130706433` and `127.0.0.1.` are the IPv4 literal `127.0.0.1`.
/// - A name is lowercased, loses one trailing dot, and must have no empty
///   label.
///
/// Refused before any parsing: an empty host, any non-ASCII byte (names
/// must already be in punycode), and any `%`, space or control byte: a
/// percent-encoded host would be decoded into something its text doesn't
/// say.
pub fn classify_host(host: &str) -> Result<Target, Refusal> {
    if host.is_empty() {
        return Err(Refusal::EmptyHost);
    }
    if !host.is_ascii() {
        return Err(Refusal::NonAscii);
    }
    if host.bytes().any(|b| b == b'%' || !b.is_ascii_graphic()) {
        return Err(Refusal::InvalidHost);
    }
    if let Some(inner) = host.strip_prefix('[') {
        let inner = inner.strip_suffix(']').ok_or(Refusal::InvalidHost)?;
        return ipv6_literal(inner);
    }
    if host.contains(':') {
        return ipv6_literal(host);
    }
    match Host::parse(host).map_err(|_| Refusal::InvalidHost)? {
        Host::Ipv4(ip) => Ok(Target::Ip(IpAddr::V4(ip))),
        Host::Ipv6(ip) => Ok(ip_target(IpAddr::V6(ip))),
        Host::Domain(name) => name_target(name),
    }
}

/// Classifies a parsed URL: `http` or `https` only, no userinfo, and its
/// host as [`classify_host`] would. The `url` crate has already applied the
/// WHATWG rules, so the host is ASCII here; [`parse_url`] refuses what the
/// raw text shouldn't hold.
pub fn classify_url(url: &Url) -> Result<UrlTarget, Refusal> {
    let secure = match url.scheme() {
        "http" => false,
        "https" => true,
        _ => return Err(Refusal::Scheme),
    };
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Refusal::Userinfo);
    }
    let target = match url.host() {
        None => return Err(Refusal::EmptyHost),
        Some(Host::Domain(name)) => classify_host(name)?,
        Some(Host::Ipv4(ip)) => Target::Ip(IpAddr::V4(ip)),
        Some(Host::Ipv6(ip)) => ip_target(IpAddr::V6(ip)),
    };
    let port = url.port_or_known_default().ok_or(Refusal::InvalidUrl)?;
    Ok(UrlTarget {
        secure,
        target,
        port,
    })
}

/// Parses an absolute URL from text the service didn't write itself (a
/// configured endpoint, a redirect's `Location`), then classifies it.
///
/// The `url` crate would turn a Unicode host into punycode and decode a
/// percent-encoded one, so the text is checked first:
/// - every byte is printable ASCII: no Unicode, no space, tab, newline or
///   control byte, which the WHATWG rules would strip or skip;
/// - it starts with `http://` or `https://` (any case);
/// - its authority, up to the first `/`, `\`, `?` or `#`, is non-empty and
///   holds no `@` (userinfo) and no `%`.
pub fn parse_url(raw: &str) -> Result<Url, Refusal> {
    if !raw.is_ascii() {
        return Err(Refusal::NonAscii);
    }
    if raw.bytes().any(|b| !b.is_ascii_graphic()) {
        return Err(Refusal::InvalidUrl);
    }
    let lower = raw.to_ascii_lowercase();
    let Some(rest) = lower
        .strip_prefix("http://")
        .or_else(|| lower.strip_prefix("https://"))
    else {
        return Err(
            if lower.starts_with("http:") || lower.starts_with("https:") {
                Refusal::InvalidUrl
            } else {
                Refusal::Scheme
            },
        );
    };
    let authority = rest.split(['/', '\\', '?', '#']).next().unwrap_or("");
    if authority.contains('@') {
        return Err(Refusal::Userinfo);
    }
    if authority.contains('%') {
        return Err(Refusal::InvalidHost);
    }
    if authority.is_empty() {
        return Err(Refusal::EmptyHost);
    }
    let url = Url::parse(raw).map_err(|_| Refusal::InvalidUrl)?;
    classify_url(&url)?;
    Ok(url)
}

/// Classifies a multiaddr that is to be dialed, by its first component:
/// `/ip4` and `/ip6` are literals, `/dns`, `/dns4`, `/dns6` and `/dnsaddr`
/// hosts as [`classify_host`] classifies them (so `/dns/x.onion` is an
/// onion, and `/dns/127.0.0.1` a literal), and `/onion3` and `/onion` are
/// onions. Any other first component is refused: `/ip6zone` scopes an
/// address to one local link, and `/unix` and `/memory` aren't network
/// targets.
pub fn classify_multiaddr(addr: &Multiaddr) -> Result<Target, Refusal> {
    match addr.iter().next() {
        Some(Protocol::Ip4(ip)) => Ok(Target::Ip(IpAddr::V4(ip))),
        Some(Protocol::Ip6(ip)) => Ok(ip_target(IpAddr::V6(ip))),
        Some(
            Protocol::Dns(name)
            | Protocol::Dns4(name)
            | Protocol::Dns6(name)
            | Protocol::Dnsaddr(name),
        ) => classify_host(&name),
        Some(Protocol::Onion3(onion)) => Ok(Target::Onion(onion_name(onion.hash()))),
        Some(Protocol::Onion(hash, _)) => Ok(Target::Onion(onion_name(&hash[..]))),
        _ => Err(Refusal::Multiaddr),
    }
}

fn ipv6_literal(text: &str) -> Result<Target, Refusal> {
    let ip: Ipv6Addr = text.parse().map_err(|_| Refusal::InvalidHost)?;
    Ok(ip_target(IpAddr::V6(ip)))
}

/// An IP literal's target: an IPv4-mapped address is its IPv4 address.
fn ip_target(ip: IpAddr) -> Target {
    Target::Ip(canonical(ip))
}

pub(crate) fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

fn name_target(name: String) -> Result<Target, Refusal> {
    let mut name = name.to_ascii_lowercase();
    if name.ends_with('.') {
        name.pop();
    }
    if name.is_empty() || name.split('.').any(str::is_empty) {
        return Err(Refusal::InvalidHost);
    }
    if name.rsplit('.').next() == Some("onion") {
        Ok(Target::Onion(name))
    } else {
        Ok(Target::Name(name))
    }
}

fn onion_name(hash: &[u8]) -> String {
    let mut name = BASE32_NOPAD.encode(hash);
    name.make_ascii_lowercase();
    name.push_str(".onion");
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_lose_one_trailing_dot_and_no_more() {
        assert_eq!(
            classify_host("Example.COM."),
            Ok(Target::Name("example.com".into()))
        );
        assert_eq!(classify_host("example.com.."), Err(Refusal::InvalidHost));
        assert_eq!(classify_host("a..b"), Err(Refusal::InvalidHost));
        assert_eq!(classify_host("."), Err(Refusal::InvalidHost));
        assert_eq!(classify_host(""), Err(Refusal::EmptyHost));
    }

    #[test]
    fn literals_need_no_brackets_outside_urls() {
        assert_eq!(classify_host("::1"), Ok(Target::Ip("::1".parse().unwrap())));
        assert_eq!(classify_host("fe80::1%eth0"), Err(Refusal::InvalidHost));
        assert_eq!(classify_host("[fe80::1%25eth0]"), Err(Refusal::InvalidHost));
        assert_eq!(classify_host("[::1"), Err(Refusal::InvalidHost));
        assert_eq!(classify_host("1:2"), Err(Refusal::InvalidHost));
    }

    #[test]
    fn hosts_with_percent_space_or_controls_are_refused() {
        for host in ["ex%61mple.com", "a b", "a\tb", "a\u{7f}b", "127.0.0.%31"] {
            assert_eq!(classify_host(host), Err(Refusal::InvalidHost), "{host:?}");
        }
    }

    #[test]
    fn onion_names_keep_their_subdomains() {
        assert_eq!(
            classify_host("www.ABC.onion"),
            Ok(Target::Onion("www.abc.onion".into()))
        );
        assert_eq!(classify_host("onion"), Ok(Target::Onion("onion".into())));
        assert_eq!(
            classify_host("onion.example"),
            Ok(Target::Name("onion.example".into()))
        );
    }
}
