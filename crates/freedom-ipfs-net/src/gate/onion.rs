//! The onion route's host (design 0007, "The onion route").

use data_encoding::BASE32_NOPAD;
use url::Url;

use super::classify::{classify_url, Target};

/// The length of a v3 onion address's label: 35 bytes in base32.
const V3_LABEL_LEN: usize = 56;
/// A v3 onion address's version byte, its last.
const V3_VERSION: u8 = 3;

/// An onion service's host as the onion route hands it to the proxy: a v3
/// address (56 base32 characters encoding the service's key, a checksum and
/// the version byte 3), then `onion`, with any subdomain labels before it.
///
/// Only [`OnionHost::new`] makes one, from a target the classifier found to
/// be an onion. So no ordinary name can take the route, and neither can a
/// malformed onion (`onion`, `foo.onion`, a retired v2 address), which a
/// proxy other than Tor might hand to its own DNS.
///
/// **The checksum isn't verified here:** a bad one fails at the proxy, which
/// is Tor's to check, and reveals nothing a well-formed address wouldn't.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OnionHost(String);

impl OnionHost {
    /// The host for `target`, if it is an onion whose service label is a
    /// well-formed v3 address.
    pub fn new(target: &Target) -> Option<Self> {
        let Target::Onion(name) = target else {
            return None;
        };
        let mut labels = name.rsplit('.');
        if labels.next() != Some("onion") {
            return None;
        }
        let service = labels.next()?;
        if service.len() != V3_LABEL_LEN
            || !service
                .bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        {
            return None;
        }
        let decoded = BASE32_NOPAD
            .decode(service.to_ascii_uppercase().as_bytes())
            .ok()?;
        if decoded.last() != Some(&V3_VERSION) {
            return None;
        }
        Some(Self(name.clone()))
    }

    /// The host name, as the proxy receives it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An `http` or `https` URL, without userinfo, whose host is an
/// [`OnionHost`], spelled exactly as that host: the only thing the onion
/// route's HTTP client takes (design 0007, "The onion route"). Only
/// [`OnionUrl::new`] makes one, from a URL the classifier found to be an
/// onion's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnionUrl(Url);

impl OnionUrl {
    /// `url`, if the classifier reads it as an `http` or `https` URL
    /// without userinfo whose host is a well-formed v3 onion address, with
    /// its host written as the classified [`OnionHost`]: lowercase, without
    /// a trailing dot. So the host the proxy receives (and the request's
    /// `Host`, and TLS's server name) is the host that was checked. The
    /// scheme, port, path and query are kept.
    pub fn new(url: &Url) -> Option<Self> {
        let target = classify_url(url).ok()?;
        let host = OnionHost::new(&target.target)?;
        let mut sent = url.clone();
        sent.set_host(Some(host.as_str())).ok()?;
        (sent.host_str() == Some(host.as_str())).then_some(Self(sent))
    }

    /// The host the URL names, if it is a well-formed onion spelled
    /// exactly as the classifier writes it.
    pub fn host(&self) -> Option<OnionHost> {
        let target = classify_url(&self.0).ok()?;
        let host = OnionHost::new(&target.target)?;
        (self.0.host_str() == Some(host.as_str())).then_some(host)
    }

    pub fn as_url(&self) -> &Url {
        &self.0
    }

    /// Any URL, unchecked: only for the tests of the onion client's own
    /// second check.
    #[cfg(test)]
    pub(crate) fn unchecked(url: Url) -> Self {
        Self(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{classify_host, classify_multiaddr};

    /// A v3 address's label: 32 key bytes, 2 checksum bytes, the version.
    fn v3_label(version: u8) -> String {
        let mut bytes = [7u8; 35];
        bytes[34] = version;
        label_of(&bytes)
    }

    fn label_of(bytes: &[u8]) -> String {
        BASE32_NOPAD.encode(bytes).to_ascii_lowercase()
    }

    #[test]
    fn only_a_well_formed_v3_address_takes_the_route() {
        let v3 = format!("{}.onion", v3_label(3));
        let host = OnionHost::new(&classify_host(&v3).unwrap()).unwrap();
        assert_eq!(host.as_str(), v3);
        // Subdomains are kept; case and a trailing dot are the classifier's.
        let www = format!("www.{}.ONION.", v3_label(3).to_ascii_uppercase());
        assert_eq!(
            OnionHost::new(&classify_host(&www).unwrap())
                .unwrap()
                .as_str(),
            format!("www.{v3}")
        );
        // From a `/onion3` component.
        let onion3 =
            classify_multiaddr(&format!("/onion3/{}:4001", v3_label(3)).parse().unwrap()).unwrap();
        assert_eq!(OnionHost::new(&onion3).unwrap().as_str(), v3);

        for refused in [
            "onion".to_string(),
            "foo.onion".to_string(),
            // A retired v2 address: 16 characters.
            "aaaaaaaaaaaaaaaa.onion".to_string(),
            // 56 characters, but another version.
            format!("{}.onion", v3_label(2)),
            // 55 and 57 characters.
            format!("{}.onion", &v3_label(3)[1..]),
            format!("a{}.onion", v3_label(3)),
            // Valid base32 ending in version 3, but 16 and 64 characters.
            format!("{}.onion", label_of(&[[7u8; 9].as_slice(), &[3]].concat())),
            format!("{}.onion", label_of(&[[7u8; 39].as_slice(), &[3]].concat())),
            // Not base32.
            format!("{}.onion", v3_label(3).replace('a', "1")),
        ] {
            let target = classify_host(&refused).unwrap();
            assert!(target.is_onion(), "{refused}");
            assert_eq!(OnionHost::new(&target), None, "{refused}");
        }
        // A `/onion` (v2) component.
        let v2 = classify_multiaddr(&"/onion/aaimaq4ygg2iegci:80".parse().unwrap()).unwrap();
        assert_eq!(OnionHost::new(&v2), None);
        // Not onions at all.
        for other in ["example.com", "127.0.0.1"] {
            assert_eq!(OnionHost::new(&classify_host(other).unwrap()), None);
        }
    }

    #[test]
    fn only_an_onion_url_the_classifier_accepts_is_one() {
        let v3 = v3_label(3);
        for accepted in [
            format!("http://{v3}.onion/"),
            format!("https://www.{v3}.onion:8443/a?b"),
        ] {
            let url = Url::parse(&accepted).unwrap();
            assert_eq!(OnionUrl::new(&url).unwrap().as_url(), &url);
        }
        // Written another way, the URL is rebuilt with the host as checked:
        // the scheme, port, path and query stay.
        for (written, sent) in [
            (
                format!("http://{v3}.onion./x"),
                format!("http://{v3}.onion/x"),
            ),
            (
                format!("https://WWW.{}.ONION.:8443/a?b#c", v3.to_ascii_uppercase()),
                format!("https://www.{v3}.onion:8443/a?b#c"),
            ),
        ] {
            let url = OnionUrl::new(&Url::parse(&written).unwrap()).unwrap();
            assert_eq!(url.as_url().as_str(), sent, "{written}");
            assert_eq!(
                url.host().map(|host| host.as_str().to_string()),
                url.as_url().host_str().map(str::to_string)
            );
        }
        // A URL spelled otherwise than its classified host is no onion URL
        // the client sends.
        let unchecked = OnionUrl::unchecked(Url::parse(&format!("http://{v3}.onion./")).unwrap());
        assert_eq!(unchecked.host(), None);
        for refused in [
            "http://example.com/".to_string(),
            "http://127.0.0.1/".to_string(),
            "http://foo.onion/".to_string(),
            format!("ftp://{v3}.onion/"),
            format!("http://user@{v3}.onion/"),
        ] {
            assert_eq!(
                OnionUrl::new(&Url::parse(&refused).unwrap()),
                None,
                "{refused}"
            );
        }
    }
}
