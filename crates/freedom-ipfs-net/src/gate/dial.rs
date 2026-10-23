//! The gate's multiaddr classifier (design 0007, "The gate"): the shapes
//! the node dials. The rest of the classification, and the address policy,
//! are `freedom-ipfs-addr`'s, which [`crate::gate`] re-exports; this part
//! stays here because it needs the multiaddr types libp2p dials.

use std::net::IpAddr;

use data_encoding::BASE32_NOPAD;
use freedom_ipfs_addr::{canonical, classify_host, DialTarget, Refusal, Shape, Target};
use multiaddr::{Multiaddr, Protocol};

/// Classifies a multiaddr that is to be dialed, after validating its whole
/// protocol stack (design 0007, "The gate"); see [`classify_dialed`].
pub fn classify_multiaddr(addr: &Multiaddr) -> Result<Target, Refusal> {
    classify_dialed(addr).map(|dial| dial.target)
}

/// Classifies a multiaddr that is to be dialed, after validating its whole
/// protocol stack (design 0007, "The gate"), with the port it reaches and
/// its shape.
///
/// Checking the first component isn't enough: libp2p's TCP transport dials
/// the last IP and TCP pair of an address, and its DNS layer resolves every
/// DNS component, wherever they are. So an address is accepted only in one
/// of the shapes the node dials, with exactly one host component, first:
///
/// - `<host>/tcp/<port>`, then nothing or one WebSocket suffix: `/ws`,
///   `/wss` or `/tls/ws` (`/x-parity-ws/<path>` and `/x-parity-wss/<path>`
///   count as `/ws` and `/wss`);
/// - an HTTP provider: `<host>`, an optional `/tcp/<port>`, then `/http`,
///   `/https` or `/tls/http`. Without `/tcp`, the port is 80 for HTTP and
///   443 for HTTPS;
/// - `<host>/udp/<port>/quic-v1`;
/// - `/dnsaddr/<name>`;
/// - `/onion3/<address>:<port>`, or `/onion/<address>:<port>`;
///
/// each optionally ending in one `/p2p/<peer>`. A host is `/ip4` or `/ip6`,
/// a literal, or `/dns`, `/dns4` or `/dns6`, classified as
/// [`classify_host`] does (so `/dns/x.onion` is an onion, and
/// `/dns/127.0.0.1` a literal).
///
/// Everything else is refused, wherever it appears: a second host
/// component, an extra trailing component, a second `/p2p`, `/sni` (the
/// pinned WebSocket transport only sees TLS when `/tls` immediately precedes
/// `/ws`, and an HTTP provider's URL would lose its TLS), `/ip6zone`,
/// `/unix`, `/memory`, `/ipcidr`, `/p2p-circuit`, `/certhash`,
/// `/webtransport`, `/webrtc*`, the old `/quic`.
pub fn classify_dialed(addr: &Multiaddr) -> Result<DialTarget, Refusal> {
    let mut protocols: Vec<Protocol<'_>> = addr.iter().collect();
    if matches!(protocols.last(), Some(Protocol::P2p(_))) {
        protocols.pop();
    }
    let (target, port, shape) = match protocols.as_slice() {
        [host, Protocol::Tcp(port), rest @ ..] => {
            let shape = match rest {
                [] => Shape::Tcp,
                [Protocol::Ws(_)] => Shape::Ws,
                [Protocol::Wss(_)] | [Protocol::Tls, Protocol::Ws(_)] => Shape::Wss,
                [Protocol::Http] => Shape::Http,
                [Protocol::Https] | [Protocol::Tls, Protocol::Http] => Shape::Https,
                _ => return Err(Refusal::Multiaddr),
            };
            (host_target(host)?, Some(*port), shape)
        }
        [host, Protocol::Http] => (host_target(host)?, Some(80), Shape::Http),
        [host, Protocol::Https] | [host, Protocol::Tls, Protocol::Http] => {
            (host_target(host)?, Some(443), Shape::Https)
        }
        [host, Protocol::Udp(port), Protocol::QuicV1] => {
            (host_target(host)?, Some(*port), Shape::QuicV1)
        }
        [Protocol::Dnsaddr(name)] => (classify_host(name)?, None, Shape::Dnsaddr),
        [Protocol::Onion3(onion)] => (
            Target::Onion(onion_name(onion.hash())),
            Some(onion.port()),
            Shape::Onion,
        ),
        [Protocol::Onion(hash, port)] => (
            Target::Onion(onion_name(&hash[..])),
            Some(*port),
            Shape::Onion,
        ),
        _ => return Err(Refusal::Multiaddr),
    };
    Ok(DialTarget {
        target,
        port,
        shape,
    })
}

/// A shape's host component.
fn host_target(host: &Protocol<'_>) -> Result<Target, Refusal> {
    match host {
        Protocol::Ip4(ip) => Ok(Target::Ip(IpAddr::V4(*ip))),
        Protocol::Ip6(ip) => Ok(Target::Ip(canonical(IpAddr::V6(*ip)))),
        Protocol::Dns(name) | Protocol::Dns4(name) | Protocol::Dns6(name) => classify_host(name),
        _ => Err(Refusal::Multiaddr),
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
    use freedom_ipfs_addr::OnionHost;

    /// A v3 address's label: 32 key bytes, 2 checksum bytes, the version.
    fn v3_label() -> String {
        let mut bytes = [7u8; 35];
        bytes[34] = 3;
        BASE32_NOPAD.encode(&bytes).to_ascii_lowercase()
    }

    #[test]
    fn an_onion3_component_takes_the_onion_route() {
        let v3 = format!("{}.onion", v3_label());
        let onion3 =
            classify_multiaddr(&format!("/onion3/{}:4001", v3_label()).parse().unwrap()).unwrap();
        assert_eq!(OnionHost::new(&onion3).unwrap().as_str(), v3);
        // A `/onion` (v2) component is an onion, but not one the route takes.
        let v2 = classify_multiaddr(&"/onion/aaimaq4ygg2iegci:80".parse().unwrap()).unwrap();
        assert!(v2.is_onion());
        assert_eq!(OnionHost::new(&v2), None);
    }
}
