//! NAT64 prefixes and what discovery knows of them (design 0007,
//! "Network-specific NAT64 prefixes").
//!
//! An IPv6-only network can translate through a network-specific prefix of
//! its own (RFC 6052), which may reach private IPv4 addresses. Addresses
//! under it look like ordinary global unicast, so in `direct` mode the gate
//! needs the prefixes discovery has learned: while discovery is pending,
//! every IPv6 target but an IPv4-mapped one waits for it, and while it is
//! unknown, IPv6 outside the fixed forms is refused. Discovery itself (which
//! resolvers, the queries, refresh) belongs to the host; this module holds
//! its result and the RFC 6052 arithmetic.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::cidr::Cidr;

/// The addresses an `ipv4only.arpa` AAAA answer embeds when a DNS64
/// synthesized it (RFC 7050 §2.2).
const IPV4ONLY_ARPA: [Ipv4Addr; 2] = [Ipv4Addr::new(192, 0, 0, 170), Ipv4Addr::new(192, 0, 0, 171)];

/// A NAT64 prefix of one of RFC 6052's lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Nat64Prefix {
    cidr: Cidr,
}

impl Nat64Prefix {
    /// The prefix lengths RFC 6052 §2.2 defines.
    pub const LENGTHS: [u8; 6] = [32, 40, 48, 56, 64, 96];

    /// The prefix of `len` bits of `addr`, its other bits cleared; `None` if
    /// `len` isn't one of [`Nat64Prefix::LENGTHS`].
    pub fn new(addr: Ipv6Addr, len: u8) -> Option<Self> {
        if !Self::LENGTHS.contains(&len) {
            return None;
        }
        let mask = u128::MAX << (128 - u32::from(len));
        let net = Ipv6Addr::from(u128::from(addr) & mask);
        Cidr::new(IpAddr::V6(net), len).map(|cidr| Self { cidr })
    }

    /// The prefix as a block.
    pub fn cidr(&self) -> Cidr {
        self.cidr
    }

    /// The IPv4 address `addr` embeds under this prefix, at the place
    /// RFC 6052 §2.2 gives for its length (bits 64–71 skipped); `None` if
    /// `addr` isn't under it.
    pub fn embedded_ipv4(&self, addr: Ipv6Addr) -> Option<Ipv4Addr> {
        if !self.cidr.contains_v6(addr) {
            return None;
        }
        Some(embedded_at(addr, self.cidr.prefix_len()))
    }

    /// Every prefix an `ipv4only.arpa` AAAA answer teaches: one for each
    /// place where it embeds `192.0.0.170` or `192.0.0.171` (RFC 7050 §3).
    /// An answer that embeds neither teaches nothing. A prefix only adds
    /// refusals, so where an answer could be read at more than one length,
    /// each is kept: that errs on the safe side.
    pub fn in_ipv4only_answer(answer: Ipv6Addr) -> Vec<Self> {
        Self::LENGTHS
            .iter()
            .filter(|len| IPV4ONLY_ARPA.contains(&embedded_at(answer, **len)))
            .filter_map(|len| Self::new(answer, *len))
            .collect()
    }
}

/// The IPv4 address embedded after a prefix of `len` bits (RFC 6052 §2.2):
/// the address's 32 bits from bit `len` on, skipping bits 64–71.
fn embedded_at(addr: Ipv6Addr, len: u8) -> Ipv4Addr {
    let bytes = addr.octets();
    let start = usize::from(len / 8);
    let mut v4 = [0u8; 4];
    let mut at = start;
    for byte in &mut v4 {
        if at == 8 {
            at += 1;
        }
        *byte = bytes.get(at).copied().unwrap_or(0);
        at += 1;
    }
    Ipv4Addr::from(v4)
}

/// What NAT64 discovery knows of the network's prefixes (design 0007,
/// "States"). The host's discovery keeps it; the gate reads it.
///
/// Every prefix a known or unknown state holds is applied: learned prefixes
/// are kept for the process's life, so a failed refresh never forgets one,
/// and a prefix only adds refusals. The state decides what else waits or is
/// refused: every IPv6 address but an IPv4-mapped one waits while pending,
/// IPv6 outside the fixed forms is refused while unknown, and every IPv6
/// address but an IPv4-mapped one is refused while incomplete.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Nat64State {
    /// The first query hasn't settled. Every IPv6 target but an IPv4-mapped
    /// one waits for it, within its own deadline, fixed forms included: the
    /// first answer may teach a prefix under one. The gate refuses such a
    /// target with
    /// [`AddressRefusal::Nat64Pending`](super::AddressRefusal::Nat64Pending)
    /// (unless its own reading already refuses it), and the caller asks
    /// again once the state changes.
    Pending,
    /// Every resolver learned has answered: the union of their prefixes.
    /// An empty set means none of them advertises a prefix.
    Known(Vec<Nat64Prefix>),
    /// A resolver failed: IPv6 targets outside the fixed forms are refused,
    /// and the prefixes learned before still apply.
    Unknown(Vec<Nat64Prefix>),
    /// More than [`Nat64State::MAX_PREFIXES`] prefixes were learned, so some
    /// were dropped, and a dropped one could map any IPv6 address: every
    /// IPv6 target is refused, the fixed forms' included. Only an
    /// IPv4-mapped address passes, judged as its IPv4 address. It keeps the
    /// first [`Nat64State::MAX_PREFIXES`].
    Incomplete(Vec<Nat64Prefix>),
}

impl Nat64State {
    /// The most distinct prefixes a state holds; past it, it is
    /// [`Nat64State::Incomplete`].
    pub const MAX_PREFIXES: usize = 16;

    /// No prefix: a network known to advertise none, or `socks5` mode, where
    /// nothing is learned because the local translator is never used.
    pub const NONE: Self = Self::Known(Vec::new());

    /// The state for `prefixes`, the union of every answer: [`Known`] with
    /// each distinct prefix once, or [`Incomplete`] with the first
    /// [`Nat64State::MAX_PREFIXES`] past that.
    ///
    /// [`Known`]: Nat64State::Known
    /// [`Incomplete`]: Nat64State::Incomplete
    pub fn known(prefixes: impl IntoIterator<Item = Nat64Prefix>) -> Self {
        match distinct(prefixes) {
            Ok(prefixes) => Self::Known(prefixes),
            Err(first) => Self::Incomplete(first),
        }
    }

    /// The state after a resolver failed, with every prefix learned so far:
    /// [`Unknown`], or [`Incomplete`] past [`Nat64State::MAX_PREFIXES`].
    ///
    /// [`Unknown`]: Nat64State::Unknown
    /// [`Incomplete`]: Nat64State::Incomplete
    pub fn unknown(prefixes: impl IntoIterator<Item = Nat64Prefix>) -> Self {
        match distinct(prefixes) {
            Ok(prefixes) => Self::Unknown(prefixes),
            Err(first) => Self::Incomplete(first),
        }
    }

    /// The learned prefixes it holds, applied in every state.
    pub fn prefixes(&self) -> &[Nat64Prefix] {
        match self {
            Self::Pending => &[],
            Self::Known(prefixes) | Self::Unknown(prefixes) | Self::Incomplete(prefixes) => {
                prefixes
            }
        }
    }
}

/// Each distinct prefix once: `Err` with the first
/// [`Nat64State::MAX_PREFIXES`] if there are more.
fn distinct(
    prefixes: impl IntoIterator<Item = Nat64Prefix>,
) -> Result<Vec<Nat64Prefix>, Vec<Nat64Prefix>> {
    let mut distinct: Vec<Nat64Prefix> = Vec::new();
    for prefix in prefixes {
        if !distinct.contains(&prefix) {
            if distinct.len() == Nat64State::MAX_PREFIXES {
                return Err(distinct);
            }
            distinct.push(prefix);
        }
    }
    Ok(distinct)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v6(text: &str) -> Ipv6Addr {
        text.parse().unwrap()
    }

    /// RFC 6052 §2.4's examples: 192.0.2.33 under each prefix length.
    #[test]
    fn embeds_at_rfc_6052_positions() {
        let examples = [
            ("2001:db8::", 32, "2001:db8:c000:221::"),
            ("2001:db8:100::", 40, "2001:db8:1c0:2:21::"),
            ("2001:db8:122::", 48, "2001:db8:122:c000:2:2100::"),
            ("2001:db8:122:300::", 56, "2001:db8:122:3c0:0:221::"),
            ("2001:db8:122:344::", 64, "2001:db8:122:344:c0:2:2100:0"),
            ("2001:db8:122:344::", 96, "2001:db8:122:344::c000:221"),
        ];
        for (prefix, len, addr) in examples {
            let prefix = Nat64Prefix::new(v6(prefix), len).unwrap();
            assert_eq!(
                prefix.embedded_ipv4(v6(addr)),
                Some(Ipv4Addr::new(192, 0, 2, 33)),
                "{addr} under /{len}"
            );
        }
        let prefix = Nat64Prefix::new(v6("2001:db8::"), 32).unwrap();
        assert_eq!(prefix.embedded_ipv4(v6("2001:db9:c000:221::")), None);
    }

    #[test]
    fn only_rfc_6052_lengths_and_cleared_host_bits() {
        assert_eq!(Nat64Prefix::new(v6("2001:db8::"), 33), None);
        assert_eq!(Nat64Prefix::new(v6("2001:db8::"), 128), None);
        let prefix = Nat64Prefix::new(v6("2001:db8:1:2::5"), 32).unwrap();
        assert_eq!(prefix.cidr().to_string(), "2001:db8::/32");
    }

    #[test]
    fn an_ipv4only_answer_teaches_the_prefix_it_embeds_at() {
        // 192.0.0.170 is c000:00aa.
        assert_eq!(
            Nat64Prefix::in_ipv4only_answer(v6("64:ff9b::c000:aa")),
            vec![Nat64Prefix::new(v6("64:ff9b::"), 96).unwrap()]
        );
        assert_eq!(
            Nat64Prefix::in_ipv4only_answer(v6("2a00:1:c000:ab::")),
            vec![Nat64Prefix::new(v6("2a00:1::"), 32).unwrap()]
        );
        assert_eq!(
            Nat64Prefix::in_ipv4only_answer(v6("2a00:1:2:3:c0:0:aa00:0")),
            vec![Nat64Prefix::new(v6("2a00:1:2:3::"), 64).unwrap()]
        );
        // Neither well-known address: nothing.
        assert!(Nat64Prefix::in_ipv4only_answer(v6("2a00:1:2:3::808:808")).is_empty());
        assert!(Nat64Prefix::in_ipv4only_answer(v6("2a00:1::1")).is_empty());
    }

    #[test]
    fn past_sixteen_distinct_prefixes_the_state_is_incomplete() {
        let prefix =
            |i: u16| Nat64Prefix::new(Ipv6Addr::new(0x2a00, 1, i, 0, 0, 0, 0, 0), 48).unwrap();
        assert_eq!(
            Nat64State::known((1..=16).map(prefix)),
            Nat64State::Known((1..=16).map(prefix).collect())
        );
        // Repeats count once.
        assert_eq!(
            Nat64State::known((1..=16).chain(1..=16).map(prefix)),
            Nat64State::Known((1..=16).map(prefix).collect())
        );
        assert_eq!(
            Nat64State::known((1..=17).map(prefix)),
            Nat64State::Incomplete((1..=16).map(prefix).collect())
        );
        assert_eq!(
            Nat64State::unknown((1..=16).map(prefix)),
            Nat64State::Unknown((1..=16).map(prefix).collect())
        );
        assert_eq!(
            Nat64State::unknown((1..=17).map(prefix)),
            Nat64State::Incomplete((1..=16).map(prefix).collect())
        );
        // No prefix, as after NODATA from every resolver: known and empty.
        assert_eq!(Nat64State::known([]), Nat64State::NONE);
    }
}
