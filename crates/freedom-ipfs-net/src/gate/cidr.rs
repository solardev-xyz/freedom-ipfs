//! Address blocks.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// An address block: an address and a prefix length, with the host bits
/// zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cidr {
    addr: IpAddr,
    len: u8,
}

impl Cidr {
    /// An IPv4 block, for the table. Its host bits must be zero (a test
    /// checks every block of the table).
    pub const fn v4(octets: [u8; 4], len: u8) -> Self {
        Self {
            addr: IpAddr::V4(Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3])),
            len,
        }
    }

    /// An IPv6 block, for the table. Its host bits must be zero (a test
    /// checks every block of the table).
    pub const fn v6(s: [u16; 8], len: u8) -> Self {
        Self {
            addr: IpAddr::V6(Ipv6Addr::new(
                s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
            )),
            len,
        }
    }

    /// A block from `<address>/<length>`: `None` if it doesn't parse, the
    /// length is too long for the family, or a host bit is set.
    pub fn parse(text: &str) -> Option<Self> {
        let (addr, len) = text.split_once('/')?;
        Self::new(addr.parse().ok()?, len.parse().ok()?)
    }

    /// The block of `len` bits at `addr`: `None` if the length is too long
    /// for the family, or a host bit is set.
    pub fn new(addr: IpAddr, len: u8) -> Option<Self> {
        let cidr = Self { addr, len };
        (len <= width(addr) && cidr.first() == addr).then_some(cidr)
    }

    /// The block's first address.
    pub fn addr(&self) -> IpAddr {
        self.addr
    }

    /// The prefix length.
    pub fn prefix_len(&self) -> u8 {
        self.len
    }

    /// Whether `ip` is inside the block. An address of the other family
    /// never is.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = mask32(self.len);
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = mask128(self.len);
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    }

    /// Whether the IPv6 address `ip` is inside the block.
    pub fn contains_v6(&self, ip: Ipv6Addr) -> bool {
        self.contains(IpAddr::V6(ip))
    }

    /// The block's first address, with any host bit cleared.
    pub fn first(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(net) => IpAddr::V4(Ipv4Addr::from(u32::from(net) & mask32(self.len))),
            IpAddr::V6(net) => IpAddr::V6(Ipv6Addr::from(u128::from(net) & mask128(self.len))),
        }
    }

    /// The block's last address.
    pub fn last(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(net) => IpAddr::V4(Ipv4Addr::from(u32::from(net) | !mask32(self.len))),
            IpAddr::V6(net) => IpAddr::V6(Ipv6Addr::from(u128::from(net) | !mask128(self.len))),
        }
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.addr {
            IpAddr::V4(addr) => write!(f, "{addr}/{}", self.len),
            // Rust writes `::ffff:0:0` as `::ffff:0.0.0.0`; the segments
            // keep the block's own notation for every block of the table.
            IpAddr::V6(addr) => write!(f, "{}/{}", v6_text(addr), self.len),
        }
    }
}

fn width(addr: IpAddr) -> u8 {
    match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

fn mask32(len: u8) -> u32 {
    match len {
        0 => 0,
        len => u32::MAX << (32 - u32::from(len.min(32))),
    }
}

fn mask128(len: u8) -> u128 {
    match len {
        0 => 0,
        len => u128::MAX << (128 - u32::from(len.min(128))),
    }
}

/// An IPv6 address in hex segments, the longest run of zero segments
/// compressed, with no dotted IPv4 tail.
fn v6_text(addr: Ipv6Addr) -> String {
    let segments = addr.segments();
    // The longest run of two or more zero segments, the first one on a tie.
    let mut best: Option<(usize, usize)> = None;
    let mut run: Option<(usize, usize)> = None;
    for (i, segment) in segments.iter().enumerate() {
        if *segment == 0 {
            let (start, len) = run.map_or((i, 1), |(start, len)| (start, len + 1));
            run = Some((start, len));
            if len >= 2 && best.is_none_or(|(_, best_len)| len > best_len) {
                best = Some((start, len));
            }
        } else {
            run = None;
        }
    }
    let hex = |range: &[u16]| {
        range
            .iter()
            .map(|segment| format!("{segment:x}"))
            .collect::<Vec<_>>()
            .join(":")
    };
    match best {
        Some((start, len)) => format!(
            "{}::{}",
            hex(&segments[..start]),
            hex(&segments[start + len..])
        ),
        None => hex(&segments),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_first_and_last() {
        let net = Cidr::v4([172, 16, 0, 0], 12);
        assert!(net.contains("172.16.0.0".parse().unwrap()));
        assert!(net.contains("172.31.255.255".parse().unwrap()));
        assert!(!net.contains("172.32.0.0".parse().unwrap()));
        assert!(!net.contains("172.15.255.255".parse().unwrap()));
        assert!(!net.contains("::ffff:172.16.0.1".parse().unwrap()));
        assert_eq!(net.last(), "172.31.255.255".parse::<IpAddr>().unwrap());

        let net = Cidr::v6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0], 32);
        assert!(net.contains("2001:db8:ffff::1".parse().unwrap()));
        assert!(!net.contains("2001:db9::".parse().unwrap()));
        assert_eq!(
            net.last(),
            "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"
                .parse::<IpAddr>()
                .unwrap()
        );

        let all = Cidr::v6([0; 8], 0);
        assert!(all.contains("ffff::".parse().unwrap()));
        let one = Cidr::v4([127, 0, 0, 1], 32);
        assert_eq!(one.first(), one.last());
    }

    #[test]
    fn parse_refuses_host_bits_and_long_prefixes() {
        assert_eq!(Cidr::parse("10.0.0.0/8"), Some(Cidr::v4([10, 0, 0, 0], 8)));
        assert_eq!(Cidr::parse("10.0.0.1/8"), None);
        assert_eq!(Cidr::parse("10.0.0.0/33"), None);
        assert_eq!(Cidr::parse("::/129"), None);
        assert_eq!(Cidr::parse("10.0.0.0"), None);
        assert_eq!(
            Cidr::parse("::ffff:0:0/96"),
            Some(Cidr::v6([0, 0, 0, 0, 0, 0xffff, 0, 0], 96))
        );
    }

    #[test]
    fn displays_in_the_tables_notation() {
        for text in [
            "0.0.0.0/8",
            "::/128",
            "::1/128",
            "::/96",
            "::ffff:0:0/96",
            "::ffff:0:0:0/96",
            "64:ff9b::/96",
            "64:ff9b:1::/48",
            "100::/64",
            "100:0:0:1::/64",
            "2001:db8::/32",
            "2620:4f:8000::/48",
            "fe80::/10",
        ] {
            assert_eq!(Cidr::parse(text).unwrap().to_string(), text);
        }
    }
}
