//! The gate's one table (design 0007, "Addresses").
//!
//! Its source is IANA's IPv4 and IPv6 special-purpose address registries
//! (RFC 6890), as the design read them on 2026-10-19: every block whose
//! *Globally Reachable* is False or N/A is refused, and so is the multicast
//! space and IPv6 outside global unicast. Where a form's embedding of an
//! IPv4 address can be read, the IPv4 policy decides instead
//! ([`EMBEDDED_FORMS`]).
//!
//! **A registry change is a design change:** the rows mirror the design's
//! tables, and a test pins them to the design's text, row by row.

use super::cidr::Cidr;

/// A row of the design's tables: its blocks, what they are, and the
/// reference.
#[derive(Debug, PartialEq, Eq)]
pub struct Row {
    pub blocks: &'static [Cidr],
    pub what: &'static str,
    pub reference: &'static str,
}

/// The refused IPv4 blocks (design 0007, "IPv4").
///
/// `192.0.0.0/24` is refused whole: it holds the service continuity prefix,
/// the dummy address, the NAT64 discovery addresses and the PCP and TURN
/// anycast addresses. The globally reachable registry blocks, AS112
/// (`192.31.196.0/24`, `192.175.48.0/24`) and AMT (`192.52.193.0/24`),
/// aren't here, so they pass.
pub static IPV4_REFUSED: &[Row] = &[
    Row {
        blocks: &[Cidr::v4([0, 0, 0, 0], 8)],
        what: "\"this network\", with 0.0.0.0 (\"this host\")",
        reference: "RFC 791 §3.2; RFC 1122 §3.2.1.3",
    },
    Row {
        blocks: &[
            Cidr::v4([10, 0, 0, 0], 8),
            Cidr::v4([172, 16, 0, 0], 12),
            Cidr::v4([192, 168, 0, 0], 16),
        ],
        what: "private use",
        reference: "RFC 1918",
    },
    Row {
        blocks: &[Cidr::v4([100, 64, 0, 0], 10)],
        what: "shared address space (CGNAT)",
        reference: "RFC 6598",
    },
    Row {
        blocks: &[Cidr::v4([127, 0, 0, 0], 8)],
        what: "loopback",
        reference: "RFC 1122 §3.2.1.3",
    },
    Row {
        blocks: &[Cidr::v4([169, 254, 0, 0], 16)],
        what: "link-local",
        reference: "RFC 3927",
    },
    Row {
        blocks: &[Cidr::v4([192, 0, 0, 0], 24)],
        what: "IETF protocol assignments, the whole block",
        reference: "RFC 6890 §2.1",
    },
    Row {
        blocks: &[
            Cidr::v4([192, 0, 2, 0], 24),
            Cidr::v4([198, 51, 100, 0], 24),
            Cidr::v4([203, 0, 113, 0], 24),
        ],
        what: "documentation",
        reference: "RFC 5737",
    },
    Row {
        blocks: &[Cidr::v4([192, 88, 99, 0], 24)],
        what: "the deprecated 6to4 relay anycast prefix, with the 6a44 relay at 192.88.99.2",
        reference: "RFC 7526; RFC 6751",
    },
    Row {
        blocks: &[Cidr::v4([198, 18, 0, 0], 15)],
        what: "benchmarking",
        reference: "RFC 2544",
    },
    Row {
        blocks: &[Cidr::v4([224, 0, 0, 0], 4)],
        what: "multicast",
        reference: "RFC 5771",
    },
    Row {
        blocks: &[Cidr::v4([240, 0, 0, 0], 4)],
        what: "reserved, with the limited broadcast address 255.255.255.255",
        reference: "RFC 1112 §4; RFC 919 §7; RFC 8190",
    },
];

/// IPv6 global unicast: only addresses inside it can pass, except as the
/// embedded-IPv4 forms allow (RFC 4291 §2.4).
pub const GLOBAL_UNICAST: Cidr = Cidr::v6([0x2000, 0, 0, 0, 0, 0, 0, 0], 3);

/// The refused IPv6 blocks outside global unicast (design 0007, "IPv6",
/// the first table). Everything else outside [`GLOBAL_UNICAST`] is refused
/// too; these rows name what a refusal is.
pub static IPV6_REFUSED_OUTSIDE: &[Row] = &[
    Row {
        blocks: &[
            Cidr::v6([0; 8], 128),
            Cidr::v6([0, 0, 0, 0, 0, 0, 0, 1], 128),
        ],
        what: "unspecified; loopback",
        reference: "RFC 4291 §2.5.2, §2.5.3",
    },
    Row {
        blocks: &[Cidr::v6([0; 8], 96)],
        what: "IPv4-compatible, deprecated",
        reference: "RFC 4291 §2.5.5.1",
    },
    Row {
        blocks: &[Cidr::v6([0, 0, 0, 0, 0xffff, 0, 0, 0], 96)],
        what: "IPv4-translated",
        reference: "RFC 2765 §2.1",
    },
    Row {
        blocks: &[Cidr::v6([0x64, 0xff9b, 1, 0, 0, 0, 0, 0], 48)],
        what: "local-use IPv4/IPv6 translation",
        reference: "RFC 8215",
    },
    Row {
        blocks: &[
            Cidr::v6([0x100, 0, 0, 0, 0, 0, 0, 0], 64),
            Cidr::v6([0x100, 0, 0, 1, 0, 0, 0, 0], 64),
        ],
        what: "discard-only; the dummy prefix",
        reference: "RFC 6666; RFC 9780",
    },
    Row {
        blocks: &[Cidr::v6([0x5f00, 0, 0, 0, 0, 0, 0, 0], 16)],
        what: "SRv6 segment identifiers",
        reference: "RFC 9602",
    },
    Row {
        blocks: &[Cidr::v6([0xfc00, 0, 0, 0, 0, 0, 0, 0], 7)],
        what: "unique local",
        reference: "RFC 4193",
    },
    Row {
        blocks: &[Cidr::v6([0xfe80, 0, 0, 0, 0, 0, 0, 0], 10)],
        what: "link-local",
        reference: "RFC 4291 §2.5.6",
    },
    Row {
        blocks: &[Cidr::v6([0xfec0, 0, 0, 0, 0, 0, 0, 0], 10)],
        what: "site-local, deprecated",
        reference: "RFC 3879",
    },
    Row {
        blocks: &[Cidr::v6([0xff00, 0, 0, 0, 0, 0, 0, 0], 8)],
        what: "multicast",
        reference: "RFC 4291 §2.7",
    },
];

/// The refused IPv6 blocks inside global unicast (design 0007, "IPv6",
/// the second table).
///
/// `2001::/23` is refused whole: Teredo (`2001::/32`), benchmarking, ORCHID
/// and ORCHIDv2, drone entity tags, the PCP, TURN, DNS-SD and AMT anycast
/// services, and AS112-v6. The globally reachable AS112 block
/// `2620:4f:8000::/48` isn't here, so it passes.
pub static IPV6_REFUSED_INSIDE: &[Row] = &[
    Row {
        blocks: &[Cidr::v6([0x2001, 0, 0, 0, 0, 0, 0, 0], 23)],
        what: "IETF protocol assignments, the whole block",
        reference: "RFC 2928",
    },
    Row {
        blocks: &[
            Cidr::v6([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0], 32),
            Cidr::v6([0x3fff, 0, 0, 0, 0, 0, 0, 0], 20),
        ],
        what: "documentation",
        reference: "RFC 3849; RFC 9637",
    },
];

/// IPv4-mapped addresses, classified as the IPv4 address in their last 32
/// bits; the connection goes to that IPv4 address.
pub const IPV4_MAPPED: Cidr = Cidr::v6([0, 0, 0, 0, 0, 0xffff, 0, 0], 96);

/// NAT64's well-known prefix: passes only if the IPv4 address in the last
/// 32 bits passes; the connection goes to the IPv6 address.
pub const NAT64_WELL_KNOWN: Cidr = Cidr::v6([0x64, 0xff9b, 0, 0, 0, 0, 0, 0], 96);

/// 6to4: passes only if the IPv4 address in bits 16–47 passes.
pub const SIX_TO_FOUR: Cidr = Cidr::v6([0x2002, 0, 0, 0, 0, 0, 0, 0], 16);

/// The starts of an ISATAP interface identifier, its bits 64–95: the
/// `u` and `g` bits may be set. Under any prefix, the IPv4 address in the
/// last 32 bits must pass, and so must the IPv6 address on its own.
pub const ISATAP_INTERFACE_IDS: [u32; 4] = [0x0000_5efe, 0x0100_5efe, 0x0200_5efe, 0x0300_5efe];

/// The IPv6 forms whose embedded IPv4 address the IPv4 policy decides
/// (design 0007, "IPv6 forms that embed IPv4"), in the design's order.
pub static EMBEDDED_FORMS: &[EmbeddedForm] = &[
    EmbeddedForm {
        name: "IPv4-mapped",
        prefix: Some(IPV4_MAPPED),
        reference: "RFC 4291 §2.5.5.2",
    },
    EmbeddedForm {
        name: "NAT64 with the well-known prefix",
        prefix: Some(NAT64_WELL_KNOWN),
        reference: "RFC 6052 §2.1, §2.2",
    },
    EmbeddedForm {
        name: "NAT64 with a learned network-specific prefix",
        prefix: None,
        reference: "RFC 6052 §2.2; RFC 7050",
    },
    EmbeddedForm {
        name: "6to4",
        prefix: Some(SIX_TO_FOUR),
        reference: "RFC 3056 §2",
    },
    EmbeddedForm {
        name: "ISATAP",
        prefix: None,
        reference: "RFC 5214 §6.1",
    },
];

/// A row of the design's table of forms that embed IPv4.
#[derive(Debug, PartialEq, Eq)]
pub struct EmbeddedForm {
    pub name: &'static str,
    /// Its fixed prefix; `None` for a learned prefix, and for ISATAP, which
    /// is found by its interface identifier under any prefix.
    pub prefix: Option<Cidr>,
    pub reference: &'static str,
}

/// The refused row holding `ip`, and the block, if any.
pub(crate) fn refused_row(
    rows: &'static [Row],
    ip: std::net::IpAddr,
) -> Option<(Cidr, &'static Row)> {
    rows.iter().find_map(|row| {
        row.blocks
            .iter()
            .find(|block| block.contains(ip))
            .map(|block| (*block, row))
    })
}
