//! The address policy (design 0007, "Addresses"): an untrusted target must
//! be a globally reachable unicast address.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::cidr::Cidr;
use super::nat64::{Nat64Prefix, Nat64State};
use super::table::{
    refused_row, Row, GLOBAL_UNICAST, IPV4_REFUSED, IPV6_REFUSED_INSIDE, IPV6_REFUSED_OUTSIDE,
    ISATAP_INTERFACE_IDS, NAT64_WELL_KNOWN, SIX_TO_FOUR,
};

/// An IPv6 form that embeds an IPv4 address the IPv4 policy decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Form {
    /// NAT64 with the well-known prefix, `64:ff9b::/96`.
    Nat64WellKnown,
    /// NAT64 with a learned network-specific prefix.
    Nat64Learned(Nat64Prefix),
    /// 6to4, `2002::/16`.
    SixToFour,
    /// An ISATAP interface identifier, under any prefix.
    Isatap,
}

impl fmt::Display for Form {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nat64WellKnown => f.write_str("NAT64 with the well-known prefix"),
            Self::Nat64Learned(prefix) => {
                write!(f, "NAT64 with the learned prefix {}", prefix.cidr())
            }
            Self::SixToFour => f.write_str("6to4"),
            Self::Isatap => f.write_str("ISATAP"),
        }
    }
}

/// Why the address policy refuses an address. Each text names the rule and
/// the block, never the address.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AddressRefusal {
    /// The address is in a refused block. An IPv4-mapped address is refused
    /// this way too: it is classified as its IPv4 address.
    #[error("in {block}: {} ({})", row.what, row.reference)]
    Block { block: Cidr, row: &'static Row },
    /// IPv6 outside global unicast (`2000::/3`), in no named block.
    #[error("IPv6 outside global unicast (2000::/3)")]
    NotGlobalUnicast,
    /// An IPv6 form that reaches the IPv4 address it embeds, which is in a
    /// refused block.
    #[error("{form} embeds an address in {block}: {} ({})", row.what, row.reference)]
    Embedded {
        form: Form,
        block: Cidr,
        row: &'static Row,
    },
    /// NAT64 discovery hasn't settled: the caller waits for it, within its
    /// own deadline, and asks again.
    #[error("IPv6 outside the fixed forms while NAT64 discovery is pending")]
    Nat64Pending,
    /// NAT64 discovery failed for a resolver: IPv6 outside the fixed forms
    /// is refused until it succeeds.
    #[error("IPv6 outside the fixed forms while NAT64 discovery has failed")]
    Nat64Unknown,
    /// NAT64 discovery learned more prefixes than it keeps.
    #[error("IPv6 outside the fixed forms while NAT64 discovery is incomplete")]
    Nat64Incomplete,
}

/// The address policy, for an untrusted target's address: `Ok` with the
/// address to connect to, or why it is refused.
///
/// - **IPv4:** refused in any of [`IPV4_REFUSED`]'s blocks.
/// - **IPv4-mapped IPv6** (`::ffff:0:0/96`) is classified as its IPv4
///   address, and the connection goes to that IPv4 address.
/// - **Other IPv6** is refused in a named block, or outside global unicast
///   unless it is under NAT64's well-known prefix. Then every embedding it
///   can be read with must pass:
///   - NAT64's well-known prefix (`64:ff9b::/96`): its last 32 bits; the
///     connection goes to the IPv6 address;
///   - 6to4 (`2002::/16`): bits 16–47;
///   - an ISATAP interface identifier, under any prefix: its last 32 bits;
///   - every learned prefix in `nat64` the address is under, in every
///     state: the IPv4 address at RFC 6052's place. A learned prefix inside
///     a fixed form's space decides too.
/// - **Then `nat64`'s state:** an address under neither fixed form (the
///   well-known prefix, 6to4) is refused while discovery is pending,
///   unknown or incomplete. The fixed forms are exempt from that only.
///
/// 6rd, 6over4 and 6a44, whose embedding depends on configuration, are
/// judged as the IPv6 address alone.
pub fn check_address(ip: IpAddr, nat64: &Nat64State) -> Result<IpAddr, AddressRefusal> {
    match ip {
        IpAddr::V4(v4) => check_v4(v4).map(|()| ip),
        IpAddr::V6(v6) => check_v6(v6, nat64),
    }
}

fn check_v4(v4: Ipv4Addr) -> Result<(), AddressRefusal> {
    match refused_row(IPV4_REFUSED, IpAddr::V4(v4)) {
        Some((block, row)) => Err(AddressRefusal::Block { block, row }),
        None => Ok(()),
    }
}

/// The IPv4 address an embedding form reaches: the form is refused if it
/// is.
fn check_embedded(form: Form, v4: Ipv4Addr) -> Result<(), AddressRefusal> {
    match refused_row(IPV4_REFUSED, IpAddr::V4(v4)) {
        Some((block, row)) => Err(AddressRefusal::Embedded { form, block, row }),
        None => Ok(()),
    }
}

fn check_v6(v6: Ipv6Addr, nat64: &Nat64State) -> Result<IpAddr, AddressRefusal> {
    // Classified as the IPv4 address, and connected to as one: no IPv6
    // packet is sent, so no translator, and no learned prefix, is involved.
    if let Some(v4) = v6.to_ipv4_mapped() {
        return check_v4(v4).map(|()| IpAddr::V4(v4));
    }
    let well_known = NAT64_WELL_KNOWN.contains_v6(v6);
    if !well_known {
        for rows in [IPV6_REFUSED_OUTSIDE, IPV6_REFUSED_INSIDE] {
            if let Some((block, row)) = refused_row(rows, IpAddr::V6(v6)) {
                return Err(AddressRefusal::Block { block, row });
            }
        }
        if !GLOBAL_UNICAST.contains_v6(v6) {
            return Err(AddressRefusal::NotGlobalUnicast);
        }
    }
    // Every embedding the address can be read with must pass: its fixed
    // form's, ISATAP's, and every learned prefix's it is under, so a
    // network-specific prefix inside a fixed form's space still decides.
    if well_known {
        check_embedded(Form::Nat64WellKnown, last_32(v6))?;
    }
    let six_to_four = SIX_TO_FOUR.contains_v6(v6);
    if six_to_four {
        let o = v6.octets();
        check_embedded(Form::SixToFour, Ipv4Addr::new(o[2], o[3], o[4], o[5]))?;
    }
    if is_isatap(v6) {
        check_embedded(Form::Isatap, last_32(v6))?;
    }
    for prefix in nat64.prefixes() {
        if let Some(v4) = prefix.embedded_ipv4(v6) {
            check_embedded(Form::Nat64Learned(*prefix), v4)?;
        }
    }
    // The fixed forms are exempt only from the refusals for what discovery
    // doesn't know yet.
    if well_known || six_to_four {
        return Ok(IpAddr::V6(v6));
    }
    match nat64 {
        Nat64State::Pending => Err(AddressRefusal::Nat64Pending),
        Nat64State::Unknown(_) => Err(AddressRefusal::Nat64Unknown),
        Nat64State::Incomplete(_) => Err(AddressRefusal::Nat64Incomplete),
        Nat64State::Known(_) => Ok(IpAddr::V6(v6)),
    }
}

fn last_32(v6: Ipv6Addr) -> Ipv4Addr {
    let o = v6.octets();
    Ipv4Addr::new(o[12], o[13], o[14], o[15])
}

/// Whether the interface identifier starts as ISATAP's does (RFC 5214
/// §6.1): bits 64–95 are `0000:5efe`, with the `u` and `g` bits free.
fn is_isatap(v6: Ipv6Addr) -> bool {
    let o = v6.octets();
    let id = u32::from_be_bytes([o[8], o[9], o[10], o[11]]);
    ISATAP_INTERFACE_IDS.contains(&id)
}
