//! Pins the gate's table to design 0007's lists ("Addresses"): "the gate's
//! code holds one table of the blocks below … and a test pins the table to
//! the lists here. A registry change is a design change."
//!
//! The design's rows are copied below verbatim from freedom-core's
//! `docs/design/0007-ipfs-service.md` ("IPv4", "IPv6", "IPv6 forms that
//! embed IPv4"). A change to the table must come with the same change to
//! the design, and to the copy here.

use freedom_ipfs_net::gate::table::{
    Row, EMBEDDED_FORMS, GLOBAL_UNICAST, IPV4_REFUSED, IPV6_REFUSED_INSIDE, IPV6_REFUSED_OUTSIDE,
    ISATAP_INTERFACE_IDS,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use freedom_ipfs_net::gate::{check_address, Cidr, Nat64State};

/// "IPv4": Refused.
const DESIGN_IPV4: &str = r#"
| Block | What it is | Reference |
|---|---|---|
| `0.0.0.0/8` | "this network", with `0.0.0.0` ("this host") | RFC 791 §3.2; RFC 1122 §3.2.1.3 |
| `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16` | private use | RFC 1918 |
| `100.64.0.0/10` | shared address space (CGNAT) | RFC 6598 |
| `127.0.0.0/8` | loopback | RFC 1122 §3.2.1.3 |
| `169.254.0.0/16` | link-local | RFC 3927 |
| `192.0.0.0/24` | IETF protocol assignments, the whole block (see below) | RFC 6890 §2.1 |
| `192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24` | documentation | RFC 5737 |
| `192.88.99.0/24` | the deprecated 6to4 relay anycast prefix, with the 6a44 relay at `192.88.99.2` | RFC 7526; RFC 6751 |
| `198.18.0.0/15` | benchmarking | RFC 2544 |
| `224.0.0.0/4` | multicast | RFC 5771 |
| `240.0.0.0/4` | reserved, with the limited broadcast address `255.255.255.255` | RFC 1112 §4; RFC 919 §7; RFC 8190 |
"#;

/// "IPv6": outside global unicast.
const DESIGN_IPV6_OUTSIDE: &str = r#"
| Block | What it is | Reference |
|---|---|---|
| `::/128`, `::1/128` | unspecified; loopback | RFC 4291 §2.5.2, §2.5.3 |
| `::/96` | IPv4-compatible, deprecated | RFC 4291 §2.5.5.1 |
| `::ffff:0:0:0/96` | IPv4-translated | RFC 2765 §2.1 |
| `64:ff9b:1::/48` | local-use IPv4/IPv6 translation | RFC 8215 |
| `100::/64`; `100:0:0:1::/64` | discard-only; the dummy prefix | RFC 6666; RFC 9780 |
| `5f00::/16` | SRv6 segment identifiers | RFC 9602 |
| `fc00::/7` | unique local | RFC 4193 |
| `fe80::/10` | link-local | RFC 4291 §2.5.6 |
| `fec0::/10` | site-local, deprecated | RFC 3879 |
| `ff00::/8` | multicast | RFC 4291 §2.7 |
"#;

/// "IPv6": Refused inside `2000::/3`.
const DESIGN_IPV6_INSIDE: &str = r#"
| Block | What it is | Reference |
|---|---|---|
| `2001::/23` | IETF protocol assignments, the whole block (see below) | RFC 2928 |
| `2001:db8::/32`, `3fff::/20` | documentation | RFC 3849; RFC 9637 |
"#;

/// "IPv6 forms that embed IPv4".
const DESIGN_EMBEDDED: &str = r#"
| Form | Where the IPv4 address is | Rule | Reference |
|---|---|---|---|
| IPv4-mapped, `::ffff:0:0/96` | the last 32 bits | **classified as** that IPv4 address; the connection goes to the IPv4 address | RFC 4291 §2.5.5.2 |
| NAT64 with the well-known prefix, `64:ff9b::/96` | the last 32 bits | passes only if the IPv4 address passes; the connection goes to the IPv6 address, through the network's translator | RFC 6052 §2.1, §2.2 |
| NAT64 with a [learned network-specific prefix](#network-specific-nat64-prefixes) | at the place RFC 6052 §2.2 gives for the prefix's length (32, 40, 48, 56, 64 or 96), with bits 64–71 skipped | as for the well-known prefix | RFC 6052 §2.2; RFC 7050 |
| 6to4, `2002::/16` | bits 16–47 | passes only if the IPv4 address passes | RFC 3056 §2 |
| ISATAP: an interface identifier starting `0000:5efe`, `0100:5efe`, `0200:5efe` or `0300:5efe`, under any prefix | the last 32 bits | passes only if the IPv4 address passes **and** the IPv6 address passes on its own | RFC 5214 §6.1 |
"#;

/// "IPv6": "Only global unicast, `2000::/3`, can pass (RFC 4291 §2.4)."
const DESIGN_GLOBAL_UNICAST: &str = "2000::/3";

/// A markdown table's body rows, as their cells.
fn rows(table: &str) -> Vec<Vec<String>> {
    table
        .lines()
        .filter(|line| line.starts_with('|'))
        .skip(2) // the header and the separator
        .map(|line| {
            line.trim_matches('|')
                .split(" | ")
                .map(|cell| cell.trim().to_string())
                .collect()
        })
        .collect()
}

/// The backquoted tokens of a cell.
fn quoted(cell: &str) -> Vec<&str> {
    cell.split('`').skip(1).step_by(2).collect()
}

/// A cell's text as the code holds it: no backquotes, no "(see below)", no
/// links.
fn plain(cell: &str) -> String {
    let mut text = cell.replace('`', "").replace(" (see below)", "");
    while let (Some(open), Some(close)) = (text.find('['), text.find("](")) {
        let Some(end) = text[close..].find(')').map(|end| close + end) else {
            break;
        };
        text = format!(
            "{}{}{}",
            &text[..open],
            &text[open + 1..close],
            &text[end + 1..]
        );
    }
    text
}

fn assert_rows_match(design: &str, code: &[Row]) {
    let design = rows(design);
    assert_eq!(design.len(), code.len(), "the number of rows");
    for (cells, row) in design.iter().zip(code) {
        let blocks: Vec<Cidr> = quoted(&cells[0])
            .into_iter()
            .map(|text| Cidr::parse(text).unwrap_or_else(|| panic!("{text} parses")))
            .collect();
        assert_eq!(blocks, row.blocks, "the blocks of {:?}", cells[0]);
        assert_eq!(plain(&cells[1]), row.what, "what {:?} is", cells[0]);
        assert_eq!(cells[2], row.reference, "the reference of {:?}", cells[0]);
    }
}

#[test]
fn the_ipv4_rows_are_the_designs() {
    assert_rows_match(DESIGN_IPV4, IPV4_REFUSED);
}

#[test]
fn the_ipv6_rows_outside_global_unicast_are_the_designs() {
    assert_rows_match(DESIGN_IPV6_OUTSIDE, IPV6_REFUSED_OUTSIDE);
    assert_eq!(Cidr::parse(DESIGN_GLOBAL_UNICAST), Some(GLOBAL_UNICAST));
}

#[test]
fn the_ipv6_rows_inside_global_unicast_are_the_designs() {
    assert_rows_match(DESIGN_IPV6_INSIDE, IPV6_REFUSED_INSIDE);
}

#[test]
fn the_embedded_forms_are_the_designs() {
    let design = rows(DESIGN_EMBEDDED);
    assert_eq!(design.len(), EMBEDDED_FORMS.len(), "the number of forms");
    for (cells, form) in design.iter().zip(EMBEDDED_FORMS) {
        let first = plain(&cells[0]);
        let name = first
            .split([',', ':'])
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        assert_eq!(name, form.name);
        let prefix = quoted(&cells[0])
            .into_iter()
            .find_map(|text| text.contains('/').then(|| Cidr::parse(text).unwrap()));
        assert_eq!(prefix, form.prefix, "the prefix of {name}");
        assert_eq!(cells[3], form.reference, "the reference of {name}");
    }
    let isatap = rows(DESIGN_EMBEDDED)
        .into_iter()
        .find(|cells| cells[0].starts_with("ISATAP"))
        .unwrap();
    let ids: Vec<u32> = quoted(&isatap[0])
        .into_iter()
        .map(|text| u32::from_str_radix(&text.replace(':', ""), 16).unwrap())
        .collect();
    assert_eq!(ids, ISATAP_INTERFACE_IDS);
}

#[test]
fn every_block_has_its_host_bits_clear() {
    for row in IPV4_REFUSED
        .iter()
        .chain(IPV6_REFUSED_OUTSIDE)
        .chain(IPV6_REFUSED_INSIDE)
    {
        for block in row.blocks {
            assert_eq!(Cidr::parse(&block.to_string()), Some(*block), "{block}");
        }
    }
    for form in EMBEDDED_FORMS {
        if let Some(prefix) = form.prefix {
            assert_eq!(Cidr::parse(&prefix.to_string()), Some(prefix), "{prefix}");
        }
    }
}

#[test]
fn the_designs_blocks_are_refused_and_their_neighbours_judged_by_the_design() {
    // Driven by the design's text, not by the code's table: dropping,
    // narrowing or widening a block fails here as well as in the pins
    // above.
    let mut blocks = Vec::new();
    for table in [DESIGN_IPV4, DESIGN_IPV6_OUTSIDE, DESIGN_IPV6_INSIDE] {
        for cells in rows(table) {
            for text in quoted(&cells[0]) {
                blocks.push(Cidr::parse(text).unwrap());
            }
        }
    }
    // 15 IPv4 blocks, 12 IPv6 outside global unicast, 3 inside.
    assert_eq!(blocks.len(), 30);
    let global = Cidr::parse(DESIGN_GLOBAL_UNICAST).unwrap();
    let design_refuses = |addr: IpAddr| {
        blocks.iter().any(|block| block.contains(addr))
            || (addr.is_ipv6() && !global.contains(addr))
    };
    for block in &blocks {
        for addr in [block.first(), block.last()] {
            assert!(
                check_address(addr, &Nat64State::NONE).is_err(),
                "{addr} in {block}"
            );
        }
        for addr in beside(block) {
            assert_eq!(
                check_address(addr, &Nat64State::NONE).is_err(),
                design_refuses(addr),
                "{addr}, beside {block}"
            );
        }
    }
}

/// The addresses just outside a block.
fn beside(block: &Cidr) -> Vec<IpAddr> {
    match (block.first(), block.last()) {
        (IpAddr::V4(first), IpAddr::V4(last)) => [
            u32::from(first).checked_sub(1),
            u32::from(last).checked_add(1),
        ]
        .into_iter()
        .flatten()
        .map(|n| IpAddr::V4(Ipv4Addr::from(n)))
        .collect(),
        (IpAddr::V6(first), IpAddr::V6(last)) => [
            u128::from(first).checked_sub(1),
            u128::from(last).checked_add(1),
        ]
        .into_iter()
        .flatten()
        .map(|n| IpAddr::V6(Ipv6Addr::from(n)))
        .collect(),
        _ => Vec::new(),
    }
}
