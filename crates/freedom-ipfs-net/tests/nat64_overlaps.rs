//! Learned NAT64 prefixes apply everywhere (design 0007, "IPv6 forms that
//! embed IPv4"): a network-specific prefix can sit inside a fixed form's
//! space, and the fixed form's own reading must not override it. Every
//! embedding an address can be read with must pass. The fixed forms are
//! exempt only from the refusals for what discovery doesn't know yet.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use freedom_ipfs_net::gate::{check_address, AddressRefusal, Form, Nat64Prefix, Nat64State};

fn v6(text: &str) -> Ipv6Addr {
    text.parse().unwrap()
}

/// `v4` embedded after `prefix`'s `len` bits, bit by bit, skipping bits
/// 64–71 (RFC 6052 §2.2), with `u` in the skipped byte: an encoder of its
/// own, not the gate's extraction.
fn embed(prefix: Ipv6Addr, len: u8, v4: Ipv4Addr, u: u8) -> Ipv6Addr {
    let mut bytes = prefix.octets();
    let mut at = usize::from(len);
    for bit in 0..32 {
        if at == 64 {
            at = 72;
        }
        let mask = 1 << (7 - at % 8);
        if u32::from(v4) & (1 << (31 - bit)) != 0 {
            bytes[at / 8] |= mask;
        } else {
            bytes[at / 8] &= !mask;
        }
        at += 1;
    }
    if len <= 64 {
        bytes[8] = u;
    }
    Ipv6Addr::from(bytes)
}

fn refused_by_learned(result: Result<IpAddr, AddressRefusal>, prefix: Nat64Prefix) -> bool {
    matches!(result, Err(AddressRefusal::Embedded { form: Form::Nat64Learned(p), .. }) if p == prefix)
}

const REFUSED: [Ipv4Addr; 3] = [
    Ipv4Addr::new(127, 0, 0, 1),
    Ipv4Addr::new(10, 0, 0, 1),
    Ipv4Addr::new(169, 254, 169, 254),
];

#[test]
fn a_learned_prefix_inside_6to4_decides_too() {
    // The adversarial review's /48: an ipv4only.arpa answer embedding
    // 192.0.0.170 teaches 2002:808:808::/48, whose 6to4 reading is 8.8.8.8.
    let learned = Nat64Prefix::in_ipv4only_answer(v6("2002:808:808:c000:0:aa00::"));
    let prefix = Nat64Prefix::new(v6("2002:808:808::"), 48).unwrap();
    assert_eq!(learned, vec![prefix]);
    // The conformance review's /96.
    let learned_96 = Nat64Prefix::in_ipv4only_answer(v6("2002:808:808::c000:aa"));
    let prefix_96 = Nat64Prefix::new(v6("2002:808:808::"), 96).unwrap();
    assert_eq!(learned_96, vec![prefix_96]);
    // Its reproduction: 2002:808:808::7f00:1 reads 127.0.0.1 under the /96.
    let reproduction: IpAddr = "2002:808:808::7f00:1".parse().unwrap();
    assert!(check_address(reproduction, &Nat64State::NONE).is_ok());
    assert!(refused_by_learned(
        check_address(reproduction, &Nat64State::known(learned_96)),
        prefix_96
    ));

    for prefix in [prefix, prefix_96] {
        let len = prefix.cidr().prefix_len();
        for v4 in REFUSED {
            let target = embed(v6("2002:808:808::"), len, v4, 0);
            assert_eq!(prefix.embedded_ipv4(target), Some(v4));
            // Its 6to4 reading passes on its own...
            assert!(
                check_address(target.into(), &Nat64State::NONE).is_ok(),
                "{target}"
            );
            // ...and the learned prefix refuses it, in every state that
            // holds the prefix.
            for state in [
                Nat64State::known([prefix]),
                Nat64State::unknown([prefix]),
                Nat64State::Incomplete(vec![prefix]),
            ] {
                assert!(
                    refused_by_learned(check_address(target.into(), &state), prefix),
                    "{target} under /{len}, {state:?}"
                );
            }
        }
        // A 6to4 address the learned prefix passes still passes, in every
        // state: the fixed form is exempt from what discovery doesn't know.
        let target = embed(v6("2002:808:808::"), len, Ipv4Addr::new(8, 8, 4, 4), 0);
        for state in [
            Nat64State::known([prefix]),
            Nat64State::unknown([prefix]),
            Nat64State::Incomplete(vec![prefix]),
        ] {
            assert_eq!(
                check_address(target.into(), &state),
                Ok(target.into()),
                "{state:?}"
            );
        }
    }
}

#[test]
fn a_learned_prefix_over_the_well_known_prefix_decides_too() {
    // A /64 at 64:ff9b:: reads a WKP address's IPv4 address from bits 72–103,
    // where the well-known prefix has zeros: 0.0.0.x, refused.
    let learned = Nat64Prefix::in_ipv4only_answer(v6("64:ff9b::c0:0:aa00:0"));
    let prefix = Nat64Prefix::new(v6("64:ff9b::"), 64).unwrap();
    assert_eq!(learned, vec![prefix]);
    let target: IpAddr = "64:ff9b::808:808".parse().unwrap();
    assert_eq!(check_address(target, &Nat64State::NONE), Ok(target));
    assert!(refused_by_learned(
        check_address(target, &Nat64State::known([prefix])),
        prefix
    ));
    assert!(refused_by_learned(
        check_address(target, &Nat64State::unknown([prefix])),
        prefix
    ));
    // Learned at /96 it is the well-known prefix itself: the same readings.
    let wkp = Nat64Prefix::in_ipv4only_answer(v6("64:ff9b::c000:aa"));
    let state = Nat64State::known(wkp);
    assert_eq!(check_address(target, &state), Ok(target));
    assert!(check_address("64:ff9b::7f00:1".parse().unwrap(), &state).is_err());
}

#[test]
fn a_mapped_address_is_connected_to_as_ipv4_whatever_is_learned() {
    // No IPv6 packet is sent for an IPv4-mapped address, so no translator
    // sees it: it is judged as its IPv4 address, under any learned prefix.
    let learned = Nat64Prefix::in_ipv4only_answer(v6("::ffff:c000:aa"));
    assert_eq!(
        learned,
        vec![Nat64Prefix::new(v6("::ffff:0:0"), 96).unwrap()]
    );
    let state = Nat64State::known(learned);
    assert_eq!(
        check_address("::ffff:8.8.8.8".parse().unwrap(), &state),
        Ok("8.8.8.8".parse().unwrap())
    );
    assert!(matches!(
        check_address("::ffff:10.0.0.1".parse().unwrap(), &state),
        Err(AddressRefusal::Block { .. })
    ));
}

/// The adversarial review's independent encoder: every prefix length, every
/// value of the skipped byte, random bits after the embedding, and two
/// overlapping prefixes in either order.
#[test]
fn every_length_every_skipped_byte_and_overlapping_prefixes() {
    let mut state = 0x0007_7777_1a5f_4567_u64;
    let mut random = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let link_local = Ipv4Addr::new(169, 254, 169, 254);
    for len in Nat64Prefix::LENGTHS {
        let prefix = Nat64Prefix::new(v6("2a22:1357:2468:1234:5678:abcd::"), len).unwrap();
        let IpAddr::V6(net) = prefix.cidr().addr() else {
            unreachable!()
        };
        for u in 0..=255 {
            let mut bytes = embed(net, len, link_local, u).octets();
            // The bits after the embedding are free.
            let used_end = if len == 96 {
                128
            } else if len + 32 > 64 {
                len + 40
            } else {
                len + 32
            };
            for byte in bytes.iter_mut().skip(usize::from(used_end / 8)) {
                *byte = random() as u8;
            }
            let address = Ipv6Addr::from(bytes);
            assert_eq!(prefix.embedded_ipv4(address), Some(link_local), "/{len}");
            assert!(
                check_address(address.into(), &Nat64State::known([prefix])).is_err(),
                "{address} under /{len}"
            );
        }
    }
    let short = Nat64Prefix::new(v6("2a22:1357::"), 32).unwrap();
    let long = Nat64Prefix::new(v6("2a22:1357:808::"), 48).unwrap();
    let address = embed(v6("2a22:1357:808::"), 48, Ipv4Addr::LOCALHOST, 0);
    for state in [
        Nat64State::known([short, long]),
        Nat64State::known([long, short]),
    ] {
        assert!(check_address(address.into(), &state).is_err(), "{state:?}");
    }
}
