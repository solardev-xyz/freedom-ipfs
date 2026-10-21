//! Design 0007's network policy tests that the gate's pure half can run
//! ("Tests first", "Network policy tests"): canonical forms, the address
//! table, embedded IPv4, and the refused multiaddrs. The cases that need a
//! socket, a resolver, a proxy or discovery's process come with the parts
//! of upstream item 3 that do I/O.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use freedom_ipfs_net::gate::table::{
    Row, GLOBAL_UNICAST, IPV4_REFUSED, IPV6_REFUSED_INSIDE, IPV6_REFUSED_OUTSIDE,
    ISATAP_INTERFACE_IDS, NAT64_WELL_KNOWN, SIX_TO_FOUR,
};
use freedom_ipfs_net::gate::{
    check_address, classify_host, classify_multiaddr, classify_url, parse_url, AddressRefusal,
    ConfiguredTargets, Form, Nat64Prefix, Nat64State, Refusal, Target,
};
use multiaddr::Multiaddr;
use url::Url;

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

fn v6(text: &str) -> Ipv6Addr {
    text.parse().unwrap()
}

fn multiaddr(text: &str) -> Multiaddr {
    text.parse().unwrap()
}

fn check(text: &str) -> Result<IpAddr, AddressRefusal> {
    check_address(ip(text), &Nat64State::NONE)
}

/// The row the refusal names, for a block refusal or an embedded one.
fn refused_by(result: Result<IpAddr, AddressRefusal>) -> &'static str {
    match result {
        Err(AddressRefusal::Block { row, .. } | AddressRefusal::Embedded { row, .. }) => row.what,
        other => panic!("not refused by a block: {other:?}"),
    }
}

const LINK_LOCAL: &str = "link-local";

// ---------------------------------------------------------------------------
// Canonical forms.

#[test]
fn upper_case_and_a_trailing_dot_are_one_name() {
    let name = Target::Name("example.com".into());
    assert_eq!(classify_host("EXAMPLE.Com."), Ok(name.clone()));
    let url = parse_url("HTTP://EXAMPLE.COM./path").unwrap();
    let target = classify_url(&url).unwrap();
    assert_eq!(target.target, name);
    assert_eq!((target.secure, target.port), (false, 80));
    assert_eq!(
        classify_multiaddr(&multiaddr("/dns4/Example.COM./tcp/443")),
        Ok(name)
    );
}

#[test]
fn numeric_ipv4_url_hosts_are_literals() {
    let loopback = Target::Ip(ip("127.0.0.1"));
    for raw in [
        "http://0x7f.1/",
        "http://2130706433/",
        "http://0177.0.0.1/",
        "http://127.0.0.1./",
        "http://127.1/",
    ] {
        let url = parse_url(raw).unwrap();
        assert_eq!(classify_url(&url).unwrap().target, loopback, "{raw}");
    }
    assert_eq!(classify_host("0x7f.1"), Ok(loopback.clone()));
    // A `/dns` component that is a literal is a literal.
    assert_eq!(
        classify_multiaddr(&multiaddr("/dns/2130706433/tcp/80")),
        Ok(loopback)
    );
    assert_eq!(refused_by(check("127.0.0.1")), "loopback");
}

#[test]
fn every_spelling_of_one_address_is_one_target() {
    let expected = Target::Ip(ip("169.254.169.254"));
    let refusal = check("169.254.169.254");
    assert_eq!(refused_by(refusal.clone()), LINK_LOCAL);
    for raw in [
        "http://[::ffff:a9fe:a9fe]/",
        "http://[::ffff:169.254.169.254]/",
        "http://[0:0:0:0:0:ffff:a9fe:a9fe]/",
        "http://[::FFFF:A9FE:A9FE]/",
        "http://169.254.169.254/",
        "http://0xa9fea9fe/",
    ] {
        let url = parse_url(raw).unwrap();
        assert_eq!(classify_url(&url).unwrap().target, expected, "{raw}");
    }
    let addr = multiaddr("/ip6/::ffff:169.254.169.254/tcp/80");
    assert_eq!(classify_multiaddr(&addr), Ok(expected));
    // The address itself, however a resolver spells it.
    for text in [
        "::ffff:169.254.169.254",
        "::ffff:a9fe:a9fe",
        "0:0:0:0:0:ffff:a9fe:a9fe",
    ] {
        assert_eq!(check(text), refusal, "{text}");
    }
    // A mapped address that passes is connected to as IPv4.
    assert_eq!(check("::ffff:8.8.8.8"), Ok(ip("8.8.8.8")));
}

#[test]
fn onions_are_classified_wherever_they_appear() {
    assert_eq!(
        classify_multiaddr(&multiaddr("/dns/X.ONION./tcp/80")),
        Ok(Target::Onion("x.onion".into()))
    );
    for dns in ["/dns4", "/dns6", "/dnsaddr"] {
        let addr = multiaddr(&format!("{dns}/sub.abc.onion"));
        assert_eq!(
            classify_multiaddr(&addr),
            Ok(Target::Onion("sub.abc.onion".into()))
        );
    }
    let url = parse_url("http://abc.ONION/").unwrap();
    assert_eq!(
        classify_url(&url).unwrap().target,
        Target::Onion("abc.onion".into())
    );
    let onion3 = "vww6ybal4bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd";
    assert_eq!(
        classify_multiaddr(&multiaddr(&format!("/onion3/{onion3}:1234"))),
        Ok(Target::Onion(format!("{onion3}.onion")))
    );
    assert_eq!(
        classify_multiaddr(&multiaddr("/onion/aaimaq4ygg2iegci:80")),
        Ok(Target::Onion("aaimaq4ygg2iegci.onion".into()))
    );
}

#[test]
fn hosts_must_already_be_punycode() {
    assert_eq!(classify_host("bücher.de"), Err(Refusal::NonAscii));
    assert_eq!(parse_url("http://bücher.de/"), Err(Refusal::NonAscii));
    assert_eq!(
        classify_multiaddr(&multiaddr("/dns/bücher.de/tcp/80")),
        Err(Refusal::NonAscii)
    );
    // Full-width dots are what IDNA would turn into dots.
    assert_eq!(classify_host("example\u{ff0e}com"), Err(Refusal::NonAscii));
    assert_eq!(
        classify_host("xn--bcher-kva.de"),
        Ok(Target::Name("xn--bcher-kva.de".into()))
    );
    // Percent-encoding would be decoded into a host the text doesn't say.
    assert_eq!(
        parse_url("http://b%C3%BCcher.de/"),
        Err(Refusal::InvalidHost)
    );
    assert_eq!(parse_url("http://127.0.0.%31/"), Err(Refusal::InvalidHost));
}

#[test]
fn urls_are_http_or_https_without_userinfo() {
    for raw in [
        "http://user@example.com/",
        "https://user:pw@example.com/",
        "http://@example.com/",
    ] {
        assert_eq!(parse_url(raw), Err(Refusal::Userinfo), "{raw}");
    }
    let url = Url::parse("http://user@example.com/").unwrap();
    assert_eq!(classify_url(&url), Err(Refusal::Userinfo));
    for raw in [
        "ftp://example.com/",
        "file:///etc/hosts",
        "socks5h://127.0.0.1:9050",
        "ws://x/",
    ] {
        assert_eq!(parse_url(raw), Err(Refusal::Scheme), "{raw}");
    }
    assert_eq!(
        classify_url(&Url::parse("wss://example.com/").unwrap()),
        Err(Refusal::Scheme)
    );
    assert_eq!(parse_url("http:/example.com"), Err(Refusal::InvalidUrl));
    assert_eq!(parse_url("http:///example.com"), Err(Refusal::EmptyHost));
    assert_eq!(parse_url("http://example.com /"), Err(Refusal::InvalidUrl));
    assert_eq!(parse_url("http://exa\tmple.com/"), Err(Refusal::InvalidUrl));
    // A backslash ends the authority, as the WHATWG rules read it: the host
    // is the one before it, never one after an `@` behind it.
    let url = parse_url("http://example.com\\@127.0.0.1/").unwrap();
    assert_eq!(
        classify_url(&url).unwrap().target,
        Target::Name("example.com".into())
    );
    let url = parse_url("https://[2001:db8::1]:8443/x").unwrap();
    let target = classify_url(&url).unwrap();
    assert_eq!(
        (target.target, target.secure, target.port),
        (Target::Ip(ip("2001:db8::1")), true, 8443)
    );
}

// ---------------------------------------------------------------------------
// Multiaddrs that are dialed.

#[test]
fn a_dialed_multiaddr_starts_with_an_address_a_name_or_an_onion() {
    for refused in [
        "/ip6zone/eth0/ip6/fe80::1/tcp/80",
        "/unix/sock",
        "/memory/1234",
        "/tcp/80",
        "/p2p/12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG",
        "/p2p-circuit",
    ] {
        assert_eq!(
            classify_multiaddr(&multiaddr(refused)),
            Err(Refusal::Multiaddr),
            "{refused}"
        );
    }
    assert_eq!(
        classify_multiaddr(&Multiaddr::empty()),
        Err(Refusal::Multiaddr)
    );
    assert_eq!(
        classify_multiaddr(&multiaddr("/ip4/8.8.8.8/tcp/4001")),
        Ok(Target::Ip(ip("8.8.8.8")))
    );
    assert_eq!(
        classify_multiaddr(&multiaddr("/ip6/2a00:1::1/udp/4001/quic-v1")),
        Ok(Target::Ip(ip("2a00:1::1")))
    );
    assert_eq!(
        classify_multiaddr(&multiaddr("/dnsaddr/bootstrap.libp2p.io")),
        Ok(Target::Name("bootstrap.libp2p.io".into()))
    );
}

// ---------------------------------------------------------------------------
// The address table, with cases generated from it.

/// An independent reading of the design's rules, for the generated
/// neighbours: in a refused block, or not global unicast unless a fixed
/// embedding decides, or a fixed embedding of a refused IPv4 address.
fn expected_refused(addr: IpAddr) -> bool {
    let in_rows = |rows: &[Row], addr: IpAddr| {
        rows.iter()
            .any(|row| row.blocks.iter().any(|block| block.contains(addr)))
    };
    let v4_refused = |v4: Ipv4Addr| in_rows(IPV4_REFUSED, IpAddr::V4(v4));
    match addr {
        IpAddr::V4(v4) => v4_refused(v4),
        IpAddr::V6(v6) => {
            let s = v6.segments();
            let o = v6.octets();
            let last = Ipv4Addr::new(o[12], o[13], o[14], o[15]);
            if s[..5] == [0; 5] && s[5] == 0xffff {
                return v4_refused(last);
            }
            if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                return v4_refused(last);
            }
            if in_rows(IPV6_REFUSED_OUTSIDE, addr) || in_rows(IPV6_REFUSED_INSIDE, addr) {
                return true;
            }
            if s[0] & 0xe000 != 0x2000 {
                return true;
            }
            if s[0] == 0x2002 && v4_refused(Ipv4Addr::new(o[2], o[3], o[4], o[5])) {
                return true;
            }
            let id = u32::from_be_bytes([o[8], o[9], o[10], o[11]]);
            ISATAP_INTERFACE_IDS.contains(&id) && v4_refused(last)
        }
    }
}

fn neighbours(block: &freedom_ipfs_net::gate::Cidr) -> Vec<IpAddr> {
    match (block.first(), block.last()) {
        (IpAddr::V4(first), IpAddr::V4(last)) => [
            u32::from(first).checked_sub(1),
            u32::from(last).checked_add(1),
        ]
        .into_iter()
        .flatten()
        .map(|n| IpAddr::V4(n.into()))
        .collect(),
        (IpAddr::V6(first), IpAddr::V6(last)) => [
            u128::from(first).checked_sub(1),
            u128::from(last).checked_add(1),
        ]
        .into_iter()
        .flatten()
        .map(|n| IpAddr::V6(n.into()))
        .collect(),
        _ => unreachable!(),
    }
}

#[test]
fn each_refused_blocks_first_and_last_address_are_refused_and_its_neighbours_judged() {
    let mut checked = 0;
    for row in IPV4_REFUSED
        .iter()
        .chain(IPV6_REFUSED_OUTSIDE)
        .chain(IPV6_REFUSED_INSIDE)
    {
        for block in row.blocks {
            for addr in [block.first(), block.last()] {
                match check_address(addr, &Nat64State::NONE) {
                    Err(AddressRefusal::Block { block: by, .. }) => {
                        assert!(by.contains(addr), "{addr} refused by {by}")
                    }
                    other => panic!("{addr} in {block}: {other:?}"),
                }
                checked += 1;
            }
            for addr in neighbours(block) {
                let refused = check_address(addr, &Nat64State::NONE).is_err();
                assert_eq!(refused, expected_refused(addr), "{addr}, beside {block}");
                checked += 1;
            }
        }
    }
    assert!(checked > 100, "{checked} addresses");
}

#[test]
fn the_globally_reachable_registry_blocks_pass() {
    for block in [
        "192.31.196.0/24",
        "192.175.48.0/24",
        "192.52.193.0/24",
        "2620:4f:8000::/48",
    ] {
        let block = freedom_ipfs_net::gate::Cidr::parse(block).unwrap();
        for addr in [block.first(), block.last()] {
            assert_eq!(check_address(addr, &Nat64State::NONE), Ok(addr), "{addr}");
        }
    }
}

#[test]
fn ipv6_outside_global_unicast_is_refused() {
    assert_eq!(GLOBAL_UNICAST.to_string(), "2000::/3");
    for text in [
        "1fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "4000::",
        "8000::1",
        "e000::",
    ] {
        assert_eq!(check(text), Err(AddressRefusal::NotGlobalUnicast), "{text}");
    }
    assert_eq!(check("2000::1"), Ok(ip("2000::1")));
    assert_eq!(check("3fff:1000::"), Ok(ip("3fff:1000::")));
}

// ---------------------------------------------------------------------------
// IPv6 forms that embed IPv4.

#[test]
fn nat64s_well_known_prefix_is_judged_by_its_ipv4_address() {
    // The motivating case: a provider record advertising
    // /ip6/64:ff9b::a9fe:a9fe/tcp/80.
    let addr = multiaddr("/ip6/64:ff9b::a9fe:a9fe/tcp/80");
    let Ok(Target::Ip(target)) = classify_multiaddr(&addr) else {
        panic!("a literal");
    };
    let refusal = check_address(target, &Nat64State::NONE);
    assert!(matches!(
        refusal,
        Err(AddressRefusal::Embedded {
            form: Form::Nat64WellKnown,
            ..
        })
    ));
    assert_eq!(refused_by(refusal), LINK_LOCAL);
    // 8.8.8.8 passes, and the connection goes to the IPv6 address.
    assert_eq!(check("64:ff9b::808:808"), Ok(ip("64:ff9b::808:808")));
    // A DNS64 answer for an ordinary name whose A record is loopback.
    assert_eq!(refused_by(check("64:ff9b::7f00:1")), "loopback");
    assert_eq!(NAT64_WELL_KNOWN.to_string(), "64:ff9b::/96");
}

#[test]
fn six_to_four_is_judged_by_its_ipv4_address() {
    assert_eq!(refused_by(check("2002:a9fe:a9fe::1")), LINK_LOCAL);
    assert_eq!(refused_by(check("2002:a00:1::1")), "private use");
    assert_eq!(check("2002:808:808::1"), Ok(ip("2002:808:808::1")));
    assert!(matches!(
        check("2002:a00:1::1"),
        Err(AddressRefusal::Embedded {
            form: Form::SixToFour,
            ..
        })
    ));
    assert_eq!(SIX_TO_FOUR.to_string(), "2002::/16");
}

#[test]
fn isatap_is_judged_by_its_ipv4_address_and_on_its_own() {
    assert_eq!(refused_by(check("2a00:1::200:5efe:a9fe:a9fe")), LINK_LOCAL);
    assert_eq!(refused_by(check("2a00:1::5efe:7f00:1")), "loopback");
    assert_eq!(refused_by(check("2a00:1::100:5efe:a00:1")), "private use");
    assert_eq!(refused_by(check("2a00:1::300:5efe:a00:1")), "private use");
    assert!(matches!(
        check("2a00:1::200:5efe:a9fe:a9fe"),
        Err(AddressRefusal::Embedded {
            form: Form::Isatap,
            ..
        })
    ));
    assert_eq!(
        check("2a00:1::200:5efe:808:808"),
        Ok(ip("2a00:1::200:5efe:808:808"))
    );
    // On its own too: a link-local ISATAP address with a global IPv4 one.
    assert_eq!(refused_by(check("fe80::200:5efe:808:808")), LINK_LOCAL);
    // Under 6to4, both embeddings are judged.
    assert_eq!(
        refused_by(check("2002:808:808::200:5efe:a9fe:a9fe")),
        LINK_LOCAL
    );
}

#[test]
fn some_forms_are_refused_whatever_they_embed() {
    for (text, row) in [
        ("::808:808", "IPv4-compatible, deprecated"),
        ("::ffff:0:808:808", "IPv4-translated"),
        ("64:ff9b:1::808:808", "local-use IPv4/IPv6 translation"),
        (
            "2001:0:4136:e378:8000:63bf:3fff:fdd2",
            "IETF protocol assignments, the whole block",
        ),
    ] {
        assert_eq!(refused_by(check(text)), row, "{text}");
    }
}

#[test]
fn forms_whose_embedding_depends_on_configuration_are_judged_as_ipv6() {
    // 6rd and 6over4 with a private embedded address, and 6a44's form.
    for text in ["2a00:1:c0a8:101::1", "2a00:1::c0a8:101"] {
        assert_eq!(check(text), Ok(ip(text)), "{text}");
    }
}

/// Each RFC 6052 prefix length: the prefix, then the address with
/// 169.254.169.254 and with 8.8.8.8 embedded at its place, worked out by
/// hand from RFC 6052 §2.2.
const LEARNED: [(&str, u8, &str, &str); 6] = [
    ("2a00:1::", 32, "2a00:1:a9fe:a9fe::", "2a00:1:808:808::"),
    (
        "2a00:1:200::",
        40,
        "2a00:1:2a9:fea9:fe::",
        "2a00:1:208:808:8::",
    ),
    (
        "2a00:1:2::",
        48,
        "2a00:1:2:a9fe:a9:fe00::",
        "2a00:1:2:808:8:800::",
    ),
    (
        "2a00:1:2:300::",
        56,
        "2a00:1:2:3a9:fe:a9fe::",
        "2a00:1:2:308:8:808::",
    ),
    (
        "2a00:1:2:3::",
        64,
        "2a00:1:2:3:a9:fea9:fe00:0",
        "2a00:1:2:3:8:808:800:0",
    ),
    (
        "2a00:1:2:3:4:5::",
        96,
        "2a00:1:2:3:4:5:a9fe:a9fe",
        "2a00:1:2:3:4:5:808:808",
    ),
];

#[test]
fn a_learned_prefix_of_each_length_is_judged_by_its_ipv4_address() {
    for (prefix, len, refused, passes) in LEARNED {
        let prefix = Nat64Prefix::new(v6(prefix), len).unwrap();
        let state = Nat64State::known([prefix]);
        let refusal = check_address(ip(refused), &state);
        assert!(
            matches!(refusal, Err(AddressRefusal::Embedded { form: Form::Nat64Learned(p), .. }) if p == prefix),
            "/{len}: {refusal:?}"
        );
        assert_eq!(refused_by(refusal), LINK_LOCAL, "/{len}");
        assert_eq!(check_address(ip(passes), &state), Ok(ip(passes)), "/{len}");
        // Without the prefix, both look like ordinary global unicast.
        assert_eq!(check(refused), Ok(ip(refused)), "/{len}");
    }
}

#[test]
fn an_ipv4only_arpa_answer_teaches_a_prefix_or_nothing() {
    // 192.0.0.170 under a /64: the answer teaches 2a00:1:2:3::/64, and an
    // address under it with 169.254.169.254 embedded is refused.
    let learned = Nat64Prefix::in_ipv4only_answer(v6("2a00:1:2:3:c0:0:aa00:0"));
    assert_eq!(
        learned,
        vec![Nat64Prefix::new(v6("2a00:1:2:3::"), 64).unwrap()]
    );
    let state = Nat64State::known(learned);
    assert_eq!(
        refused_by(check_address(ip("2a00:1:2:3:a9:fea9:fe00:0"), &state)),
        LINK_LOCAL
    );
    // An answer that embeds neither address teaches nothing; NODATA from
    // every resolver gives a known, empty set.
    assert!(Nat64Prefix::in_ipv4only_answer(v6("2a00:1:2:3::808:808")).is_empty());
    assert_eq!(Nat64State::known([]), Nat64State::NONE);
}

#[test]
fn ipv6_outside_the_fixed_forms_waits_or_is_refused_until_the_prefixes_are_known() {
    let global = ip("2a00:1::1");
    assert_eq!(
        check_address(global, &Nat64State::Pending),
        Err(AddressRefusal::Nat64Pending)
    );
    assert_eq!(
        check_address(global, &Nat64State::Unknown),
        Err(AddressRefusal::Nat64Unknown)
    );
    assert_eq!(
        check_address(global, &Nat64State::Incomplete),
        Err(AddressRefusal::Nat64Incomplete)
    );
    // ISATAP's prefix isn't fixed either.
    assert_eq!(
        check_address(ip("2a00:1::200:5efe:808:808"), &Nat64State::Unknown),
        Err(AddressRefusal::Nat64Unknown)
    );
    // IPv4, and the fixed forms, are judged as always.
    for state in [
        Nat64State::Pending,
        Nat64State::Unknown,
        Nat64State::Incomplete,
    ] {
        for text in [
            "8.8.8.8",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
        ] {
            assert!(
                check_address(ip(text), &state).is_ok(),
                "{text} while {state:?}"
            );
        }
        assert_eq!(
            refused_by(check_address(ip("10.0.0.1"), &state)),
            "private use"
        );
    }
}

#[test]
fn past_sixteen_prefixes_ipv6_outside_the_fixed_forms_is_refused() {
    let prefix = |i: u16| Nat64Prefix::new(Ipv6Addr::new(0x2a00, 1, i, 0, 0, 0, 0, 0), 48).unwrap();
    let target = ip("2a00:1:11::a9fe:a9fe");
    // Sixteen are kept, and the 17th's address would look like global
    // unicast.
    assert_eq!(
        check_address(target, &Nat64State::known((1..=16).map(prefix))),
        Ok(target)
    );
    let state = Nat64State::known((1..=17).map(prefix));
    assert_eq!(state, Nat64State::Incomplete);
    assert_eq!(
        check_address(target, &state),
        Err(AddressRefusal::Nat64Incomplete)
    );
}

// ---------------------------------------------------------------------------
// Configured targets.

#[test]
fn configured_targets_are_exempt_by_exact_host_and_port() {
    let mut configured = ConfiguredTargets::new();
    for raw in [
        "https://Delegated-IPFS.dev./routing/v1",
        "http://127.0.0.1:5001/routing/v1",
        "http://[::ffff:192.168.1.5]:8080/",
    ] {
        configured.add_url(&parse_url(raw).unwrap()).unwrap();
    }
    configured.add_endpoint("127.0.0.1:9150".parse().unwrap());
    let name = Target::Name("delegated-ipfs.dev".into());
    assert!(configured.contains(&name, 443));
    assert!(!configured.contains(&name, 80));
    assert!(!configured.contains(&Target::Name("cid.contact".into()), 443));
    let loopback = Target::Ip(ip("127.0.0.1"));
    assert!(configured.contains(&loopback, 5001));
    assert!(configured.contains(&loopback, 9150));
    assert!(!configured.contains(&loopback, 5002));
    assert!(configured.contains(&Target::Ip(ip("192.168.1.5")), 8080));
    // A configured endpoint carries no credentials and is http or https.
    let with_userinfo = Url::parse("https://user:pw@example.com/").unwrap();
    assert_eq!(configured.add_url(&with_userinfo), Err(Refusal::Userinfo));
    let ftp = Url::parse("ftp://example.com/").unwrap();
    assert_eq!(configured.add_url(&ftp), Err(Refusal::Scheme));
}
