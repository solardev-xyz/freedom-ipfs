//! The two reviews' probes of #44 (design 0007, item 3, the gate), kept as
//! tests:
//! - every URL `parse_url` accepts reaches the target and port that reqwest
//!   and the http crate read from it, and its numeric hosts are refused;
//! - configured targets match by exact host and port, after normalizing;
//! - no public function of the gate panics, on random text, bytes and
//!   addresses, and on inputs of 1 MiB.

use freedom_ipfs_net::gate::*;
use multiaddr::Multiaddr;
use std::net::{Ipv4Addr, Ipv6Addr};

#[test]
fn accepted_urls_reach_the_target_reqwest_and_http_read() {
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let hosts = [
        "127.1",
        "0177.1",
        "0x7f.1",
        "2130706433",
        "169.254.169.254.",
        "0xa9fea9fe",
        "[::ffff:127.0.0.1]",
        "[::127.0.0.1]",
        "[::ffff:0:127.0.0.1]",
        "[fe80::1%25eth0]",
        "[127.0.0.1]",
        "a.onion",
        "A.ONION.",
        "onion",
        "a..onion",
        "a.onion..",
        "a%2eonion",
        "a。onion",
        "a．onion",
        "a｡onion",
        "localhost",
        "example.com",
        "EXAMPLE.COM.",
        "xn--bcher-kva.example",
        "a\\127.0.0.1",
        "a@127.1",
        "a%40127.1",
        "a\t.onion",
        "a\n.onion",
        "a\r.onion",
        "127.0.0.1#@8.8.8.8",
        "8.8.8.8?@127.1",
        "8.8.8.8\\@127.1",
    ];
    let ports = [
        "", ":80", ":443", ":0", ":65535", ":65536", ":00080", ":", ":-1", ":+80", ":0x50",
    ];
    let mut accepted = 0;
    for host in hosts {
        for port in ports {
            for scheme in ["http", "https", "HTTP", "HtTpS"] {
                let raw = format!("{scheme}://{host}{port}/x");
                if let Ok(url) = parse_url(&raw) {
                    accepted += 1;
                    let classified = classify_url(&url).unwrap();
                    let built = client.get(url.clone()).build().unwrap();
                    assert_eq!(built.url(), &url);
                    let uri: http::Uri = url.as_str().parse().unwrap();
                    assert_eq!(
                        classify_host(uri.host().unwrap()).unwrap(),
                        classified.target,
                        "{raw}"
                    );
                    assert_eq!(
                        uri.port_u16()
                            .unwrap_or(if classified.secure { 443 } else { 80 }),
                        classified.port
                    );
                    if let Target::Ip(ip) = classified.target {
                        // All numeric hosts in this corpus are local, reserved, or embedding one.
                        if !raw.starts_with(&format!("{scheme}://8.8.8.8")) {
                            assert!(
                                check_address(ip, &Nat64State::NONE).is_err(),
                                "{raw} -> {ip}"
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(accepted > 100, "{accepted} URLs accepted");
}

#[test]
fn configured_host_port_is_exact_after_normalizing() {
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&parse_url("http://EXAMPLE.com.:80/x").unwrap())
        .unwrap();
    configured
        .add_url(&parse_url("https://[::ffff:127.0.0.1]/").unwrap())
        .unwrap();
    for raw in [
        "http://example.com/",
        "http://EXAMPLE.COM.:80/",
        "https://example.com:80/",
    ] {
        let t = classify_url(&parse_url(raw).unwrap()).unwrap();
        assert!(configured.contains(&t.target, t.port));
    }
    for raw in [
        "http://example.com:81/",
        "https://example.com/",
        "http://example.com.evil/",
        "http://127.1:80/",
    ] {
        let t = classify_url(&parse_url(raw).unwrap()).unwrap();
        assert!(!configured.contains(&t.target, t.port));
    }
    for raw in ["https://127.1/", "https://[::ffff:7f00:1]:443/"] {
        let t = classify_url(&parse_url(raw).unwrap()).unwrap();
        assert!(configured.contains(&t.target, t.port));
    }
}

fn rand64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
fn exercise_text(text: &str) {
    let _ = classify_host(text);
    let _ = Cidr::parse(text);
    let _ = parse_url(text);
    if let Ok(url) = url::Url::parse(text) {
        let _ = classify_url(&url);
        let mut configured = ConfiguredTargets::new();
        let _ = configured.add_url(&url);
    }
    if let Ok(addr) = text.parse::<Multiaddr>() {
        let _ = classify_multiaddr(&addr);
    }
}

#[test]
fn public_functions_never_panic() {
    let mut rng = 0x7621_3456_789a_bcde_u64;
    let seeds = [
        "http://127.1/",
        "http://[::ffff:169.254.169.254]/",
        "https://abc.ONION./",
        "http://user@127.1/",
        "/dns/a.onion/tcp/80",
        "/ip4/8.8.8.8/ip4/127.0.0.1/tcp/80",
        "2a00:1:2:3::/64",
    ];
    for i in 0..20_000 {
        let len = (rand64(&mut rng) % 256) as usize;
        let raw: Vec<u8> = (0..len).map(|_| rand64(&mut rng) as u8).collect();
        exercise_text(&String::from_utf8_lossy(&raw));
        let ascii: String = raw.iter().map(|b| (b % 128) as char).collect();
        exercise_text(&ascii);
        if let Ok(addr) = Multiaddr::try_from(raw.clone()) {
            let _ = classify_multiaddr(&addr);
        }
        let mut seed = seeds[i % seeds.len()].as_bytes().to_vec();
        for _ in 0..=rand64(&mut rng) % 4 {
            let at = (rand64(&mut rng) as usize) % (seed.len() + 1);
            seed.insert(at, rand64(&mut rng) as u8);
        }
        exercise_text(&String::from_utf8_lossy(&seed));
        let bits = (u128::from(rand64(&mut rng)) << 64) | u128::from(rand64(&mut rng));
        let v6 = Ipv6Addr::from(bits);
        let v4 = Ipv4Addr::from(rand64(&mut rng) as u32);
        let len = rand64(&mut rng) as u8;
        let _ = Nat64Prefix::new(v6, len);
        let discovered = Nat64Prefix::in_ipv4only_answer(v6);
        let known = Nat64State::known(discovered.clone());
        let unknown = Nat64State::unknown(discovered.clone());
        let incomplete = Nat64State::Incomplete(discovered);
        for state in [&Nat64State::Pending, &known, &unknown, &incomplete] {
            let _ = check_address(v6.into(), state).map_err(|e| e.to_string());
            let _ = check_address(v4.into(), state).map_err(|e| e.to_string());
        }
        for cidr in [Cidr::v4(v4.octets(), len), Cidr::v6(v6.segments(), len)] {
            let _ = cidr.first();
            let _ = cidr.last();
            let _ = cidr.contains(v4.into());
            let _ = cidr.contains_v6(v6);
            let _ = cidr.to_string();
        }
        if let Some(cidr) = Cidr::new(v6.into(), len) {
            let _ = cidr.to_string();
        }
        let addr = Multiaddr::empty().with(multiaddr::Protocol::Dns(
            String::from_utf8_lossy(&raw).into_owned().into(),
        ));
        let _ = classify_multiaddr(&addr);
    }
    for text in [
        "a".repeat(1_048_576),
        ".".repeat(1_048_576),
        "http://".to_string() + &"a".repeat(1_048_576),
    ] {
        exercise_text(&text);
    }
}
