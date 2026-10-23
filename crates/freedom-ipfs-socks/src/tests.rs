//! The byte layer, byte for byte: what goes on the wire, and every answer
//! the proxy can give at each step.

use super::*;

/// Fills with 0x11: credentials a test can predict.
fn elevens(bytes: &mut [u8]) -> Result<(), RandomFailed> {
    bytes.fill(0x11);
    Ok(())
}

#[test]
fn the_greeting_offers_username_and_password_alone() {
    assert_eq!(greeting(), [5, 1, 0x02]);
}

#[test]
fn rfc_1929s_request_is_the_fixed_name_and_the_password() {
    let credentials = Credentials::new(elevens).unwrap();
    let mut expected = vec![0x01, 12];
    expected.extend_from_slice(b"freedom-ipfs");
    expected.push(32);
    expected.extend_from_slice("11".repeat(16).as_bytes());
    assert_eq!(credentials.request(), expected);
    assert_eq!(credentials.username(), Credentials::USERNAME);
    assert_eq!(credentials.password(), "11".repeat(16));
}

#[test]
fn credentials_need_the_randomness_and_are_never_shown() {
    fn failing(_: &mut [u8]) -> Result<(), RandomFailed> {
        Err(RandomFailed)
    }
    assert_eq!(Credentials::new(failing), Err(RandomFailed));
    let credentials = Credentials::new(elevens).unwrap();
    for shown in [format!("{credentials:?}"), format!("{credentials:#?}")] {
        assert!(!shown.contains(credentials.password()), "{shown}");
        assert!(!shown.contains(Credentials::USERNAME), "{shown}");
    }
}

#[test]
fn the_choice_must_be_username_and_password() {
    assert!(check_choice([5, 0x02]).is_ok());
    for refused in [[5, 0x00], [5, 0x01], [5, 0xFF]] {
        assert!(
            matches!(check_choice(refused), Err(SocksError::MethodRefused)),
            "{refused:?}"
        );
    }
    // Another version comes first: outside the protocol.
    for broken in [[4, 0x02], [4, 0x00], [0, 0]] {
        assert!(
            matches!(check_choice(broken), Err(SocksError::Protocol)),
            "{broken:?}"
        );
    }
}

#[test]
fn rfc_1929s_status_must_be_success_in_its_version() {
    assert!(check_auth_status([1, 0]).is_ok());
    assert!(matches!(
        check_auth_status([1, 1]),
        Err(SocksError::CredentialsRefused)
    ));
    assert!(matches!(
        check_auth_status([1, 0xFF]),
        Err(SocksError::CredentialsRefused)
    ));
    assert!(matches!(
        check_auth_status([5, 0]),
        Err(SocksError::Protocol)
    ));
    assert!(matches!(
        check_auth_status([5, 1]),
        Err(SocksError::Protocol)
    ));
}

#[test]
fn requests_byte_for_byte() {
    // CONNECT to an IPv4 literal.
    assert_eq!(
        encode_request(CONNECT, &Destination::Ip("8.8.4.4:4001".parse().unwrap())).unwrap(),
        [5, 1, 0, 1, 8, 8, 4, 4, 0x0f, 0xa1]
    );
    // CONNECT to an IPv6 literal.
    let mut v6 = vec![5, 1, 0, 4];
    v6.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
    v6.extend_from_slice(&[0, 80]);
    assert_eq!(
        encode_request(
            CONNECT,
            &Destination::Ip("[2001:db8::1]:80".parse().unwrap())
        )
        .unwrap(),
        v6
    );
    // RESOLVE by name, port 0.
    let mut resolve = vec![5, 0xF0, 0, 3, 11];
    resolve.extend_from_slice(b"example.com");
    resolve.extend_from_slice(&[0, 0]);
    assert_eq!(
        encode_request(RESOLVE, &Destination::Name("example.com", 0)).unwrap(),
        resolve
    );
    // The longest name SOCKS5 carries: 255 bytes.
    let longest = "a".repeat(255);
    let encoded = encode_request(CONNECT, &Destination::Name(&longest, 443)).unwrap();
    assert_eq!(encoded[..5], [5, 1, 0, 3, 255]);
    assert_eq!(&encoded[5..260], longest.as_bytes());
    assert_eq!(encoded[260..], [1, 0xbb]);
    // Longer, or empty: refused before anything is sent.
    for name in ["a".repeat(256), String::new()] {
        assert!(matches!(
            encode_request(RESOLVE, &Destination::Name(&name, 0)),
            Err(SocksError::NameTooLong)
        ));
    }
}

#[test]
fn reply_heads() {
    assert_eq!(check_reply_head([5, 0, 0, 1]).unwrap(), BoundShape::Ipv4);
    assert_eq!(check_reply_head([5, 0, 0, 4]).unwrap(), BoundShape::Ipv6);
    assert_eq!(check_reply_head([5, 0, 0, 3]).unwrap(), BoundShape::Name);
    // Another version, a nonzero reserved byte: outside the protocol, before
    // the code is read.
    for broken in [[4, 0, 0, 1], [5, 0, 1, 1], [4, 7, 0, 1]] {
        assert!(
            matches!(check_reply_head(broken), Err(SocksError::Protocol)),
            "{broken:?}"
        );
    }
    // A refusal, with its code, before the type is judged.
    assert!(matches!(
        check_reply_head([5, 7, 0, 1]),
        Err(SocksError::Refused(7))
    ));
    assert!(matches!(
        check_reply_head([5, 4, 0, 9]),
        Err(SocksError::Refused(4))
    ));
    // An unknown address type.
    assert!(matches!(
        check_reply_head([5, 0, 0, 9]),
        Err(SocksError::Protocol)
    ));
    assert_eq!(BoundShape::Ipv4.ip_len(), Some(4));
    assert_eq!(BoundShape::Ipv6.ip_len(), Some(16));
    assert_eq!(BoundShape::Name.ip_len(), None);
}

#[test]
fn bound_addresses_and_resolves_answer() {
    assert_eq!(
        parse_ip(BoundShape::Ipv4, &[8, 8, 4, 4]).unwrap(),
        Bound::Ip("8.8.4.4".parse().unwrap())
    );
    let v6 = [0x20, 1, 0x48, 0x60].repeat(4);
    assert_eq!(
        parse_ip(BoundShape::Ipv6, &v6).unwrap(),
        Bound::Ip("2001:4860:2001:4860:2001:4860:2001:4860".parse().unwrap())
    );
    for (shape, bytes) in [
        (BoundShape::Ipv4, vec![1, 2, 3]),
        (BoundShape::Ipv6, vec![0; 4]),
        (BoundShape::Name, vec![0; 4]),
    ] {
        assert!(matches!(parse_ip(shape, &bytes), Err(SocksError::Protocol)));
    }
    assert_eq!(
        resolved(Bound::Ip("8.8.4.4".parse().unwrap())).unwrap(),
        "8.8.4.4".parse::<IpAddr>().unwrap()
    );
    for none in [
        Bound::Ip(Ipv4Addr::UNSPECIFIED.into()),
        Bound::Ip(Ipv6Addr::UNSPECIFIED.into()),
        Bound::Name,
    ] {
        assert!(matches!(resolved(none), Err(SocksError::EmptyAnswer)));
    }
}

#[test]
fn errors_name_the_step_and_nothing_else() {
    let credentials = Credentials::new(elevens).unwrap();
    let errors = [
        SocksError::Unreachable(io::Error::from(io::ErrorKind::ConnectionRefused)),
        SocksError::Io(io::Error::from(io::ErrorKind::TimedOut)),
        SocksError::Protocol,
        SocksError::MethodRefused,
        SocksError::CredentialsRefused,
        SocksError::Refused(1),
        SocksError::EmptyAnswer,
        SocksError::NameTooLong,
    ];
    for error in &errors {
        for text in [error.to_string(), format!("{error:?}")] {
            assert!(!text.contains(credentials.password()), "{text}");
            assert!(!text.contains(Credentials::USERNAME), "{text}");
            assert!(!text.contains("example.com"), "{text}");
        }
    }
    assert_eq!(
        SocksError::Refused(7).to_string(),
        "the SOCKS5 proxy refused the request (reply 0x07)"
    );
}
