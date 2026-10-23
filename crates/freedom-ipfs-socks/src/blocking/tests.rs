//! The blocking driver against a scripted proxy: the same exchange, byte
//! for byte, as `freedom-ipfs-net`'s async driver's tests check, plus its
//! deadline.

use super::*;
use crate::{RandomFailed, ATYP_DOMAIN, ATYP_IPV6, USER_PASS, USER_PASS_VERSION};
use std::net::{Ipv6Addr, TcpListener};
use std::thread::JoinHandle;
use std::time::Duration;

const NO_AUTH: u8 = 0x00;
const ACCEPTED: [u8; 2] = [USER_PASS_VERSION, 0];

fn elevens(bytes: &mut [u8]) -> Result<(), RandomFailed> {
    bytes.fill(0x11);
    Ok(())
}

fn client(proxy: SocketAddr) -> Client {
    Client::new(proxy, Credentials::new(elevens).unwrap())
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

/// The greeting offering username and password alone, then RFC 1929's
/// request with the test's credentials.
fn greeting_and_credentials() -> Vec<u8> {
    let mut sent = vec![5, 1, USER_PASS, USER_PASS_VERSION, 12];
    sent.extend_from_slice(b"freedom-ipfs");
    sent.push(32);
    sent.extend_from_slice("11".repeat(16).as_bytes());
    sent
}

/// A proxy that answers the greeting with `choice`, RFC 1929's request (if
/// it chose that method) with `status`, then the request with `reply`, raw,
/// and returns what the client sent.
fn scripted(choice: [u8; 2], status: [u8; 2], reply: Vec<u8>) -> (SocketAddr, JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let task = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut head = [0u8; 2];
        stream.read_exact(&mut head).unwrap();
        let mut methods = vec![0u8; usize::from(head[1])];
        stream.read_exact(&mut methods).unwrap();
        stream.write_all(&choice).unwrap();
        let mut seen = [head.as_slice(), &methods].concat();
        if choice[1] != USER_PASS {
            return seen;
        }
        let mut head = [0u8; 2];
        stream.read_exact(&mut head).unwrap();
        let mut user = vec![0u8; usize::from(head[1])];
        stream.read_exact(&mut user).unwrap();
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).unwrap();
        let mut pass = vec![0u8; usize::from(len[0])];
        stream.read_exact(&mut pass).unwrap();
        seen.extend_from_slice(&head);
        seen.extend_from_slice(&user);
        seen.extend_from_slice(&len);
        seen.extend_from_slice(&pass);
        stream.write_all(&status).unwrap();
        if status != ACCEPTED {
            return seen;
        }
        let mut request = vec![0u8; 512];
        let n = stream.read(&mut request).unwrap();
        seen.extend_from_slice(&request[..n]);
        stream.write_all(&reply).unwrap();
        seen
    });
    (addr, task)
}

#[test]
fn resolve_takes_one_address_and_nothing_else() {
    let cases: [(Vec<u8>, Option<IpAddr>); 6] = [
        (
            vec![5, 0, 0, 1, 8, 8, 4, 4, 0, 0],
            Some("8.8.4.4".parse().unwrap()),
        ),
        (
            [
                vec![5, 0, 0, 4],
                [0x20, 1, 0x48, 0x60].repeat(4),
                vec![0, 0],
            ]
            .concat(),
            Some("2001:4860:2001:4860:2001:4860:2001:4860".parse().unwrap()),
        ),
        (vec![5, 0, 0, 1, 0, 0, 0, 0, 0, 0], None),
        ([vec![5, 0, 0, 4], vec![0; 16], vec![0, 0]].concat(), None),
        (vec![5, 0, 0, 3, 3, b'a', b'.', b'b', 0, 0], None),
        (vec![5, 4, 0, 1, 0, 0, 0, 0, 0, 0], None),
    ];
    for (reply, expected) in cases {
        let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, reply.clone());
        let answer = client(proxy).resolve("example.com", soon());
        assert_eq!(answer.as_ref().ok(), expected.as_ref(), "{reply:?}");
        let mut sent = greeting_and_credentials();
        sent.extend_from_slice(&[5, RESOLVE, 0, 3, 11]);
        sent.extend_from_slice(b"example.com");
        sent.extend_from_slice(&[0, 0]);
        assert_eq!(seen.join().unwrap(), sent);
    }
}

#[test]
fn connect_sends_an_ip_literal_or_an_onion_name() {
    let ok = vec![5, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, ok.clone());
    let stream = client(proxy)
        .connect_ip("[2001:db8::1]:4001".parse().unwrap(), soon())
        .unwrap();
    // The stream comes back with no timeout of the exchange's.
    assert_eq!(stream.read_timeout().unwrap(), None);
    assert_eq!(stream.write_timeout().unwrap(), None);
    let mut sent = greeting_and_credentials();
    sent.extend_from_slice(&[5, CONNECT, 0, ATYP_IPV6]);
    sent.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
    sent.extend_from_slice(&4001u16.to_be_bytes());
    assert_eq!(seen.join().unwrap(), sent);

    let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, ok);
    let onion = format!(
        "{}.onion",
        data_encoding::BASE32_NOPAD
            .encode(&[[9u8; 34].as_slice(), &[3]].concat())
            .to_ascii_lowercase()
    );
    let host = OnionHost::new(&freedom_ipfs_addr::Target::Onion(onion.clone())).unwrap();
    client(proxy).connect_onion(&host, 80, soon()).unwrap();
    let mut sent = greeting_and_credentials();
    sent.extend_from_slice(&[5, CONNECT, 0, ATYP_DOMAIN, 62]);
    sent.extend_from_slice(onion.as_bytes());
    sent.extend_from_slice(&80u16.to_be_bytes());
    assert_eq!(seen.join().unwrap(), sent);
}

#[test]
fn a_proxy_that_wont_take_the_credentials_fails_with_no_fallback() {
    for choice in [[5, NO_AUTH], [5, 0xFF]] {
        let (proxy, seen) = scripted(choice, ACCEPTED, Vec::new());
        assert!(matches!(
            client(proxy).resolve("example.com", soon()),
            Err(SocksError::MethodRefused)
        ));
        assert_eq!(seen.join().unwrap(), [5, 1, USER_PASS], "{choice:?}");
    }
    let (proxy, seen) = scripted([5, USER_PASS], [USER_PASS_VERSION, 1], Vec::new());
    assert!(matches!(
        client(proxy).resolve("example.com", soon()),
        Err(SocksError::CredentialsRefused)
    ));
    assert_eq!(seen.join().unwrap(), greeting_and_credentials());
    let (proxy, _) = scripted([5, USER_PASS], [5, 0], Vec::new());
    assert!(matches!(
        client(proxy).resolve("example.com", soon()),
        Err(SocksError::Protocol)
    ));
}

#[test]
fn a_broken_reply_fails() {
    for reply in [
        vec![4, 0, 0, 1, 8, 8, 4, 4, 0, 0],
        vec![5, 0, 1, 1, 8, 8, 4, 4, 0, 0],
        vec![5, 0, 0, 9, 8, 8, 4, 4, 0, 0],
        vec![5, 0, 0, 1, 8, 8],
    ] {
        let (proxy, _) = scripted([5, USER_PASS], ACCEPTED, reply.clone());
        assert!(
            client(proxy).resolve("example.com", soon()).is_err(),
            "{reply:?}"
        );
    }
    let refused = vec![5, 7, 0, 1, 0, 0, 0, 0, 0, 0];
    let (proxy, _) = scripted([5, USER_PASS], ACCEPTED, refused);
    assert!(matches!(
        client(proxy).resolve("example.com", soon()),
        Err(SocksError::Refused(7))
    ));
    let long = "a".repeat(256);
    assert!(matches!(
        client("127.0.0.1:9".parse().unwrap()).resolve(&long, soon()),
        Err(SocksError::NameTooLong)
    ));
}

/// A proxy that takes the connection and never answers: the call fails at
/// its deadline, as broken off.
#[test]
fn the_deadline_bounds_every_step() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy = listener.local_addr().unwrap();
    let held = std::thread::spawn(move || listener.accept().map(|(stream, _)| stream));
    let started = Instant::now();
    let deadline = started + Duration::from_millis(300);
    let error = client(proxy).resolve("example.com", deadline).unwrap_err();
    assert!(
        matches!(&error, SocksError::Io(e) if e.kind() == io::ErrorKind::TimedOut),
        "{error:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    drop(held);
    // A deadline already past: nothing is attempted.
    let error = client(proxy)
        .resolve("example.com", Instant::now())
        .unwrap_err();
    assert!(
        matches!(&error, SocksError::Unreachable(e) if e.kind() == io::ErrorKind::TimedOut),
        "{error:?}"
    );
}

#[test]
fn an_unreachable_proxy_and_the_credentials_never_shown() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let closed = listener.local_addr().unwrap();
    drop(listener);
    let client = client(closed);
    let error = client.resolve("example.com", soon()).unwrap_err();
    assert!(matches!(error, SocksError::Unreachable(_)), "{error:?}");
    for shown in [
        format!("{client:?}"),
        format!("{client:#?}"),
        error.to_string(),
        format!("{error:?}"),
    ] {
        assert!(!shown.contains(client.credentials().password()), "{shown}");
        assert!(!shown.contains(Credentials::USERNAME), "{shown}");
    }
}
