//! The SOCKS5 client `socks5` mode goes through (design 0007, "Modes",
//! `socks5`; cargo feature `socks`).
//!
//! - **`RESOLVE`** (Tor's extension, command `0xF0`) resolves an ordinary
//!   name through the proxy. The answer must be one address, which the
//!   caller checks before anything connects to it: a domain-typed answer,
//!   and `0.0.0.0` or `::`, count as no answer.
//! - **`CONNECT`** to an IP literal, the checked address; or, on the onion
//!   route only, to an [`OnionHost`] by name.
//!
//! One client per network, so every request the network sends through the
//! proxy authenticates the same way (`SocksAuth`): with the network's own
//! credentials (RFC 1929), so the proxy keeps its streams on circuits of
//! their own (design 0007, "Stream isolation"). The greeting offers only
//! username/password: a proxy that won't take it fails the request, and
//! nothing falls back to no authentication. The proxy's endpoint is an IP
//! literal: reaching it needs no DNS. Nothing here has a deadline of its
//! own: the dial's caller bounds it (a swarm's connection timeout), and the
//! network's capability drops it.

#[cfg(feature = "test-util")]
pub mod fixture;

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::gate::OnionHost;
use crate::resolve::{LookupError, LookupFuture, NameLookup};

const VERSION: u8 = 5;
#[cfg(test)]
const NO_AUTH: u8 = 0x00;
/// RFC 1929's method, and its sub-negotiation's version.
const USER_PASS: u8 = 0x02;
const USER_PASS_VERSION: u8 = 0x01;
const CONNECT: u8 = 0x01;
/// Tor's `RESOLVE` extension.
const RESOLVE: u8 = 0xF0;
const ATYP_IPV4: u8 = 1;
const ATYP_DOMAIN: u8 = 3;
const ATYP_IPV6: u8 = 4;

/// A network's SOCKS5 client.
#[derive(Debug)]
pub(crate) struct SocksClient {
    proxy: SocketAddr,
    auth: SocksAuth,
}

/// A network's SOCKS5 credentials (RFC 1929; design 0007, "Stream
/// isolation"): a fixed username, and a password of 128 random bits from
/// the operating system, in hex, made when the network is built. Every
/// stream the network sends through the proxy carries them, so Arti keeps
/// its streams on circuits of their own, apart from every other network's.
///
/// Never shown: `Debug` prints neither part, nothing implements `Display`,
/// and no error or log carries them. They leave the process only to the
/// proxy.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SocksCredentials {
    username: &'static str,
    password: String,
}

impl SocksCredentials {
    /// The username every network sends: only the password tells networks
    /// apart.
    pub(crate) const USERNAME: &'static str = "freedom-ipfs";

    /// New credentials, the password's 16 bytes from `random`: the OS's
    /// randomness ([`os_random`]), or a test's. Its failure fails them:
    /// nothing falls back to a fixed password, or to no authentication.
    pub(crate) fn new(random: Random) -> Result<Self, RandomFailed> {
        let mut bytes = [0u8; 16];
        random(&mut bytes)?;
        let password = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(Self {
            username: Self::USERNAME,
            password,
        })
    }

    pub(crate) fn username(&self) -> &str {
        self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    /// RFC 1929's request: the version, then each part with its length.
    fn request(&self) -> Vec<u8> {
        let (user, pass) = (self.username.as_bytes(), self.password.as_bytes());
        let mut request = Vec::with_capacity(3 + user.len() + pass.len());
        request.push(USER_PASS_VERSION);
        // Both are far under 255 bytes: a fixed name, and 32 hex digits.
        request.push(user.len() as u8);
        request.extend_from_slice(user);
        request.push(pass.len() as u8);
        request.extend_from_slice(pass);
        request
    }
}

impl fmt::Debug for SocksCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SocksCredentials(redacted)")
    }
}

/// Where the password's bits come from: fills the buffer, or fails.
pub(crate) type Random = fn(&mut [u8]) -> Result<(), RandomFailed>;

/// The operating system's randomness, through its fallible API.
pub(crate) fn os_random(bytes: &mut [u8]) -> Result<(), RandomFailed> {
    getrandom::fill(bytes).map_err(|_| RandomFailed)
}

/// The operating system's randomness failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the operating system's randomness failed")]
pub struct RandomFailed;

/// How the client authenticates to the proxy: the one method its greeting
/// offers, username/password, alone. A proxy that picks another fails the
/// request; nothing falls back.
#[derive(Clone, Debug)]
pub(crate) enum SocksAuth {
    /// RFC 1929, with the network's credentials (method `0x02`).
    UserPass(SocksCredentials),
}

impl SocksAuth {
    /// The method the greeting offers, alone.
    fn method(&self) -> u8 {
        match self {
            Self::UserPass(_) => USER_PASS,
        }
    }

    /// The method's sub-negotiation, once the proxy has picked it.
    async fn authenticate(&self, stream: &mut TcpStream) -> Result<(), SocksError> {
        match self {
            Self::UserPass(credentials) => {
                stream
                    .write_all(&credentials.request())
                    .await
                    .map_err(SocksError::Io)?;
                let mut status = [0u8; 2];
                stream
                    .read_exact(&mut status)
                    .await
                    .map_err(SocksError::Io)?;
                if status[0] != USER_PASS_VERSION {
                    return Err(SocksError::Protocol);
                }
                if status[1] != 0 {
                    return Err(SocksError::CredentialsRefused);
                }
                Ok(())
            }
        }
    }
}

/// The gate's resolver's lookup under `socks5`: Tor's `RESOLVE` through the
/// network's proxy, one address per name, never a local resolver. The gate's
/// resolver checks the answer.
#[derive(Debug)]
pub(crate) struct SocksLookup(pub(crate) Arc<SocksClient>);

impl NameLookup for SocksLookup {
    fn lookup(&self, name: &str) -> LookupFuture {
        let client = self.0.clone();
        let name = name.to_string();
        Box::pin(async move {
            client
                .resolve(&name)
                .await
                .map(|ip| vec![ip])
                .map_err(|err| LookupError(err.to_string()))
        })
    }
}

/// Where a request goes.
enum Destination<'a> {
    Ip(SocketAddr),
    Name(&'a str, u16),
}

/// The address a reply carries.
enum Bound {
    Ip(IpAddr),
    Name,
}

impl SocksClient {
    /// The client of a network whose credentials are `credentials`.
    pub(crate) fn new(proxy: SocketAddr, credentials: SocksCredentials) -> Self {
        Self {
            proxy,
            auth: SocksAuth::UserPass(credentials),
        }
    }

    /// The credentials every stream of its network carries.
    pub(crate) fn credentials(&self) -> &SocksCredentials {
        match &self.auth {
            SocksAuth::UserPass(credentials) => credentials,
        }
    }

    /// Resolves an ordinary name through the proxy (`RESOLVE`): one
    /// address, never `0.0.0.0` or `::`, or an error.
    pub(crate) async fn resolve(&self, name: &str) -> Result<IpAddr, SocksError> {
        let (_, bound) = self.request(RESOLVE, Destination::Name(name, 0)).await?;
        match bound {
            Bound::Ip(ip) if !ip.is_unspecified() => Ok(ip),
            Bound::Ip(_) | Bound::Name => Err(SocksError::EmptyAnswer),
        }
    }

    /// A stream to `addr`, an address the gate checked, through the proxy.
    pub(crate) async fn connect_ip(&self, addr: SocketAddr) -> Result<TcpStream, SocksError> {
        let (stream, _) = self.request(CONNECT, Destination::Ip(addr)).await?;
        Ok(stream)
    }

    /// A stream to an onion service, by name, through the proxy: the onion
    /// route.
    pub(crate) async fn connect_onion(
        &self,
        host: &OnionHost,
        port: u16,
    ) -> Result<TcpStream, SocksError> {
        let (stream, _) = self
            .request(CONNECT, Destination::Name(host.as_str(), port))
            .await?;
        Ok(stream)
    }

    async fn request(
        &self,
        command: u8,
        destination: Destination<'_>,
    ) -> Result<(TcpStream, Bound), SocksError> {
        let request = encode_request(command, &destination)?;
        let mut stream = TcpStream::connect(self.proxy)
            .await
            .map_err(SocksError::Unreachable)?;
        let method = self.auth.method();
        stream
            .write_all(&[VERSION, 1, method])
            .await
            .map_err(SocksError::Io)?;
        let mut choice = [0u8; 2];
        stream
            .read_exact(&mut choice)
            .await
            .map_err(SocksError::Io)?;
        if choice[0] != VERSION {
            return Err(SocksError::Protocol);
        }
        if choice[1] != method {
            return Err(SocksError::MethodRefused);
        }
        self.auth.authenticate(&mut stream).await?;
        stream.write_all(&request).await.map_err(SocksError::Io)?;
        let mut head = [0u8; 4];
        stream.read_exact(&mut head).await.map_err(SocksError::Io)?;
        if head[0] != VERSION || head[2] != 0 {
            return Err(SocksError::Protocol);
        }
        if head[1] != 0 {
            return Err(SocksError::Refused(head[1]));
        }
        let bound = read_bound(&mut stream, head[3]).await?;
        Ok((stream, bound))
    }
}

/// A request: the version, the command, and the destination with its port.
fn encode_request(command: u8, destination: &Destination<'_>) -> Result<Vec<u8>, SocksError> {
    let mut request = vec![VERSION, command, 0];
    let port = match destination {
        Destination::Ip(addr) => {
            match addr.ip() {
                IpAddr::V4(ip) => {
                    request.push(ATYP_IPV4);
                    request.extend_from_slice(&ip.octets());
                }
                IpAddr::V6(ip) => {
                    request.push(ATYP_IPV6);
                    request.extend_from_slice(&ip.octets());
                }
            }
            addr.port()
        }
        Destination::Name(name, port) => {
            let len = u8::try_from(name.len()).map_err(|_| SocksError::NameTooLong)?;
            if len == 0 {
                return Err(SocksError::NameTooLong);
            }
            request.push(ATYP_DOMAIN);
            request.push(len);
            request.extend_from_slice(name.as_bytes());
            *port
        }
    };
    request.extend_from_slice(&port.to_be_bytes());
    Ok(request)
}

/// A reply's bound address and port, after its first four bytes.
async fn read_bound(stream: &mut TcpStream, atyp: u8) -> Result<Bound, SocksError> {
    let bound = match atyp {
        ATYP_IPV4 => {
            let mut ip = [0u8; 4];
            stream.read_exact(&mut ip).await.map_err(SocksError::Io)?;
            Bound::Ip(IpAddr::V4(Ipv4Addr::from(ip)))
        }
        ATYP_IPV6 => {
            let mut ip = [0u8; 16];
            stream.read_exact(&mut ip).await.map_err(SocksError::Io)?;
            Bound::Ip(IpAddr::V6(Ipv6Addr::from(ip)))
        }
        ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await.map_err(SocksError::Io)?;
            let mut name = vec![0u8; usize::from(len[0])];
            stream.read_exact(&mut name).await.map_err(SocksError::Io)?;
            Bound::Name
        }
        _ => return Err(SocksError::Protocol),
    };
    let mut port = [0u8; 2];
    stream.read_exact(&mut port).await.map_err(SocksError::Io)?;
    Ok(bound)
}

/// Why a request through the proxy failed. Each text names the step, never
/// the target, the answer or any credential.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SocksError {
    /// No connection to the proxy.
    #[error("the SOCKS5 proxy can't be reached: {0}")]
    Unreachable(#[source] io::Error),
    /// The exchange with the proxy broke off.
    #[error("the exchange with the SOCKS5 proxy failed: {0}")]
    Io(#[source] io::Error),
    /// The proxy answered outside the protocol.
    #[error("the SOCKS5 proxy answered outside the protocol")]
    Protocol,
    /// The proxy didn't accept the one authentication method offered.
    #[error("the SOCKS5 proxy refused the authentication method")]
    MethodRefused,
    /// The proxy refused the network's credentials.
    #[error("the SOCKS5 proxy refused the credentials")]
    CredentialsRefused,
    /// The proxy refused the request, with this reply code.
    #[error("the SOCKS5 proxy refused the request (reply {0:#04x})")]
    Refused(u8),
    /// `RESOLVE` answered no usable address.
    #[error("the SOCKS5 proxy's RESOLVE answered no address")]
    EmptyAnswer,
    /// A name SOCKS5 can't carry: empty, or longer than 255 bytes.
    #[error("the name can't be carried in a SOCKS5 request")]
    NameTooLong,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Fills with 0x11: credentials a test can predict.
    fn elevens(bytes: &mut [u8]) -> Result<(), RandomFailed> {
        bytes.fill(0x11);
        Ok(())
    }

    fn credentials() -> SocksCredentials {
        SocksCredentials::new(elevens).unwrap()
    }

    fn client(proxy: SocketAddr) -> SocksClient {
        SocksClient::new(proxy, credentials())
    }

    /// What the client sends first: the greeting offering username and
    /// password alone, then RFC 1929's request with `credentials()`.
    fn greeting_and_credentials() -> Vec<u8> {
        let mut sent = vec![5, 1, USER_PASS, USER_PASS_VERSION, 12];
        sent.extend_from_slice(b"freedom-ipfs");
        sent.push(32);
        sent.extend_from_slice("11".repeat(16).as_bytes());
        sent
    }

    /// A proxy that answers the greeting with `choice`, RFC 1929's request
    /// (if it chose that method) with `status`, then the request with
    /// `reply`, raw, and returns what the client sent.
    async fn scripted(
        choice: [u8; 2],
        status: [u8; 2],
        reply: Vec<u8>,
    ) -> (SocketAddr, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // The greeting, by the count of methods it declares.
            let mut head = [0u8; 2];
            stream.read_exact(&mut head).await.unwrap();
            let mut methods = vec![0u8; usize::from(head[1])];
            stream.read_exact(&mut methods).await.unwrap();
            stream.write_all(&choice).await.unwrap();
            let mut seen = [head.as_slice(), &methods].concat();
            if choice[1] != USER_PASS {
                return seen;
            }
            let mut head = [0u8; 2];
            stream.read_exact(&mut head).await.unwrap();
            let mut user = vec![0u8; usize::from(head[1])];
            stream.read_exact(&mut user).await.unwrap();
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await.unwrap();
            let mut pass = vec![0u8; usize::from(len[0])];
            stream.read_exact(&mut pass).await.unwrap();
            seen.extend_from_slice(&head);
            seen.extend_from_slice(&user);
            seen.extend_from_slice(&len);
            seen.extend_from_slice(&pass);
            stream.write_all(&status).await.unwrap();
            if status != [USER_PASS_VERSION, 0] {
                return seen;
            }
            let mut request = vec![0u8; 512];
            let n = stream.read(&mut request).await.unwrap();
            seen.extend_from_slice(&request[..n]);
            stream.write_all(&reply).await.unwrap();
            seen
        });
        (addr, task)
    }

    const ACCEPTED: [u8; 2] = [USER_PASS_VERSION, 0];

    #[tokio::test]
    async fn resolve_takes_one_address_and_nothing_else() {
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
            let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, reply.clone()).await;
            let answer = client(proxy).resolve("example.com").await;
            assert_eq!(answer.as_ref().ok(), expected.as_ref(), "{reply:?}");
            let mut sent = greeting_and_credentials();
            sent.extend_from_slice(&[5, RESOLVE, 0, 3, 11]);
            sent.extend_from_slice(b"example.com");
            sent.extend_from_slice(&[0, 0]);
            assert_eq!(seen.await.unwrap(), sent);
        }
    }

    #[tokio::test]
    async fn connect_sends_an_ip_literal_or_an_onion_name() {
        let ok = vec![5, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, ok.clone()).await;
        client(proxy)
            .connect_ip("[2001:db8::1]:4001".parse().unwrap())
            .await
            .unwrap();
        let mut sent = greeting_and_credentials();
        sent.extend_from_slice(&[5, CONNECT, 0, ATYP_IPV6]);
        sent.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        sent.extend_from_slice(&4001u16.to_be_bytes());
        assert_eq!(seen.await.unwrap(), sent);

        let (proxy, seen) = scripted([5, USER_PASS], ACCEPTED, ok).await;
        let onion = format!(
            "{}.onion",
            data_encoding::BASE32_NOPAD
                .encode(&[[9u8; 34].as_slice(), &[3]].concat())
                .to_ascii_lowercase()
        );
        let host = OnionHost::new(&crate::gate::Target::Onion(onion.clone())).unwrap();
        client(proxy).connect_onion(&host, 80).await.unwrap();
        let mut sent = greeting_and_credentials();
        sent.extend_from_slice(&[5, CONNECT, 0, ATYP_DOMAIN, 62]);
        sent.extend_from_slice(onion.as_bytes());
        sent.extend_from_slice(&80u16.to_be_bytes());
        assert_eq!(seen.await.unwrap(), sent);
    }

    #[tokio::test]
    async fn a_proxy_that_wont_take_the_credentials_fails_with_no_fallback() {
        // It picks no authentication, or none acceptable: nothing more is
        // sent, the credentials included.
        for choice in [[5, NO_AUTH], [5, 0xFF]] {
            let (proxy, seen) = scripted(choice, ACCEPTED, Vec::new()).await;
            assert!(matches!(
                client(proxy).resolve("example.com").await,
                Err(SocksError::MethodRefused)
            ));
            assert_eq!(seen.await.unwrap(), [5, 1, USER_PASS], "{choice:?}");
        }
        // It refuses them: nothing more is sent.
        let (proxy, seen) = scripted([5, USER_PASS], [USER_PASS_VERSION, 1], Vec::new()).await;
        assert!(matches!(
            client(proxy).resolve("example.com").await,
            Err(SocksError::CredentialsRefused)
        ));
        assert_eq!(seen.await.unwrap(), greeting_and_credentials());
        // Its RFC 1929 reply is in another version.
        let (proxy, _) = scripted([5, USER_PASS], [5, 0], Vec::new()).await;
        assert!(matches!(
            client(proxy).resolve("example.com").await,
            Err(SocksError::Protocol)
        ));
    }

    #[tokio::test]
    async fn a_broken_reply_fails() {
        for reply in [
            // Another version, a nonzero reserved byte, an unknown type.
            vec![4, 0, 0, 1, 8, 8, 4, 4, 0, 0],
            vec![5, 0, 1, 1, 8, 8, 4, 4, 0, 0],
            vec![5, 0, 0, 9, 8, 8, 4, 4, 0, 0],
            // Truncated.
            vec![5, 0, 0, 1, 8, 8],
        ] {
            let (proxy, _) = scripted([5, USER_PASS], ACCEPTED, reply.clone()).await;
            assert!(
                client(proxy).resolve("example.com").await.is_err(),
                "{reply:?}"
            );
        }
        let refused = vec![5, 7, 0, 1, 0, 0, 0, 0, 0, 0];
        let (proxy, _) = scripted([5, USER_PASS], ACCEPTED, refused).await;
        assert!(matches!(
            client(proxy).resolve("example.com").await,
            Err(SocksError::Refused(7))
        ));
        // A name SOCKS5 can't carry is refused before the proxy is asked.
        let long = "a".repeat(256);
        assert!(matches!(
            client("127.0.0.1:9".parse().unwrap()).resolve(&long).await,
            Err(SocksError::NameTooLong)
        ));
    }

    #[test]
    fn credentials_are_the_fixed_username_and_128_random_bits() {
        let one = SocksCredentials::new(os_random).unwrap();
        let two = SocksCredentials::new(os_random).unwrap();
        assert_eq!(one.username(), SocksCredentials::USERNAME);
        assert_eq!(one.password().len(), 32);
        assert!(one.password().bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(one.password(), two.password());
        // The OS's randomness failing fails them: no fixed password.
        fn failing(_: &mut [u8]) -> Result<(), RandomFailed> {
            Err(RandomFailed)
        }
        assert_eq!(SocksCredentials::new(failing), Err(RandomFailed));
    }

    #[tokio::test]
    async fn the_credentials_are_never_shown() {
        let credentials = SocksCredentials::new(os_random).unwrap();
        let client = SocksClient::new("127.0.0.1:9".parse().unwrap(), credentials.clone());
        let mut shown = vec![
            format!("{credentials:?}"),
            format!("{:?}", SocksAuth::UserPass(credentials.clone())),
            format!("{client:?}"),
            format!("{client:#?}"),
        ];
        // Every error, as its text and its Debug, a real one included.
        let unreachable = client.resolve("example.com").await.unwrap_err();
        let errors = [
            unreachable,
            SocksError::Protocol,
            SocksError::MethodRefused,
            SocksError::CredentialsRefused,
            SocksError::Refused(1),
            SocksError::EmptyAnswer,
            SocksError::NameTooLong,
        ];
        for error in &errors {
            shown.push(error.to_string());
            shown.push(format!("{error:?}"));
        }
        shown.push(RandomFailed.to_string());
        for text in shown {
            assert!(!text.contains(credentials.password()), "{text}");
            assert!(!text.contains(SocksCredentials::USERNAME), "{text}");
        }
    }
}
