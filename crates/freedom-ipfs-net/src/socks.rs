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
//! proxy authenticates the same way (`SocksAuth`). The proxy's endpoint
//! is an IP literal: reaching it needs no DNS. Nothing here has a deadline
//! of its own: the dial's caller bounds it (a swarm's connection timeout),
//! and the network's capability drops it.

#[cfg(feature = "test-util")]
pub mod fixture;

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::gate::OnionHost;
use crate::resolve::{LookupError, LookupFuture, NameLookup};

const VERSION: u8 = 5;
const NO_AUTH: u8 = 0x00;
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

/// How the client authenticates to the proxy: the one method its greeting
/// offers. A proxy that picks another fails the request; nothing falls back.
///
/// Whether each instance sends credentials of its own, so that the proxy
/// keeps its streams on circuits of their own, is design 0007's open
/// question 11. Credentials would be a second variant, offered alone, sent
/// by RFC 1929's sub-negotiation in [`SocksAuth::authenticate`], and made
/// once per network where the network builds its client.
#[derive(Clone, Debug)]
pub(crate) enum SocksAuth {
    /// No authentication (method `0x00`).
    None,
}

impl SocksAuth {
    /// The method the greeting offers, alone.
    fn method(&self) -> u8 {
        match self {
            Self::None => NO_AUTH,
        }
    }

    /// The method's sub-negotiation, once the proxy has picked it.
    async fn authenticate(&self, _stream: &mut TcpStream) -> Result<(), SocksError> {
        match self {
            Self::None => Ok(()),
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
    pub(crate) fn new(proxy: SocketAddr) -> Self {
        Self {
            proxy,
            auth: SocksAuth::None,
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

    /// A proxy that answers the greeting with `choice`, then the request
    /// with `reply`, raw, and returns what the client sent.
    async fn scripted(
        choice: [u8; 2],
        reply: Vec<u8>,
    ) -> (SocketAddr, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            stream.write_all(&choice).await.unwrap();
            let mut seen = greeting.to_vec();
            if choice[1] == NO_AUTH {
                let mut request = vec![0u8; 512];
                let n = stream.read(&mut request).await.unwrap();
                seen.extend_from_slice(&request[..n]);
                stream.write_all(&reply).await.unwrap();
            }
            seen
        });
        (addr, task)
    }

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
            let (proxy, seen) = scripted([5, NO_AUTH], reply.clone()).await;
            let answer = SocksClient::new(proxy).resolve("example.com").await;
            assert_eq!(answer.as_ref().ok(), expected.as_ref(), "{reply:?}");
            let mut sent = vec![5, 1, NO_AUTH, 5, RESOLVE, 0, 3, 11];
            sent.extend_from_slice(b"example.com");
            sent.extend_from_slice(&[0, 0]);
            assert_eq!(seen.await.unwrap(), sent);
        }
    }

    #[tokio::test]
    async fn connect_sends_an_ip_literal_or_an_onion_name() {
        let ok = vec![5, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        let (proxy, seen) = scripted([5, NO_AUTH], ok.clone()).await;
        SocksClient::new(proxy)
            .connect_ip("[2001:db8::1]:4001".parse().unwrap())
            .await
            .unwrap();
        let mut sent = vec![5, 1, NO_AUTH, 5, CONNECT, 0, ATYP_IPV6];
        sent.extend_from_slice(&"2001:db8::1".parse::<Ipv6Addr>().unwrap().octets());
        sent.extend_from_slice(&4001u16.to_be_bytes());
        assert_eq!(seen.await.unwrap(), sent);

        let (proxy, seen) = scripted([5, NO_AUTH], ok).await;
        let onion = format!(
            "{}.onion",
            data_encoding::BASE32_NOPAD
                .encode(&[[9u8; 34].as_slice(), &[3]].concat())
                .to_ascii_lowercase()
        );
        let host = OnionHost::new(&crate::gate::Target::Onion(onion.clone())).unwrap();
        SocksClient::new(proxy)
            .connect_onion(&host, 80)
            .await
            .unwrap();
        let mut sent = vec![5, 1, NO_AUTH, 5, CONNECT, 0, ATYP_DOMAIN, 62];
        sent.extend_from_slice(onion.as_bytes());
        sent.extend_from_slice(&80u16.to_be_bytes());
        assert_eq!(seen.await.unwrap(), sent);
    }

    #[tokio::test]
    async fn a_refused_method_or_a_broken_reply_fails() {
        // The proxy wants another method: nothing more is sent.
        let (proxy, seen) = scripted([5, 0xFF], Vec::new()).await;
        assert!(matches!(
            SocksClient::new(proxy).resolve("example.com").await,
            Err(SocksError::MethodRefused)
        ));
        assert_eq!(seen.await.unwrap(), [5, 1, NO_AUTH]);
        for reply in [
            // Another version, a nonzero reserved byte, an unknown type.
            vec![4, 0, 0, 1, 8, 8, 4, 4, 0, 0],
            vec![5, 0, 1, 1, 8, 8, 4, 4, 0, 0],
            vec![5, 0, 0, 9, 8, 8, 4, 4, 0, 0],
            // Truncated.
            vec![5, 0, 0, 1, 8, 8],
        ] {
            let (proxy, _) = scripted([5, NO_AUTH], reply.clone()).await;
            assert!(
                SocksClient::new(proxy)
                    .resolve("example.com")
                    .await
                    .is_err(),
                "{reply:?}"
            );
        }
        let refused = vec![5, 7, 0, 1, 0, 0, 0, 0, 0, 0];
        let (proxy, _) = scripted([5, NO_AUTH], refused).await;
        assert!(matches!(
            SocksClient::new(proxy).resolve("example.com").await,
            Err(SocksError::Refused(7))
        ));
        // A name SOCKS5 can't carry is refused before the proxy is asked.
        let long = "a".repeat(256);
        assert!(matches!(
            SocksClient::new("127.0.0.1:9".parse().unwrap())
                .resolve(&long)
                .await,
            Err(SocksError::NameTooLong)
        ));
    }
}
