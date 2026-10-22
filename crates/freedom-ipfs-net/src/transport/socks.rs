//! TCP through the SOCKS5 proxy, `socks5`'s libp2p transport (design 0007,
//! "Modes", `socks5`, and "The onion route"; cargo feature `socks`).

use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::{BoxFuture, Pending};
use futures::FutureExt;
use libp2p::core::transport::{DialOpts, ListenerId, TransportError, TransportEvent};
use libp2p::Transport;
use multiaddr::{Multiaddr, Protocol};

use crate::dial::{DialPolicy, DialRefusal};
use crate::gate::{canonical, classify_dialed, OnionHost, Shape, Target};
use crate::socks::{SocksClient, SocksError};

/// TCP through the proxy, under the policy's gate, for `<host>/tcp/<port>`
/// and `/onion3/<address>:<port>` (each with an optional `/p2p`):
/// - **an IP literal:** checked by the address policy (configured targets
///   exempt by exact address and port), then `CONNECT` to it;
/// - **a name:** resolved through the proxy (`RESOLVE`); the answer is
///   checked as the name's (a configured name and port keep their
///   exemption), then `CONNECT` to that address. A failed, empty or refused
///   answer fails the dial, and no `CONNECT` carries the name;
/// - **an onion** (`/onion3`, or an onion `/dns*`): a well-formed v3
///   address only ([`OnionHost`]), `CONNECT` by name, with no `RESOLVE`.
///
/// A WebSocket address is the WebSocket transport's, which dials this one
/// with the address's TCP part: it is "not supported" here, so an `Or`
/// above tries it there. Every other shape is refused (a `/dnsaddr` must be
/// expanded before it is dialed; the proxy carries no QUIC), and so is
/// every target the policy refuses: `TransportError::Other`, so an `Or`
/// doesn't try it elsewhere. Nothing listens through a proxy.
///
/// It sits below the outer gate, whose permit holds each dial: a revoke
/// drops it wherever it is, `RESOLVE` included.
#[derive(Clone, Debug)]
pub struct Socks5Transport {
    client: Arc<SocksClient>,
    policy: DialPolicy,
}

impl Socks5Transport {
    pub(crate) fn new(client: Arc<SocksClient>, policy: DialPolicy) -> Self {
        Self { client, policy }
    }
}

/// Why a dial through the proxy failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Socks5DialError {
    /// The policy refused the target, or the address `RESOLVE` answered.
    #[error("refused by the network policy: {0}")]
    Refused(#[source] DialRefusal),
    /// The proxy failed the request, or couldn't be reached.
    #[error(transparent)]
    Socks(#[from] SocksError),
}

type Dial = BoxFuture<'static, Result<libp2p::tcp::tokio::TcpStream, Socks5DialError>>;

impl Transport for Socks5Transport {
    type Output = libp2p::tcp::tokio::TcpStream;
    type Error = Socks5DialError;
    type ListenerUpgrade = Pending<Result<Self::Output, Self::Error>>;
    type Dial = Dial;

    fn listen_on(
        &mut self,
        _id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        Err(TransportError::MultiaddrNotSupported(addr))
    }

    fn remove_listener(&mut self, _id: ListenerId) -> bool {
        false
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        _opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        let refused = |refusal| TransportError::Other(Socks5DialError::Refused(refusal));
        let dial =
            classify_dialed(&addr).map_err(|refusal| refused(DialRefusal::Target(refusal)))?;
        match dial.shape {
            Shape::Tcp | Shape::Onion => {}
            Shape::Ws | Shape::Wss => return Err(TransportError::MultiaddrNotSupported(addr)),
            _ => return Err(refused(DialRefusal::Transport)),
        }
        // The capability, the shape and the onion route, and an IP
        // literal's address.
        self.policy.check_now(&addr).map_err(refused)?;
        let Some(port) = dial.port else {
            return Err(refused(DialRefusal::Transport));
        };
        let client = self.client.clone();
        Ok(match dial.target {
            Target::Ip(ip) => async move {
                let stream = client.connect_ip(SocketAddr::new(ip, port)).await?;
                Ok(libp2p::tcp::tokio::TcpStream(stream))
            }
            .boxed(),
            Target::Onion(_) => {
                let host = OnionHost::new(&dial.target).ok_or(refused(DialRefusal::OnionRoute))?;
                async move {
                    let stream = client.connect_onion(&host, port).await?;
                    Ok(libp2p::tcp::tokio::TcpStream(stream))
                }
                .boxed()
            }
            Target::Name(name) => {
                let policy = self.policy.clone();
                async move {
                    let ip = canonical(client.resolve(&name).await?);
                    let answered = with_host(&addr, ip);
                    policy
                        .check_answer_now(&addr, &answered)
                        .map_err(Socks5DialError::Refused)?;
                    let stream = client.connect_ip(SocketAddr::new(ip, port)).await?;
                    Ok(libp2p::tcp::tokio::TcpStream(stream))
                }
                .boxed()
            }
        })
    }

    fn poll(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        Poll::Pending
    }
}

/// `addr` with its host component replaced by `ip`: the address the
/// answer puts in place of the name.
fn with_host(addr: &Multiaddr, ip: IpAddr) -> Multiaddr {
    addr.iter()
        .map(|protocol| match protocol {
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) => match ip {
                IpAddr::V4(v4) => Protocol::Ip4(v4),
                IpAddr::V6(v6) => Protocol::Ip6(v6),
            },
            other => other,
        })
        .collect()
}
