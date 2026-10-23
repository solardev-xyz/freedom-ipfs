//! A blocking SOCKS5 client on std's sockets: the exchange the crate's byte
//! layer defines, for a host that resolves and dials on threads of its own
//! (freedom-core design 0008: freedom-radicled, under heartwood's resolver
//! thread).
//!
//! Every call has one absolute deadline. The connection to the proxy waits
//! at most until then, and so does every read and write of the exchange:
//! each one the socket makes, not just each step, so a proxy that answers a
//! byte at a time can't carry the exchange past it. A step that completes
//! after the deadline fails too. A call past its deadline fails as broken
//! off ([`SocksError::Io`], `TimedOut`). A stream that
//! [`Client::connect_ip`] or [`Client::connect_onion`] returns has its
//! timeouts cleared: what happens on it next is its caller's.

use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use freedom_ipfs_addr::OnionHost;

use crate::{
    check_auth_status, check_choice, check_reply_head, encode_request, greeting, parse_ip,
    resolved, Bound, BoundShape, Credentials, Destination, SocksError, CONNECT, RESOLVE,
};

/// A blocking SOCKS5 client: one proxy, one set of credentials, which every
/// stream it opens carries.
#[derive(Debug)]
pub struct Client {
    proxy: SocketAddr,
    credentials: Credentials,
}

impl Client {
    /// A client of `proxy`, an IP literal (reaching it needs no DNS).
    pub fn new(proxy: SocketAddr, credentials: Credentials) -> Self {
        Self { proxy, credentials }
    }

    /// The credentials every stream carries.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Resolves an ordinary name through the proxy (`RESOLVE`): one
    /// address, never `0.0.0.0` or `::`, or an error.
    pub fn resolve(&self, name: &str, deadline: Instant) -> Result<IpAddr, SocksError> {
        let (_, bound) = self.request(RESOLVE, Destination::Name(name, 0), deadline)?;
        resolved(bound)
    }

    /// A stream to `addr`, an address its caller checked, through the proxy.
    pub fn connect_ip(&self, addr: SocketAddr, deadline: Instant) -> Result<TcpStream, SocksError> {
        let (stream, _) = self.request(CONNECT, Destination::Ip(addr), deadline)?;
        Ok(stream)
    }

    /// A stream to an onion service, by name, through the proxy: the onion
    /// route, which only the classifier's onion targets can take.
    pub fn connect_onion(
        &self,
        host: &OnionHost,
        port: u16,
        deadline: Instant,
    ) -> Result<TcpStream, SocksError> {
        let (stream, _) =
            self.request(CONNECT, Destination::Name(host.as_str(), port), deadline)?;
        Ok(stream)
    }

    fn request(
        &self,
        command: u8,
        destination: Destination<'_>,
        deadline: Instant,
    ) -> Result<(TcpStream, Bound), SocksError> {
        let request = encode_request(command, &destination)?;
        let left = remaining(deadline).map_err(SocksError::Unreachable)?;
        let stream =
            TcpStream::connect_timeout(&self.proxy, left).map_err(SocksError::Unreachable)?;
        let mut io = Bounded { stream, deadline };
        io.write_all(&greeting())?;
        check_choice(io.read_array()?)?;
        io.write_all(&self.credentials.request())?;
        check_auth_status(io.read_array()?)?;
        io.write_all(&request)?;
        let shape = check_reply_head(io.read_array()?)?;
        let bound = match shape.ip_len() {
            Some(len) => {
                let mut ip = [0u8; 16];
                io.read_exact(&mut ip[..len])?;
                parse_ip(shape, &ip[..len])?
            }
            None => {
                let [len] = io.read_array::<1>()?;
                let mut name = vec![0u8; usize::from(len)];
                io.read_exact(&mut name)?;
                debug_assert_eq!(shape, BoundShape::Name);
                Bound::Name
            }
        };
        io.read_array::<2>()?;
        let stream = io.stream;
        stream.set_read_timeout(None).map_err(SocksError::Io)?;
        stream.set_write_timeout(None).map_err(SocksError::Io)?;
        Ok((stream, bound))
    }
}

/// The time left before `deadline`, or `TimedOut`.
fn remaining(deadline: Instant) -> io::Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        return Err(io::Error::from(io::ErrorKind::TimedOut));
    }
    Ok(left)
}

/// A stream whose every read and write waits at most its timeout: a
/// socket. A trait, so a test can stand in a stream that trickles.
trait Timed: Read + Write {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
}

impl Timed for TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }
}

/// The exchange's stream, bounded by the deadline: before every read and
/// write the socket makes, its timeout is set to the time left, so the
/// deadline is absolute however the bytes arrive; and a step that completes
/// past it fails.
struct Bounded<S> {
    stream: S,
    deadline: Instant,
}

impl<S: Timed> Bounded<S> {
    fn write_all(&mut self, mut bytes: &[u8]) -> Result<(), SocksError> {
        while !bytes.is_empty() {
            let left = remaining(self.deadline).map_err(SocksError::Io)?;
            self.stream
                .set_write_timeout(Some(left))
                .map_err(SocksError::Io)?;
            match self.stream.write(bytes) {
                Ok(0) => return Err(SocksError::Io(io::Error::from(io::ErrorKind::WriteZero))),
                Ok(written) => bytes = bytes.get(written..).unwrap_or_default(),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(SocksError::Io(timed_out(e))),
            }
        }
        self.in_time()
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<(), SocksError> {
        while !buf.is_empty() {
            let left = remaining(self.deadline).map_err(SocksError::Io)?;
            self.stream
                .set_read_timeout(Some(left))
                .map_err(SocksError::Io)?;
            match self.stream.read(buf) {
                Ok(0) => {
                    return Err(SocksError::Io(io::Error::from(
                        io::ErrorKind::UnexpectedEof,
                    )))
                }
                Ok(read) => buf = std::mem::take(&mut buf).get_mut(read..).unwrap_or_default(),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(SocksError::Io(timed_out(e))),
            }
        }
        self.in_time()
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], SocksError> {
        let mut bytes = [0u8; N];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    /// A step done, but only if before the deadline: one that a socket's
    /// timer let finish late still fails.
    fn in_time(&self) -> Result<(), SocksError> {
        remaining(self.deadline).map(drop).map_err(SocksError::Io)
    }
}

/// A socket timeout surfaces as `WouldBlock` on some platforms: it is the
/// deadline, so it is `TimedOut`.
fn timed_out(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::WouldBlock {
        io::Error::from(io::ErrorKind::TimedOut)
    } else {
        error
    }
}

#[cfg(test)]
mod tests;
