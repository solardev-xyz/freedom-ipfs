//! The SOCKS5 client's byte layer (freedom-core design 0007, "Modes",
//! `socks5`, and "Stream isolation"; design 0008, "Network policy"), with no
//! I/O of its own, and a [`blocking`] driver on it.
//!
//! Two drivers share it: `freedom-ipfs-net`'s async one (tokio, for
//! freedom-ipfsd), and [`blocking`] (std, for freedom-radicled, whose
//! resolver runs on a thread). Each makes the same exchange, in the same
//! order, through these functions, so the bytes on the wire and every check
//! of the proxy's answers are this crate's:
//!
//! 1. the greeting ([`greeting`]): username/password (RFC 1929, `0x02`)
//!    offered **alone**. The proxy's choice ([`check_choice`]) must be that
//!    method: anything else, no authentication included, fails the request,
//!    and nothing falls back;
//! 2. RFC 1929's request ([`Credentials::request`]) and its status
//!    ([`check_auth_status`]);
//! 3. the request ([`encode_request`]): `CONNECT` to an IP literal, or by
//!    name on the onion route only, or Tor's `RESOLVE` (`0xF0`) by name;
//! 4. the reply's head ([`check_reply_head`]), its bound address
//!    ([`BoundShape`], [`parse_ip`]) and its port; for `RESOLVE`, the one
//!    address the caller checks before anything connects to it
//!    ([`resolved`]): a domain-typed answer, and `0.0.0.0` or `::`, count as
//!    no answer.
//!
//! **Credentials** ([`Credentials`]) are a fixed username and a password of
//! 128 random bits, in hex: every stream a network (or a process) sends
//! carries them, so Tor keeps its streams on circuits of their own. They are
//! never shown: no `Debug` prints them, nothing implements `Display`, and no
//! error carries them. **Errors** ([`SocksError`]) name the step, never the
//! target, the answer or a credential.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub mod blocking;

/// SOCKS's version.
pub const VERSION: u8 = 5;
/// RFC 1929's method, the only one offered.
pub const USER_PASS: u8 = 0x02;
/// RFC 1929's sub-negotiation version.
pub const USER_PASS_VERSION: u8 = 0x01;
/// `CONNECT`.
pub const CONNECT: u8 = 0x01;
/// Tor's `RESOLVE` extension.
pub const RESOLVE: u8 = 0xF0;
/// Address types.
pub const ATYP_IPV4: u8 = 1;
pub const ATYP_DOMAIN: u8 = 3;
pub const ATYP_IPV6: u8 = 4;

/// A network's SOCKS5 credentials (RFC 1929): a fixed username, and a
/// password of 128 random bits from `random`, in hex, made once.
///
/// Never shown: `Debug` prints neither part, nothing implements `Display`,
/// and no error or log carries them. They leave the process only to the
/// proxy.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    username: &'static str,
    password: String,
}

impl Credentials {
    /// The username every network sends: only the password tells networks
    /// apart.
    pub const USERNAME: &'static str = "freedom-ipfs";

    /// New credentials, the password's 16 bytes from `random`: the OS's
    /// randomness, or a test's. Its failure fails them: nothing falls back
    /// to a fixed password, or to no authentication.
    pub fn new(random: Random) -> Result<Self, RandomFailed> {
        let mut bytes = [0u8; 16];
        random(&mut bytes)?;
        let password = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(Self {
            username: Self::USERNAME,
            password,
        })
    }

    pub fn username(&self) -> &str {
        self.username
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    /// RFC 1929's request: the version, then each part with its length.
    pub fn request(&self) -> Vec<u8> {
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

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SocksCredentials(redacted)")
    }
}

/// Where the password's bits come from: fills the buffer, or fails.
pub type Random = fn(&mut [u8]) -> Result<(), RandomFailed>;

/// The operating system's randomness failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the operating system's randomness failed")]
pub struct RandomFailed;

/// Where a request goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Destination<'a> {
    Ip(SocketAddr),
    Name(&'a str, u16),
}

/// The address a reply carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    Ip(IpAddr),
    Name,
}

/// How the reply's bound address is laid out, after its head: then comes
/// its port, two bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundShape {
    /// Four bytes.
    Ipv4,
    /// Sixteen bytes.
    Ipv6,
    /// One length byte, then that many bytes.
    Name,
}

impl BoundShape {
    /// How many bytes an IP-typed address takes; `None` for a name, whose
    /// length comes first.
    pub fn ip_len(self) -> Option<usize> {
        match self {
            BoundShape::Ipv4 => Some(4),
            BoundShape::Ipv6 => Some(16),
            BoundShape::Name => None,
        }
    }
}

/// The greeting: version 5, one method, username/password.
pub fn greeting() -> [u8; 3] {
    [VERSION, 1, USER_PASS]
}

/// The proxy's choice of method: it must be username/password. Another
/// version is outside the protocol; any other method, no authentication
/// included, fails the request with no fallback.
pub fn check_choice(choice: [u8; 2]) -> Result<(), SocksError> {
    if choice[0] != VERSION {
        return Err(SocksError::Protocol);
    }
    if choice[1] != USER_PASS {
        return Err(SocksError::MethodRefused);
    }
    Ok(())
}

/// RFC 1929's status: its version, then success.
pub fn check_auth_status(status: [u8; 2]) -> Result<(), SocksError> {
    if status[0] != USER_PASS_VERSION {
        return Err(SocksError::Protocol);
    }
    if status[1] != 0 {
        return Err(SocksError::CredentialsRefused);
    }
    Ok(())
}

/// A request: the version, the command, and the destination with its port.
/// A name SOCKS5 can't carry, empty or longer than 255 bytes, is refused
/// before anything is sent.
pub fn encode_request(command: u8, destination: &Destination<'_>) -> Result<Vec<u8>, SocksError> {
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

/// The reply's first four bytes: the version and a zero reserved byte, or
/// it is outside the protocol; then success, or the proxy's refusal with
/// its code; then the bound address's type.
pub fn check_reply_head(head: [u8; 4]) -> Result<BoundShape, SocksError> {
    if head[0] != VERSION || head[2] != 0 {
        return Err(SocksError::Protocol);
    }
    if head[1] != 0 {
        return Err(SocksError::Refused(head[1]));
    }
    match head[3] {
        ATYP_IPV4 => Ok(BoundShape::Ipv4),
        ATYP_IPV6 => Ok(BoundShape::Ipv6),
        ATYP_DOMAIN => Ok(BoundShape::Name),
        _ => Err(SocksError::Protocol),
    }
}

/// An IP-typed bound address from its bytes ([`BoundShape::ip_len`]).
pub fn parse_ip(shape: BoundShape, bytes: &[u8]) -> Result<Bound, SocksError> {
    match (shape, bytes.len()) {
        (BoundShape::Ipv4, 4) => {
            let mut ip = [0u8; 4];
            ip.copy_from_slice(bytes);
            Ok(Bound::Ip(IpAddr::V4(Ipv4Addr::from(ip))))
        }
        (BoundShape::Ipv6, 16) => {
            let mut ip = [0u8; 16];
            ip.copy_from_slice(bytes);
            Ok(Bound::Ip(IpAddr::V6(Ipv6Addr::from(ip))))
        }
        _ => Err(SocksError::Protocol),
    }
}

/// `RESOLVE`'s answer: one address, never `0.0.0.0` or `::`, and never a
/// name.
pub fn resolved(bound: Bound) -> Result<IpAddr, SocksError> {
    match bound {
        Bound::Ip(ip) if !ip.is_unspecified() => Ok(ip),
        Bound::Ip(_) | Bound::Name => Err(SocksError::EmptyAnswer),
    }
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
mod tests;
