// Adapted from libp2p-dns 0.45.0, `src/lib.rs`: its `Transport`, which
// resolves a multiaddr's `/dns`, `/dns4`, `/dns6` and `/dnsaddr` components
// before the transport below dials it. libp2p-dns 0.45 resolves with hickory
// 0.26, but needs libp2p-core 0.44 (libp2p 0.57); libp2p 0.56's own,
// libp2p-dns 0.44, resolves with hickory 0.25, whose message decoder
// reserves room for as many records as a header claims before reading any
// (RUSTSEC-2026-0491). So the layer is here, over libp2p 0.56's transports
// and the network's own resolver ([`DnsResolver`]). Changed from libp2p-dns:
// - it resolves through a [`DnsResolver`] alone, with no `Resolver` trait,
//   and has no constructor of its own configuration (the network's
//   [`crate::resolve::DnsConfig`] builds the resolver);
// - an answer with no address of the type asked for is an error, where
//   libp2p-dns panics on one (`expect`), and a replaced component's index
//   is never `expect`ed either;
// - a resolved address goes on as a [`Protocol`] built from the address,
//   and errors are reported through `tracing` as before;
// - the module's documentation and these notes.
//
// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! The DNS layer of the libp2p transports: a [`Transport`] that dials
//! through the transport below it once every DNS component of the address
//! is resolved (`/dns`, `/dns4` and `/dns6` to IP addresses, `/dnsaddr` to
//! the multiaddrs its TXT records give), as libp2p's own (libp2p-dns) does.

use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{error, fmt, str};

use futures::future::{self, BoxFuture};
use futures::{FutureExt, TryFutureExt};
use hickory_resolver::net::NetError;
use hickory_resolver::proto::rr::RData;
use libp2p::core::transport::{DialOpts, ListenerId, TransportError, TransportEvent};
use multiaddr::{Multiaddr, Protocol};
use parking_lot::Mutex;

use crate::resolve::DnsResolver;

/// The prefix for `dnsaddr` protocol TXT record lookups.
const DNSADDR_PREFIX: &str = "_dnsaddr.";

/// The maximum number of dialing attempts to resolved addresses.
const MAX_DIAL_ATTEMPTS: usize = 16;

/// The maximum number of DNS lookups when dialing.
///
/// This limit is primarily a safeguard against too many, possibly
/// even cyclic, indirections in the addresses obtained from the
/// TXT records of a `/dnsaddr`.
const MAX_DNS_LOOKUPS: usize = 32;

/// The maximum number of TXT records applicable for the address
/// being dialed that are considered for further lookups as a
/// result of a single `/dnsaddr` lookup.
const MAX_TXT_RECORDS: usize = 16;

/// A transport that performs DNS lookups when dialing `Multiaddr`esses.
pub struct Transport<T> {
    /// The underlying transport.
    inner: Arc<Mutex<T>>,
    /// The DNS resolver used when dialing addresses with DNS components.
    resolver: DnsResolver,
}

impl<T> Transport<T> {
    /// `inner`, its dials' DNS components resolved through `resolver`.
    pub fn new(inner: T, resolver: DnsResolver) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            resolver,
        }
    }
}

impl<T> fmt::Debug for Transport<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("dns::Transport")
    }
}

impl<T> libp2p::Transport for Transport<T>
where
    T: libp2p::Transport + Send + Unpin + 'static,
    T::Error: Send,
    T::Dial: Send,
{
    type Output = T::Output;
    type Error = Error<T::Error>;
    type ListenerUpgrade = future::MapErr<T::ListenerUpgrade, fn(T::Error) -> Self::Error>;
    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn listen_on(
        &mut self,
        id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        self.inner
            .lock()
            .listen_on(id, addr)
            .map_err(|e| e.map(Error::Transport))
    }

    fn remove_listener(&mut self, id: ListenerId) -> bool {
        self.inner.lock().remove_listener(id)
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        dial_opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        Ok(self.do_dial(addr, dial_opts))
    }

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        let mut inner = self.inner.lock();
        libp2p::Transport::poll(Pin::new(&mut *inner), cx).map(|event| {
            event
                .map_upgrade(|upgr| upgr.map_err::<_, fn(_) -> _>(Error::Transport))
                .map_err(Error::Transport)
        })
    }
}

impl<T> Transport<T>
where
    T: libp2p::Transport + Send + Unpin + 'static,
    T::Error: Send,
    T::Dial: Send,
{
    fn do_dial(
        &mut self,
        addr: Multiaddr,
        dial_opts: DialOpts,
    ) -> BoxFuture<'static, Result<T::Output, Error<T::Error>>> {
        let resolver = self.resolver.clone();
        let inner = self.inner.clone();

        // Asynchronously resolve all DNS names in the address before proceeding
        // with dialing on the underlying transport.
        async move {
            let mut dial_errors: Vec<Error<T::Error>> = Vec::new();
            let mut dns_lookups = 0;
            let mut dial_attempts = 0;
            let mut unresolved = vec![addr];

            // Resolve (i.e. replace) all DNS protocol components, initiating
            // dialing attempts as soon as there is another fully resolved
            // address.
            while let Some(addr) = unresolved.pop() {
                if let Some((i, name)) = addr.iter().enumerate().find(|(_, p)| {
                    matches!(
                        p,
                        Protocol::Dns(_)
                            | Protocol::Dns4(_)
                            | Protocol::Dns6(_)
                            | Protocol::Dnsaddr(_)
                    )
                }) {
                    if dns_lookups == MAX_DNS_LOOKUPS {
                        tracing::debug!(address=%addr, "Too many DNS lookups, dropping unresolved address");
                        dial_errors.push(Error::TooManyLookups);
                        // There may still be fully resolved addresses in `unresolved`,
                        // so keep going until `unresolved` is empty.
                        continue;
                    }
                    dns_lookups += 1;
                    match resolve(&name, &resolver).await {
                        Err(e) => {
                            // Record the resolution error.
                            dial_errors.push(Error::ResolveError(e));
                        }
                        Ok(Resolved::Ips(ips)) => {
                            for ip in ips {
                                tracing::trace!(protocol=%name, resolved=%ip);
                                if let Some(addr) = addr.replace(i, |_| Some(Protocol::from(ip))) {
                                    unresolved.push(addr);
                                }
                            }
                        }
                        Ok(Resolved::Addrs(addrs)) => {
                            let suffix = addr.iter().skip(i + 1).collect::<Multiaddr>();
                            let prefix = addr.iter().take(i).collect::<Multiaddr>();
                            let mut n = 0;
                            for a in addrs {
                                if a.ends_with(&suffix) {
                                    if n < MAX_TXT_RECORDS {
                                        n += 1;
                                        tracing::trace!(protocol=%name, resolved=%a);
                                        let addr =
                                            prefix.iter().chain(a.iter()).collect::<Multiaddr>();
                                        unresolved.push(addr);
                                    } else {
                                        tracing::debug!(
                                            resolved=%a,
                                            "Too many TXT records, dropping resolved"
                                        );
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // We have a fully resolved address, so try to dial it.
                    tracing::debug!(address=%addr, "Dialing address");

                    let dial = inner.lock().dial(addr, dial_opts);
                    let result = match dial {
                        Ok(out) => {
                            // We only count attempts that the inner transport
                            // actually accepted, i.e. for which it produced
                            // a dialing future.
                            dial_attempts += 1;
                            out.await.map_err(Error::Transport)
                        }
                        Err(TransportError::MultiaddrNotSupported(a)) => {
                            Err(Error::MultiaddrNotSupported(a))
                        }
                        Err(TransportError::Other(err)) => Err(Error::Transport(err)),
                    };

                    match result {
                        Ok(out) => return Ok(out),
                        Err(err) => {
                            tracing::debug!("Dial error: {:?}.", err);
                            dial_errors.push(err);

                            if unresolved.is_empty() {
                                break;
                            }

                            if dial_attempts == MAX_DIAL_ATTEMPTS {
                                tracing::debug!(
                                    "Aborting dialing after {} attempts.",
                                    MAX_DIAL_ATTEMPTS
                                );
                                break;
                            }
                        }
                    }
                }
            }

            // If we have any dial errors, aggregate them.
            // Otherwise there were no valid DNS records for the given address to begin with
            // (i.e. DNS lookups succeeded but produced no records relevant for the given `addr`).
            if !dial_errors.is_empty() {
                Err(Error::Dial(dial_errors))
            } else {
                Err(Error::ResolveError(NetError::from(
                    "No Matching Records Found",
                )))
            }
        }
        .boxed()
    }
}

/// The possible errors of a [`Transport`] wrapped transport.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Error<TErr> {
    /// The underlying transport encountered an error.
    Transport(TErr),
    /// DNS resolution failed.
    #[allow(clippy::enum_variant_names)]
    ResolveError(NetError),
    /// DNS resolution was successful, but the underlying transport refused the resolved address.
    MultiaddrNotSupported(Multiaddr),
    /// DNS resolution involved too many lookups.
    ///
    /// DNS resolution on dialing performs up to 32 DNS lookups. If these
    /// are not sufficient to obtain a fully-resolved address, this error
    /// is returned and the DNS records for the domain(s) being dialed
    /// should be investigated.
    TooManyLookups,
    /// Multiple dial errors were encountered.
    Dial(Vec<Error<TErr>>),
}

impl<TErr> fmt::Display for Error<TErr>
where
    TErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport(err) => write!(f, "{err}"),
            Error::ResolveError(err) => write!(f, "{err}"),
            Error::MultiaddrNotSupported(a) => write!(f, "Unsupported resolved address: {a}"),
            Error::TooManyLookups => write!(f, "Too many DNS lookups"),
            Error::Dial(errs) => {
                write!(f, "Multiple dial errors occurred:")?;
                for err in errs {
                    write!(f, "\n - {err}")?;
                }
                Ok(())
            }
        }
    }
}

impl<TErr> error::Error for Error<TErr>
where
    TErr: error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Transport(err) => Some(err),
            Error::ResolveError(err) => Some(err),
            Error::MultiaddrNotSupported(_) => None,
            Error::TooManyLookups => None,
            Error::Dial(errs) => errs.last().and_then(|e| e.source()),
        }
    }
}

/// The successful outcome of [`resolve`] for a DNS component.
enum Resolved {
    /// A `/dns`, `/dns4` or `/dns6` name's addresses, each an alternative
    /// for the component, at least one.
    Ips(Vec<IpAddr>),
    /// The given `Protocol` has been resolved to a new list of `Multiaddr`s
    /// obtained from DNS TXT records representing possible alternatives.
    /// These addresses may contain further DNS names that need resolving.
    Addrs(Vec<Multiaddr>),
}

/// Resolves the domain name of a `Dns`, `Dns4`, `Dns6` or `Dnsaddr`
/// protocol component: `Dns` to its IPv4 and IPv6 addresses, `Dns4` to its
/// IPv4 ones and `Dns6` to its IPv6 ones (an answer with none is an error),
/// and `Dnsaddr` to the multiaddrs of its TXT records. Another component
/// isn't one [`Transport`] resolves: an error.
async fn resolve(proto: &Protocol<'_>, resolver: &DnsResolver) -> Result<Resolved, NetError> {
    let ips: Vec<IpAddr> = match proto {
        Protocol::Dns(name) => resolver.lookup_ip(name.to_string()).await?.iter().collect(),
        Protocol::Dns4(name) => resolver
            .ipv4_lookup(name.to_string())
            .await?
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                RData::A(ip) => Some(IpAddr::V4(ip.0)),
                _ => None,
            })
            .collect(),
        Protocol::Dns6(name) => resolver
            .ipv6_lookup(name.to_string())
            .await?
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                RData::AAAA(ip) => Some(IpAddr::V6(ip.0)),
                _ => None,
            })
            .collect(),
        Protocol::Dnsaddr(name) => {
            let txts = resolver.txt_lookup([DNSADDR_PREFIX, name].concat()).await?;
            let mut addrs = Vec::new();
            for txt in txts
                .answers()
                .iter()
                .filter_map(|record| match &record.data {
                    RData::TXT(txt) => Some(txt),
                    _ => None,
                })
            {
                if let Some(chars) = txt.txt_data.first() {
                    match parse_dnsaddr_txt(chars) {
                        Err(e) => {
                            // Skip over seemingly invalid entries.
                            tracing::debug!("Invalid TXT record: {:?}", e);
                        }
                        Ok(a) => {
                            addrs.push(a);
                        }
                    }
                }
            }
            return Ok(Resolved::Addrs(addrs));
        }
        _ => return Err(NetError::from("not a DNS component")),
    };
    if ips.is_empty() {
        return Err(NetError::from("no address of the type asked for"));
    }
    Ok(Resolved::Ips(ips))
}

/// Parses a `<character-string>` of a `dnsaddr` TXT record.
fn parse_dnsaddr_txt(txt: &[u8]) -> io::Result<Multiaddr> {
    let s = str::from_utf8(txt).map_err(invalid_data)?;
    match s.strip_prefix("dnsaddr=") {
        None => Err(invalid_data("Missing `dnsaddr=` prefix.")),
        Some(a) => Ok(Multiaddr::try_from(a).map_err(invalid_data)?),
    }
}

fn invalid_data(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dnsaddr_txt_is_its_multiaddr_after_the_prefix() {
        assert_eq!(
            parse_dnsaddr_txt(b"dnsaddr=/ip4/192.0.2.1/tcp/4001").unwrap(),
            "/ip4/192.0.2.1/tcp/4001".parse::<Multiaddr>().unwrap()
        );
        for bad in [&b"/ip4/192.0.2.1/tcp/4001"[..], b"dnsaddr=/nope", b"\xff"] {
            assert!(parse_dnsaddr_txt(bad).is_err());
        }
    }
}
