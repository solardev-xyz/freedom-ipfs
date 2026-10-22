//! A SOCKS5 test proxy (cargo feature `test-util`), for this crate's tests
//! and its dependents' (design 0007's tests: "A SOCKS fixture with
//! `RESOLVE`").
//!
//! It records every connection and every request (the methods offered, any
//! credentials, the command and the destination), answers Tor's `RESOLVE`
//! from a table, and forwards a `CONNECT` only to the local listener a test
//! mapped its destination to: a test picks public addresses and onion
//! names, and nothing leaves the machine. Anything unmapped is refused.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// What the proxy answers a `RESOLVE` for a name with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// An address, as Tor answers.
    Ip(IpAddr),
    /// A domain-typed answer.
    Domain(String),
    /// A failure, with this reply code.
    Reply(u8),
    /// No answer, ever: the request stays open until the client goes.
    Hang,
}

/// Where a request went.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Destination {
    Ip(SocketAddr),
    Name(String, u16),
}

/// One request the proxy received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// The methods the greeting offered.
    pub methods: Vec<u8>,
    /// RFC 1929's username and password, if that method was used.
    pub credentials: Option<(Vec<u8>, Vec<u8>)>,
    /// `0x01` for `CONNECT`, `0xF0` for `RESOLVE`.
    pub command: u8,
    pub destination: Destination,
}

impl Request {
    pub fn is_connect(&self) -> bool {
        self.command == 0x01
    }

    pub fn is_resolve(&self) -> bool {
        self.command == 0xF0
    }
}

#[derive(Debug, Default)]
struct Config {
    answers: HashMap<String, Answer>,
    forward: HashMap<Destination, SocketAddr>,
    /// The methods the proxy accepts, in its order of preference.
    methods: Vec<u8>,
    resolve_unsupported: bool,
}

#[derive(Debug, Default)]
struct Shared {
    config: Mutex<Config>,
    /// Every greeting's methods, refused ones included.
    greetings: Mutex<Vec<Vec<u8>>>,
    requests: Mutex<Vec<Request>>,
    connections: AtomicUsize,
    /// The local addresses of the proxy's own connections to the listeners
    /// it forwards to.
    outbound: Mutex<Vec<SocketAddr>>,
}

/// The test proxy. It stops when dropped.
#[derive(Debug)]
pub struct SocksFixture {
    addr: SocketAddr,
    shared: Arc<Shared>,
    task: JoinHandle<()>,
}

impl SocksFixture {
    /// A proxy on a loopback port that accepts no authentication (method
    /// `0x00`) or a username and password (`0x02`), answers `RESOLVE` from
    /// an empty table (reply `0x04` for an unknown name), and forwards
    /// nothing yet.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared::default());
        shared.config.lock().unwrap().methods = vec![0x00, 0x02];
        let accepted = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                accepted.connections.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(stream, accepted.clone()));
            }
        });
        Self { addr, shared, task }
    }

    /// The proxy's endpoint.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Answers `RESOLVE` for `name` with `answer`.
    pub fn answer(&self, name: &str, answer: Answer) {
        self.config().answers.insert(name.to_string(), answer);
    }

    /// Forwards a `CONNECT` to `destination` to the local listener at `to`.
    pub fn forward(&self, destination: Destination, to: SocketAddr) {
        self.config().forward.insert(destination, to);
    }

    /// Accepts only `methods`, in this order of preference.
    pub fn accept_methods(&self, methods: &[u8]) {
        self.config().methods = methods.to_vec();
    }

    /// Answers every `RESOLVE` with `0x07`, command not supported, as a
    /// proxy without Tor's extension does.
    pub fn refuse_resolve(&self) {
        self.config().resolve_unsupported = true;
    }

    /// The methods every greeting so far offered, in order: those of a
    /// greeting the proxy refused (no method it accepts) included, which
    /// leave no [`Request`].
    pub fn greetings(&self) -> Vec<Vec<u8>> {
        self.shared.greetings.lock().unwrap().clone()
    }

    /// Every request so far, in order.
    pub fn requests(&self) -> Vec<Request> {
        self.shared.requests.lock().unwrap().clone()
    }

    /// The connections accepted so far.
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }

    /// The local addresses of the proxy's own connections to the listeners
    /// it forwarded `CONNECT`s to: in the same process, they tell the
    /// proxy's sockets apart from those of the code under test.
    pub fn outbound(&self) -> Vec<SocketAddr> {
        self.shared.outbound.lock().unwrap().clone()
    }

    /// Stops accepting: later connections are refused, as by a proxy that
    /// is down.
    pub fn stop(self) {
        drop(self);
    }

    fn config(&self) -> std::sync::MutexGuard<'_, Config> {
        self.shared.config.lock().unwrap()
    }
}

impl Drop for SocksFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A listener that reads the TLS ClientHello each connection opens with,
/// records the server name it carries (SNI; `None` without one), and drops
/// the connection.
#[derive(Debug)]
pub struct ClientHelloCatcher {
    addr: SocketAddr,
    names: Arc<Mutex<Vec<Option<String>>>>,
    task: JoinHandle<()>,
}

impl ClientHelloCatcher {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let names = Arc::new(Mutex::new(Vec::new()));
        let recorded = names.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    continue;
                };
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut buffer = [0u8; 4096];
                    loop {
                        match stream.read(&mut buffer).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => bytes.extend_from_slice(&buffer[..n]),
                        }
                        if let Some(name) = client_hello_server_name(&bytes) {
                            recorded.lock().unwrap().push(name);
                            return;
                        }
                    }
                });
            }
        });
        Self { addr, names, task }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The server names of the ClientHellos received so far.
    pub fn names(&self) -> Vec<Option<String>> {
        self.names.lock().unwrap().clone()
    }
}

impl Drop for ClientHelloCatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The server name a complete TLS ClientHello at the start of `bytes`
/// carries: `None` until it has all arrived, `Some(None)` without one.
fn client_hello_server_name(bytes: &[u8]) -> Option<Option<String>> {
    fn u16_at(bytes: &[u8], at: usize) -> Option<usize> {
        Some(usize::from(u16::from_be_bytes([
            *bytes.get(at)?,
            *bytes.get(at + 1)?,
        ])))
    }
    // The record header, then the handshake header (type 1, ClientHello).
    if bytes.len() < 6 {
        return None;
    }
    if bytes[0] != 22 || bytes[5] != 1 {
        return Some(None);
    }
    let end = 5 + u16_at(bytes, 3)?;
    if bytes.len() < end {
        return None;
    }
    let hello = &bytes[..end];
    // Version and random, then the session ID, the cipher suites and the
    // compression methods.
    let mut at = 9 + 2 + 32;
    let parsed = (|| {
        at += 1 + usize::from(*hello.get(at)?);
        at += 2 + u16_at(hello, at)?;
        at += 1 + usize::from(*hello.get(at)?);
        let extensions_end = at + 2 + u16_at(hello, at)?;
        at += 2;
        while at + 4 <= extensions_end.min(end) {
            let kind = u16_at(hello, at)?;
            let len = u16_at(hello, at + 2)?;
            if kind == 0 {
                // The server name list: its length, then type 0 (host
                // name), the name's length and the name.
                let name_len = u16_at(hello, at + 4 + 3)?;
                let start = at + 4 + 5;
                return std::str::from_utf8(hello.get(start..start + name_len)?)
                    .ok()
                    .map(str::to_string);
            }
            at += 4 + len;
        }
        None
    })();
    Some(parsed)
}

/// An HTTP/1.1 server that records each request's path and `Host` header,
/// and answers it from `respond`, given the path and the host: a status, a
/// `Location` if any, and a body. Each connection carries one request. It
/// counts the connections it accepts, those that carry no request
/// included.
#[derive(Debug)]
pub struct HttpServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<ServedRequest>>>,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

/// A request an [`HttpServer`] served: its path, and its `Host` header.
pub type ServedRequest = (String, Option<String>);

/// An [`HttpServer`]'s answer.
#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub location: Option<String>,
    pub body: String,
}

impl Reply {
    pub fn ok(body: &str) -> Self {
        Self {
            status: 200,
            location: None,
            body: body.to_string(),
        }
    }

    pub fn redirect(location: &str) -> Self {
        Self {
            status: 302,
            location: Some(location.to_string()),
            body: String::new(),
        }
    }
}

impl HttpServer {
    pub async fn start(
        respond: impl Fn(&str, Option<&str>) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let respond = Arc::new(respond);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    continue;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                let (recorded, respond) = (recorded.clone(), respond.clone());
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buffer = [0u8; 4096];
                    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                        match stream.read(&mut buffer).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buffer[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&head).into_owned();
                    let mut lines = head.split("\r\n");
                    let path = lines
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("")
                        .to_string();
                    let host = lines.find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("host")
                            .then(|| value.trim().to_string())
                    });
                    recorded.lock().unwrap().push((path.clone(), host.clone()));
                    let reply = respond(&path, host.as_deref());
                    let location = reply
                        .location
                        .map(|location| format!("Location: {location}\r\n"))
                        .unwrap_or_default();
                    let response = format!(
                        "HTTP/1.1 {} X\r\n{location}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                        reply.status,
                        reply.body.len(),
                        reply.body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            addr,
            requests,
            connections,
            task,
        }
    }

    /// The connections accepted so far, with or without a request.
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Each request's path and `Host` header, in order.
    pub fn requests(&self) -> Vec<ServedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(mut stream: TcpStream, shared: Arc<Shared>) {
    let _ = exchange(&mut stream, &shared).await;
}

async fn exchange(stream: &mut TcpStream, shared: &Shared) -> std::io::Result<()> {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).await?;
    let mut methods = vec![0u8; usize::from(head[1])];
    stream.read_exact(&mut methods).await?;
    shared.greetings.lock().unwrap().push(methods.clone());
    let accepted = shared.config.lock().unwrap().methods.clone();
    let Some(method) = accepted.into_iter().find(|method| methods.contains(method)) else {
        stream.write_all(&[5, 0xFF]).await?;
        return Ok(());
    };
    stream.write_all(&[5, method]).await?;
    let credentials = if method == 0x02 {
        let mut version = [0u8; 2];
        stream.read_exact(&mut version).await?;
        let mut user = vec![0u8; usize::from(version[1])];
        stream.read_exact(&mut user).await?;
        let mut len = [0u8; 1];
        stream.read_exact(&mut len).await?;
        let mut password = vec![0u8; usize::from(len[0])];
        stream.read_exact(&mut password).await?;
        stream.write_all(&[1, 0]).await?;
        Some((user, password))
    } else {
        None
    };
    let mut request = [0u8; 4];
    stream.read_exact(&mut request).await?;
    let destination = match request[3] {
        1 => {
            let mut ip = [0u8; 4];
            stream.read_exact(&mut ip).await?;
            Destination::Ip(SocketAddr::new(ip.into(), read_port(stream).await?))
        }
        4 => {
            let mut ip = [0u8; 16];
            stream.read_exact(&mut ip).await?;
            Destination::Ip(SocketAddr::new(ip.into(), read_port(stream).await?))
        }
        3 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut name = vec![0u8; usize::from(len[0])];
            stream.read_exact(&mut name).await?;
            let name = String::from_utf8_lossy(&name).into_owned();
            Destination::Name(name, read_port(stream).await?)
        }
        _ => return Ok(()),
    };
    shared.requests.lock().unwrap().push(Request {
        methods,
        credentials,
        command: request[1],
        destination: destination.clone(),
    });
    match request[1] {
        0x01 => {
            let to = shared
                .config
                .lock()
                .unwrap()
                .forward
                .get(&destination)
                .copied();
            let Some(mut target) = (match to {
                Some(to) => TcpStream::connect(to).await.ok(),
                None => None,
            }) else {
                return reply(stream, 0x04, None).await;
            };
            if let Ok(local) = target.local_addr() {
                shared.outbound.lock().unwrap().push(local);
            }
            reply(stream, 0x00, Some("0.0.0.0".parse().unwrap())).await?;
            let _ = tokio::io::copy_bidirectional(stream, &mut target).await;
            Ok(())
        }
        0xF0 => {
            let (unsupported, answer) = {
                let config = shared.config.lock().unwrap();
                let name = match &destination {
                    Destination::Name(name, _) => name.clone(),
                    Destination::Ip(addr) => addr.ip().to_string(),
                };
                (
                    config.resolve_unsupported,
                    config.answers.get(&name).cloned(),
                )
            };
            if unsupported {
                return reply(stream, 0x07, None).await;
            }
            match answer {
                Some(Answer::Ip(ip)) => reply(stream, 0x00, Some(ip)).await,
                Some(Answer::Domain(name)) => {
                    let mut out = vec![5, 0, 0, 3, name.len() as u8];
                    out.extend_from_slice(name.as_bytes());
                    out.extend_from_slice(&[0, 0]);
                    stream.write_all(&out).await
                }
                Some(Answer::Reply(code)) => reply(stream, code, None).await,
                Some(Answer::Hang) => {
                    let mut rest = Vec::new();
                    let _ = stream.read_to_end(&mut rest).await;
                    Ok(())
                }
                None => reply(stream, 0x04, None).await,
            }
        }
        _ => reply(stream, 0x07, None).await,
    }
}

async fn read_port(stream: &mut TcpStream) -> std::io::Result<u16> {
    let mut port = [0u8; 2];
    stream.read_exact(&mut port).await?;
    Ok(u16::from_be_bytes(port))
}

async fn reply(stream: &mut TcpStream, code: u8, ip: Option<IpAddr>) -> std::io::Result<()> {
    let mut out = vec![5, code, 0];
    match ip.unwrap_or(IpAddr::from([0, 0, 0, 0])) {
        IpAddr::V4(ip) => {
            out.push(1);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            out.push(4);
            out.extend_from_slice(&ip.octets());
        }
    }
    out.extend_from_slice(&[0, 0]);
    stream.write_all(&out).await
}
