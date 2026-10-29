use async_trait::async_trait;
use cid::Cid;
use freedom_ipfs_net::gate::{classify_host, Refusal, Target};
use freedom_ipfs_net::{ClientConfig, HttpClient, HttpError, Refused};
use futures::stream::{FuturesUnordered, StreamExt};
use ipld_core::ipld::Ipld;
use libp2p_identity::{PeerId, PublicKey};
use multihash::Multihash;
use prost::Message;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const DEFAULT_DELEGATED_ROUTER: &str = "https://delegated-ipfs.dev/routing/v1";
const IPNS_RECORD_CONTENT_TYPE: &str = "application/vnd.ipfs.ipns-record";
const IPNS_SIGNATURE_PREFIX: &[u8] = b"ipns-signature:";
const IPNS_RECORD_MAX_SIZE: usize = 10 * 1024;
const LIBP2P_KEY_CODEC: u64 = 0x72;
const IDENTITY_HASH: u64 = 0x00;
const DEFAULT_NAME_CACHE_TTL: Duration = Duration::from_secs(60);
const DEFAULT_NAMESYS_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
/// The DoH and delegated IPNS clients' configuration: a 10 s timeout, and
/// reqwest's default redirects.
pub const HTTP_CLIENT_CONFIG: ClientConfig = ClientConfig::new(
    DEFAULT_NAMESYS_HTTP_TIMEOUT,
    ClientConfig::REQWEST_DEFAULT_REDIRECTS,
);
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_TXT: u16 = 16;
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_AAAA: u16 = 28;
const MAX_DOH_CNAME_DEPTH: usize = 8;

#[derive(Debug, Error)]
pub enum NamesysError {
    #[error("dnslink record not found for {0}")]
    NotFound(String),
    #[error("invalid dnslink record: {0}")]
    InvalidDnslink(String),
    #[error("invalid IPNS name: {0}")]
    InvalidIpnsName(String),
    #[error("invalid IPNS record: {0}")]
    InvalidIpnsRecord(String),
    #[error("expired IPNS record")]
    ExpiredIpnsRecord,
    #[error("IPNS signature verification failed")]
    InvalidIpnsSignature,
    #[error("http resolver: {0}")]
    Http(#[from] reqwest::Error),
    /// The request wasn't sent: the network's capability is revoked, or
    /// its client couldn't be built.
    #[error("http resolver: {0}")]
    Network(HttpError),
}

impl From<HttpError> for NamesysError {
    fn from(err: HttpError) -> Self {
        match err {
            HttpError::Http(err) => Self::Http(err),
            other => Self::Network(other),
        }
    }
}

pub type Result<T> = std::result::Result<T, NamesysError>;

#[async_trait]
pub trait DnsTxtResolver: Send + Sync {
    async fn txt_lookup(&self, name: &str) -> Result<Vec<String>>;

    async fn txt_lookup_with_ttl(&self, name: &str) -> Result<Vec<DnsTxtRecord>> {
        Ok(self
            .txt_lookup(name)
            .await?
            .into_iter()
            .map(DnsTxtRecord::new)
            .collect())
    }
}

#[async_trait]
pub trait IpnsResolver: Send + Sync {
    async fn resolve_ipns(&self, name: &str) -> Result<IpnsRecord>;
}

#[async_trait]
impl<T> IpnsResolver for Arc<T>
where
    T: IpnsResolver + ?Sized,
{
    async fn resolve_ipns(&self, name: &str) -> Result<IpnsRecord> {
        self.as_ref().resolve_ipns(name).await
    }
}

#[async_trait]
pub trait NameResolver: Send + Sync {
    async fn resolve_name(&self, name: &str) -> Result<String>;

    async fn resolve_name_with_ttl(&self, name: &str) -> Result<ResolvedName> {
        self.resolve_name(name).await.map(ResolvedName::new)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsTxtRecord {
    pub value: String,
    pub ttl: Option<Duration>,
}

impl DnsTxtRecord {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            ttl: None,
        }
    }

    pub fn with_ttl(value: impl Into<String>, ttl: Duration) -> Self {
        Self {
            value: value.into(),
            ttl: Some(ttl),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedName {
    pub value: String,
    pub ttl: Option<Duration>,
}

impl ResolvedName {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            ttl: None,
        }
    }

    pub fn with_ttl(value: impl Into<String>, ttl: Duration) -> Self {
        Self {
            value: value.into(),
            ttl: Some(ttl),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedNameResolver<R> {
    inner: R,
    ttl: Duration,
    cache: Arc<Mutex<HashMap<String, CachedName>>>,
}

impl<R> CachedNameResolver<R> {
    pub fn new(inner: R) -> Self {
        Self::with_ttl(inner, DEFAULT_NAME_CACHE_TTL)
    }

    pub fn with_ttl(inner: R, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl<R> NameResolver for CachedNameResolver<R>
where
    R: NameResolver,
{
    async fn resolve_name(&self, name: &str) -> Result<String> {
        self.resolve_name_with_ttl(name)
            .await
            .map(|resolved| resolved.value)
    }

    async fn resolve_name_with_ttl(&self, name: &str) -> Result<ResolvedName> {
        // An IPNS name isn't cached here, by the name as spelled and without
        // its record's sequence or validity: a canonical IPNS cache (the
        // gateway's `PersistentIpnsResolver`) keeps it, so another spelling,
        // or a cached entry outliving its record's validity, can't answer
        // with a record it has replaced.
        if is_ipns_name(name) {
            return self.inner.resolve_name_with_ttl(name).await;
        }
        if let Some(value) = self.cached(name) {
            tracing::info!(
                phase = "name_cache",
                name,
                cache_hit = true,
                resolved_target = %value
            );
            return Ok(ResolvedName::new(value));
        }

        tracing::info!(phase = "name_cache", name, cache_hit = false);
        let resolved = self.inner.resolve_name_with_ttl(name).await?;
        self.store(name, &resolved.value, resolved.ttl);
        Ok(resolved)
    }
}

impl<R> CachedNameResolver<R> {
    fn cached(&self, name: &str) -> Option<String> {
        let mut cache = self.cache.lock().ok()?;
        let entry = cache.get(name)?;
        if entry.expires_at > Instant::now() {
            return Some(entry.value.clone());
        }
        cache.remove(name);
        None
    }

    fn store(&self, name: &str, value: &str, ttl: Option<Duration>) {
        let ttl = ttl.map_or(self.ttl, |ttl| ttl.min(self.ttl));
        if ttl.is_zero() {
            return;
        }
        let expires_at = Instant::now().checked_add(ttl).unwrap_or_else(Instant::now);
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(
                name.to_string(),
                CachedName {
                    value: value.to_string(),
                    expires_at,
                },
            );
        }
    }
}

#[derive(Debug, Clone)]
struct CachedName {
    value: String,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
pub struct CloudflareDohResolver {
    client: HttpClient,
    endpoint: String,
}

impl Default for CloudflareDohResolver {
    fn default() -> Self {
        Self {
            client: HttpClient::legacy(HTTP_CLIENT_CONFIG),
            endpoint: DEFAULT_DOH_ENDPOINT.to_string(),
        }
    }
}

/// The DoH endpoint a resolver asks unless told another.
pub const DEFAULT_DOH_ENDPOINT: &str = "https://cloudflare-dns.com/dns-query";

impl CloudflareDohResolver {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    /// A resolver for `endpoint` that sends through `client`: a network's
    /// client for [`HTTP_CLIENT_CONFIG`], whose capability every query asks.
    pub fn with_client(endpoint: impl Into<String>, client: HttpClient) -> Self {
        Self {
            client,
            endpoint: endpoint.into(),
        }
    }

    async fn txt_lookup_response(&self, name: &str) -> Result<DohResponse> {
        self.client
            .get(&self.endpoint)
            .query(&[("name", name), ("type", "TXT")])
            .header("accept", "application/dns-json")
            .send()
            .await?
            .error_for_status()?
            .json::<DohResponse>()
            .await
            .map_err(NamesysError::from)
    }

    /// Under a gated network policy (`direct`), a name this resolver is
    /// asked, or a CNAME's target it would follow, is classified first
    /// (design 0007, check 2: an alias is an expansion result): an onion, an
    /// IP literal or an invalid name is refused before any query for it.
    /// Under the legacy policy names and aliases are queried as before.
    ///
    /// The name classified is the text as it came, and the name asked for
    /// is the classifier's canonical one: one trailing dot removed, and an
    /// empty label left by another refused, not trimmed away.
    fn admit_name(&self, name: &str) -> Result<String> {
        if !self.gated() {
            return Ok(name.trim_end_matches('.').to_string());
        }
        let refused = |refused: Refused| Err(NamesysError::Network(HttpError::Refused(refused)));
        match classify_host(name) {
            Ok(Target::Name(name)) => Ok(name),
            Ok(Target::Onion(_)) => refused(Refused::Onion),
            Ok(Target::Ip(_)) => refused(Refused::Target(Refusal::InvalidHost)),
            Err(refusal) => refused(Refused::Target(refusal)),
        }
    }

    /// Whether the resolver's network has a gate (`direct`).
    fn gated(&self) -> bool {
        self.client.network().dial_policy().is_gated()
    }

    /// Under a gated policy, the records of one response that answer for
    /// the chain at `chain.current` (design 0007, check 2: an alias is an
    /// expansion result). The chain of CNAMEs the response holds is followed
    /// from there, each alias classified ([`CloudflareDohResolver::admit_name`])
    /// before anything it owns is taken:
    /// - a name may own one alias at most: two refuse the lookup, rather than
    ///   one chosen by the answers' order;
    /// - a name already met in this lookup (across its responses) refuses it:
    ///   a loop has no last name;
    /// - past [`MAX_DOH_CNAME_DEPTH`] aliases, nothing is taken;
    /// - the `wanted` records are taken only from a name with no alias, and
    ///   only those it owns: a record owned by any other name is ignored.
    ///
    /// Real DoH JSON names every record's owner. An alias, or a record of
    /// the kind asked for, without a valid one (its `name` missing, null,
    /// or not a name the gate's classifier accepts: empty, blank, padded)
    /// can't be placed in the chain: the lookup is refused, whatever the
    /// answers' order. A record of another kind is left alone.
    fn owned_answers<'a>(
        &self,
        answers: &'a [DohAnswer],
        chain: &mut AliasChain,
        wanted: impl Fn(&DohAnswer) -> bool,
    ) -> Result<ChainStep<'a>> {
        if answers
            .iter()
            .any(|answer| (answer.is_cname() || wanted(answer)) && answer.owner().is_none())
        {
            return Err(chain.refused("an answer doesn't name a valid owner"));
        }
        loop {
            let owned = answers
                .iter()
                .enumerate()
                .filter(|(_, answer)| answer.is_cname() && answer.is_owned_by(&chain.current))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let alias = match owned.as_slice() {
                [] => {
                    return Ok(ChainStep::Records(
                        answers
                            .iter()
                            .filter(|answer| wanted(answer) && answer.is_owned_by(&chain.current))
                            .collect(),
                    ))
                }
                [index] => *index,
                _ => return Err(chain.refused("a name in its chain has several aliases")),
            };
            if chain.followed == MAX_DOH_CNAME_DEPTH {
                return Ok(ChainStep::TooLong);
            }
            chain.followed += 1;
            chain.ttl = min_ttl(chain.ttl, answers[alias].ttl.map(Duration::from_secs));
            let next = self.admit_name(&answers[alias].data)?;
            if !chain.visited.insert(next.clone()) {
                return Err(chain.refused("its aliases loop"));
            }
            chain.current = next;
        }
    }

    pub async fn ip_lookup(&self, name: &str) -> Result<Vec<IpAddr>> {
        let mut addrs = self.ip_lookup_type(name, DNS_TYPE_A).await?;
        addrs.extend(self.ip_lookup_type(name, DNS_TYPE_AAAA).await?);
        addrs.sort();
        addrs.dedup();
        Ok(addrs)
    }

    async fn ip_lookup_response(&self, name: &str, record_type: u16) -> Result<DohResponse> {
        let record_type_query = record_type.to_string();
        Ok(self
            .client
            .get(&self.endpoint)
            .query(&[("name", name), ("type", record_type_query.as_str())])
            .header("accept", "application/dns-json")
            .send()
            .await?
            .error_for_status()?
            .json::<DohResponse>()
            .await?)
    }

    async fn ip_lookup_type(&self, name: &str, record_type: u16) -> Result<Vec<IpAddr>> {
        let mut current = self.admit_name(name)?;
        if self.gated() {
            let mut chain = AliasChain::new(name, current);
            loop {
                let asked = chain.current.clone();
                let response = self.ip_lookup_response(&asked, record_type).await?;
                let answers = response.answer.unwrap_or_default();
                let step = self.owned_answers(&answers, &mut chain, |answer| {
                    answer.record_type == Some(record_type)
                })?;
                let ChainStep::Records(owned) = step else {
                    return Ok(Vec::new());
                };
                let addrs = owned
                    .iter()
                    .filter_map(|answer| answer.data.parse::<IpAddr>().ok())
                    .collect::<Vec<_>>();
                // An alias with nothing of its own in this response is asked
                // for itself; `owned_answers` bounds how many.
                if !addrs.is_empty() || chain.current == asked {
                    return Ok(addrs);
                }
            }
        }

        for _ in 0..=MAX_DOH_CNAME_DEPTH {
            let response = self.ip_lookup_response(&current, record_type).await?;
            let answers = response.answer.unwrap_or_default();
            let addrs = answers
                .iter()
                .filter(|answer| answer.record_type == Some(record_type))
                .filter_map(|answer| answer.data.parse::<IpAddr>().ok())
                .collect::<Vec<_>>();
            if !addrs.is_empty() {
                return Ok(addrs);
            }

            let Some(cname) = answers.iter().find(|answer| answer.is_cname()) else {
                return Ok(Vec::new());
            };
            current = self.admit_name(&cname.data)?;
        }

        Ok(Vec::new())
    }
}

#[async_trait]
impl DnsTxtResolver for CloudflareDohResolver {
    async fn txt_lookup(&self, name: &str) -> Result<Vec<String>> {
        Ok(self
            .txt_lookup_with_ttl(name)
            .await?
            .into_iter()
            .map(|record| record.value)
            .collect())
    }

    async fn txt_lookup_with_ttl(&self, name: &str) -> Result<Vec<DnsTxtRecord>> {
        let mut current = self.admit_name(name)?;
        let mut cname_ttl = None;
        if self.gated() {
            let mut chain = AliasChain::new(name, current);
            loop {
                let asked = chain.current.clone();
                let response = self.txt_lookup_response(&asked).await?;
                let answers = response.answer.unwrap_or_default();
                let step = self.owned_answers(&answers, &mut chain, DohAnswer::is_txt)?;
                let ChainStep::Records(owned) = step else {
                    return Err(NamesysError::NotFound(name.to_string()));
                };
                let records = owned
                    .iter()
                    .map(|answer| DnsTxtRecord {
                        value: unquote_txt(&answer.data),
                        ttl: min_ttl(chain.ttl, answer.ttl.map(Duration::from_secs)),
                    })
                    .collect::<Vec<_>>();
                if !records.is_empty() || chain.current == asked {
                    return Ok(records);
                }
            }
        }

        for _ in 0..=MAX_DOH_CNAME_DEPTH {
            let response = self.txt_lookup_response(&current).await?;
            let answers = response.answer.unwrap_or_default();
            let txt_records = answers
                .iter()
                .filter(|answer| answer.is_txt())
                .map(|answer| DnsTxtRecord {
                    value: unquote_txt(&answer.data),
                    ttl: min_ttl(cname_ttl, answer.ttl.map(Duration::from_secs)),
                })
                .collect::<Vec<_>>();
            if !txt_records.is_empty() {
                return Ok(txt_records);
            }

            let Some(cname) = answers.iter().find(|answer| answer.is_cname()) else {
                return Ok(Vec::new());
            };
            cname_ttl = min_ttl(cname_ttl, cname.ttl.map(Duration::from_secs));
            current = self.admit_name(&cname.data)?;
        }

        Err(NamesysError::NotFound(name.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct DelegatedIpnsResolver {
    endpoints: Vec<String>,
    client: HttpClient,
}

impl Default for DelegatedIpnsResolver {
    fn default() -> Self {
        Self::new(DEFAULT_DELEGATED_ROUTER)
    }
}

impl DelegatedIpnsResolver {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_endpoints([endpoint])
    }

    pub fn with_endpoints<I, S>(endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::with_endpoints_and_client(endpoints, HttpClient::legacy(HTTP_CLIENT_CONFIG))
    }

    /// A resolver for `endpoints` that sends through `client`: a network's
    /// client for [`HTTP_CLIENT_CONFIG`], whose capability every lookup asks.
    pub fn with_endpoints_and_client<I, S>(endpoints: I, client: HttpClient) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.into().trim_end_matches('/').to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            endpoints.push(DEFAULT_DELEGATED_ROUTER.to_string());
        }
        Self { endpoints, client }
    }

    async fn resolve_ipns_from_endpoint(&self, endpoint: &str, name: &str) -> Result<IpnsRecord> {
        let lookup = normalize_ipns_name_for_routing(name)?;
        let url = format!("{endpoint}/ipns/{lookup}");
        let response = self
            .client
            .get(url)
            .header("accept", IPNS_RECORD_CONTENT_TYPE)
            .send()
            .await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(NamesysError::NotFound(name.to_string()));
        }
        let response = response.error_for_status()?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if content_type != IPNS_RECORD_CONTENT_TYPE {
            return Err(NamesysError::NotFound(name.to_string()));
        }

        let bytes = limited_response_bytes(response, IPNS_RECORD_MAX_SIZE).await?;
        verify_ipns_record(name, &bytes)
    }
}

#[async_trait]
impl IpnsResolver for DelegatedIpnsResolver {
    /// Every endpoint is asked, and the best record any of them answers
    /// with wins ([`compare_ipns_records`]): one router's stale record
    /// can't stand in for a newer one another router has.
    async fn resolve_ipns(&self, name: &str) -> Result<IpnsRecord> {
        if self.endpoints.len() == 1 {
            return self
                .resolve_ipns_from_endpoint(&self.endpoints[0], name)
                .await;
        }

        let mut queries = self
            .endpoints
            .iter()
            .map(|endpoint| self.resolve_ipns_from_endpoint(endpoint, name))
            .collect::<FuturesUnordered<_>>();
        let mut best = None;
        let mut saw_not_found = false;
        let mut first_error = None;

        while let Some(result) = queries.next().await {
            match result {
                Ok(record) => best = Some(better_ipns_record(best, record)),
                Err(NamesysError::NotFound(_)) => saw_not_found = true,
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if let Some(record) = best {
            return Ok(record);
        }
        if saw_not_found {
            Err(NamesysError::NotFound(name.to_string()))
        } else {
            Err(first_error.unwrap_or_else(|| {
                NamesysError::NotFound("no delegated IPNS endpoints configured".into())
            }))
        }
    }
}

/// Orders two verified records of one name as IPNS does (the spec's record
/// selection, Kubo's `ipns.Compare`): the higher sequence number is newer;
/// at the same sequence, the later validity (expiry) wins. `Equal` when
/// neither is newer.
pub fn compare_ipns_records(a: &IpnsRecord, b: &IpnsRecord) -> Ordering {
    a.sequence
        .cmp(&b.sequence)
        .then_with(|| ipns_record_expiry(a).cmp(&ipns_record_expiry(b)))
}

/// The newer of `best` and `record`, keeping `best` when neither is newer.
pub fn better_ipns_record(best: Option<IpnsRecord>, record: IpnsRecord) -> IpnsRecord {
    match best {
        Some(best) if compare_ipns_records(&best, &record) != Ordering::Less => best,
        _ => record,
    }
}

/// When a record stops being valid: its EOL validity, `None` if it has
/// none that parses (a verified record always has one).
pub fn ipns_record_expiry(record: &IpnsRecord) -> Option<OffsetDateTime> {
    record
        .validity
        .as_deref()
        .and_then(|validity| OffsetDateTime::parse(validity, &Rfc3339).ok())
}

/// One spelling for an IPNS name, whatever form it came in (a peer ID or
/// a `libp2p-key` CID in any base): its CIDv1, in base32. What a store
/// keys a name's records by.
pub fn canonical_ipns_name(name: &str) -> Result<String> {
    normalize_ipns_name_for_routing(name)
}

async fn limited_response_bytes(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(NamesysError::InvalidIpnsRecord(format!(
                "record exceeds {max_bytes} byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Default)]
pub struct DefaultNameResolver<D = CloudflareDohResolver, I = DelegatedIpnsResolver> {
    dnslink: D,
    ipns: I,
}

impl<D, I> DefaultNameResolver<D, I> {
    pub fn new(dnslink: D, ipns: I) -> Self {
        Self { dnslink, ipns }
    }
}

#[async_trait]
impl<D, I> NameResolver for DefaultNameResolver<D, I>
where
    D: DnsTxtResolver,
    I: IpnsResolver,
{
    async fn resolve_name(&self, name: &str) -> Result<String> {
        self.resolve_name_with_ttl(name)
            .await
            .map(|resolved| resolved.value)
    }

    async fn resolve_name_with_ttl(&self, name: &str) -> Result<ResolvedName> {
        match resolve_dnslink_with_ttl(&self.dnslink, name).await {
            Ok(path) => Ok(path),
            Err(NamesysError::NotFound(_)) if is_ipns_name(name) => {
                self.ipns.resolve_ipns(name).await.map(resolved_ipns_record)
            }
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FallbackIpnsResolver<P, F> {
    primary: P,
    fallback: F,
}

impl<P, F> FallbackIpnsResolver<P, F> {
    pub fn new(primary: P, fallback: F) -> Self {
        Self { primary, fallback }
    }
}

#[async_trait]
impl<P, F> IpnsResolver for FallbackIpnsResolver<P, F>
where
    P: IpnsResolver,
    F: IpnsResolver,
{
    async fn resolve_ipns(&self, name: &str) -> Result<IpnsRecord> {
        let primary_err = match self.primary.resolve_ipns(name).await {
            Ok(record) => return Ok(record),
            Err(err) => err,
        };

        match self.fallback.resolve_ipns(name).await {
            Ok(record) => Ok(record),
            Err(_) => Err(primary_err),
        }
    }
}

pub async fn resolve_dnslink(resolver: &dyn DnsTxtResolver, domain: &str) -> Result<String> {
    resolve_dnslink_with_ttl(resolver, domain)
        .await
        .map(|resolved| resolved.value)
}

pub async fn resolve_dnslink_with_ttl(
    resolver: &dyn DnsTxtResolver,
    domain: &str,
) -> Result<ResolvedName> {
    let lookup = if domain.starts_with("_dnslink.") {
        domain.to_string()
    } else {
        format!("_dnslink.{domain}")
    };
    let records = resolver.txt_lookup_with_ttl(&lookup).await?;
    for record in records {
        if let Some(value) = parse_dnslink_txt(&record.value)? {
            return Ok(ResolvedName {
                value,
                ttl: record.ttl,
            });
        }
    }
    Err(NamesysError::NotFound(domain.to_string()))
}

fn resolved_ipns_record(record: IpnsRecord) -> ResolvedName {
    ResolvedName {
        value: record.value,
        ttl: (record.ttl > 0).then(|| Duration::from_nanos(record.ttl)),
    }
}

pub fn parse_dnslink_txt(record: &str) -> Result<Option<String>> {
    let record = record.trim();
    let Some(value) = record.strip_prefix("dnslink=") else {
        return Ok(None);
    };
    if value.starts_with("/ipfs/") || value.starts_with("/ipns/") {
        Ok(Some(value.to_string()))
    } else {
        Err(NamesysError::InvalidDnslink(value.to_string()))
    }
}

pub fn is_ipns_name(name: &str) -> bool {
    parse_ipns_name(name).is_ok()
}

pub fn verify_ipns_record(name: &str, bytes: &[u8]) -> Result<IpnsRecord> {
    verify_ipns_record_at(name, bytes, OffsetDateTime::now_utc())
}

fn verify_ipns_record_at(name: &str, bytes: &[u8], now: OffsetDateTime) -> Result<IpnsRecord> {
    if bytes.len() > IPNS_RECORD_MAX_SIZE {
        return Err(NamesysError::InvalidIpnsRecord(format!(
            "record exceeds {IPNS_RECORD_MAX_SIZE} byte limit"
        )));
    }
    let name = parse_ipns_name(name)?;
    let entry = IpnsEntry::decode(bytes)
        .map_err(|err| NamesysError::InvalidIpnsRecord(format!("protobuf decode failed: {err}")))?;
    let signature = entry
        .signature_v2
        .as_deref()
        .filter(|signature| !signature.is_empty())
        .ok_or_else(|| NamesysError::InvalidIpnsRecord("missing signatureV2".into()))?;
    let data = entry
        .data
        .as_deref()
        .filter(|data| !data.is_empty())
        .ok_or_else(|| NamesysError::InvalidIpnsRecord("missing data".into()))?;

    let public_key = extract_public_key(&name, entry.pub_key.as_deref())?;
    if public_key.to_peer_id() != name.peer_id {
        return Err(NamesysError::InvalidIpnsRecord(
            "public key does not match IPNS name".into(),
        ));
    }

    let record_data = parse_ipns_data(data)?;
    let mut signed = Vec::with_capacity(IPNS_SIGNATURE_PREFIX.len() + data.len());
    signed.extend_from_slice(IPNS_SIGNATURE_PREFIX);
    signed.extend_from_slice(data);
    if !public_key.verify(&signed, signature) {
        return Err(NamesysError::InvalidIpnsSignature);
    }

    verify_legacy_fields_match(&entry, &record_data)?;
    if record_data.validity_type != 0 {
        return Err(NamesysError::InvalidIpnsRecord(
            "unsupported validity type".into(),
        ));
    }
    let validity = std::str::from_utf8(&record_data.validity)
        .map_err(|_| NamesysError::InvalidIpnsRecord("validity is not utf-8".into()))?;
    let expires = OffsetDateTime::parse(validity, &Rfc3339)
        .map_err(|err| NamesysError::InvalidIpnsRecord(format!("invalid validity: {err}")))?;
    if expires <= now {
        return Err(NamesysError::ExpiredIpnsRecord);
    }

    let value = String::from_utf8(record_data.value.clone())
        .map_err(|_| NamesysError::InvalidIpnsRecord("value is not utf-8".into()))?;
    if !value.starts_with("/ipfs/") && !value.starts_with("/ipns/") {
        return Err(NamesysError::InvalidIpnsRecord(format!(
            "unsupported value path: {value}"
        )));
    }

    Ok(IpnsRecord {
        value,
        sequence: record_data.sequence,
        validity: Some(validity.to_string()),
        ttl: record_data.ttl,
    })
}

fn normalize_ipns_name_for_routing(name: &str) -> Result<String> {
    let cid = ipns_name_as_cid(name)?;
    Ok(cid.to_string())
}

pub fn ipns_dht_record_key(name: &str) -> Result<Vec<u8>> {
    let mut key = b"/ipns/".to_vec();
    key.extend_from_slice(&ipns_name_as_cid(name)?.hash().to_bytes());
    Ok(key)
}

fn ipns_name_as_cid(name: &str) -> Result<Cid> {
    if let Ok(cid) = name.parse::<Cid>() {
        if cid.codec() == LIBP2P_KEY_CODEC {
            return Ok(cid);
        }
    }
    let peer_id =
        PeerId::from_str(name).map_err(|err| NamesysError::InvalidIpnsName(err.to_string()))?;
    let hash = Multihash::<64>::from_bytes(&peer_id.to_bytes())
        .map_err(|err| NamesysError::InvalidIpnsName(err.to_string()))?;
    Ok(Cid::new_v1(LIBP2P_KEY_CODEC, hash))
}

fn parse_ipns_name(name: &str) -> Result<IpnsName> {
    if let Ok(cid) = name.parse::<Cid>() {
        if cid.codec() != LIBP2P_KEY_CODEC {
            return Err(NamesysError::InvalidIpnsName(format!(
                "expected libp2p-key codec, got {}",
                cid.codec()
            )));
        }
        let hash = cid.hash();
        let peer_id = PeerId::from_bytes(&hash.to_bytes())
            .map_err(|err| NamesysError::InvalidIpnsName(err.to_string()))?;
        let inline_public_key = (hash.code() == IDENTITY_HASH).then(|| hash.digest().to_vec());
        return Ok(IpnsName {
            peer_id,
            inline_public_key,
        });
    }

    let peer_id =
        PeerId::from_str(name).map_err(|err| NamesysError::InvalidIpnsName(err.to_string()))?;
    let hash = Multihash::<64>::from_bytes(&peer_id.to_bytes())
        .map_err(|err| NamesysError::InvalidIpnsName(err.to_string()))?;
    Ok(IpnsName {
        peer_id,
        inline_public_key: (hash.code() == IDENTITY_HASH).then(|| hash.digest().to_vec()),
    })
}

fn extract_public_key(name: &IpnsName, record_key: Option<&[u8]>) -> Result<PublicKey> {
    if let Some(record_key) = record_key {
        return PublicKey::try_decode_protobuf(record_key).map_err(|err| {
            NamesysError::InvalidIpnsRecord(format!("public key decode failed: {err}"))
        });
    }
    let inline = name.inline_public_key.as_deref().ok_or_else(|| {
        NamesysError::InvalidIpnsRecord("record omitted public key for hashed IPNS name".into())
    })?;
    PublicKey::try_decode_protobuf(inline).map_err(|err| {
        NamesysError::InvalidIpnsRecord(format!("inline public key decode failed: {err}"))
    })
}

fn parse_ipns_data(data: &[u8]) -> Result<IpnsRecordData> {
    let value: Ipld = serde_ipld_dagcbor::from_slice(data)
        .map_err(|err| NamesysError::InvalidIpnsRecord(format!("data decode failed: {err}")))?;
    let Ipld::Map(map) = value else {
        return Err(NamesysError::InvalidIpnsRecord(
            "data must be a DAG-CBOR map".into(),
        ));
    };
    Ok(IpnsRecordData {
        value: required_bytes(&map, "Value")?,
        validity: required_bytes(&map, "Validity")?,
        validity_type: required_u64(&map, "ValidityType")?,
        sequence: required_u64(&map, "Sequence")?,
        ttl: required_u64(&map, "TTL")?,
    })
}

fn verify_legacy_fields_match(entry: &IpnsEntry, data: &IpnsRecordData) -> Result<()> {
    if let Some(value) = &entry.value {
        if value != &data.value {
            return Err(NamesysError::InvalidIpnsRecord(
                "legacy value does not match signed data".into(),
            ));
        }
    }
    if let Some(validity) = &entry.validity {
        if validity != &data.validity {
            return Err(NamesysError::InvalidIpnsRecord(
                "legacy validity does not match signed data".into(),
            ));
        }
    }
    if let Some(validity_type) = entry.validity_type {
        if validity_type as u64 != data.validity_type {
            return Err(NamesysError::InvalidIpnsRecord(
                "legacy validityType does not match signed data".into(),
            ));
        }
    }
    if let Some(sequence) = entry.sequence {
        if sequence != data.sequence {
            return Err(NamesysError::InvalidIpnsRecord(
                "legacy sequence does not match signed data".into(),
            ));
        }
    }
    if let Some(ttl) = entry.ttl {
        if ttl != data.ttl {
            return Err(NamesysError::InvalidIpnsRecord(
                "legacy ttl does not match signed data".into(),
            ));
        }
    }
    Ok(())
}

fn required_bytes(map: &std::collections::BTreeMap<String, Ipld>, key: &str) -> Result<Vec<u8>> {
    match map.get(key) {
        Some(Ipld::Bytes(bytes)) => Ok(bytes.clone()),
        Some(_) => Err(NamesysError::InvalidIpnsRecord(format!(
            "{key} must be bytes"
        ))),
        None => Err(NamesysError::InvalidIpnsRecord(format!("missing {key}"))),
    }
}

fn required_u64(map: &std::collections::BTreeMap<String, Ipld>, key: &str) -> Result<u64> {
    match map.get(key) {
        Some(Ipld::Integer(value)) if *value >= 0 && *value <= u64::MAX as i128 => {
            Ok(*value as u64)
        }
        Some(_) => Err(NamesysError::InvalidIpnsRecord(format!(
            "{key} must be a non-negative integer"
        ))),
        None => Err(NamesysError::InvalidIpnsRecord(format!("missing {key}"))),
    }
}

fn unquote_txt(input: &str) -> String {
    let trimmed = input.trim();
    if !trimmed.starts_with('"') {
        return trimmed.to_string();
    }

    let mut output = String::new();
    let mut chars = trimmed.chars().peekable();
    let mut parsed_quoted_segment = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                parsed_quoted_segment = true;
                while let Some(ch) = chars.next() {
                    match ch {
                        '"' => break,
                        '\\' => {
                            if let Some(escaped) = chars.next() {
                                output.push(escaped);
                            }
                        }
                        other => output.push(other),
                    }
                }
            }
            other if other.is_whitespace() && parsed_quoted_segment => {}
            _ => return trimmed.to_string(),
        }
    }

    output
}

#[derive(Debug, Deserialize)]
struct DohResponse {
    #[serde(rename = "Answer")]
    answer: Option<Vec<DohAnswer>>,
}

/// A gated lookup's chain of aliases, across its responses.
struct AliasChain {
    /// The name the lookup was asked for, for its errors.
    asked: String,
    /// The name the chain has reached.
    current: String,
    /// Every name the chain has met.
    visited: std::collections::HashSet<String>,
    /// The aliases followed.
    followed: usize,
    /// Their lowest TTL.
    ttl: Option<Duration>,
}

impl AliasChain {
    fn new(asked: &str, current: String) -> Self {
        Self {
            asked: asked.to_string(),
            visited: std::collections::HashSet::from([current.clone()]),
            current,
            followed: 0,
            ttl: None,
        }
    }

    fn refused(&self, why: &str) -> NamesysError {
        NamesysError::NotFound(format!("{}: {why}", self.asked))
    }
}

/// What one response gave a gated lookup's chain.
enum ChainStep<'a> {
    /// The records the chain's last name owns (none, if it owns none here
    /// and the chain moved: that name is asked for next).
    Records(Vec<&'a DohAnswer>),
    /// More aliases than [`MAX_DOH_CNAME_DEPTH`]: nothing is taken.
    TooLong,
}

#[derive(Debug, Deserialize)]
struct DohAnswer {
    /// The record's owner: under a gated policy, only the records the
    /// queried name, or an admitted alias of it, owns are taken.
    name: Option<String>,
    data: String,
    #[serde(rename = "TTL")]
    ttl: Option<u64>,
    #[serde(rename = "type")]
    record_type: Option<u16>,
}

impl DohAnswer {
    fn is_txt(&self) -> bool {
        self.record_type
            .is_none_or(|record_type| record_type == DNS_TYPE_TXT)
    }

    fn is_cname(&self) -> bool {
        self.record_type == Some(DNS_TYPE_CNAME)
    }

    /// The record's owner, canonical as the gate's classifier makes a name
    /// (lowercase, one trailing dot removed): `None` when it has none, or
    /// one the classifier refuses or reads as an address.
    fn owner(&self) -> Option<String> {
        match classify_host(self.name.as_deref()?) {
            Ok(Target::Name(name) | Target::Onion(name)) => Some(name),
            Ok(Target::Ip(_)) | Err(_) => None,
        }
    }

    /// Whether `name` (canonical) owns the record. A record without a valid
    /// owner is no name's.
    fn is_owned_by(&self, name: &str) -> bool {
        self.owner().is_some_and(|owner| owner == name)
    }
}

fn min_ttl(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(ttl), None) | (None, Some(ttl)) => Some(ttl),
        (None, None) => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpnsRecord {
    pub value: String,
    pub sequence: u64,
    pub validity: Option<String>,
    pub ttl: u64,
}

pub fn verify_ipns_record_placeholder(record: IpnsRecord) -> Result<IpnsRecord> {
    if record.value.starts_with("/ipfs/") || record.value.starts_with("/ipns/") {
        Ok(record)
    } else {
        Err(NamesysError::InvalidDnslink(record.value))
    }
}

#[derive(Debug)]
struct IpnsName {
    peer_id: PeerId,
    inline_public_key: Option<Vec<u8>>,
}

#[derive(Debug)]
struct IpnsRecordData {
    value: Vec<u8>,
    validity: Vec<u8>,
    validity_type: u64,
    sequence: u64,
    ttl: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IpnsEntry {
    #[prost(bytes = "vec", optional, tag = "1")]
    value: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "2")]
    signature_v1: Option<Vec<u8>>,
    #[prost(enumeration = "ValidityType", optional, tag = "3")]
    validity_type: Option<i32>,
    #[prost(bytes = "vec", optional, tag = "4")]
    validity: Option<Vec<u8>>,
    #[prost(uint64, optional, tag = "5")]
    sequence: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    ttl: Option<u64>,
    #[prost(bytes = "vec", optional, tag = "7")]
    pub_key: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "8")]
    signature_v2: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "9")]
    data: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum ValidityType {
    Eol = 0,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipld_core::ipld::Ipld;
    use libp2p_identity::Keypair;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const FUTURE: &str = "2126-01-01T00:00:00.000000000Z";
    const PAST: &str = "2020-01-01T00:00:00.000000000Z";

    #[test]
    fn parses_dnslink_records() {
        assert_eq!(
            parse_dnslink_txt("dnslink=/ipfs/bafyexample").unwrap(),
            Some("/ipfs/bafyexample".to_string())
        );
        assert_eq!(parse_dnslink_txt("not-dnslink").unwrap(), None);
        assert!(parse_dnslink_txt("dnslink=https://example.com").is_err());
    }

    #[test]
    fn unquotes_split_dns_txt_character_strings() {
        assert_eq!(
            unquote_txt(r#""dnslink=/ipfs/bafkq" "addwgevxmmraojswg33smq""#),
            "dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq"
        );
        assert_eq!(
            unquote_txt(r#""value with \"quote\"""#),
            "value with \"quote\""
        );
        assert_eq!(unquote_txt("not quoted"), "not quoted");
    }

    #[tokio::test]
    async fn default_name_resolver_uses_pluggable_dnslink_resolver() {
        let resolver = DefaultNameResolver::new(
            StaticDnsTxtResolver {
                expected_name: "_dnslink.example.test",
                records: vec![
                    DnsTxtRecord::new("unrelated=txt"),
                    DnsTxtRecord::new("dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq"),
                ],
            },
            StaticIpnsResolver { record: None },
        );

        let resolved = resolver.resolve_name("example.test").await.unwrap();

        assert_eq!(resolved, "/ipfs/bafkqaddwgevxmmraojswg33smq");
    }

    #[tokio::test]
    async fn dnslink_resolution_preserves_dns_ttl() {
        let resolver = StaticDnsTxtResolver {
            expected_name: "_dnslink.example.test",
            records: vec![DnsTxtRecord::with_ttl(
                "dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(42),
            )],
        };

        let resolved = resolve_dnslink_with_ttl(&resolver, "example.test")
            .await
            .unwrap();

        assert_eq!(resolved.value, "/ipfs/bafkqaddwgevxmmraojswg33smq");
        assert_eq!(resolved.ttl, Some(Duration::from_secs(42)));
    }

    #[tokio::test]
    async fn cloudflare_doh_resolver_preserves_answer_ttl() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request)
                .await
                .unwrap();
            let body = r#"{"Answer":[{"data":"\"dnslink=/ipfs/bafkq\" \"addwgevxmmraojswg33smq\"","TTL":120}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/dns-json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                .await
                .unwrap();
        });

        let resolver = CloudflareDohResolver::new(format!("http://{addr}/dns-query"));
        let records = resolver
            .txt_lookup_with_ttl("_dnslink.example.test")
            .await
            .unwrap();

        assert_eq!(
            records,
            vec![DnsTxtRecord::with_ttl(
                "dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(120)
            )]
        );
    }

    #[tokio::test]
    async fn cloudflare_doh_resolver_follows_cname_to_txt_answer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2048];
                let len = tokio::io::AsyncReadExt::read(&mut stream, &mut request)
                    .await
                    .unwrap();
                let request = String::from_utf8_lossy(&request[..len]);
                let body = if request.contains("name=_dnslink.example.test") {
                    r#"{"Answer":[{"data":"_dnslink.target.test.","TTL":60,"type":5}]}"#
                } else if request.contains("name=_dnslink.target.test") {
                    r#"{"Answer":[{"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\"","TTL":300,"type":16}]}"#
                } else {
                    r#"{"Answer":[]}"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/dns-json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                    .await
                    .unwrap();
            }
        });

        let resolver = CloudflareDohResolver::new(format!("http://{addr}/dns-query"));
        let records = resolver
            .txt_lookup_with_ttl("_dnslink.example.test")
            .await
            .unwrap();

        assert_eq!(
            records,
            vec![DnsTxtRecord::with_ttl(
                "dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60)
            )]
        );
    }

    /// Under `direct`, a DNSLink lookup's alias is classified before it is
    /// followed (#47 adversarial review, 47-A1): an onion alias is refused
    /// with no query for it. The legacy policy follows it, as before.
    #[tokio::test]
    async fn an_onion_alias_is_refused_under_direct_and_followed_under_legacy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = asked.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2048];
                let len = tokio::io::AsyncReadExt::read(&mut stream, &mut request)
                    .await
                    .unwrap();
                let request = String::from_utf8_lossy(&request[..len]).to_string();
                let line = request.lines().next().unwrap_or("").to_string();
                recorded.lock().unwrap().push(line.clone());
                let body = if line.contains("name=_dnslink.example.test") {
                    r#"{"Answer":[{"name":"_dnslink.example.test.","data":"_dnslink.secret.onion.","TTL":60,"type":5}]}"#
                } else {
                    r#"{"Answer":[{"name":"_dnslink.secret.onion.","data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\"","TTL":300,"type":16}]}"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/dns-json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        let endpoint = format!("http://127.0.0.1:{}/dns-query", addr.port());
        let direct = freedom_ipfs_net::Network::new(
            freedom_ipfs_net::NetworkPolicy::direct(
                freedom_ipfs_net::gate::ConfiguredTargets::new(),
                freedom_ipfs_net::gate::parse_url(&endpoint).unwrap(),
                freedom_ipfs_net::Addresses::Unchecked,
            )
            .unwrap(),
        )
        .unwrap();
        let onion_asked = || {
            asked
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains("secret.onion"))
        };

        let resolver =
            CloudflareDohResolver::with_client(&endpoint, direct.http_client(HTTP_CLIENT_CONFIG));
        let err = resolver
            .txt_lookup_with_ttl("_dnslink.example.test")
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                NamesysError::Network(HttpError::Refused(freedom_ipfs_net::Refused::Onion))
            ),
            "{err:?}"
        );
        assert!(!onion_asked());
        // Asked directly for an onion name: refused, and not asked either.
        assert!(resolver.ip_lookup("x.onion").await.is_err());
        assert!(!onion_asked());

        let legacy = CloudflareDohResolver::with_client(
            &endpoint,
            freedom_ipfs_net::Network::legacy().http_client(HTTP_CLIENT_CONFIG),
        );
        let records = legacy
            .txt_lookup_with_ttl("_dnslink.example.test")
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert!(onion_asked());
    }

    /// A DoH server answering raw JSON Answer arrays by `(name, type)`,
    /// recording every query's name and type.
    async fn answering_doh(
        answers: Vec<(&'static str, &'static str, &'static str)>,
    ) -> (String, Arc<std::sync::Mutex<Vec<(String, String)>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = asked.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2048];
                let len = tokio::io::AsyncReadExt::read(&mut stream, &mut request)
                    .await
                    .unwrap();
                let request = String::from_utf8_lossy(&request[..len]).to_string();
                let target = request.split_whitespace().nth(1).unwrap_or("").to_string();
                let query = target.split_once('?').map_or("", |(_, query)| query);
                let field = |key: &str| {
                    query
                        .split('&')
                        .find_map(|pair| pair.strip_prefix(&format!("{key}=")))
                        .unwrap_or("")
                        .to_string()
                };
                let (name, kind) = (field("name"), field("type"));
                recorded.lock().unwrap().push((name.clone(), kind.clone()));
                let answer = answers
                    .iter()
                    .find(|(n, k, _)| *n == name && *k == kind)
                    .map_or("[]", |(_, _, answer)| answer);
                let body = format!(r#"{{"Status":0,"Answer":{answer}}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/dns-json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        (format!("http://127.0.0.1:{}/dns-query", addr.port()), asked)
    }

    fn direct_resolver(endpoint: &str) -> CloudflareDohResolver {
        let network = freedom_ipfs_net::Network::new(
            freedom_ipfs_net::NetworkPolicy::direct(
                freedom_ipfs_net::gate::ConfiguredTargets::new(),
                freedom_ipfs_net::gate::parse_url(endpoint).unwrap(),
                freedom_ipfs_net::Addresses::Unchecked,
            )
            .unwrap(),
        )
        .unwrap();
        CloudflareDohResolver::with_client(endpoint, network.http_client(HTTP_CLIENT_CONFIG))
    }

    /// An alias and its final record in one response (#47 review round 2,
    /// 47-R2-M1): under `direct` the alias is classified before the record
    /// it owns is taken, for A, AAAA and TXT alike. An ordinary inline alias
    /// (the control) still resolves; an onion one is refused. Under the
    /// legacy policy the onion one resolves, as before.
    #[tokio::test]
    async fn an_inline_alias_is_classified_before_its_record() {
        let (endpoint, asked) = answering_doh(vec![
            ("plain.test", "1", r#"[{"name":"plain.test.","type":5,"TTL":60,"data":"alias.test."},{"name":"alias.test.","type":1,"TTL":60,"data":"8.8.8.8"}]"#),
            ("hidden.test", "1", r#"[{"name":"hidden.test.","type":5,"TTL":60,"data":"secret.onion."},{"name":"secret.onion.","type":1,"TTL":60,"data":"8.8.8.8"}]"#),
            ("plain.test", "28", r#"[{"name":"plain.test.","type":5,"TTL":60,"data":"alias.test."},{"name":"alias.test.","type":28,"TTL":60,"data":"2a00:1::1"}]"#),
            ("hidden.test", "28", r#"[{"name":"hidden.test.","type":5,"TTL":60,"data":"secret.onion."},{"name":"secret.onion.","type":28,"TTL":60,"data":"2a00:1::1"}]"#),
            ("_dnslink.plain.test", "TXT", r#"[{"name":"_dnslink.plain.test.","type":5,"TTL":60,"data":"_dnslink.alias.test."},{"name":"_dnslink.alias.test.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}]"#),
            ("_dnslink.hidden.test", "TXT", r#"[{"name":"_dnslink.hidden.test.","type":5,"TTL":60,"data":"_dnslink.secret.onion."},{"name":"_dnslink.secret.onion.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}]"#),
        ])
        .await;
        let direct = direct_resolver(&endpoint);
        let legacy = CloudflareDohResolver::with_client(
            &endpoint,
            freedom_ipfs_net::Network::legacy().http_client(HTTP_CLIENT_CONFIG),
        );
        for (kind, plain, hidden) in [
            (
                "A",
                direct.ip_lookup_type("plain.test", DNS_TYPE_A).await,
                direct.ip_lookup_type("hidden.test", DNS_TYPE_A).await,
            ),
            (
                "AAAA",
                direct.ip_lookup_type("plain.test", DNS_TYPE_AAAA).await,
                direct.ip_lookup_type("hidden.test", DNS_TYPE_AAAA).await,
            ),
        ] {
            assert_eq!(plain.unwrap().len(), 1, "{kind}: the control resolves");
            assert!(
                matches!(
                    hidden,
                    Err(NamesysError::Network(HttpError::Refused(
                        freedom_ipfs_net::Refused::Onion
                    )))
                ),
                "{kind}: {hidden:?}"
            );
        }
        assert_eq!(
            direct
                .txt_lookup("_dnslink.plain.test")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(direct.txt_lookup("_dnslink.hidden.test").await.is_err());
        // The legacy policy takes any record of the type, as before.
        assert_eq!(
            legacy
                .ip_lookup_type("hidden.test", DNS_TYPE_A)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            legacy
                .txt_lookup("_dnslink.hidden.test")
                .await
                .unwrap()
                .len(),
            1
        );
        // No query for the onion name was ever made.
        assert!(!asked
            .lock()
            .unwrap()
            .iter()
            .any(|(name, _)| name.contains("onion")));
    }

    /// The chain's depth is its own bound, whatever the responses' layout
    /// (#47 review round 3, 47-R3-m1): eight aliases answered one per
    /// response resolve under `direct` as under the legacy policy, for A,
    /// AAAA and TXT; a ninth is refused, and eight answered inline resolve
    /// too.
    #[tokio::test]
    async fn eight_aliases_resolve_in_every_layout_and_a_ninth_doesnt() {
        let leak = |text: String| -> &'static str { Box::leak(text.into_boxed_str()) };
        let mut answers = Vec::new();
        for (prefix, links) in [("eight", 8), ("nine", 9)] {
            for index in 0..links {
                for kind in ["1", "28", "TXT"] {
                    answers.push((
                        leak(format!("{prefix}{index}.test")),
                        kind,
                        leak(format!(
                            r#"[{{"name":"{prefix}{index}.test.","type":5,"TTL":60,"data":"{prefix}{}.test."}}]"#,
                            index + 1
                        )),
                    ));
                }
            }
            let last = format!("{prefix}{links}.test");
            answers.push((
                leak(last.clone()),
                "1",
                leak(format!(
                    r#"[{{"name":"{last}.","type":1,"TTL":60,"data":"8.8.8.8"}}]"#
                )),
            ));
            answers.push((
                leak(last.clone()),
                "28",
                leak(format!(
                    r#"[{{"name":"{last}.","type":28,"TTL":60,"data":"2a00:1::1"}}]"#
                )),
            ));
            answers.push((
                leak(last.clone()),
                "TXT",
                leak(format!(
                    r#"[{{"name":"{last}.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}}]"#
                )),
            ));
        }
        let inline = (0..8)
            .map(|index| {
                format!(
                    r#"{{"name":"inline{index}.test.","type":5,"TTL":60,"data":"inline{}.test."}}"#,
                    index + 1
                )
            })
            .chain([r#"{"name":"inline8.test.","type":1,"TTL":60,"data":"8.8.8.8"}"#.to_string()])
            .collect::<Vec<_>>()
            .join(",");
        answers.push(("inline0.test", "1", leak(format!("[{inline}]"))));
        let (endpoint, _) = answering_doh(answers).await;
        let direct = direct_resolver(&endpoint);
        let legacy = CloudflareDohResolver::with_client(
            &endpoint,
            freedom_ipfs_net::Network::legacy().http_client(HTTP_CLIENT_CONFIG),
        );
        for (record_type, expected) in [(DNS_TYPE_A, "8.8.8.8"), (DNS_TYPE_AAAA, "2a00:1::1")] {
            let expected = vec![expected.parse::<IpAddr>().unwrap()];
            assert_eq!(
                legacy
                    .ip_lookup_type("eight0.test", record_type)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                direct
                    .ip_lookup_type("eight0.test", record_type)
                    .await
                    .unwrap(),
                expected
            );
            assert!(direct
                .ip_lookup_type("nine0.test", record_type)
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(legacy.txt_lookup("eight0.test").await.unwrap().len(), 1);
        assert_eq!(direct.txt_lookup("eight0.test").await.unwrap().len(), 1);
        assert!(direct.txt_lookup("nine0.test").await.is_err());
        assert_eq!(
            direct
                .ip_lookup_type("inline0.test", DNS_TYPE_A)
                .await
                .unwrap(),
            vec!["8.8.8.8".parse::<IpAddr>().unwrap()]
        );
    }

    /// A loop across responses is refused as soon as a name comes back
    /// (#47 adversarial review round 3, 47-R3-A1), not after the depth
    /// bound: two queries, then an error.
    #[tokio::test]
    async fn a_loop_across_responses_is_refused_when_a_name_comes_back() {
        let (endpoint, asked) = answering_doh(vec![
            ("ping.test", "1", r#"[{"name":"ping.test.","type":5,"TTL":60,"data":"pong.test."}]"#),
            ("pong.test", "1", r#"[{"name":"pong.test.","type":5,"TTL":60,"data":"PING.test."}]"#),
            ("_dnslink.ping.test", "TXT", r#"[{"name":"_dnslink.ping.test.","type":5,"TTL":60,"data":"_dnslink.pong.test."}]"#),
            ("_dnslink.pong.test", "TXT", r#"[{"name":"_dnslink.pong.test.","type":5,"TTL":60,"data":"_dnslink.ping.test."}]"#),
        ])
        .await;
        let direct = direct_resolver(&endpoint);
        assert!(direct
            .ip_lookup_type("ping.test", DNS_TYPE_A)
            .await
            .is_err());
        assert!(direct.txt_lookup("_dnslink.ping.test").await.is_err());
        assert_eq!(
            asked.lock().unwrap().len(),
            4,
            "{:?}",
            asked.lock().unwrap()
        );
    }

    /// Malformed owners, for AAAA and TXT as for A (#47 adversarial review
    /// round 5): an empty, blank, tabbed or padded owner on an alias or on a
    /// record of the kind asked for refuses the lookup under `direct`; the
    /// well-formed control resolves.
    #[tokio::test]
    async fn malformed_owners_refuse_aaaa_and_txt_lookups_too() {
        let mut answers = vec![
            (
                "good.test",
                "28",
                r#"[{"name":"good.test.","type":5,"TTL":60,"data":"alias.test."},{"name":"alias.test.","type":28,"TTL":60,"data":"2a00:1::1"}]"#,
            ),
            (
                "_dnslink.good.test",
                "TXT",
                r#"[{"name":"_dnslink.good.test.","type":5,"TTL":60,"data":"_dnslink.alias.test."},{"name":"_dnslink.alias.test.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}]"#,
            ),
        ];
        let leak = |text: String| -> &'static str { Box::leak(text.into_boxed_str()) };
        for (label, owner) in [
            ("empty", ""),
            ("space", " "),
            ("tab", "\\t"),
            ("padded", "x.test. "),
        ] {
            answers.push((
                leak(format!("alias-{label}.test")),
                "28",
                leak(format!(
                    r#"[{{"name":"alias-{label}.test.","type":5,"TTL":60,"data":"safe.test."}},{{"name":"{owner}","type":5,"TTL":60,"data":"secret.onion."}},{{"name":"safe.test.","type":28,"TTL":60,"data":"2a00:1::2"}}]"#
                )),
            ));
            answers.push((
                leak(format!("_dnslink.record-{label}.test")),
                "TXT",
                leak(format!(
                    r#"[{{"name":"_dnslink.record-{label}.test.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}},{{"name":"{owner}","type":16,"TTL":60,"data":"\"dnslink=/ipfs/other\""}}]"#
                )),
            ));
        }
        let (endpoint, _) = answering_doh(answers).await;
        let direct = direct_resolver(&endpoint);
        assert_eq!(
            direct
                .ip_lookup_type("good.test", DNS_TYPE_AAAA)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            direct.txt_lookup("_dnslink.good.test").await.unwrap().len(),
            1
        );
        for label in ["empty", "space", "tab", "padded"] {
            let aaaa = direct
                .ip_lookup_type(&format!("alias-{label}.test"), DNS_TYPE_AAAA)
                .await;
            assert!(aaaa.is_err(), "{label}: {aaaa:?}");
            let txt = direct
                .txt_lookup(&format!("_dnslink.record-{label}.test"))
                .await;
            assert!(txt.is_err(), "{label}: {txt:?}");
        }
    }

    /// A record whose owner is outside the chain is ignored under `direct`
    /// (#47 adversarial review round 2): an A record owned by another name,
    /// with no alias leading to it; the same record owned by the queried
    /// name (the control) is taken.
    #[tokio::test]
    async fn a_record_owned_outside_the_chain_is_ignored() {
        let (endpoint, _) = answering_doh(vec![
            ("own.test", "1", r#"[{"name":"own.test.","type":1,"TTL":60,"data":"8.8.8.8"}]"#),
            ("other.test", "1", r#"[{"name":"unrelated.test.","type":1,"TTL":60,"data":"8.8.8.8"}]"#),
            ("_dnslink.other.test", "TXT", r#"[{"name":"_dnslink.unrelated.test.","type":16,"TTL":60,"data":"\"dnslink=/ipfs/bafkqaddwgevxmmraojswg33smq\""}]"#),
        ])
        .await;
        let direct = direct_resolver(&endpoint);
        assert_eq!(
            direct.ip_lookup("own.test").await.unwrap(),
            vec!["8.8.8.8".parse::<IpAddr>().unwrap()]
        );
        assert!(direct.ip_lookup("other.test").await.unwrap().is_empty());
        assert!(direct
            .txt_lookup("_dnslink.other.test")
            .await
            .unwrap()
            .is_empty());
    }

    /// A name is classified as written under `direct` (#47 review round 2,
    /// 47-R2-m3): one trailing dot is removed, and an empty label left by
    /// more is refused, not trimmed into another name, for the name asked
    /// and for an alias. The legacy policy trims, as before.
    #[tokio::test]
    async fn trailing_dots_are_classified_not_trimmed() {
        let (endpoint, asked) = answering_doh(vec![
            (
                "one-dot.test",
                "1",
                r#"[{"name":"one-dot.test.","type":1,"TTL":60,"data":"8.8.8.8"}]"#,
            ),
            (
                "via-alias.test",
                "1",
                r#"[{"name":"via-alias.test.","type":5,"TTL":60,"data":"empty-label.test.."}]"#,
            ),
            (
                "via-good-alias.test",
                "1",
                r#"[{"name":"via-good-alias.test.","type":5,"TTL":60,"data":"one-dot.test."}]"#,
            ),
        ])
        .await;
        let direct = direct_resolver(&endpoint);
        // One dot: the canonical name is asked for.
        assert_eq!(direct.ip_lookup("one-dot.test.").await.unwrap().len(), 1);
        assert_eq!(
            direct.ip_lookup("via-good-alias.test").await.unwrap().len(),
            1
        );
        // Several dots: refused, the trimmed name never asked for.
        for name in ["two-dots.test..", "three-dots.test..."] {
            assert!(direct.ip_lookup(name).await.is_err(), "{name}");
        }
        assert!(direct.ip_lookup("via-alias.test").await.is_err());
        let asked_names = asked
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for never in ["two-dots.test", "three-dots.test", "empty-label.test"] {
            assert!(
                !asked_names.iter().any(|name| name == never),
                "{never}: {asked_names:?}"
            );
        }
        // The legacy policy trims every dot, as before.
        let legacy = CloudflareDohResolver::with_client(
            &endpoint,
            freedom_ipfs_net::Network::legacy().http_client(HTTP_CLIENT_CONFIG),
        );
        let _ = legacy.ip_lookup("two-dots.test..").await;
        assert!(asked
            .lock()
            .unwrap()
            .iter()
            .any(|(name, _)| name == "two-dots.test"));
    }

    #[tokio::test]
    async fn cached_name_resolver_reuses_successful_resolution() {
        let count = Arc::new(AtomicUsize::new(0));
        let resolver = CachedNameResolver::with_ttl(
            CountingNameResolver {
                count: count.clone(),
                ttl: None,
            },
            Duration::from_secs(60),
        );

        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/1"
        );
        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/1"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cached_name_resolver_honors_dynamic_zero_ttl() {
        let count = Arc::new(AtomicUsize::new(0));
        let resolver = CachedNameResolver::with_ttl(
            CountingNameResolver {
                count: count.clone(),
                ttl: Some(Duration::ZERO),
            },
            Duration::from_secs(60),
        );

        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/1"
        );
        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/2"
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cached_name_resolver_caps_dynamic_ttl() {
        let count = Arc::new(AtomicUsize::new(0));
        let resolver = CachedNameResolver::with_ttl(
            CountingNameResolver {
                count: count.clone(),
                ttl: Some(Duration::from_secs(60)),
            },
            Duration::from_millis(1),
        );

        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/1"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            resolver.resolve_name("example.test").await.unwrap(),
            "/ipfs/2"
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fallback_ipns_resolver_uses_fallback_after_primary_failure() {
        let resolver = FallbackIpnsResolver::new(
            StaticIpnsResolver { record: None },
            StaticIpnsResolver {
                record: Some(IpnsRecord {
                    value: "/ipfs/bafkqaddwgevxmmraojswg33smq".to_string(),
                    sequence: 42,
                    validity: None,
                    ttl: 0,
                }),
            },
        );

        let record = resolver.resolve_ipns("k51fallback").await.unwrap();

        assert_eq!(record.sequence, 42);
    }

    #[tokio::test]
    async fn delegated_ipns_resolver_tries_configured_endpoint_fallbacks() {
        let (name, record_bytes) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, true);
        let first_hits = Arc::new(AtomicUsize::new(0));
        let second_hits = Arc::new(AtomicUsize::new(0));
        let first =
            start_delegated_ipns_server("404 Not Found", None, Vec::new(), first_hits.clone());
        let second = start_delegated_ipns_server(
            "200 OK",
            Some(IPNS_RECORD_CONTENT_TYPE),
            record_bytes,
            second_hits.clone(),
        );
        let resolver = DelegatedIpnsResolver::with_endpoints([first, second]);

        let record = resolver.resolve_ipns(&name).await.unwrap();

        assert_eq!(record.value, "/ipfs/bafkqaddwgevxmmraojswg33smq");
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delegated_ipns_resolver_rejects_oversized_records_before_verification() {
        let (name, _record_bytes) =
            signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, true);
        let hits = Arc::new(AtomicUsize::new(0));
        let endpoint = start_delegated_ipns_server(
            "200 OK",
            Some(IPNS_RECORD_CONTENT_TYPE),
            vec![0u8; IPNS_RECORD_MAX_SIZE + 1],
            hits,
        );
        let resolver = DelegatedIpnsResolver::with_endpoints([endpoint]);

        assert!(matches!(
            resolver.resolve_ipns(&name).await,
            Err(NamesysError::InvalidIpnsRecord(_))
        ));
    }

    #[test]
    fn verifies_v2_ipns_record_with_inline_ed25519_key() {
        let (name, record) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, true);
        let verified = verify_ipns_record(&name, &record).unwrap();
        assert_eq!(verified.value, "/ipfs/bafkqaddwgevxmmraojswg33smq");
        assert_eq!(verified.sequence, 7);
        assert_eq!(verified.ttl, 300_000_000_000);
    }

    #[test]
    fn rejects_tampered_ipns_signature() {
        let (name, mut record) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, true);
        let last = record.last_mut().unwrap();
        *last ^= 0x01;
        assert!(matches!(
            verify_ipns_record(&name, &record),
            Err(NamesysError::InvalidIpnsSignature) | Err(NamesysError::InvalidIpnsRecord(_))
        ));
    }

    #[test]
    fn rejects_expired_ipns_record() {
        let (name, record) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", PAST, true);
        assert!(matches!(
            verify_ipns_record(&name, &record),
            Err(NamesysError::ExpiredIpnsRecord)
        ));
    }

    #[test]
    fn rejects_legacy_field_mismatch() {
        let (name, record) = signed_record_with_legacy_value(
            "/ipfs/bafkqaddwgevxmmraojswg33smq",
            "/ipfs/bafkqahtwgevxmmrao5uxi2bamjzg623fnyqhg2lhnzqxi5lsmuqhmmi",
            FUTURE,
        );
        assert!(matches!(
            verify_ipns_record(&name, &record),
            Err(NamesysError::InvalidIpnsRecord(_))
        ));
    }

    #[test]
    fn verifies_v2_only_ipns_record() {
        let (name, record) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, false);
        let verified = verify_ipns_record(&name, &record).unwrap();
        assert_eq!(verified.value, "/ipfs/bafkqaddwgevxmmraojswg33smq");
    }

    #[test]
    fn builds_binary_ipns_dht_record_key() {
        let (name, _record) = signed_record("/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, false);
        let key = ipns_dht_record_key(&name).unwrap();

        assert!(key.starts_with(b"/ipns/"));
        assert_eq!(
            &key[6..],
            ipns_name_as_cid(&name).unwrap().hash().to_bytes()
        );
    }

    fn signed_record(value: &str, validity: &str, include_legacy: bool) -> (String, Vec<u8>) {
        signed_record_inner(value, value, validity, include_legacy)
    }

    fn signed_record_with_legacy_value(
        value: &str,
        legacy_value: &str,
        validity: &str,
    ) -> (String, Vec<u8>) {
        signed_record_inner(value, legacy_value, validity, true)
    }

    fn signed_record_inner(
        value: &str,
        legacy_value: &str,
        validity: &str,
        include_legacy: bool,
    ) -> (String, Vec<u8>) {
        let keypair = Keypair::generate_ed25519();
        let public = keypair.public();
        let name = Cid::new_v1(
            LIBP2P_KEY_CODEC,
            Multihash::<64>::from_bytes(&public.to_peer_id().to_bytes()).unwrap(),
        )
        .to_string();
        let data = ipns_data(value, validity);
        let mut signed = IPNS_SIGNATURE_PREFIX.to_vec();
        signed.extend_from_slice(&data);
        let signature = keypair.sign(&signed).unwrap();

        let entry = IpnsEntry {
            value: include_legacy.then(|| legacy_value.as_bytes().to_vec()),
            signature_v1: None,
            validity_type: include_legacy.then_some(ValidityType::Eol as i32),
            validity: include_legacy.then(|| validity.as_bytes().to_vec()),
            sequence: include_legacy.then_some(7),
            ttl: include_legacy.then_some(300_000_000_000),
            pub_key: None,
            signature_v2: Some(signature),
            data: Some(data),
        };
        (name, entry.encode_to_vec())
    }

    fn ipns_data(value: &str, validity: &str) -> Vec<u8> {
        ipns_data_with_sequence(value, validity, 7)
    }

    fn ipns_data_with_sequence(value: &str, validity: &str, sequence: u64) -> Vec<u8> {
        let mut map = BTreeMap::new();
        map.insert("Sequence".to_string(), Ipld::Integer(sequence.into()));
        map.insert("TTL".to_string(), Ipld::Integer(300_000_000_000));
        map.insert(
            "Validity".to_string(),
            Ipld::Bytes(validity.as_bytes().to_vec()),
        );
        map.insert("ValidityType".to_string(), Ipld::Integer(0));
        map.insert("Value".to_string(), Ipld::Bytes(value.as_bytes().to_vec()));
        serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap()
    }

    /// The IPNS name of `keypair`, and a v2-only record it signs.
    fn signed_by(
        keypair: &Keypair,
        value: &str,
        validity: &str,
        sequence: u64,
    ) -> (String, Vec<u8>) {
        let name = Cid::new_v1(
            LIBP2P_KEY_CODEC,
            Multihash::<64>::from_bytes(&keypair.public().to_peer_id().to_bytes()).unwrap(),
        )
        .to_string();
        let data = ipns_data_with_sequence(value, validity, sequence);
        let mut signed = IPNS_SIGNATURE_PREFIX.to_vec();
        signed.extend_from_slice(&data);
        let entry = IpnsEntry {
            value: None,
            signature_v1: None,
            validity_type: None,
            validity: None,
            sequence: None,
            ttl: None,
            pub_key: None,
            signature_v2: Some(keypair.sign(&signed).unwrap()),
            data: Some(data),
        };
        (name, entry.encode_to_vec())
    }

    /// A delegated router that answers one IPNS lookup with `body`, after
    /// `delay`.
    fn start_delayed_ipns_server(body: Vec<u8>, delay: Duration) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request);
            std::thread::sleep(delay);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {IPNS_RECORD_CONTENT_TYPE}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
        });
        format!("http://{addr}/routing/v1")
    }

    #[tokio::test]
    async fn delegated_ipns_resolver_answers_with_the_newest_record_any_router_has() {
        let keypair = Keypair::generate_ed25519();
        let (name, stale) = signed_by(&keypair, "/ipfs/bafkqaddwgevxmmraojswg33smq", FUTURE, 3);
        let (_, newest) = signed_by(&keypair, "/ipfs/bafkqac3jobxhgzlbmq", FUTURE, 9);
        // The stale record comes first, from either router: the newer one
        // still wins.
        for (first, second) in [
            (stale.clone(), newest.clone()),
            (newest.clone(), stale.clone()),
        ] {
            let resolver = DelegatedIpnsResolver::with_endpoints([
                start_delayed_ipns_server(first, Duration::ZERO),
                start_delayed_ipns_server(second, Duration::from_millis(200)),
            ]);
            let record = resolver.resolve_ipns(&name).await.unwrap();
            assert_eq!(
                (record.sequence, record.value.as_str()),
                (9, "/ipfs/bafkqac3jobxhgzlbmq")
            );
        }
        // At the same sequence, the later validity wins.
        let (_, sooner) = signed_by(
            &keypair,
            "/ipfs/bafkqaddwgevxmmraojswg33smq",
            "2125-01-01T00:00:00Z",
            9,
        );
        let resolver = DelegatedIpnsResolver::with_endpoints([
            start_delayed_ipns_server(sooner, Duration::ZERO),
            start_delayed_ipns_server(newest, Duration::from_millis(200)),
        ]);
        assert_eq!(
            resolver.resolve_ipns(&name).await.unwrap().value,
            "/ipfs/bafkqac3jobxhgzlbmq"
        );
    }

    #[test]
    fn ipns_records_order_by_sequence_then_validity() {
        let record = |sequence, validity: Option<&str>| IpnsRecord {
            value: "/ipfs/x".into(),
            sequence,
            validity: validity.map(str::to_string),
            ttl: 0,
        };
        let later = Some("2126-01-01T00:00:00Z");
        let sooner = Some("2125-01-01T00:00:00Z");
        assert_eq!(
            compare_ipns_records(&record(2, sooner), &record(1, later)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_ipns_records(&record(1, later), &record(1, sooner)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_ipns_records(&record(1, later), &record(1, later)),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_ipns_records(&record(1, None), &record(1, sooner)),
            std::cmp::Ordering::Less
        );
        // Neither newer: the first is kept.
        let kept = better_ipns_record(
            Some(record(1, later)),
            IpnsRecord {
                value: "/ipfs/y".into(),
                ..record(1, later)
            },
        );
        assert_eq!(kept.value, "/ipfs/x");
        assert_eq!(
            better_ipns_record(Some(record(1, later)), record(2, sooner)).sequence,
            2
        );
        assert_eq!(better_ipns_record(None, record(1, later)).sequence, 1);
    }

    #[test]
    fn an_ipns_name_has_one_canonical_spelling() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let (cid_name, _) = signed_by(&keypair, "/ipfs/x", FUTURE, 1);
        let base36 = Cid::try_from(cid_name.as_str())
            .unwrap()
            .to_string_of_base(cid::multibase::Base::Base36Lower)
            .unwrap();
        let canonical = canonical_ipns_name(&peer_id.to_string()).unwrap();
        assert_eq!(canonical, cid_name);
        assert_eq!(canonical_ipns_name(&base36).unwrap(), canonical);
        assert!(canonical_ipns_name("not-a-name").is_err());
    }

    fn start_delegated_ipns_server(
        status: &'static str,
        content_type: Option<&'static str>,
        body: Vec<u8>,
        hits: Arc<AtomicUsize>,
    ) -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request);
            hits.fetch_add(1, Ordering::SeqCst);
            let content_type_header = content_type
                .map(|content_type| format!("Content-Type: {content_type}\r\n"))
                .unwrap_or_default();
            let header = format!(
                "HTTP/1.1 {status}\r\n{content_type_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(&body).unwrap();
        });
        format!("http://{addr}/routing/v1")
    }

    struct CountingNameResolver {
        count: Arc<AtomicUsize>,
        ttl: Option<Duration>,
    }

    #[async_trait]
    impl NameResolver for CountingNameResolver {
        async fn resolve_name(&self, _name: &str) -> Result<String> {
            let value = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("/ipfs/{value}"))
        }

        async fn resolve_name_with_ttl(&self, _name: &str) -> Result<ResolvedName> {
            let value = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(ResolvedName {
                value: format!("/ipfs/{value}"),
                ttl: self.ttl,
            })
        }
    }

    struct StaticIpnsResolver {
        record: Option<IpnsRecord>,
    }

    struct StaticDnsTxtResolver {
        expected_name: &'static str,
        records: Vec<DnsTxtRecord>,
    }

    #[async_trait]
    impl DnsTxtResolver for StaticDnsTxtResolver {
        async fn txt_lookup(&self, name: &str) -> Result<Vec<String>> {
            Ok(self
                .txt_lookup_with_ttl(name)
                .await?
                .into_iter()
                .map(|record| record.value)
                .collect())
        }

        async fn txt_lookup_with_ttl(&self, name: &str) -> Result<Vec<DnsTxtRecord>> {
            if name == self.expected_name {
                Ok(self.records.clone())
            } else {
                Err(NamesysError::NotFound(name.to_string()))
            }
        }
    }

    #[async_trait]
    impl IpnsResolver for StaticIpnsResolver {
        async fn resolve_ipns(&self, _name: &str) -> Result<IpnsRecord> {
            self.record
                .clone()
                .ok_or_else(|| NamesysError::NotFound("static".into()))
        }
    }
}
