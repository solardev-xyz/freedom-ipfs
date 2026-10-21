use async_trait::async_trait;
use cid::Cid;
use freedom_ipfs_net::{ClientConfig, HttpClient, HttpError};
use futures::stream::{FuturesUnordered, StreamExt};
use ipld_core::ipld::Ipld;
use libp2p_identity::{PeerId, PublicKey};
use multihash::Multihash;
use prost::Message;
use serde::Deserialize;
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

    pub async fn ip_lookup(&self, name: &str) -> Result<Vec<IpAddr>> {
        let mut addrs = self.ip_lookup_type(name, DNS_TYPE_A).await?;
        addrs.extend(self.ip_lookup_type(name, DNS_TYPE_AAAA).await?);
        addrs.sort();
        addrs.dedup();
        Ok(addrs)
    }

    async fn ip_lookup_type(&self, name: &str, record_type: u16) -> Result<Vec<IpAddr>> {
        let mut current = name.trim_end_matches('.').to_string();
        let record_type_query = record_type.to_string();

        for _ in 0..=MAX_DOH_CNAME_DEPTH {
            let response = self
                .client
                .get(&self.endpoint)
                .query(&[
                    ("name", current.as_str()),
                    ("type", record_type_query.as_str()),
                ])
                .header("accept", "application/dns-json")
                .send()
                .await?
                .error_for_status()?
                .json::<DohResponse>()
                .await?;
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
            current = cname.data.trim_end_matches('.').to_string();
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
        let mut current = name.trim_end_matches('.').to_string();
        let mut cname_ttl = None;

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
            current = cname.data.trim_end_matches('.').to_string();
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
        let mut saw_not_found = false;
        let mut first_error = None;

        while let Some(result) = queries.next().await {
            match result {
                Ok(record) => return Ok(record),
                Err(NamesysError::NotFound(_)) => saw_not_found = true,
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
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

#[derive(Debug, Deserialize)]
struct DohAnswer {
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
        let mut map = BTreeMap::new();
        map.insert("Sequence".to_string(), Ipld::Integer(7));
        map.insert("TTL".to_string(), Ipld::Integer(300_000_000_000));
        map.insert(
            "Validity".to_string(),
            Ipld::Bytes(validity.as_bytes().to_vec()),
        );
        map.insert("ValidityType".to_string(), Ipld::Integer(0));
        map.insert("Value".to_string(), Ipld::Bytes(value.as_bytes().to_vec()));
        serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap()
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
