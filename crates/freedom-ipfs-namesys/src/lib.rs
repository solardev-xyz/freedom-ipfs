use async_trait::async_trait;
use cid::Cid;
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
    client: reqwest::Client,
    endpoint: String,
}

impl Default for CloudflareDohResolver {
    fn default() -> Self {
        Self {
            client: timeout_http_client(DEFAULT_NAMESYS_HTTP_TIMEOUT),
            endpoint: "https://cloudflare-dns.com/dns-query".to_string(),
        }
    }
}

impl CloudflareDohResolver {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
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
    client: reqwest::Client,
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
        let mut endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.into().trim_end_matches('/').to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            endpoints.push(DEFAULT_DELEGATED_ROUTER.to_string());
        }
        Self {
            endpoints,
            client: timeout_http_client(DEFAULT_NAMESYS_HTTP_TIMEOUT),
        }
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

/// The legacy (V1) fields against the signed data. Each one present must
/// equal its signed counterpart. And once `signatureV1` or the legacy
/// `value` is non-empty, all five must, an absent one read as its protobuf
/// default, as the IPNS record specification and boxo's `validation.go`
/// require: a record with a `signatureV1` and no legacy copies is refused.
fn verify_legacy_fields_match(entry: &IpnsEntry, data: &IpnsRecordData) -> Result<()> {
    let non_empty =
        |field: &Option<Vec<u8>>| field.as_deref().is_some_and(|bytes| !bytes.is_empty());
    let all = non_empty(&entry.signature_v1) || non_empty(&entry.value);
    // Each field: whether its legacy copy, if present, equals the signed
    // value, and whether the signed value is the field's default.
    let fields = [
        (
            "value",
            entry.value.as_ref().map(|value| *value == data.value),
            data.value.is_empty(),
        ),
        (
            "validity",
            entry
                .validity
                .as_ref()
                .map(|validity| *validity == data.validity),
            data.validity.is_empty(),
        ),
        (
            "validityType",
            entry
                .validity_type
                .map(|validity_type| u64::try_from(validity_type).ok() == Some(data.validity_type)),
            data.validity_type == 0,
        ),
        (
            "sequence",
            entry.sequence.map(|sequence| sequence == data.sequence),
            data.sequence == 0,
        ),
        ("ttl", entry.ttl.map(|ttl| ttl == data.ttl), data.ttl == 0),
    ];
    for (field, equal, signed_is_default) in fields {
        if !equal.unwrap_or(!all || signed_is_default) {
            return Err(NamesysError::InvalidIpnsRecord(format!(
                "legacy {field} does not match signed data"
            )));
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

fn timeout_http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .expect("namesystem HTTP client config is valid")
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

    /// freedom-core's vector `ed25519-signature-v1-alone`
    /// (vectors/dweb-verify-cases.json): an honest V2-only record, with a
    /// one-byte signatureV1 and none of the legacy fields. boxo compares
    /// all five legacy fields with the signed data once signatureV1 is
    /// there, and refuses it; so does this node now. Without the
    /// signatureV1, or with an empty one, it verifies.
    #[test]
    fn a_signature_v1_without_the_legacy_fields_is_refused() {
        let name = "k51qzi5uqu5dja9it0ilm72uimu8sunfn5hbu86wwb6yxrl8kif10fkruhtcwi";
        let record = hex(concat!(
            "42404036874a462beab0f610bb9d9e7f757f59ddf9adbc9f82d02975688df3d8",
            "a425359d992b7c4c40d444daf8457d50d462c22e8891df511d996a7ed9a464c4",
            "620b4a9801a56354544c1b00000045d964b8006556616c756558412f69706673",
            "2f626166796265696775776a6d64647672346969616836656b6776726f766235",
            "7a66623665673563366c6f696b7876786c787a6c776561356461626d68536571",
            "75656e6365016856616c6964697479581e323039392d30312d30315430303a30",
            "303a30302e3030303030303030305a6c56616c69646974795479706500120101",
        ));
        let now = OffsetDateTime::from_unix_timestamp(1_760_000_000).unwrap();
        assert_eq!(
            verify_ipns_record_at(name, &record, now)
                .unwrap_err()
                .to_string(),
            "invalid IPNS record: legacy value does not match signed data"
        );

        let mut entry = IpnsEntry::decode(record.as_slice()).unwrap();
        assert_eq!(entry.signature_v1, Some(vec![0x01]));
        for signature_v1 in [None, Some(Vec::new())] {
            entry.signature_v1 = signature_v1;
            let verified = verify_ipns_record_at(name, &entry.encode_to_vec(), now).unwrap();
            assert_eq!(
                verified.value,
                "/ipfs/bafybeiguwjmddvr4iiah6ekgvrovb5zfb6eg5c6loikxvxlxzlwea5dabm"
            );
            assert_eq!(verified.sequence, 1);
            assert_eq!(
                verified.validity.as_deref(),
                Some("2099-01-01T00:00:00.000000000Z")
            );
            assert_eq!(verified.ttl, 300_000_000_000);
        }
    }

    /// Once signatureV1 or the legacy value is non-empty, every legacy
    /// field must equal the signed data, an absent one read as its
    /// protobuf default (an empty string, or 0). Otherwise only the ones
    /// present are compared.
    #[test]
    fn legacy_fields_are_all_compared_once_signature_v1_or_value_is_set() {
        let keypair = Keypair::generate_ed25519();
        let value = "/ipfs/bafkqaddwgevxmmraojswg33smq";
        let (name, full) = legacy_entry(&keypair, value, 7);
        let verify = |entry: &IpnsEntry| {
            verify_ipns_record(&name, &entry.encode_to_vec())
                .map(|record| record.value)
                .map_err(|err| err.to_string())
        };
        let refused = |field: &str| {
            Err(format!(
                "invalid IPNS record: legacy {field} does not match signed data"
            ))
        };
        type Clear = fn(&mut IpnsEntry);
        let clear: [(&str, Clear); 5] = [
            ("value", |entry| entry.value = None),
            ("validity", |entry| entry.validity = None),
            ("validityType", |entry| entry.validity_type = None),
            ("sequence", |entry| entry.sequence = None),
            ("ttl", |entry| entry.ttl = None),
        ];

        // All five copies, with a signatureV1 or without one.
        assert_eq!(verify(&full), Ok(value.to_string()));
        let without_v1 = IpnsEntry {
            signature_v1: None,
            ..full.clone()
        };
        assert_eq!(verify(&without_v1), Ok(value.to_string()));

        for (field, clear) in clear {
            // A signatureV1 asks for every field. An absent validityType
            // reads as 0, EOL, which is the signed one, so it matches.
            let mut entry = full.clone();
            clear(&mut entry);
            let expected = if field == "validityType" {
                Ok(value.to_string())
            } else {
                refused(field)
            };
            assert_eq!(verify(&entry), expected, "signatureV1, no {field}");
            // So does the legacy value, when it is there.
            let mut entry = without_v1.clone();
            clear(&mut entry);
            let expected = if field == "value" || field == "validityType" {
                Ok(value.to_string())
            } else {
                refused(field)
            };
            assert_eq!(verify(&entry), expected, "legacy value, no {field}");
        }

        // An absent field reads as its default, and matches a signed 0.
        let (name, mut entry) = legacy_entry(&keypair, value, 0);
        entry.sequence = None;
        assert_eq!(
            verify_ipns_record(&name, &entry.encode_to_vec())
                .map(|record| record.sequence)
                .map_err(|err| err.to_string()),
            Ok(0)
        );

        // With neither, each field present is still compared, and the
        // absent ones aren't.
        let (name, full) = legacy_entry(&keypair, value, 7);
        let only_sequence = |sequence| IpnsEntry {
            value: None,
            signature_v1: None,
            validity_type: None,
            validity: None,
            sequence: Some(sequence),
            ttl: None,
            ..full.clone()
        };
        let verify = |entry: &IpnsEntry| {
            verify_ipns_record(&name, &entry.encode_to_vec())
                .map(|record| record.value)
                .map_err(|err| err.to_string())
        };
        assert_eq!(verify(&only_sequence(7)), Ok(value.to_string()));
        assert_eq!(verify(&only_sequence(8)), refused("sequence"));
    }

    /// A record `keypair` signs for `value` at `sequence`, with a
    /// signatureV1 and all five legacy fields copied from its signed data;
    /// and its name. The signatureV1's bytes aren't checked, as boxo
    /// doesn't check them.
    fn legacy_entry(keypair: &Keypair, value: &str, sequence: u64) -> (String, IpnsEntry) {
        let name = Cid::new_v1(
            LIBP2P_KEY_CODEC,
            Multihash::<64>::from_bytes(&keypair.public().to_peer_id().to_bytes()).unwrap(),
        )
        .to_string();
        let mut map = BTreeMap::new();
        map.insert("Sequence".to_string(), Ipld::Integer(sequence.into()));
        map.insert("TTL".to_string(), Ipld::Integer(300_000_000_000));
        map.insert(
            "Validity".to_string(),
            Ipld::Bytes(FUTURE.as_bytes().to_vec()),
        );
        map.insert("ValidityType".to_string(), Ipld::Integer(0));
        map.insert("Value".to_string(), Ipld::Bytes(value.as_bytes().to_vec()));
        let data = serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap();
        let mut signed = IPNS_SIGNATURE_PREFIX.to_vec();
        signed.extend_from_slice(&data);
        let entry = IpnsEntry {
            value: Some(value.as_bytes().to_vec()),
            signature_v1: Some(vec![0x01]),
            validity_type: Some(ValidityType::Eol as i32),
            validity: Some(FUTURE.as_bytes().to_vec()),
            sequence: Some(sequence),
            ttl: Some(300_000_000_000),
            pub_key: None,
            signature_v2: Some(keypair.sign(&signed).unwrap()),
            data: Some(data),
        };
        (name, entry)
    }

    /// Bytes from hex.
    fn hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
            .collect()
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
