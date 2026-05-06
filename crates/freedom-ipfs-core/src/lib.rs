use cid::Cid;
use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use serde::Serialize;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const CODEC_DAG_PB: u64 = 0x70;
pub const CODEC_RAW: u64 = 0x55;
pub const HASH_IDENTITY: u64 = 0x00;
pub const HASH_SHA2_256: u64 = 0x12;
pub const DEFAULT_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;
const DEFAULT_PROGRESS_MAX_EVENTS: usize = 192;
const DEFAULT_PROGRESS_RETAIN_COMPLETED: Duration = Duration::from_secs(90);
const MAX_PROGRESS_STRING_BYTES: usize = 512;
const MAX_PROGRESS_ERROR_BYTES: usize = 256;
const EMPTY_ROOTS_CAR_V1_HEADER: &[u8] = &[
    0xa2, 0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x01, 0x65, b'r', b'o', b'o', b't', b's',
    0x80,
];

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid cid: {0}")]
    InvalidCid(String),
    #[error("unsupported multihash code {0}")]
    UnsupportedHash(u64),
    #[error("cid hash mismatch for {cid}")]
    HashMismatch { cid: Cid },
    #[error("block is too large: {actual} bytes > {max} bytes")]
    BlockTooLarge { actual: usize, max: usize },
    #[error("invalid car: {0}")]
    InvalidCar(String),
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    cid: Cid,
    data: Vec<u8>,
}

impl Block {
    pub fn new(cid: Cid, data: Vec<u8>) -> Result<Self> {
        verify_block(&cid, &data)?;
        Ok(Self { cid, data })
    }

    pub fn unchecked(cid: Cid, data: Vec<u8>) -> Self {
        Self { cid, data }
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn codec(&self) -> u64 {
        self.cid.codec()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (Cid, Vec<u8>) {
        (self.cid, self.data)
    }
}

pub trait BlockProvider: Send + Sync {
    fn get_block(&self, cid: &Cid) -> Result<Option<Block>>;

    fn retain_block(&self, _cid: &Cid) -> Result<()> {
        Ok(())
    }

    fn release_block(&self, _cid: &Cid) {}
}

#[derive(Clone, Debug)]
pub struct ProgressTracker {
    inner: Arc<ProgressInner>,
}

#[derive(Debug)]
struct ProgressInner {
    next_id: AtomicU64,
    state: Mutex<ProgressState>,
}

#[derive(Debug)]
struct ProgressState {
    entries: VecDeque<ProgressEntry>,
    max_events: usize,
    retain_completed: Duration,
}

#[derive(Clone, Debug)]
pub struct ProgressTarget {
    inner: Arc<ProgressTargetInner>,
}

#[derive(Debug)]
struct ProgressTargetInner {
    tracker: ProgressTracker,
    id: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ProgressUpdate {
    pub phase: Option<String>,
    pub status: Option<String>,
    pub source: Option<String>,
    pub message: Option<String>,
    pub root: Option<String>,
    pub bytes_loaded: Option<u64>,
    pub bytes_total: Option<u64>,
    pub blocks_loaded: Option<u64>,
    pub blocks_total: Option<u64>,
    pub providers_found: Option<u64>,
    pub candidate_peers: Option<u64>,
    pub active_subrequests: Option<u64>,
    pub retry_count: Option<u64>,
    pub last_error_code: Option<Option<String>>,
    pub last_error_message: Option<Option<String>>,
}

#[derive(Clone, Debug, Serialize)]
struct ProgressSnapshot {
    generated_at_unix_ms: u64,
    active_count: usize,
    events: Vec<ProgressSnapshotEntry>,
}

#[derive(Clone, Debug, Serialize)]
struct ProgressSnapshotEntry {
    id: u64,
    parent_id: Option<u64>,
    kind: String,
    status: String,
    path: String,
    root: Option<String>,
    phase: String,
    source: Option<String>,
    message: String,
    bytes_loaded: u64,
    bytes_total: Option<u64>,
    blocks_loaded: u64,
    blocks_total: Option<u64>,
    providers_found: u64,
    candidate_peers: u64,
    active_subrequests: u64,
    elapsed_ms: u64,
    retry_count: u64,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

#[derive(Clone, Debug)]
struct ProgressEntry {
    id: u64,
    parent_id: Option<u64>,
    kind: String,
    status: String,
    path: String,
    root: Option<String>,
    phase: String,
    source: Option<String>,
    message: String,
    bytes_loaded: u64,
    bytes_total: Option<u64>,
    blocks_loaded: u64,
    blocks_total: Option<u64>,
    providers_found: u64,
    candidate_peers: u64,
    active_subrequests: u64,
    retry_count: u64,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    started_at: Instant,
    updated_at: Instant,
    completed_at: Option<Instant>,
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new(
            DEFAULT_PROGRESS_MAX_EVENTS,
            DEFAULT_PROGRESS_RETAIN_COMPLETED,
        )
    }
}

impl ProgressTracker {
    pub fn new(max_events: usize, retain_completed: Duration) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                next_id: AtomicU64::new(1),
                state: Mutex::new(ProgressState {
                    entries: VecDeque::new(),
                    max_events: max_events.max(1),
                    retain_completed,
                }),
            }),
        }
    }

    pub fn start(
        &self,
        kind: impl Into<String>,
        path: impl Into<String>,
        parent_id: Option<u64>,
    ) -> ProgressTarget {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.start_with_id(id, kind, path, parent_id)
    }

    pub fn start_with_id(
        &self,
        id: u64,
        kind: impl Into<String>,
        path: impl Into<String>,
        parent_id: Option<u64>,
    ) -> ProgressTarget {
        let now = Instant::now();
        let phase = "started".to_string();
        let entry = ProgressEntry {
            id,
            parent_id,
            kind: truncate_progress_string(kind.into(), MAX_PROGRESS_STRING_BYTES),
            status: "active".to_string(),
            path: truncate_progress_string(path.into(), MAX_PROGRESS_STRING_BYTES),
            root: None,
            message: message_for_phase(&phase).to_string(),
            phase,
            source: None,
            bytes_loaded: 0,
            bytes_total: None,
            blocks_loaded: 0,
            blocks_total: None,
            providers_found: 0,
            candidate_peers: 0,
            active_subrequests: 0,
            retry_count: 0,
            last_error_code: None,
            last_error_message: None,
            started_at: now,
            updated_at: now,
            completed_at: None,
        };
        if let Ok(mut state) = self.inner.state.lock() {
            if let Some(index) = state.entries.iter().position(|entry| entry.id == id) {
                state.entries.remove(index);
            }
            state.entries.push_back(entry);
            state.prune(now);
        }
        ProgressTarget {
            inner: Arc::new(ProgressTargetInner {
                tracker: self.clone(),
                id,
            }),
        }
    }

    pub fn update(&self, id: u64, update: ProgressUpdate) {
        let now = Instant::now();
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == id) else {
            return;
        };
        entry.apply(update, now);
        state.prune(now);
    }

    pub fn complete(&self, id: u64) {
        self.finish(id, "completed", "completed", None, None);
    }

    pub fn cancel(&self, id: u64) {
        self.finish(id, "cancelled", "cancelled", None, None);
    }

    fn cancel_if_active(&self, id: u64) {
        let now = Instant::now();
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == id) else {
            return;
        };
        if entry.status != "active" {
            return;
        }
        entry.status = "cancelled".to_string();
        entry.phase = "cancelled".to_string();
        entry.message = message_for_phase("cancelled").to_string();
        entry.completed_at = Some(now);
        entry.updated_at = now;
        state.prune(now);
    }

    pub fn fail(&self, id: u64, code: impl Into<String>, message: impl Into<String>) {
        self.finish(
            id,
            "failed",
            "failed",
            Some(code.into()),
            Some(message.into()),
        );
    }

    fn finish(
        &self,
        id: u64,
        status: &str,
        phase: &str,
        error_code: Option<String>,
        error_message: Option<String>,
    ) {
        let now = Instant::now();
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == id) else {
            return;
        };
        entry.status = status.to_string();
        entry.phase = phase.to_string();
        entry.message = message_for_phase(phase).to_string();
        entry.completed_at = Some(now);
        entry.updated_at = now;
        if let Some(code) = error_code {
            entry.last_error_code = Some(truncate_progress_string(code, MAX_PROGRESS_ERROR_BYTES));
        }
        if let Some(message) = error_message {
            entry.last_error_message =
                Some(truncate_progress_string(message, MAX_PROGRESS_ERROR_BYTES));
        }
        state.prune(now);
    }

    pub fn snapshot_json(&self) -> String {
        let snapshot = self.snapshot();
        serde_json::to_string(&snapshot).unwrap_or_else(|_| {
            "{\"generated_at_unix_ms\":0,\"active_count\":0,\"events\":[]}".to_string()
        })
    }

    pub fn clear(&self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.entries.clear();
        }
    }

    fn snapshot(&self) -> ProgressSnapshot {
        let now = Instant::now();
        let generated_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        let Ok(mut state) = self.inner.state.lock() else {
            return ProgressSnapshot {
                generated_at_unix_ms,
                active_count: 0,
                events: Vec::new(),
            };
        };
        state.prune(now);
        let active_count = state
            .entries
            .iter()
            .filter(|entry| entry.status == "active")
            .count();
        let events = state
            .entries
            .iter()
            .map(|entry| entry.snapshot(now))
            .collect();
        ProgressSnapshot {
            generated_at_unix_ms,
            active_count,
            events,
        }
    }
}

impl ProgressTarget {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn update(&self, update: ProgressUpdate) {
        self.inner.tracker.update(self.inner.id, update);
    }

    pub fn phase(&self, phase: impl Into<String>) {
        self.update(ProgressUpdate {
            phase: Some(phase.into()),
            ..ProgressUpdate::default()
        });
    }

    pub fn source_phase(&self, phase: impl Into<String>, source: impl Into<String>) {
        self.update(ProgressUpdate {
            phase: Some(phase.into()),
            source: Some(source.into()),
            ..ProgressUpdate::default()
        });
    }

    pub fn bytes(&self, loaded: u64, total: Option<u64>) {
        self.update(ProgressUpdate {
            bytes_loaded: Some(loaded),
            bytes_total: total,
            ..ProgressUpdate::default()
        });
    }

    pub fn root(&self, root: impl Into<String>) {
        self.update(ProgressUpdate {
            root: Some(root.into()),
            ..ProgressUpdate::default()
        });
    }

    pub fn complete(&self) {
        self.inner.tracker.complete(self.inner.id);
    }

    pub fn cancel(&self) {
        self.inner.tracker.cancel(self.inner.id);
    }

    pub fn fail(&self, code: impl Into<String>, message: impl Into<String>) {
        self.inner.tracker.fail(self.inner.id, code, message);
    }
}

impl Drop for ProgressTargetInner {
    fn drop(&mut self) {
        self.tracker.cancel_if_active(self.id);
    }
}

impl ProgressState {
    fn prune(&mut self, now: Instant) {
        let retain_completed = self.retain_completed;
        self.entries.retain(|entry| {
            entry.status == "active"
                || entry
                    .completed_at
                    .is_none_or(|completed| now.duration_since(completed) <= retain_completed)
        });
        while self.entries.len() > self.max_events {
            let Some(index) = self
                .entries
                .iter()
                .position(|entry| entry.status != "active")
            else {
                break;
            };
            self.entries.remove(index);
        }
    }
}

impl ProgressEntry {
    fn apply(&mut self, update: ProgressUpdate, now: Instant) {
        if let Some(phase) = update.phase {
            self.phase = truncate_progress_string(phase, MAX_PROGRESS_STRING_BYTES);
            self.message = message_for_phase(&self.phase).to_string();
        }
        if let Some(status) = update.status {
            self.status = truncate_progress_string(status, MAX_PROGRESS_STRING_BYTES);
            if self.status != "active" {
                self.completed_at = Some(now);
            }
        }
        if let Some(source) = update.source {
            self.source = Some(truncate_progress_string(source, MAX_PROGRESS_STRING_BYTES));
        }
        if let Some(message) = update.message {
            self.message = truncate_progress_string(message, MAX_PROGRESS_STRING_BYTES);
        }
        if let Some(root) = update.root {
            self.root = Some(truncate_progress_string(root, MAX_PROGRESS_STRING_BYTES));
        }
        if let Some(bytes_loaded) = update.bytes_loaded {
            self.bytes_loaded = bytes_loaded;
        }
        if let Some(bytes_total) = update.bytes_total {
            self.bytes_total = Some(bytes_total);
        }
        if let Some(blocks_loaded) = update.blocks_loaded {
            self.blocks_loaded = blocks_loaded;
        }
        if let Some(blocks_total) = update.blocks_total {
            self.blocks_total = Some(blocks_total);
        }
        if let Some(providers_found) = update.providers_found {
            self.providers_found = providers_found;
        }
        if let Some(candidate_peers) = update.candidate_peers {
            self.candidate_peers = candidate_peers;
        }
        if let Some(active_subrequests) = update.active_subrequests {
            self.active_subrequests = active_subrequests;
        }
        if let Some(retry_count) = update.retry_count {
            self.retry_count = retry_count;
        }
        if let Some(last_error_code) = update.last_error_code {
            self.last_error_code = last_error_code
                .map(|value| truncate_progress_string(value, MAX_PROGRESS_ERROR_BYTES));
        }
        if let Some(last_error_message) = update.last_error_message {
            self.last_error_message = last_error_message
                .map(|value| truncate_progress_string(value, MAX_PROGRESS_ERROR_BYTES));
        }
        self.updated_at = now;
    }

    fn snapshot(&self, now: Instant) -> ProgressSnapshotEntry {
        ProgressSnapshotEntry {
            id: self.id,
            parent_id: self.parent_id,
            kind: self.kind.clone(),
            status: self.status.clone(),
            path: self.path.clone(),
            root: self.root.clone(),
            phase: self.phase.clone(),
            source: self.source.clone(),
            message: self.message.clone(),
            bytes_loaded: self.bytes_loaded,
            bytes_total: self.bytes_total,
            blocks_loaded: self.blocks_loaded,
            blocks_total: self.blocks_total,
            providers_found: self.providers_found,
            candidate_peers: self.candidate_peers,
            active_subrequests: self.active_subrequests,
            elapsed_ms: now.duration_since(self.started_at).as_millis() as u64,
            retry_count: self.retry_count,
            last_error_code: self.last_error_code.clone(),
            last_error_message: self.last_error_message.clone(),
        }
    }
}

fn message_for_phase(phase: &str) -> &'static str {
    match phase {
        "queued" => "Queued",
        "started" => "Loading",
        "resolving_name" => "Resolving IPNS name",
        "name_resolved" => "Name resolved",
        "checking_cache" => "Checking cache",
        "cache_hit" => "Loaded from cache",
        "cache_miss" => "Cache miss",
        "provider_lookup" => "Finding providers",
        "providers_found" => "Providers found",
        "provider_diversity_low" => "Finding more providers",
        "dht_fallback_started" => "Searching DHT",
        "fetching_bitswap" => "Trying Bitswap peers",
        "fetching_http_provider" => "Fetching from HTTP provider",
        "first_byte" => "Receiving content",
        "streaming" => "Receiving content",
        "retrying" => "Retrying slow provider",
        "completed" => "Loaded",
        "cancelled" => "Cancelled",
        "failed" => "Load failed",
        _ => "Loading",
    }
}

fn truncate_progress_string(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

pub fn parse_cid(input: &str) -> Result<Cid> {
    input
        .parse::<Cid>()
        .map_err(|err| CoreError::InvalidCid(err.to_string()))
}

pub fn cid_to_string(cid: &Cid) -> String {
    cid.to_string()
}

pub fn cid_from_data(codec: u64, data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(codec, hash)
}

pub fn verify_block(cid: &Cid, data: &[u8]) -> Result<()> {
    if data.len() > DEFAULT_MAX_BLOCK_SIZE {
        return Err(CoreError::BlockTooLarge {
            actual: data.len(),
            max: DEFAULT_MAX_BLOCK_SIZE,
        });
    }

    let hash = cid.hash();
    match hash.code() {
        HASH_SHA2_256 => {
            let expected = Code::Sha2_256.digest(data);
            if expected.digest() == hash.digest() {
                Ok(())
            } else {
                Err(CoreError::HashMismatch { cid: *cid })
            }
        }
        HASH_IDENTITY => {
            if hash.digest() == data {
                Ok(())
            } else {
                Err(CoreError::HashMismatch { cid: *cid })
            }
        }
        code => Err(CoreError::UnsupportedHash(code)),
    }
}

#[derive(Debug, Clone)]
pub struct CarBlock {
    pub cid: Cid,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CarFile {
    pub header: Vec<u8>,
    pub blocks: Vec<CarBlock>,
}

pub fn parse_car_v1(bytes: &[u8]) -> Result<CarFile> {
    let mut offset = 0;
    let header_len = read_varint(bytes, &mut offset)?;
    if header_len == 0 || offset + header_len > bytes.len() {
        return Err(CoreError::InvalidCar("invalid header length".into()));
    }
    let header = bytes[offset..offset + header_len].to_vec();
    offset += header_len;

    let mut blocks = Vec::new();
    while offset < bytes.len() {
        let section_len = read_varint(bytes, &mut offset)?;
        if section_len == 0 || offset + section_len > bytes.len() {
            return Err(CoreError::InvalidCar("invalid block section length".into()));
        }

        let section = &bytes[offset..offset + section_len];
        offset += section_len;

        let mut cursor = Cursor::new(section);
        let cid = Cid::read_bytes(&mut cursor)
            .map_err(|err| CoreError::InvalidCar(format!("invalid block cid: {err}")))?;
        let cid_len = cursor.position() as usize;
        if cid_len > section.len() {
            return Err(CoreError::InvalidCar(
                "block cid consumed beyond section".into(),
            ));
        }
        let data = section[cid_len..].to_vec();
        verify_block(&cid, &data)?;
        blocks.push(CarBlock { cid, data });
    }

    Ok(CarFile { header, blocks })
}

pub fn encode_car_v1(blocks: &[CarBlock]) -> Vec<u8> {
    let mut car = encode_varint(EMPTY_ROOTS_CAR_V1_HEADER.len());
    car.extend_from_slice(EMPTY_ROOTS_CAR_V1_HEADER);

    for block in blocks {
        let mut section = block.cid.to_bytes();
        section.extend_from_slice(&block.data);
        car.extend_from_slice(&encode_varint(section.len()));
        car.extend_from_slice(&section);
    }

    car
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<usize> {
    let input = bytes
        .get(*offset..)
        .ok_or_else(|| CoreError::InvalidCar("unexpected end of input".into()))?;
    let before = input.len();
    let (value, rest) = unsigned_varint::decode::usize(input)
        .map_err(|err| CoreError::InvalidCar(format!("invalid varint: {err}")))?;
    *offset += before - rest.len();
    Ok(value)
}

pub fn encode_varint(value: usize) -> Vec<u8> {
    let mut buf = unsigned_varint::encode::usize_buffer();
    unsigned_varint::encode::usize(value, &mut buf).to_vec()
}

pub fn cid_from_multihash(codec: u64, mh: Multihash<64>) -> Cid {
    Cid::new_v1(codec, mh)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cid_round_trips_and_verifies_raw_block() {
        let data = b"hello freedom ipfs";
        let cid = cid_from_data(CODEC_RAW, data);
        assert_eq!(cid.codec(), CODEC_RAW);
        verify_block(&cid, data).unwrap();
        assert!(verify_block(&cid, b"tampered").is_err());
        assert_eq!(parse_cid(&cid.to_string()).unwrap(), cid);
    }

    #[test]
    fn parses_minimal_car() {
        let data = b"car payload";
        let cid = cid_from_data(CODEC_RAW, data);
        let mut section = cid.to_bytes();
        section.extend_from_slice(data);

        let header = data_encoding::HEXLOWER
            .decode(b"a26776657273696f6e0165726f6f747380")
            .unwrap();
        let mut car = encode_varint(header.len());
        car.extend_from_slice(&header);
        car.extend_from_slice(&encode_varint(section.len()));
        car.extend_from_slice(&section);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert_eq!(parsed.blocks[0].data, data);
    }

    #[test]
    fn encodes_car_round_trip() {
        let data = b"export payload";
        let cid = cid_from_data(CODEC_RAW, data);
        let car = encode_car_v1(&[CarBlock {
            cid,
            data: data.to_vec(),
        }]);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert_eq!(parsed.blocks[0].data, data);
    }

    #[test]
    fn encodes_and_parses_empty_raw_block() {
        let data = Vec::new();
        let cid = cid_from_data(CODEC_RAW, &data);
        let car = encode_car_v1(&[CarBlock {
            cid,
            data: data.clone(),
        }]);

        let parsed = parse_car_v1(&car).unwrap();
        assert_eq!(parsed.blocks.len(), 1);
        assert_eq!(parsed.blocks[0].cid, cid);
        assert!(parsed.blocks[0].data.is_empty());
    }

    #[test]
    fn progress_tracker_records_updates_and_json_snapshot() {
        let tracker = ProgressTracker::default();
        let target = tracker.start("gateway_request", "/ipns/ipfs.tech/", None);
        target.phase("resolving_name");
        target.update(ProgressUpdate {
            root: Some("ipfs.tech".to_string()),
            providers_found: Some(3),
            ..ProgressUpdate::default()
        });
        target.complete();

        let json: serde_json::Value = serde_json::from_str(&tracker.snapshot_json()).unwrap();
        assert_eq!(json["active_count"], 0);
        assert_eq!(json["events"][0]["id"], target.id());
        assert_eq!(json["events"][0]["kind"], "gateway_request");
        assert_eq!(json["events"][0]["status"], "completed");
        assert_eq!(json["events"][0]["root"], "ipfs.tech");
        assert_eq!(json["events"][0]["providers_found"], 3);
        assert_eq!(json["events"][0]["message"], "Loaded");
    }

    #[test]
    fn progress_tracker_prunes_completed_but_keeps_active() {
        let tracker = ProgressTracker::new(2, Duration::from_secs(90));
        let first = tracker.start("gateway_request", "/ipfs/first", None);
        first.complete();
        let active = tracker.start("gateway_request", "/ipfs/active", None);
        let second = tracker.start("gateway_request", "/ipfs/second", None);
        second.complete();

        let json: serde_json::Value = serde_json::from_str(&tracker.snapshot_json()).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event["id"] == active.id()));
        assert!(events.iter().any(|event| event["id"] == second.id()));
        assert!(!events.iter().any(|event| event["id"] == first.id()));
    }

    #[test]
    fn progress_target_drop_cancels_still_active_entries() {
        let tracker = ProgressTracker::default();
        let target = tracker.start("gateway_request", "/ipfs/example", None);
        let id = target.id();
        let clone = target.clone();
        drop(target);

        let json: serde_json::Value = serde_json::from_str(&tracker.snapshot_json()).unwrap();
        let event = json["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["id"] == id)
            .unwrap();
        assert_eq!(event["status"], "active");

        drop(clone);
        let json: serde_json::Value = serde_json::from_str(&tracker.snapshot_json()).unwrap();
        let event = json["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["id"] == id)
            .unwrap();
        assert_eq!(event["status"], "cancelled");
        assert_eq!(event["phase"], "cancelled");
    }
}
