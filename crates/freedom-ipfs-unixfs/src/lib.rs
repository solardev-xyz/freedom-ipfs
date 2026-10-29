use cid::Cid;
use freedom_ipfs_core::{Block, BlockProvider, CODEC_DAG_PB, CODEC_RAW};
use multihash::Multihash;
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

pub const DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY: usize = 256;

const HAMT_MURMUR3_X64_64: u64 = 0x22;
/// The HAMT fanouts read, as boxo reads them: the powers of two from 8 to
/// 1024.
const HAMT_FANOUTS: std::ops::RangeInclusive<u64> = 8..=1024;
const HAMT_MAX_SHARDS_VISITED: usize = 1024;
const UNIXFS_METADATA_CACHE_MAX_BLOCK_BYTES: usize = 64 * 1024;
const UNIXFS_PATH_CACHE_MAX_PATH_BYTES: usize = 1024;
const UNIXFS_RANGE_BATCH_MAX_BLOCKS: usize = 4;

#[derive(Debug, Error)]
pub enum UnixfsError {
    #[error("block not found: {0}")]
    NotFound(Cid),
    #[error("path segment not found: {0}")]
    PathNotFound(String),
    #[error("unsupported codec {0}")]
    UnsupportedCodec(u64),
    #[error("unsupported unixfs node type {0}")]
    UnsupportedNodeType(i32),
    #[error("path resolves to a directory")]
    IsDirectory,
    #[error("path requires a directory but found file")]
    NotDirectory,
    #[error("invalid dag-pb: {0}")]
    InvalidDagPb(String),
    #[error("block provider: {0}")]
    Provider(String),
}

pub type Result<T> = std::result::Result<T, UnixfsError>;

#[derive(Clone, PartialEq, Message)]
struct PbNode {
    #[prost(bytes = "vec", optional, tag = "1")]
    data: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "2")]
    links: Vec<PbLink>,
}

#[derive(Clone, PartialEq, Message)]
struct PbLink {
    #[prost(bytes = "vec", optional, tag = "1")]
    hash: Option<Vec<u8>>,
    #[prost(string, optional, tag = "2")]
    name: Option<String>,
    #[prost(uint64, optional, tag = "3")]
    tsize: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct UnixfsData {
    #[prost(enumeration = "DataType", optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(bytes = "vec", optional, tag = "2")]
    data: Option<Vec<u8>>,
    #[prost(uint64, optional, tag = "3")]
    filesize: Option<u64>,
    #[prost(uint64, repeated, tag = "4")]
    blocksizes: Vec<u64>,
    #[prost(uint64, optional, tag = "5")]
    hash_type: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    fanout: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum DataType {
    Raw = 0,
    Directory = 1,
    File = 2,
    Metadata = 3,
    Symlink = 4,
    HamtShard = 5,
}

#[derive(Debug, Clone)]
pub struct ResolvedNode {
    pub cid: Cid,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Raw,
    File,
    Directory,
    HamtShard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    pub cid: Cid,
    pub size: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LinkTarget {
    cid: Cid,
    size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct UnixfsResolver {
    metadata_cache: Arc<UnixfsMetadataCache>,
}

impl UnixfsResolver {
    pub fn new() -> Self {
        Self::with_metadata_cache_capacity(DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY)
    }

    pub fn without_metadata_cache() -> Self {
        Self::with_metadata_cache_capacity(0)
    }

    pub fn with_metadata_cache_capacity(capacity: usize) -> Self {
        Self {
            metadata_cache: Arc::new(UnixfsMetadataCache::new(capacity)),
        }
    }

    pub fn metadata_cache_stats(&self) -> UnixfsMetadataCacheStats {
        self.metadata_cache.stats()
    }

    pub fn clear_metadata_cache(&self) {
        self.metadata_cache.clear();
    }

    pub fn resolve_path(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<ResolvedNode> {
        UnixfsContext::cached(provider, &self.metadata_cache).resolve_path(root, path)
    }

    pub fn resolve_path_with_raw_link_tsize_hint(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        UnixfsContext::cached(provider, &self.metadata_cache)
            .resolve_path_with_raw_link_tsize_hint(root, path)
    }

    pub fn read_file(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache).read_file(root, path)
    }

    pub fn list_directory(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>> {
        UnixfsContext::cached(provider, &self.metadata_cache).list_directory(root, path)
    }

    pub fn file_size(&self, provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<u64> {
        UnixfsContext::cached(provider, &self.metadata_cache).file_size(root, path)
    }

    pub fn file_size_cid(&self, provider: &dyn BlockProvider, cid: &Cid) -> Result<u64> {
        UnixfsContext::cached(provider, &self.metadata_cache).file_size_cid(cid)
    }

    pub fn read_file_range(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache)
            .read_file_range(root, path, start, end)
    }

    pub fn read_file_cid_range(
        &self,
        provider: &dyn BlockProvider,
        cid: &Cid,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache).read_file_cid_range(cid, start, end)
    }
}

impl Default for UnixfsResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnixfsMetadataCacheStats {
    pub capacity: usize,
    pub len: usize,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub oversized_skips: u64,
    pub path_len: usize,
    pub path_hits: u64,
    pub path_misses: u64,
    pub path_inserts: u64,
    pub path_evictions: u64,
    pub path_oversized_skips: u64,
    pub file_size_len: usize,
    pub file_size_hits: u64,
    pub file_size_misses: u64,
    pub file_size_inserts: u64,
    pub file_size_evictions: u64,
}

#[derive(Debug)]
struct UnixfsMetadataCache {
    inner: Mutex<UnixfsMetadataCacheInner>,
}

impl UnixfsMetadataCache {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(UnixfsMetadataCacheInner {
                capacity,
                ..UnixfsMetadataCacheInner::default()
            }),
        }
    }

    fn get(&self, cid: &Cid) -> Option<DecodedDagPb> {
        let mut inner = self.lock_inner();
        let decoded = inner.entries.get(cid).cloned();
        if let Some(decoded) = decoded {
            inner.hits = inner.hits.saturating_add(1);
            touch_metadata_cache_order(&mut inner, cid);
            Some(decoded)
        } else {
            inner.misses = inner.misses.saturating_add(1);
            None
        }
    }

    fn get_path(&self, root: &Cid, path: &str) -> Option<ResolvedNode> {
        if path.len() > UNIXFS_PATH_CACHE_MAX_PATH_BYTES {
            return None;
        }

        let key = UnixfsPathCacheKey::new(root, path);
        let mut inner = self.lock_inner();
        let resolved = inner.path_entries.get(&key).cloned();
        if let Some(resolved) = resolved {
            inner.path_hits = inner.path_hits.saturating_add(1);
            touch_path_cache_order(&mut inner, &key);
            Some(resolved)
        } else {
            inner.path_misses = inner.path_misses.saturating_add(1);
            None
        }
    }

    fn get_file_size(&self, cid: &Cid) -> Option<u64> {
        let mut inner = self.lock_inner();
        let size = inner.file_size_entries.get(cid).copied();
        if let Some(size) = size {
            inner.file_size_hits = inner.file_size_hits.saturating_add(1);
            touch_file_size_cache_order(&mut inner, cid);
            Some(size)
        } else {
            inner.file_size_misses = inner.file_size_misses.saturating_add(1);
            None
        }
    }

    fn insert(&self, cid: &Cid, encoded_len: usize, decoded: DecodedDagPb) {
        let mut inner = self.lock_inner();
        if inner.capacity == 0 {
            return;
        }
        if encoded_len > UNIXFS_METADATA_CACHE_MAX_BLOCK_BYTES {
            inner.oversized_skips = inner.oversized_skips.saturating_add(1);
            return;
        }

        if inner.entries.insert(*cid, decoded).is_none() {
            inner.inserts = inner.inserts.saturating_add(1);
        }
        touch_metadata_cache_order(&mut inner, cid);

        while inner.entries.len() > inner.capacity {
            let Some(evicted) = inner.order.pop_front() else {
                break;
            };
            if inner.entries.remove(&evicted).is_some() {
                inner.evictions = inner.evictions.saturating_add(1);
            }
        }
    }

    fn insert_path(&self, root: &Cid, path: &str, resolved: ResolvedNode) {
        let mut inner = self.lock_inner();
        if inner.capacity == 0 {
            return;
        }
        if path.len() > UNIXFS_PATH_CACHE_MAX_PATH_BYTES {
            inner.path_oversized_skips = inner.path_oversized_skips.saturating_add(1);
            return;
        }

        let key = UnixfsPathCacheKey::new(root, path);
        if inner.path_entries.insert(key.clone(), resolved).is_none() {
            inner.path_inserts = inner.path_inserts.saturating_add(1);
        }
        touch_path_cache_order(&mut inner, &key);

        while inner.path_entries.len() > inner.capacity {
            let Some(evicted) = inner.path_order.pop_front() else {
                break;
            };
            if inner.path_entries.remove(&evicted).is_some() {
                inner.path_evictions = inner.path_evictions.saturating_add(1);
            }
        }
    }

    fn insert_file_size(&self, cid: &Cid, size: u64) {
        let mut inner = self.lock_inner();
        if inner.capacity == 0 {
            return;
        }

        if inner.file_size_entries.insert(*cid, size).is_none() {
            inner.file_size_inserts = inner.file_size_inserts.saturating_add(1);
        }
        touch_file_size_cache_order(&mut inner, cid);

        while inner.file_size_entries.len() > inner.capacity {
            let Some(evicted) = inner.file_size_order.pop_front() else {
                break;
            };
            if inner.file_size_entries.remove(&evicted).is_some() {
                inner.file_size_evictions = inner.file_size_evictions.saturating_add(1);
            }
        }
    }

    fn clear(&self) {
        let mut inner = self.lock_inner();
        inner.entries.clear();
        inner.order.clear();
        inner.path_entries.clear();
        inner.path_order.clear();
        inner.file_size_entries.clear();
        inner.file_size_order.clear();
    }

    fn stats(&self) -> UnixfsMetadataCacheStats {
        let inner = self.lock_inner();
        UnixfsMetadataCacheStats {
            capacity: inner.capacity,
            len: inner.entries.len(),
            hits: inner.hits,
            misses: inner.misses,
            inserts: inner.inserts,
            evictions: inner.evictions,
            oversized_skips: inner.oversized_skips,
            path_len: inner.path_entries.len(),
            path_hits: inner.path_hits,
            path_misses: inner.path_misses,
            path_inserts: inner.path_inserts,
            path_evictions: inner.path_evictions,
            path_oversized_skips: inner.path_oversized_skips,
            file_size_len: inner.file_size_entries.len(),
            file_size_hits: inner.file_size_hits,
            file_size_misses: inner.file_size_misses,
            file_size_inserts: inner.file_size_inserts,
            file_size_evictions: inner.file_size_evictions,
        }
    }

    fn lock_inner(&self) -> MutexGuard<'_, UnixfsMetadataCacheInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug, Default)]
struct UnixfsMetadataCacheInner {
    capacity: usize,
    entries: HashMap<Cid, DecodedDagPb>,
    order: VecDeque<Cid>,
    path_entries: HashMap<UnixfsPathCacheKey, ResolvedNode>,
    path_order: VecDeque<UnixfsPathCacheKey>,
    file_size_entries: HashMap<Cid, u64>,
    file_size_order: VecDeque<Cid>,
    hits: u64,
    misses: u64,
    inserts: u64,
    evictions: u64,
    oversized_skips: u64,
    path_hits: u64,
    path_misses: u64,
    path_inserts: u64,
    path_evictions: u64,
    path_oversized_skips: u64,
    file_size_hits: u64,
    file_size_misses: u64,
    file_size_inserts: u64,
    file_size_evictions: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct UnixfsPathCacheKey {
    root: Cid,
    path: String,
}

impl UnixfsPathCacheKey {
    fn new(root: &Cid, path: &str) -> Self {
        Self {
            root: *root,
            path: path.to_string(),
        }
    }
}

fn touch_metadata_cache_order(inner: &mut UnixfsMetadataCacheInner, cid: &Cid) {
    inner.order.retain(|candidate| candidate != cid);
    inner.order.push_back(*cid);
}

fn touch_path_cache_order(inner: &mut UnixfsMetadataCacheInner, key: &UnixfsPathCacheKey) {
    inner.path_order.retain(|candidate| candidate != key);
    inner.path_order.push_back(key.clone());
}

fn touch_file_size_cache_order(inner: &mut UnixfsMetadataCacheInner, cid: &Cid) {
    inner.file_size_order.retain(|candidate| candidate != cid);
    inner.file_size_order.push_back(*cid);
}

#[derive(Clone, Debug)]
struct DecodedDagPb {
    node: PbNode,
    data: UnixfsData,
    kind: DataType,
}

struct UnixfsContext<'a> {
    provider: &'a dyn BlockProvider,
    metadata_cache: Option<&'a UnixfsMetadataCache>,
}

impl<'a> UnixfsContext<'a> {
    fn uncached(provider: &'a dyn BlockProvider) -> Self {
        Self {
            provider,
            metadata_cache: None,
        }
    }

    fn cached(provider: &'a dyn BlockProvider, metadata_cache: &'a UnixfsMetadataCache) -> Self {
        Self {
            provider,
            metadata_cache: Some(metadata_cache),
        }
    }

    fn get_block(&self, cid: &Cid) -> Result<Block> {
        self.provider
            .get_block(cid)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .ok_or(UnixfsError::NotFound(*cid))
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Vec<u8>> {
        self.provider
            .get_block_range(cid, start, end)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .ok_or(UnixfsError::NotFound(*cid))
    }

    fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> Result<Vec<Vec<u8>>> {
        self.provider
            .get_block_ranges(ranges)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .into_iter()
            .zip(ranges.iter())
            .map(|(bytes, (cid, _, _))| bytes.ok_or(UnixfsError::NotFound(*cid)))
            .collect()
    }

    fn dag_pb(&self, cid: &Cid) -> Result<DecodedDagPb> {
        if let Some(cache) = self.metadata_cache {
            if let Some(decoded) = cache.get(cid) {
                return Ok(decoded);
            }
        }

        let block = self.get_block(cid)?;
        if block.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::UnsupportedCodec(block.codec()));
        }

        let node = decode_pb_node(block.data())?;
        let data = decode_unixfs_data(&node)?;
        let kind = data_type(&data)?;
        let decoded = DecodedDagPb { node, data, kind };
        if let Some(cache) = self.metadata_cache {
            cache.insert(cid, block.data().len(), decoded.clone());
        }
        Ok(decoded)
    }

    fn resolve_path(&self, root: &Cid, path: &str) -> Result<ResolvedNode> {
        if let Some(cache) = self.metadata_cache {
            if let Some(resolved) = cache.get_path(root, path) {
                return Ok(resolved);
            }
        }

        let resolved = self.resolve_path_uncached(root, path)?;
        if let Some(cache) = self.metadata_cache {
            cache.insert_path(root, path, resolved.clone());
        }
        Ok(resolved)
    }

    fn resolve_path_with_raw_link_tsize_hint(
        &self,
        root: &Cid,
        path: &str,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        let mut current = LinkTarget {
            cid: *root,
            size: None,
        };
        let mut segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .peekable();

        if segments.peek().is_none() {
            return self.classify_with_raw_link_tsize_hint(&current);
        }

        for segment in segments {
            if current.cid.codec() != CODEC_DAG_PB {
                self.get_block(&current.cid)?;
                return Err(UnixfsError::NotDirectory);
            }

            let decoded = self.dag_pb(&current.cid)?;
            current = match decoded.kind {
                DataType::Directory => find_link_target(&decoded.node, segment)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?,
                DataType::HamtShard => self
                    .find_hamt_link_target(&decoded.node, &decoded.data, segment)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?,
                _ => return Err(UnixfsError::NotDirectory),
            };
        }

        self.classify_with_raw_link_tsize_hint(&current)
    }

    fn resolve_path_uncached(&self, root: &Cid, path: &str) -> Result<ResolvedNode> {
        let mut current = *root;
        let mut segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .peekable();

        if segments.peek().is_none() {
            return self.classify(&current);
        }

        for segment in segments {
            if current.codec() != CODEC_DAG_PB {
                self.get_block(&current)?;
                return Err(UnixfsError::NotDirectory);
            }

            let decoded = self.dag_pb(&current)?;
            match decoded.kind {
                DataType::Directory => {
                    current = find_link(&decoded.node, segment)?
                        .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
                }
                DataType::HamtShard => {
                    current = self
                        .find_hamt_link(&decoded.node, &decoded.data, segment)?
                        .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
                }
                _ => return Err(UnixfsError::NotDirectory),
            }
        }

        self.classify(&current)
    }

    fn read_file(&self, root: &Cid, path: &str) -> Result<Vec<u8>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.read_file_cid(&resolved.cid)
    }

    fn list_directory(&self, root: &Cid, path: &str) -> Result<Vec<DirectoryEntry>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => self.list_directory_cid(&resolved.cid),
            NodeKind::Raw | NodeKind::File => Err(UnixfsError::NotDirectory),
        }
    }

    fn file_size(&self, root: &Cid, path: &str) -> Result<u64> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.file_size_cid(&resolved.cid)
    }

    fn read_file_range(&self, root: &Cid, path: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.read_file_cid_range(&resolved.cid, start, end)
    }
}

pub fn resolve_path(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<ResolvedNode> {
    UnixfsContext::uncached(provider).resolve_path(root, path)
}

pub fn read_file(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<Vec<u8>> {
    UnixfsContext::uncached(provider).read_file(root, path)
}

pub fn list_directory(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
) -> Result<Vec<DirectoryEntry>> {
    UnixfsContext::uncached(provider).list_directory(root, path)
}

pub fn file_size(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<u64> {
    UnixfsContext::uncached(provider).file_size(root, path)
}

pub fn read_file_range(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    UnixfsContext::uncached(provider).read_file_range(root, path, start, end)
}

impl UnixfsContext<'_> {
    fn list_directory_cid(&self, cid: &Cid) -> Result<Vec<DirectoryEntry>> {
        if cid.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::NotDirectory);
        }

        let decoded = self.dag_pb(cid)?;
        let mut entries = match decoded.kind {
            DataType::Directory => directory_entries(&decoded.node.links)?,
            DataType::HamtShard => self.hamt_directory_entries(&decoded.node, &decoded.data)?,
            _ => return Err(UnixfsError::NotDirectory),
        };
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn hamt_directory_entries(
        &self,
        node: &PbNode,
        data: &UnixfsData,
    ) -> Result<Vec<DirectoryEntry>> {
        let root = validate_hamt(data)?;
        let mut entries = Vec::new();
        let mut pending = Vec::new();
        collect_hamt_entries(&node.links, root.prefix_len, &mut entries, &mut pending)?;

        let mut visited = 0usize;
        while let Some(cid) = pending.pop() {
            visited += 1;
            if visited > HAMT_MAX_SHARDS_VISITED {
                return Err(UnixfsError::InvalidDagPb(format!(
                    "HAMT traversal exceeded {HAMT_MAX_SHARDS_VISITED} shards"
                )));
            }
            if cid.codec() != CODEC_DAG_PB {
                return Err(UnixfsError::NotDirectory);
            }

            let shard = self.dag_pb(&cid)?;
            if shard.kind != DataType::HamtShard {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT bucket link did not resolve to a HAMT shard".into(),
                ));
            }
            let layout = validate_hamt(&shard.data)?;
            collect_hamt_entries(
                &shard.node.links,
                layout.prefix_len,
                &mut entries,
                &mut pending,
            )?;
        }

        Ok(entries)
    }

    fn read_file_cid(&self, cid: &Cid) -> Result<Vec<u8>> {
        match cid.codec() {
            CODEC_RAW => Ok(self.get_block(cid)?.data().to_vec()),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        let mut out = decoded.data.data.unwrap_or_default();
                        for link in &decoded.node.links {
                            let child = link_cid(link)?;
                            out.extend_from_slice(&self.read_file_cid(&child)?);
                        }
                        Ok(out)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn file_size_cid(&self, cid: &Cid) -> Result<u64> {
        if let Some(cache) = self.metadata_cache {
            if let Some(size) = cache.get_file_size(cid) {
                return Ok(size);
            }
        }

        let size = self.file_size_cid_uncached(cid)?;
        if let Some(cache) = self.metadata_cache {
            cache.insert_file_size(cid, size);
        }
        Ok(size)
    }

    fn file_size_cid_uncached(&self, cid: &Cid) -> Result<u64> {
        match cid.codec() {
            CODEC_RAW => Ok(self.get_block(cid)?.data().len() as u64),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        if let Some(filesize) = decoded.data.filesize {
                            return Ok(filesize);
                        }
                        let mut size = decoded
                            .data
                            .data
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u64);
                        for link in &decoded.node.links {
                            size = size.saturating_add(self.file_size_cid(&link_cid(link)?)?);
                        }
                        Ok(size)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn read_file_cid_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Vec<u8>> {
        match cid.codec() {
            CODEC_RAW => self.get_block_range(cid, start, end),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        let mut out = Vec::new();
                        let mut offset = 0u64;
                        if let Some(inline) = decoded.data.data.as_deref() {
                            append_intersection(&mut out, inline, offset, start, end);
                            offset = offset.saturating_add(inline.len() as u64);
                        }

                        let mut child_ranges = Vec::new();
                        for (index, link) in decoded.node.links.iter().enumerate() {
                            let child = link_cid(link)?;
                            let child_size = decoded
                                .data
                                .blocksizes
                                .get(index)
                                .copied()
                                .map(Ok)
                                .unwrap_or_else(|| self.file_size_cid(&child))?;
                            if child_size == 0 {
                                continue;
                            }
                            let child_end = offset.saturating_add(child_size - 1);
                            if ranges_intersect(offset, child_end, start, end) {
                                let range_start = start.saturating_sub(offset);
                                let range_end = end.min(child_end).saturating_sub(offset);
                                child_ranges.push((child, range_start, range_end));
                            }
                            offset = offset.saturating_add(child_size);
                            if offset > end {
                                break;
                            }
                        }
                        let mut raw_batch = Vec::new();
                        for (child, range_start, range_end) in child_ranges {
                            if child.codec() == CODEC_RAW {
                                raw_batch.push((child, range_start, range_end));
                                if raw_batch.len() >= UNIXFS_RANGE_BATCH_MAX_BLOCKS {
                                    for bytes in self.get_block_ranges(&raw_batch)? {
                                        out.extend_from_slice(&bytes);
                                    }
                                    raw_batch.clear();
                                }
                            } else {
                                if !raw_batch.is_empty() {
                                    for bytes in self.get_block_ranges(&raw_batch)? {
                                        out.extend_from_slice(&bytes);
                                    }
                                    raw_batch.clear();
                                }
                                out.extend_from_slice(&self.read_file_cid_range(
                                    &child,
                                    range_start,
                                    range_end,
                                )?);
                            }
                        }
                        if !raw_batch.is_empty() {
                            for bytes in self.get_block_ranges(&raw_batch)? {
                                out.extend_from_slice(&bytes);
                            }
                        }
                        Ok(out)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn classify(&self, cid: &Cid) -> Result<ResolvedNode> {
        let kind = match cid.codec() {
            CODEC_RAW => {
                self.get_block(cid)?;
                NodeKind::Raw
            }
            CODEC_DAG_PB => match self.dag_pb(cid)?.kind {
                DataType::Raw | DataType::File => NodeKind::File,
                DataType::Directory => NodeKind::Directory,
                DataType::HamtShard => NodeKind::HamtShard,
                other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
            },
            _ => {
                let block = self.get_block(cid)?;
                return Err(UnixfsError::UnsupportedCodec(block.codec()));
            }
        };

        Ok(ResolvedNode { cid: *cid, kind })
    }

    fn classify_with_raw_link_tsize_hint(
        &self,
        target: &LinkTarget,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        match target.cid.codec() {
            CODEC_RAW => {
                if let Some(size) = target.size.filter(|size| *size > 0) {
                    return Ok((
                        ResolvedNode {
                            cid: target.cid,
                            kind: NodeKind::Raw,
                        },
                        Some(size),
                    ));
                }

                self.get_block(&target.cid)?;
                Ok((
                    ResolvedNode {
                        cid: target.cid,
                        kind: NodeKind::Raw,
                    },
                    None,
                ))
            }
            CODEC_DAG_PB => {
                let kind = match self.dag_pb(&target.cid)?.kind {
                    DataType::Raw | DataType::File => NodeKind::File,
                    DataType::Directory => NodeKind::Directory,
                    DataType::HamtShard => NodeKind::HamtShard,
                    other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
                };
                Ok((
                    ResolvedNode {
                        cid: target.cid,
                        kind,
                    },
                    None,
                ))
            }
            _ => {
                let block = self.get_block(&target.cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn find_hamt_link(&self, node: &PbNode, data: &UnixfsData, name: &str) -> Result<Option<Cid>> {
        self.find_hamt_entry(node, data, name)?
            .as_ref()
            .map(link_cid)
            .transpose()
    }

    fn find_hamt_link_target(
        &self,
        node: &PbNode,
        data: &UnixfsData,
        name: &str,
    ) -> Result<Option<LinkTarget>> {
        self.find_hamt_entry(node, data, name)?
            .as_ref()
            .map(link_target)
            .transpose()
    }

    /// `name`'s link in the HAMT whose root shard is `node`, found as the
    /// UnixFS spec's HAMT lookup does (and boxo's): the name's
    /// murmur3-x64-64 hash, read from its most significant end, gives each
    /// shard log2(fanout) bits, its bucket. The shard's bitfield says
    /// whether the bucket is occupied, and its link is the one at the
    /// bucket's rank among the occupied buckets. That link's name starts
    /// with the bucket's index in uppercase hex, as many digits as
    /// fanout - 1 has: named by the index alone, it is the next level's
    /// shard; by the index and `name`, the entry. An unoccupied bucket, or
    /// a link named otherwise, means `name` isn't in the directory. A
    /// lookup reads one shard a level, until the 64-bit hash has too few
    /// bits left for a bucket: at most 21 levels, at fanout 8.
    fn find_hamt_entry(
        &self,
        node: &PbNode,
        data: &UnixfsData,
        name: &str,
    ) -> Result<Option<PbLink>> {
        let hash = murmur3_x64_64(name.as_bytes());
        let mut used = 0;
        let mut next: Option<DecodedDagPb> = None;
        loop {
            let (links, data) = next.as_ref().map_or((&node.links, data), |shard| {
                (&shard.node.links, &shard.data)
            });
            let shard = validate_hamt(data)?;
            if used + shard.bits > u64::BITS {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT deeper than its 64-bit hash".into(),
                ));
            }
            let bucket = (hash << used >> (u64::BITS - shard.bits)) as usize;
            used += shard.bits;
            if !shard.occupied(bucket) {
                return Ok(None);
            }
            let link = links.get(shard.rank(bucket)).ok_or_else(|| {
                UnixfsError::InvalidDagPb("HAMT bitfield has more buckets than links".into())
            })?;
            // boxo takes the prefix's length and doesn't compare it; a link
            // at this rank under another bucket's prefix is malformed, and
            // the name is absent (as freedom-core's reader has it).
            let prefix = format!("{bucket:0width$X}", width = shard.prefix_len);
            let Some(rest) = link
                .name
                .as_deref()
                .and_then(|link_name| link_name.strip_prefix(prefix.as_str()))
            else {
                return Ok(None);
            };
            if !rest.is_empty() {
                return Ok((rest == name).then(|| link.clone()));
            }
            let cid = link_cid(link)?;
            if cid.codec() != CODEC_DAG_PB {
                return Err(UnixfsError::NotDirectory);
            }
            let shard = self.dag_pb(&cid)?;
            if shard.kind != DataType::HamtShard {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT bucket link did not resolve to a HAMT shard".into(),
                ));
            }
            next = Some(shard);
        }
    }
}

fn directory_entries(links: &[PbLink]) -> Result<Vec<DirectoryEntry>> {
    let mut entries = Vec::new();
    for link in links {
        let Some(name) = link.name.as_ref().filter(|name| !name.is_empty()) else {
            continue;
        };
        entries.push(DirectoryEntry {
            name: name.clone(),
            cid: link_cid(link)?,
            size: link.tsize,
        });
    }
    Ok(entries)
}

fn collect_hamt_entries(
    links: &[PbLink],
    prefix_len: usize,
    entries: &mut Vec<DirectoryEntry>,
    pending: &mut Vec<Cid>,
) -> Result<()> {
    for link in links {
        let Some(link_name) = link.name.as_deref() else {
            continue;
        };
        let link_name = link_name.as_bytes();
        if link_name.len() == prefix_len {
            pending.push(link_cid(link)?);
        } else if link_name.len() > prefix_len {
            entries.push(DirectoryEntry {
                name: String::from_utf8_lossy(&link_name[prefix_len..]).to_string(),
                cid: link_cid(link)?,
                size: link.tsize,
            });
        }
    }
    Ok(())
}

fn append_intersection(out: &mut Vec<u8>, bytes: &[u8], offset: u64, start: u64, end: u64) {
    if bytes.is_empty() {
        return;
    }
    let block_end = offset.saturating_add(bytes.len() as u64 - 1);
    if !ranges_intersect(offset, block_end, start, end) {
        return;
    }
    out.extend_from_slice(&slice_bytes(
        bytes,
        start.saturating_sub(offset),
        end.min(block_end).saturating_sub(offset),
    ));
}

fn slice_bytes(bytes: &[u8], start: u64, end: u64) -> Vec<u8> {
    if bytes.is_empty() || start > end || start >= bytes.len() as u64 {
        return Vec::new();
    }
    let start = start.min(bytes.len() as u64) as usize;
    let end = end.min(bytes.len() as u64 - 1) as usize;
    bytes[start..=end].to_vec()
}

fn ranges_intersect(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    a_start <= b_end && b_start <= a_end
}

fn decode_pb_node(data: &[u8]) -> Result<PbNode> {
    PbNode::decode(data).map_err(|err| UnixfsError::InvalidDagPb(err.to_string()))
}

fn decode_unixfs_data(node: &PbNode) -> Result<UnixfsData> {
    let data = node
        .data
        .as_deref()
        .ok_or_else(|| UnixfsError::InvalidDagPb("missing UnixFS data".into()))?;
    UnixfsData::decode(data).map_err(|err| UnixfsError::InvalidDagPb(err.to_string()))
}

fn data_type(data: &UnixfsData) -> Result<DataType> {
    let value = data.r#type.unwrap_or(DataType::Raw as i32);
    DataType::try_from(value).map_err(|_| UnixfsError::UnsupportedNodeType(value))
}

fn find_link(node: &PbNode, name: &str) -> Result<Option<Cid>> {
    node.links
        .iter()
        .find(|link| link.name.as_deref() == Some(name))
        .map(link_cid)
        .transpose()
}

fn find_link_target(node: &PbNode, name: &str) -> Result<Option<LinkTarget>> {
    node.links
        .iter()
        .find(|link| link.name.as_deref() == Some(name))
        .map(link_target)
        .transpose()
}

/// A HAMT shard's layout, read from its UnixFS data as boxo's `makeShard`
/// reads it.
struct HamtLayout<'a> {
    /// The hash bits a bucket takes: log2(fanout).
    bits: u32,
    /// The hex digits of a link's bucket prefix: as many as fanout - 1 has.
    prefix_len: usize,
    /// The occupied buckets: a big-endian integer whose bit `i` is bucket
    /// `i`, its leading zero bytes possibly left out.
    bitfield: &'a [u8],
}

impl HamtLayout<'_> {
    fn occupied(&self, bucket: usize) -> bool {
        let bytes = self.bitfield.len();
        bucket / 8 < bytes && self.bitfield[bytes - 1 - bucket / 8] >> (bucket % 8) & 1 == 1
    }

    /// The occupied buckets below `bucket`: the index of its link.
    fn rank(&self, bucket: usize) -> usize {
        let bytes = self.bitfield.len();
        let whole = (bucket / 8).min(bytes);
        let mut rank: u32 = self.bitfield[bytes - whole..]
            .iter()
            .map(|byte| byte.count_ones())
            .sum();
        if bucket / 8 < bytes {
            let below = (1u8 << (bucket % 8)) - 1;
            rank += (self.bitfield[bytes - 1 - bucket / 8] & below).count_ones();
        }
        rank as usize
    }
}

/// A HAMT shard's layout, or an error when boxo wouldn't read it: a hash
/// other than murmur3-x64-64, a fanout other than a power of two from 8 to
/// 1024, a bitfield wider than fanout / 8 bytes, or file fields.
fn validate_hamt(data: &UnixfsData) -> Result<HamtLayout<'_>> {
    let fanout = data
        .fanout
        .filter(|fanout| fanout.is_power_of_two() && HAMT_FANOUTS.contains(fanout));
    let (Some(HAMT_MURMUR3_X64_64), Some(fanout)) = (data.hash_type, fanout) else {
        return Err(UnixfsError::InvalidDagPb(format!(
            "unsupported HAMT parameters hashType={:?} fanout={:?}",
            data.hash_type, data.fanout
        )));
    };
    if data.filesize.is_some() || !data.blocksizes.is_empty() {
        return Err(UnixfsError::InvalidDagPb(
            "HAMT shard carried file-only UnixFS fields".into(),
        ));
    }
    let bitfield = data.data.as_deref().unwrap_or_default();
    if bitfield.len() as u64 > fanout / 8 {
        return Err(UnixfsError::InvalidDagPb(format!(
            "HAMT bitfield of {} bytes, more than fanout {fanout}'s {}",
            bitfield.len(),
            fanout / 8
        )));
    }
    Ok(HamtLayout {
        bits: fanout.trailing_zeros(),
        prefix_len: format!("{:X}", fanout - 1).len(),
        bitfield,
    })
}

/// MurmurHash3 x64 128 (Austin Appleby's), its first 64 bits (`h1`): the
/// UnixFS HAMT's hash function, multicodec murmur3-x64-64 (0x22), as
/// go-unixfs and boxo take it from `murmur3.Sum64`.
fn murmur3_x64_64(key: &[u8]) -> u64 {
    murmur3_x64_128(key, 0).0
}

/// MurmurHash3 x64 128 of `key` with `seed`: `(h1, h2)`.
fn murmur3_x64_128(key: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let mix_k1 = |k1: u64| k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    let mix_k2 = |k2: u64| k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
    let (mut h1, mut h2) = (u64::from(seed), u64::from(seed));
    let mut blocks = key.chunks_exact(16);
    for block in &mut blocks {
        let (low, high) = block.split_at(8);
        let k1 = u64::from_le_bytes(low.try_into().unwrap_or_default());
        let k2 = u64::from_le_bytes(high.try_into().unwrap_or_default());
        h1 ^= mix_k1(k1);
        h1 = h1
            .rotate_left(27)
            .wrapping_add(h2)
            .wrapping_mul(5)
            .wrapping_add(0x52dc_e729);
        h2 ^= mix_k2(k2);
        h2 = h2
            .rotate_left(31)
            .wrapping_add(h1)
            .wrapping_mul(5)
            .wrapping_add(0x3849_5ab5);
    }
    let tail = blocks.remainder();
    let (mut k1, mut k2) = (0u64, 0u64);
    for (index, &byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= u64::from(byte) << (8 * index);
        } else {
            k2 |= u64::from(byte) << (8 * (index - 8));
        }
    }
    if tail.len() > 8 {
        h2 ^= mix_k2(k2);
    }
    if !tail.is_empty() {
        h1 ^= mix_k1(k1);
    }
    let len = key.len() as u64;
    h1 ^= len;
    h2 ^= len;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

fn link_target(link: &PbLink) -> Result<LinkTarget> {
    Ok(LinkTarget {
        cid: link_cid(link)?,
        size: link.tsize,
    })
}

fn link_cid(link: &PbLink) -> Result<Cid> {
    let bytes = link
        .hash
        .as_deref()
        .ok_or_else(|| UnixfsError::InvalidDagPb("link is missing hash".into()))?;

    let mut cursor = Cursor::new(bytes);
    if let Ok(cid) = Cid::read_bytes(&mut cursor) {
        if cursor.position() as usize == bytes.len() {
            return Ok(cid);
        }
    }

    let mh = Multihash::<64>::from_bytes(bytes)
        .map_err(|err| UnixfsError::InvalidDagPb(format!("invalid link multihash: {err}")))?;
    Ok(Cid::new_v1(CODEC_DAG_PB, mh))
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{
        block_data_range, cid_from_data, Block, Result as CoreResult, CODEC_DAG_PB, CODEC_RAW,
    };
    use freedom_ipfs_store::SqliteBlockStore;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn unixfs_data(kind: DataType, data: &[u8]) -> Vec<u8> {
        UnixfsData {
            r#type: Some(kind as i32),
            data: Some(data.to_vec()),
            filesize: Some(data.len() as u64),
            blocksizes: Vec::new(),
            hash_type: None,
            fanout: None,
        }
        .encode_to_vec()
    }

    fn pb_file(data: &[u8], links: Vec<PbLink>) -> Vec<u8> {
        pb_file_with_metadata(data, links, data.len() as u64, Vec::new())
    }

    fn pb_file_with_metadata(
        data: &[u8],
        links: Vec<PbLink>,
        filesize: u64,
        blocksizes: Vec<u64>,
    ) -> Vec<u8> {
        PbNode {
            data: Some(
                UnixfsData {
                    r#type: Some(DataType::File as i32),
                    data: Some(data.to_vec()),
                    filesize: Some(filesize),
                    blocksizes,
                    hash_type: None,
                    fanout: None,
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    fn pb_directory(links: Vec<PbLink>) -> Vec<u8> {
        PbNode {
            data: Some(unixfs_data(DataType::Directory, &[])),
            links,
        }
        .encode_to_vec()
    }

    fn pb_hamt(links: Vec<PbLink>) -> Vec<u8> {
        pb_hamt_at(256, links)
    }

    /// A HAMT shard at `fanout` as boxo writes one: each link's bucket read
    /// from its name's hex prefix, the links in bucket order, and a
    /// bitfield of exactly their buckets, its leading zero bytes left out.
    fn pb_hamt_at(fanout: u64, mut links: Vec<PbLink>) -> Vec<u8> {
        let width = format!("{:X}", fanout - 1).len();
        let bucket_of = |link: &PbLink| {
            usize::from_str_radix(&link.name.as_deref().unwrap()[..width], 16).unwrap()
        };
        links.sort_by_key(bucket_of);
        let buckets: Vec<usize> = links.iter().map(bucket_of).collect();
        let mut bitfield = hamt_bitfield(&buckets, (fanout / 8) as usize);
        let zeros = bitfield.iter().take_while(|&&byte| byte == 0).count();
        bitfield.drain(..zeros);
        hamt_shard(fanout, bitfield, links)
    }

    /// A HAMT shard at `fanout` with exactly this bitfield and these links.
    fn hamt_shard(fanout: u64, bitfield: Vec<u8>, links: Vec<PbLink>) -> Vec<u8> {
        PbNode {
            data: Some(
                UnixfsData {
                    r#type: Some(DataType::HamtShard as i32),
                    data: Some(bitfield),
                    filesize: None,
                    blocksizes: Vec::new(),
                    hash_type: Some(HAMT_MURMUR3_X64_64),
                    fanout: Some(fanout),
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    /// A `bytes`-byte HAMT bitfield with these buckets set: a big-endian
    /// integer whose bit `i` is bucket `i`.
    fn hamt_bitfield(buckets: &[usize], bytes: usize) -> Vec<u8> {
        let mut bitfield = vec![0; bytes];
        for &bucket in buckets {
            bitfield[bytes - 1 - bucket / 8] |= 1 << (bucket % 8);
        }
        bitfield
    }

    fn link(name: &str, cid: &Cid) -> PbLink {
        PbLink {
            hash: Some(cid.to_bytes()),
            name: Some(name.to_string()),
            tsize: None,
        }
    }

    type RangeBatchCalls = Arc<Mutex<Vec<Vec<(Cid, u64, u64)>>>>;

    #[derive(Clone)]
    struct CountingProvider {
        blocks: Arc<HashMap<Cid, Vec<u8>>>,
        calls: Arc<Mutex<HashMap<Cid, usize>>>,
        range_calls: Arc<Mutex<HashMap<Cid, usize>>>,
        range_batch_calls: RangeBatchCalls,
    }

    impl CountingProvider {
        fn new(blocks: HashMap<Cid, Vec<u8>>) -> Self {
            Self {
                blocks: Arc::new(blocks),
                calls: Arc::new(Mutex::new(HashMap::new())),
                range_calls: Arc::new(Mutex::new(HashMap::new())),
                range_batch_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn call_count(&self, cid: &Cid) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(cid)
                .unwrap_or(&0)
        }

        fn range_call_count(&self, cid: &Cid) -> usize {
            *self
                .range_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(cid)
                .unwrap_or(&0)
        }

        fn range_batch_calls(&self) -> Vec<Vec<(Cid, u64, u64)>> {
            self.range_batch_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl BlockProvider for CountingProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            if let Some(data) = self.blocks.get(cid) {
                let mut calls = self
                    .calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls.entry(*cid).or_default() += 1;
                return Ok(Some(Block::unchecked(*cid, data.clone())));
            }
            Ok(None)
        }

        fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
            if let Some(data) = self.blocks.get(cid) {
                let mut calls = self
                    .range_calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls.entry(*cid).or_default() += 1;
                return Ok(Some(block_data_range(data, start, end)));
            }
            Ok(None)
        }

        fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> CoreResult<Vec<Option<Vec<u8>>>> {
            self.range_batch_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ranges.to_vec());
            ranges
                .iter()
                .map(|(cid, start, end)| self.get_block_range(cid, *start, *end))
                .collect()
        }
    }

    #[test]
    fn reads_raw_block_as_file() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"raw leaf";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        assert_eq!(read_file(&store, &cid, "").unwrap(), data);
    }

    #[test]
    fn resolves_directory_and_reads_linked_file() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let leaf_data = b"linked bytes";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        store.put_block(&leaf_cid, leaf_data).unwrap();

        let file_data = pb_file(b"prefix ", vec![link("leaf", &leaf_cid)]);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        store.put_block(&file_cid, &file_data).unwrap();

        let dir_data = pb_directory(vec![link("index.html", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        store.put_block(&dir_cid, &dir_data).unwrap();

        assert_eq!(
            read_file(&store, &dir_cid, "index.html").unwrap(),
            b"prefix linked bytes"
        );
    }

    #[test]
    fn lists_directory_entries_without_reading_child_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let alpha_data = b"alpha";
        let alpha_cid = cid_from_data(CODEC_RAW, alpha_data);
        let beta_data = b"beta";
        let beta_cid = cid_from_data(CODEC_RAW, beta_data);

        let dir_data = pb_directory(vec![
            PbLink {
                tsize: Some(5),
                ..link("beta.txt", &beta_cid)
            },
            PbLink {
                tsize: Some(4),
                ..link("alpha.txt", &alpha_cid)
            },
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        store.put_block(&dir_cid, &dir_data).unwrap();

        let entries = list_directory(&store, &dir_cid, "").unwrap();

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.cid, entry.size))
                .collect::<Vec<_>>(),
            vec![
                ("alpha.txt", alpha_cid, Some(4)),
                ("beta.txt", beta_cid, Some(5))
            ]
        );
    }

    #[test]
    fn raw_link_tsize_hint_resolves_without_reading_raw_leaf() {
        let leaf_data = b"linked raw leaf";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        let dir_data = pb_directory(vec![PbLink {
            tsize: Some(leaf_data.len() as u64),
            ..link("app.js", &leaf_cid)
        }]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        let provider = CountingProvider::new(HashMap::from([
            (leaf_cid, leaf_data.to_vec()),
            (dir_cid, dir_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        let (resolved, size_hint) = resolver
            .resolve_path_with_raw_link_tsize_hint(&provider, &dir_cid, "app.js")
            .unwrap();

        assert_eq!(resolved.cid, leaf_cid);
        assert_eq!(resolved.kind, NodeKind::Raw);
        assert_eq!(size_hint, Some(leaf_data.len() as u64));
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&leaf_cid), 0);
        assert_eq!(
            resolver
                .read_file_cid_range(&provider, &leaf_cid, 0, 5)
                .unwrap(),
            b"linked"
        );
        assert_eq!(provider.range_call_count(&leaf_cid), 1);
    }

    #[test]
    fn cached_resolver_reuses_directory_metadata_across_sibling_paths() {
        let alpha_data = b"alpha";
        let alpha_cid = cid_from_data(CODEC_RAW, alpha_data);
        let beta_data = b"beta";
        let beta_cid = cid_from_data(CODEC_RAW, beta_data);

        let dir_data = pb_directory(vec![
            link("alpha.txt", &alpha_cid),
            link("beta.txt", &beta_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider = CountingProvider::new(HashMap::from([
            (alpha_cid, alpha_data.to_vec()),
            (beta_cid, beta_data.to_vec()),
            (dir_cid, dir_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver
                .file_size(&provider, &dir_cid, "alpha.txt")
                .unwrap(),
            alpha_data.len() as u64
        );
        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "beta.txt").unwrap(),
            beta_data.len() as u64
        );

        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(
            resolver.metadata_cache_stats(),
            UnixfsMetadataCacheStats {
                capacity: 8,
                len: 1,
                hits: 1,
                misses: 1,
                inserts: 1,
                evictions: 0,
                oversized_skips: 0,
                path_len: 2,
                path_hits: 0,
                path_misses: 2,
                path_inserts: 2,
                path_evictions: 0,
                path_oversized_skips: 0,
                file_size_len: 2,
                file_size_hits: 0,
                file_size_misses: 2,
                file_size_inserts: 2,
                file_size_evictions: 0,
            }
        );
    }

    #[test]
    fn cached_resolver_reuses_file_metadata_across_ranges() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);

        let provider = CountingProvider::new(HashMap::from([(file_cid, file_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver
                .read_file_range(&provider, &file_cid, "", 0, 1)
                .unwrap(),
            b"ab"
        );
        assert_eq!(
            resolver
                .read_file_range(&provider, &file_cid, "", 2, 4)
                .unwrap(),
            b"cde"
        );

        assert_eq!(provider.call_count(&file_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.path_misses, 1);
        assert_eq!(stats.path_inserts, 1);
        assert_eq!(stats.path_hits, 1);
    }

    #[test]
    fn cached_resolver_reuses_raw_file_sizes() {
        let data = b"raw file size";
        let cid = cid_from_data(CODEC_RAW, data);
        let provider = CountingProvider::new(HashMap::from([(cid, data.to_vec())]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(resolver.file_size(&provider, &cid, "").unwrap(), 13);
        assert_eq!(resolver.file_size(&provider, &cid, "").unwrap(), 13);

        assert_eq!(provider.call_count(&cid), 2);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_misses, 1);
        assert_eq!(stats.file_size_inserts, 1);
        assert_eq!(stats.file_size_hits, 1);
    }

    #[test]
    fn cached_resolver_reuses_path_resolution_across_repeated_reads() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let dir_data = pb_directory(vec![link("file.txt", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider =
            CountingProvider::new(HashMap::from([(file_cid, file_data), (dir_cid, dir_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );
        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );

        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.path_len, 1);
        assert_eq!(stats.path_misses, 1);
        assert_eq!(stats.path_inserts, 1);
        assert_eq!(stats.path_hits, 1);
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_misses, 1);
        assert_eq!(stats.file_size_inserts, 1);
        assert_eq!(stats.file_size_hits, 1);
    }

    #[test]
    fn cid_direct_range_reads_skip_path_resolution_cache() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let dir_data = pb_directory(vec![link("file.txt", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider =
            CountingProvider::new(HashMap::from([(file_cid, file_data), (dir_cid, dir_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );
        let before = resolver.metadata_cache_stats();

        assert_eq!(
            resolver
                .read_file_cid_range(&provider, &file_cid, 1, 3)
                .unwrap(),
            b"bcd"
        );

        let after = resolver.metadata_cache_stats();
        assert_eq!(after.path_hits, before.path_hits);
        assert_eq!(after.path_misses, before.path_misses);
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
    }

    #[test]
    fn raw_cid_range_uses_provider_range_read() {
        let data = b"0123456789".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let provider = CountingProvider::new(HashMap::from([(cid, data)]));
        let resolver = UnixfsResolver::default();

        assert_eq!(
            resolver.read_file_cid_range(&provider, &cid, 2, 5).unwrap(),
            b"2345"
        );

        assert_eq!(provider.call_count(&cid), 0);
        assert_eq!(provider.range_call_count(&cid), 1);
    }

    #[test]
    fn file_range_batches_adjacent_raw_child_ranges() {
        let first_data = b"abcdef";
        let first_cid = cid_from_data(CODEC_RAW, first_data);
        let second_data = b"ghijkl";
        let second_cid = cid_from_data(CODEC_RAW, second_data);
        let third_data = b"mnopqr";
        let third_cid = cid_from_data(CODEC_RAW, third_data);
        let file_data = pb_file_with_metadata(
            b"",
            vec![
                link("first", &first_cid),
                link("second", &second_cid),
                link("third", &third_cid),
            ],
            (first_data.len() + second_data.len() + third_data.len()) as u64,
            vec![
                first_data.len() as u64,
                second_data.len() as u64,
                third_data.len() as u64,
            ],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let provider = CountingProvider::new(HashMap::from([
            (first_cid, first_data.to_vec()),
            (second_cid, second_data.to_vec()),
            (third_cid, third_data.to_vec()),
            (file_cid, file_data),
        ]));

        assert_eq!(
            read_file_range(&provider, &file_cid, "", 4, 9).unwrap(),
            b"efghij"
        );

        assert_eq!(
            provider.range_batch_calls(),
            vec![vec![(first_cid, 4, 5), (second_cid, 0, 3)]]
        );
    }

    #[test]
    fn metadata_cache_evicts_when_capacity_is_exceeded() {
        let first_data = pb_file(b"first", Vec::new());
        let first_cid = cid_from_data(CODEC_DAG_PB, &first_data);
        let second_data = pb_file(b"second", Vec::new());
        let second_cid = cid_from_data(CODEC_DAG_PB, &second_data);

        let provider = CountingProvider::new(HashMap::from([
            (first_cid, first_data),
            (second_cid, second_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(1);

        assert_eq!(resolver.file_size(&provider, &first_cid, "").unwrap(), 5);
        assert_eq!(resolver.file_size(&provider, &second_cid, "").unwrap(), 6);
        assert_eq!(resolver.file_size(&provider, &first_cid, "").unwrap(), 5);

        assert_eq!(provider.call_count(&first_cid), 2);
        assert_eq!(provider.call_count(&second_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.capacity, 1);
        assert_eq!(stats.len, 1);
        assert_eq!(stats.evictions, 2);
        assert_eq!(stats.path_len, 1);
        assert_eq!(stats.path_evictions, 2);
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_evictions, 2);
    }

    #[test]
    fn reads_file_range_across_inline_and_linked_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let leaf_data = b"linked bytes";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        store.put_block(&leaf_cid, leaf_data).unwrap();

        let file_data = pb_file_with_metadata(
            b"prefix ",
            vec![link("leaf", &leaf_cid)],
            19,
            vec![leaf_data.len() as u64],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        store.put_block(&file_cid, &file_data).unwrap();

        assert_eq!(file_size(&store, &file_cid, "").unwrap(), 19);
        assert_eq!(
            read_file_range(&store, &file_cid, "", 3, 12).unwrap(),
            b"fix linked"
        );
    }

    /// `name`'s bucket at `level` of a HAMT: two uppercase hex digits of
    /// its hash's byte `level`, most significant first.
    fn bucket(name: &str, level: usize) -> String {
        format!(
            "{:02X}",
            murmur3_x64_64(name.as_bytes()).to_be_bytes()[level]
        )
    }

    /// `count` bits of `name`'s hash from bit `from`, most significant
    /// first, read off the hash written out in binary.
    fn hash_bits(name: &str, from: usize, count: usize) -> usize {
        let binary = format!("{:064b}", murmur3_x64_64(name.as_bytes()));
        usize::from_str_radix(&binary[from..from + count], 2).unwrap()
    }

    /// `name`'s link prefix at `level` of a HAMT at `fanout`: its bucket,
    /// the level's log2(fanout) bits of its hash, in as many uppercase hex
    /// digits as fanout - 1 has.
    fn prefix_at(name: &str, fanout: u64, level: usize) -> String {
        let bits = fanout.trailing_zeros() as usize;
        let width = format!("{:X}", fanout - 1).len();
        format!("{:0width$X}", hash_bits(name, level * bits, bits))
    }

    /// A chain of HAMT shards at `fanout` along `name`'s buckets at levels
    /// 0 to `levels - 1`, each linking the next, the last linking `bottom`;
    /// the root's CID (`bottom`'s, if `levels` is 0).
    fn hamt_chain(
        blocks: &mut HashMap<Cid, Vec<u8>>,
        fanout: u64,
        name: &str,
        levels: usize,
        bottom: Vec<u8>,
    ) -> Cid {
        let mut block = bottom;
        for level in (0..levels).rev() {
            let cid = cid_from_data(CODEC_DAG_PB, &block);
            blocks.insert(cid, block);
            block = pb_hamt_at(fanout, vec![link(&prefix_at(name, fanout, level), &cid)]);
        }
        let root = cid_from_data(CODEC_DAG_PB, &block);
        blocks.insert(root, block);
        root
    }

    /// Every fanout boxo reads, the powers of two from 8 to 1024: a name's
    /// bucket is the next log2(fanout) bits of its hash at each level, a
    /// link's prefix has as many hex digits as fanout - 1, and a HAMT is
    /// read as deep as the hash has whole buckets for, and no deeper.
    #[test]
    fn hamts_of_every_fanout_from_8_to_1024_are_read() {
        let file = cid_from_data(CODEC_RAW, b"x");
        let name = "entry.txt";
        for (fanout, width, levels) in [
            (8, 1, 21),
            (16, 1, 16),
            (32, 2, 12),
            (64, 2, 10),
            (128, 2, 9),
            (256, 2, 8),
            (512, 3, 7),
            (1024, 3, 6),
        ] {
            assert_eq!(prefix_at(name, fanout, 0).len(), width, "fanout {fanout}");
            let entry_at = |level| {
                let prefix = prefix_at(name, fanout, level);
                pb_hamt_at(fanout, vec![link(&format!("{prefix}{name}"), &file)])
            };
            let mut blocks = HashMap::from([(file, b"x".to_vec())]);
            let one = hamt_chain(&mut blocks, fanout, name, 0, entry_at(0));
            let two = hamt_chain(&mut blocks, fanout, name, 1, entry_at(1));
            // The entry at the last level with a whole bucket, and a shard
            // one level below it.
            let deepest = hamt_chain(&mut blocks, fanout, name, levels - 1, entry_at(levels - 1));
            let empty = pb_hamt_at(fanout, Vec::new());
            let too_deep = hamt_chain(&mut blocks, fanout, name, levels, empty);
            let provider = CountingProvider::new(blocks);

            for root in [one, two, deepest] {
                assert_eq!(
                    read_file(&provider, &root, name).unwrap(),
                    b"x",
                    "fanout {fanout}"
                );
            }
            assert_eq!(
                read_file(&provider, &too_deep, name)
                    .unwrap_err()
                    .to_string(),
                "invalid dag-pb: HAMT deeper than its 64-bit hash",
                "fanout {fanout}"
            );
            assert!(
                matches!(
                    read_file(&provider, &two, "absent.txt"),
                    Err(UnixfsError::PathNotFound(_))
                ),
                "fanout {fanout}"
            );
            let listed = list_directory(&provider, &two, "").unwrap();
            assert_eq!(
                listed
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>(),
                [name],
                "fanout {fanout}"
            );
        }

        // Each shard is read at its own fanout: a root at 8 takes 3 bits,
        // its shard at 1024 the next 10.
        let child = pb_hamt_at(
            1024,
            vec![link(
                &format!("{:03X}{name}", hash_bits(name, 3, 10)),
                &file,
            )],
        );
        let child_cid = cid_from_data(CODEC_DAG_PB, &child);
        let root = pb_hamt_at(8, vec![link(&prefix_at(name, 8, 0), &child_cid)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root);
        let provider = CountingProvider::new(HashMap::from([
            (root_cid, root),
            (child_cid, child),
            (file, b"x".to_vec()),
        ]));
        assert_eq!(read_file(&provider, &root_cid, name).unwrap(), b"x");

        // Any other fanout, or no fanout, or another hash, is refused.
        let unsupported = |fanout: Option<u64>, hash_type: Option<u64>| {
            let shard = PbNode {
                data: Some(
                    UnixfsData {
                        r#type: Some(DataType::HamtShard as i32),
                        data: None,
                        filesize: None,
                        blocksizes: Vec::new(),
                        hash_type,
                        fanout,
                    }
                    .encode_to_vec(),
                ),
                links: Vec::new(),
            }
            .encode_to_vec();
            let cid = cid_from_data(CODEC_DAG_PB, &shard);
            let provider = CountingProvider::new(HashMap::from([(cid, shard)]));
            let refused = format!(
                "invalid dag-pb: unsupported HAMT parameters hashType={hash_type:?} fanout={fanout:?}"
            );
            assert_eq!(
                read_file(&provider, &cid, name).unwrap_err().to_string(),
                refused
            );
            assert_eq!(
                list_directory(&provider, &cid, "").unwrap_err().to_string(),
                refused
            );
        };
        for fanout in [
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(4),
            Some(12),
            Some(255),
            Some(2048),
        ] {
            unsupported(fanout, Some(HAMT_MURMUR3_X64_64));
        }
        unsupported(Some(256), Some(0x12));
        unsupported(Some(256), None);
    }

    /// The shard's bitfield decides, as boxo's does: an unoccupied bucket
    /// is absent whatever the links say, the bucket's link is the one at
    /// its rank among the occupied buckets, and a bitfield wider than
    /// fanout / 8 bytes, or naming more buckets than there are links, is
    /// refused. Leading zero bytes may be left out.
    #[test]
    fn a_hamt_lookup_follows_the_shards_bitfield() {
        let file = cid_from_data(CODEC_RAW, b"x");
        let other_file = cid_from_data(CODEC_RAW, b"y");
        let name = "entry.txt";
        let bucket = hash_bits(name, 0, 8);
        assert!(bucket > 0, "the fixture needs a bucket above 0");
        let entry = link(&format!("{bucket:02X}{name}"), &file);
        let read = |bitfield: Vec<u8>, links: Vec<PbLink>| {
            let root = hamt_shard(256, bitfield, links);
            let cid = cid_from_data(CODEC_DAG_PB, &root);
            let provider = CountingProvider::new(HashMap::from([
                (cid, root),
                (file, b"x".to_vec()),
                (other_file, b"y".to_vec()),
            ]));
            read_file(&provider, &cid, name).map_err(|err| err.to_string())
        };
        let absent = Err(format!("path segment not found: {name}"));

        // The bucket's bit set: found, the bitfield at its full 32 bytes or
        // with its leading zero bytes left out.
        assert_eq!(
            read(hamt_bitfield(&[bucket], 32), vec![entry.clone()]),
            Ok(b"x".to_vec())
        );
        assert_eq!(
            read(
                hamt_bitfield(&[bucket], bucket / 8 + 1),
                vec![entry.clone()]
            ),
            Ok(b"x".to_vec())
        );
        // The adjacent bucket's bit instead, or no bitfield: absent, though
        // the link names the entry under its own bucket.
        assert_eq!(
            read(hamt_bitfield(&[bucket ^ 1], 32), vec![entry.clone()]),
            absent
        );
        assert_eq!(read(Vec::new(), vec![entry.clone()]), absent);
        // 33 bytes, even with the extra byte zero: refused.
        assert_eq!(
            read(hamt_bitfield(&[bucket], 33), vec![entry.clone()]),
            Err("invalid dag-pb: HAMT bitfield of 33 bytes, more than fanout 256's 32".into())
        );
        // Every bucket set, and one link: the bucket's rank is past it.
        assert_eq!(
            read(vec![0xff; 32], vec![entry.clone()]),
            Err("invalid dag-pb: HAMT bitfield has more buckets than links".into())
        );

        // Two occupied buckets: the link at the bucket's rank is read, so
        // with the links out of bucket order the entry isn't found.
        let other = bucket ^ 0x80;
        let other_link = link(&format!("{other:02X}other.txt"), &other_file);
        let bitfield = hamt_bitfield(&[bucket, other], 32);
        let (in_order, swapped) = if other < bucket {
            (
                vec![other_link.clone(), entry.clone()],
                vec![entry.clone(), other_link],
            )
        } else {
            (
                vec![entry.clone(), other_link.clone()],
                vec![other_link, entry.clone()],
            )
        };
        assert_eq!(read(bitfield.clone(), in_order), Ok(b"x".to_vec()));
        assert_eq!(read(bitfield, swapped), absent);
    }

    /// SMHasher's verification value for MurmurHash3_x64_128: each prefix
    /// of the bytes 0..256 hashed with seed 256 - its length, then the hash
    /// of those hashes, read as four little-endian bytes.
    #[test]
    fn murmur3_matches_smhashers_verification_value() {
        let mut key = Vec::with_capacity(256);
        let mut hashes = Vec::with_capacity(256 * 16);
        for i in 0..256u32 {
            let (h1, h2) = murmur3_x64_128(&key, 256 - i);
            hashes.extend_from_slice(&h1.to_le_bytes());
            hashes.extend_from_slice(&h2.to_le_bytes());
            key.push(i as u8);
        }
        let (h1, _) = murmur3_x64_128(&hashes, 0);
        assert_eq!(h1 as u32, 0x6384_ba69);
        // The UnixFS HAMT's hash is the first 64 bits, with seed 0.
        assert_eq!(murmur3_x64_64(b"hello"), murmur3_x64_128(b"hello", 0).0);
        assert_eq!(murmur3_x64_64(b""), 0);
    }

    /// Finding 6 of design 0009: a lookup scanned every shard until it met
    /// the name, up to 1,024 of them. It now follows the name's hash: one
    /// shard a level, and no other shard is read.
    #[test]
    fn a_hamt_lookup_reads_only_the_shards_on_its_names_path() {
        let name = "target.txt";
        let file = cid_from_data(CODEC_RAW, b"target");
        let mut blocks = HashMap::from([(file, b"target".to_vec())]);
        let mut put = |block: Vec<u8>| {
            let cid = cid_from_data(CODEC_DAG_PB, &block);
            blocks.insert(cid, block);
            cid
        };
        let path = put(pb_hamt(vec![link(
            &format!("{}{name}", bucket(name, 1)),
            &file,
        )]));
        // Every other bucket of the root holds a shard of its own.
        let mut links = vec![link(&bucket(name, 0), &path)];
        let mut others = Vec::new();
        for index in 0..=255u8 {
            let prefix = format!("{index:02X}");
            if prefix != bucket(name, 0) {
                let other = put(pb_hamt(vec![link(
                    &format!("{prefix}other-{index}"),
                    &file,
                )]));
                others.push(other);
                links.push(link(&prefix, &other));
            }
        }
        let root = put(pb_hamt(links));
        let provider = CountingProvider::new(blocks);

        assert_eq!(read_file(&provider, &root, name).unwrap(), b"target");
        assert_eq!(provider.call_count(&path), 1);
        assert!(others.iter().all(|other| provider.call_count(other) == 0));
        // A name whose bucket holds another entry is absent, read at once.
        assert!(matches!(
            read_file(&provider, &root, "missing.txt"),
            Err(UnixfsError::PathNotFound(_))
        ));
        let read: usize = others.iter().map(|other| provider.call_count(other)).sum();
        assert!(read <= 1, "{read} shards read for one missing name");
    }

    /// An entry in a bucket its name doesn't hash to isn't found (the old
    /// scan found it anywhere), and a HAMT deeper than its 64-bit hash is
    /// refused.
    #[test]
    fn a_hamt_entry_is_found_only_in_its_bucket_and_depth_is_bounded() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let file = cid_from_data(CODEC_RAW, b"x");
        store.put_block(&file, b"x").unwrap();
        let name = "misplaced.txt";
        let wrong = format!(
            "{:02X}",
            murmur3_x64_64(name.as_bytes()).to_be_bytes()[0] ^ 1
        );
        let root = pb_hamt(vec![link(&format!("{wrong}{name}"), &file)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root);
        store.put_block(&root_cid, &root).unwrap();
        assert!(matches!(
            read_file(&store, &root_cid, name),
            Err(UnixfsError::PathNotFound(_))
        ));

        // Shards along every one of the name's eight buckets, and a ninth.
        let name = "deep.txt";
        let mut child = pb_hamt(Vec::new());
        for level in (0..8).rev() {
            let child_cid = cid_from_data(CODEC_DAG_PB, &child);
            store.put_block(&child_cid, &child).unwrap();
            child = pb_hamt(vec![link(&bucket(name, level), &child_cid)]);
        }
        let deep = cid_from_data(CODEC_DAG_PB, &child);
        store.put_block(&deep, &child).unwrap();
        assert_eq!(
            read_file(&store, &deep, name).unwrap_err().to_string(),
            "invalid dag-pb: HAMT deeper than its 64-bit hash"
        );
    }

    #[test]
    fn resolves_single_level_hamt_shard() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hamt index";
        let file_cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&file_cid, data).unwrap();

        let hamt_data = pb_hamt(vec![link(
            &format!("{}index.html", bucket("index.html", 0)),
            &file_cid,
        )]);
        let hamt_cid = cid_from_data(CODEC_DAG_PB, &hamt_data);
        store.put_block(&hamt_cid, &hamt_data).unwrap();

        assert_eq!(read_file(&store, &hamt_cid, "index.html").unwrap(), data);
    }

    #[test]
    fn resolves_nested_hamt_shard() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"nested hamt";
        let file_cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&file_cid, data).unwrap();

        let child_data = pb_hamt(vec![link(
            &format!("{}nested.txt", bucket("nested.txt", 1)),
            &file_cid,
        )]);
        let child_cid = cid_from_data(CODEC_DAG_PB, &child_data);
        store.put_block(&child_cid, &child_data).unwrap();

        let root_data = pb_hamt(vec![link(&bucket("nested.txt", 0), &child_cid)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root_data);
        store.put_block(&root_cid, &root_data).unwrap();

        assert_eq!(read_file(&store, &root_cid, "nested.txt").unwrap(), data);
    }

    #[test]
    fn lists_nested_hamt_shard_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let alpha_cid = cid_from_data(CODEC_RAW, b"alpha");
        let nested_cid = cid_from_data(CODEC_RAW, b"nested");

        let child_data = pb_hamt(vec![link("CDnested.txt", &nested_cid)]);
        let child_cid = cid_from_data(CODEC_DAG_PB, &child_data);
        store.put_block(&child_cid, &child_data).unwrap();

        let root_data = pb_hamt(vec![
            link("AB", &child_cid),
            link("EFalpha.txt", &alpha_cid),
        ]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root_data);
        store.put_block(&root_cid, &root_data).unwrap();

        let entries = list_directory(&store, &root_cid, "").unwrap();

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.cid))
                .collect::<Vec<_>>(),
            vec![("alpha.txt", alpha_cid), ("nested.txt", nested_cid)]
        );
    }
}
