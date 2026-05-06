use cid::Cid;
use freedom_ipfs_core::{BlockProvider, CODEC_DAG_PB, CODEC_RAW};
use multihash::Multihash;
use prost::Message;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use thiserror::Error;

const HAMT_MURMUR3_X64_64: u64 = 0x22;
const HAMT_FANOUT_256: u64 = 256;
const HAMT_LINK_PREFIX_LEN: usize = 2;
const HAMT_MAX_SHARDS_VISITED: usize = 1024;
const MAX_CACHED_DIRECTORY_ENTRIES: usize = 256;
const MAX_CACHED_PATH_SEGMENT_BYTES: usize = 256;

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

#[derive(Debug)]
pub struct UnixfsPathCache {
    inner: Mutex<UnixfsPathCacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnixfsPathCacheStats {
    pub capacity: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
}

impl UnixfsPathCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(UnixfsPathCacheInner::new(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |inner| inner.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.clear();
        }
    }

    pub fn stats(&self) -> UnixfsPathCacheStats {
        let (capacity, entries) = self
            .inner
            .lock()
            .map(|inner| (inner.capacity(), inner.len()))
            .unwrap_or_default();
        UnixfsPathCacheStats {
            capacity,
            entries,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    fn get_kind(&self, cid: &Cid) -> Option<NodeKind> {
        self.get(&UnixfsPathCacheKey::NodeKind(*cid))
            .and_then(|entry| match entry {
                UnixfsPathCacheEntry::NodeKind(kind) => Some(kind),
                _ => None,
            })
    }

    fn insert_kind(&self, cid: Cid, kind: NodeKind) {
        self.insert(
            UnixfsPathCacheKey::NodeKind(cid),
            UnixfsPathCacheEntry::NodeKind(kind),
        );
    }

    fn get_file_size(&self, cid: &Cid) -> Option<u64> {
        self.get(&UnixfsPathCacheKey::FileSize(*cid))
            .and_then(|entry| match entry {
                UnixfsPathCacheEntry::FileSize(size) => Some(size),
                _ => None,
            })
    }

    fn insert_file_size(&self, cid: Cid, size: u64) {
        self.insert(
            UnixfsPathCacheKey::FileSize(cid),
            UnixfsPathCacheEntry::FileSize(size),
        );
    }

    fn get_directory_link(&self, directory: &Cid, name: &str) -> Option<Cid> {
        if name.len() > MAX_CACHED_PATH_SEGMENT_BYTES {
            return None;
        }
        self.get(&UnixfsPathCacheKey::DirectoryLink {
            directory: *directory,
            name: name.to_string(),
        })
        .and_then(|entry| match entry {
            UnixfsPathCacheEntry::DirectoryLink(cid) => Some(cid),
            _ => None,
        })
    }

    fn insert_directory_link(&self, directory: Cid, name: String, cid: Cid) {
        if name.len() > MAX_CACHED_PATH_SEGMENT_BYTES {
            return;
        }
        self.insert(
            UnixfsPathCacheKey::DirectoryLink { directory, name },
            UnixfsPathCacheEntry::DirectoryLink(cid),
        );
    }

    fn get_directory_entries(&self, cid: &Cid) -> Option<Vec<DirectoryEntry>> {
        self.get(&UnixfsPathCacheKey::DirectoryEntries(*cid))
            .and_then(|entry| match entry {
                UnixfsPathCacheEntry::DirectoryEntries(entries) => Some(entries),
                _ => None,
            })
    }

    fn insert_directory_entries(&self, cid: Cid, entries: Vec<DirectoryEntry>) {
        if entries.len() > MAX_CACHED_DIRECTORY_ENTRIES {
            return;
        }
        self.insert(
            UnixfsPathCacheKey::DirectoryEntries(cid),
            UnixfsPathCacheEntry::DirectoryEntries(entries),
        );
    }

    fn get(&self, key: &UnixfsPathCacheKey) -> Option<UnixfsPathCacheEntry> {
        let entry = self.inner.lock().ok()?.get(key);
        if entry.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        entry
    }

    fn insert(&self, key: UnixfsPathCacheKey, entry: UnixfsPathCacheEntry) {
        if let Ok(mut inner) = self.inner.lock() {
            let stats = inner.insert(key, entry);
            if stats.inserted {
                self.inserts.fetch_add(1, Ordering::Relaxed);
            }
            if stats.evictions > 0 {
                self.evictions
                    .fetch_add(stats.evictions as u64, Ordering::Relaxed);
            }
        }
    }
}

impl Default for UnixfsPathCache {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[derive(Debug)]
struct UnixfsPathCacheInner {
    capacity: usize,
    entries: HashMap<UnixfsPathCacheKey, UnixfsPathCacheEntry>,
    order: VecDeque<UnixfsPathCacheKey>,
}

impl UnixfsPathCacheInner {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    fn get(&self, key: &UnixfsPathCacheKey) -> Option<UnixfsPathCacheEntry> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: UnixfsPathCacheKey, entry: UnixfsPathCacheEntry) -> CacheInsertStats {
        if self.capacity == 0 {
            return CacheInsertStats::default();
        }

        let inserted = self.entries.insert(key.clone(), entry).is_none();
        if inserted {
            self.order.push_back(key);
        }

        let mut evictions = 0usize;
        while self.entries.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if self.entries.remove(&oldest).is_some() {
                evictions += 1;
            }
        }

        CacheInsertStats {
            inserted,
            evictions,
        }
    }
}

#[derive(Debug, Default)]
struct CacheInsertStats {
    inserted: bool,
    evictions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum UnixfsPathCacheKey {
    DirectoryLink { directory: Cid, name: String },
    DirectoryEntries(Cid),
    FileSize(Cid),
    NodeKind(Cid),
}

#[derive(Debug, Clone)]
enum UnixfsPathCacheEntry {
    DirectoryLink(Cid),
    DirectoryEntries(Vec<DirectoryEntry>),
    FileSize(u64),
    NodeKind(NodeKind),
}

pub fn resolve_path(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<ResolvedNode> {
    resolve_path_inner(provider, root, path, None)
}

pub fn resolve_path_with_cache(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    cache: &UnixfsPathCache,
) -> Result<ResolvedNode> {
    resolve_path_inner(provider, root, path, Some(cache))
}

fn resolve_path_inner(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    cache: Option<&UnixfsPathCache>,
) -> Result<ResolvedNode> {
    let mut current = *root;
    let mut segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .peekable();

    if segments.peek().is_none() {
        return classify(provider, &current, cache);
    }

    for segment in segments {
        if let Some(cid) = cache.and_then(|cache| cache.get_directory_link(&current, segment)) {
            current = cid;
            continue;
        }

        let block = provider
            .get_block(&current)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .ok_or(UnixfsError::NotFound(current))?;

        if block.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::NotDirectory);
        }

        let node = decode_pb_node(block.data())?;
        let data = decode_unixfs_data(&node)?;
        match data_type(&data)? {
            DataType::Directory => {
                if let Some(cache) = cache {
                    cache.insert_kind(current, NodeKind::Directory);
                    cache_directory_links_lossy(cache, current, &node.links);
                }
                current = find_link(&node, segment)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
            }
            DataType::HamtShard => {
                if let Some(cache) = cache {
                    cache.insert_kind(current, NodeKind::HamtShard);
                }
                current = find_hamt_link(provider, current, &node, &data, segment, cache)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
            }
            _ => return Err(UnixfsError::NotDirectory),
        }
    }

    classify(provider, &current, cache)
}

pub fn read_file(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<Vec<u8>> {
    let resolved = resolve_path_inner(provider, root, path, None)?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
        NodeKind::Raw | NodeKind::File => {}
    }
    read_file_cid(provider, &resolved.cid, None)
}

pub fn list_directory(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
) -> Result<Vec<DirectoryEntry>> {
    let resolved = resolve_path_inner(provider, root, path, None)?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => {
            list_directory_cid(provider, &resolved.cid, None)
        }
        NodeKind::Raw | NodeKind::File => Err(UnixfsError::NotDirectory),
    }
}

pub fn list_directory_with_cache(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    cache: &UnixfsPathCache,
) -> Result<Vec<DirectoryEntry>> {
    let resolved = resolve_path_inner(provider, root, path, Some(cache))?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => {
            list_directory_cid(provider, &resolved.cid, Some(cache))
        }
        NodeKind::Raw | NodeKind::File => Err(UnixfsError::NotDirectory),
    }
}

pub fn file_size(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<u64> {
    let resolved = resolve_path_inner(provider, root, path, None)?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
        NodeKind::Raw | NodeKind::File => {}
    }
    file_size_cid(provider, &resolved.cid, None)
}

pub fn file_size_with_cache(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    cache: &UnixfsPathCache,
) -> Result<u64> {
    let resolved = resolve_path_inner(provider, root, path, Some(cache))?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
        NodeKind::Raw | NodeKind::File => {}
    }
    file_size_cid(provider, &resolved.cid, Some(cache))
}

pub fn read_file_range(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    let resolved = resolve_path_inner(provider, root, path, None)?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
        NodeKind::Raw | NodeKind::File => {}
    }
    read_file_cid_range(provider, &resolved.cid, start, end, None)
}

pub fn read_file_range_with_cache(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    start: u64,
    end: u64,
    cache: &UnixfsPathCache,
) -> Result<Vec<u8>> {
    let resolved = resolve_path_inner(provider, root, path, Some(cache))?;
    match resolved.kind {
        NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
        NodeKind::Raw | NodeKind::File => {}
    }
    read_file_cid_range(provider, &resolved.cid, start, end, Some(cache))
}

fn list_directory_cid(
    provider: &dyn BlockProvider,
    cid: &Cid,
    cache: Option<&UnixfsPathCache>,
) -> Result<Vec<DirectoryEntry>> {
    if let Some(entries) = cache.and_then(|cache| cache.get_directory_entries(cid)) {
        return Ok(entries);
    }

    let block = provider
        .get_block(cid)
        .map_err(|err| UnixfsError::Provider(err.to_string()))?
        .ok_or(UnixfsError::NotFound(*cid))?;
    if block.codec() != CODEC_DAG_PB {
        return Err(UnixfsError::NotDirectory);
    }

    let node = decode_pb_node(block.data())?;
    let data = decode_unixfs_data(&node)?;
    let mut entries = match data_type(&data)? {
        DataType::Directory => directory_entries(&node.links)?,
        DataType::HamtShard => hamt_directory_entries(provider, &node, &data, cache)?,
        _ => return Err(UnixfsError::NotDirectory),
    };
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    if let Some(cache) = cache {
        cache.insert_kind(
            *cid,
            match data_type(&data)? {
                DataType::Directory => NodeKind::Directory,
                DataType::HamtShard => NodeKind::HamtShard,
                _ => unreachable!(),
            },
        );
        cache_directory_entries(cache, *cid, &entries);
    }
    Ok(entries)
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

fn hamt_directory_entries(
    provider: &dyn BlockProvider,
    node: &PbNode,
    data: &UnixfsData,
    cache: Option<&UnixfsPathCache>,
) -> Result<Vec<DirectoryEntry>> {
    validate_hamt(data)?;
    let mut entries = Vec::new();
    let mut pending = Vec::new();
    collect_hamt_entries(&node.links, &mut entries, &mut pending)?;

    let mut visited = 0usize;
    while let Some(cid) = pending.pop() {
        visited += 1;
        if visited > HAMT_MAX_SHARDS_VISITED {
            return Err(UnixfsError::InvalidDagPb(format!(
                "HAMT traversal exceeded {HAMT_MAX_SHARDS_VISITED} shards"
            )));
        }

        let block = provider
            .get_block(&cid)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .ok_or(UnixfsError::NotFound(cid))?;
        if block.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::NotDirectory);
        }
        let shard = decode_pb_node(block.data())?;
        let shard_data = decode_unixfs_data(&shard)?;
        if data_type(&shard_data)? != DataType::HamtShard {
            return Err(UnixfsError::InvalidDagPb(
                "HAMT bucket link did not resolve to a HAMT shard".into(),
            ));
        }
        validate_hamt(&shard_data)?;
        if let Some(cache) = cache {
            cache.insert_kind(cid, NodeKind::HamtShard);
        }
        collect_hamt_entries(&shard.links, &mut entries, &mut pending)?;
    }

    Ok(entries)
}

fn collect_hamt_entries(
    links: &[PbLink],
    entries: &mut Vec<DirectoryEntry>,
    pending: &mut Vec<Cid>,
) -> Result<()> {
    for link in links {
        let Some(link_name) = link.name.as_deref() else {
            continue;
        };
        let link_name = link_name.as_bytes();
        if link_name.len() == HAMT_LINK_PREFIX_LEN {
            pending.push(link_cid(link)?);
        } else if link_name.len() > HAMT_LINK_PREFIX_LEN {
            entries.push(DirectoryEntry {
                name: String::from_utf8_lossy(&link_name[HAMT_LINK_PREFIX_LEN..]).to_string(),
                cid: link_cid(link)?,
                size: link.tsize,
            });
        }
    }
    Ok(())
}

fn read_file_cid(
    provider: &dyn BlockProvider,
    cid: &Cid,
    cache: Option<&UnixfsPathCache>,
) -> Result<Vec<u8>> {
    let block = provider
        .get_block(cid)
        .map_err(|err| UnixfsError::Provider(err.to_string()))?
        .ok_or(UnixfsError::NotFound(*cid))?;

    match block.codec() {
        CODEC_RAW => {
            if let Some(cache) = cache {
                cache.insert_kind(*cid, NodeKind::Raw);
                cache.insert_file_size(*cid, block.data().len() as u64);
            }
            Ok(block.data().to_vec())
        }
        CODEC_DAG_PB => {
            let node = decode_pb_node(block.data())?;
            let data = decode_unixfs_data(&node)?;
            match data_type(&data)? {
                DataType::Raw | DataType::File => {
                    if let Some(cache) = cache {
                        cache.insert_kind(*cid, NodeKind::File);
                        if let Some(filesize) = data.filesize {
                            cache.insert_file_size(*cid, filesize);
                        }
                    }
                    let mut out = data.data.unwrap_or_default();
                    for link in &node.links {
                        let child = link_cid(link)?;
                        out.extend_from_slice(&read_file_cid(provider, &child, cache)?);
                    }
                    Ok(out)
                }
                DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
            }
        }
        codec => Err(UnixfsError::UnsupportedCodec(codec)),
    }
}

fn file_size_cid(
    provider: &dyn BlockProvider,
    cid: &Cid,
    cache: Option<&UnixfsPathCache>,
) -> Result<u64> {
    if let Some(size) = cache.and_then(|cache| cache.get_file_size(cid)) {
        return Ok(size);
    }

    let block = provider
        .get_block(cid)
        .map_err(|err| UnixfsError::Provider(err.to_string()))?
        .ok_or(UnixfsError::NotFound(*cid))?;

    let size = match block.codec() {
        CODEC_RAW => {
            if let Some(cache) = cache {
                cache.insert_kind(*cid, NodeKind::Raw);
            }
            block.data().len() as u64
        }
        CODEC_DAG_PB => {
            let node = decode_pb_node(block.data())?;
            let data = decode_unixfs_data(&node)?;
            match data_type(&data)? {
                DataType::Raw | DataType::File => {
                    if let Some(cache) = cache {
                        cache.insert_kind(*cid, NodeKind::File);
                    }
                    if let Some(filesize) = data.filesize {
                        filesize
                    } else {
                        let mut size = data.data.as_ref().map_or(0, |bytes| bytes.len() as u64);
                        for link in &node.links {
                            size = size.saturating_add(file_size_cid(
                                provider,
                                &link_cid(link)?,
                                cache,
                            )?);
                        }
                        size
                    }
                }
                DataType::Directory | DataType::HamtShard => return Err(UnixfsError::IsDirectory),
                other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
            }
        }
        codec => return Err(UnixfsError::UnsupportedCodec(codec)),
    };
    if let Some(cache) = cache {
        cache.insert_file_size(*cid, size);
    }
    Ok(size)
}

fn read_file_cid_range(
    provider: &dyn BlockProvider,
    cid: &Cid,
    start: u64,
    end: u64,
    cache: Option<&UnixfsPathCache>,
) -> Result<Vec<u8>> {
    let block = provider
        .get_block(cid)
        .map_err(|err| UnixfsError::Provider(err.to_string()))?
        .ok_or(UnixfsError::NotFound(*cid))?;

    match block.codec() {
        CODEC_RAW => {
            if let Some(cache) = cache {
                cache.insert_kind(*cid, NodeKind::Raw);
                cache.insert_file_size(*cid, block.data().len() as u64);
            }
            Ok(slice_bytes(block.data(), start, end))
        }
        CODEC_DAG_PB => {
            let node = decode_pb_node(block.data())?;
            let data = decode_unixfs_data(&node)?;
            match data_type(&data)? {
                DataType::Raw | DataType::File => {
                    if let Some(cache) = cache {
                        cache.insert_kind(*cid, NodeKind::File);
                        if let Some(filesize) = data.filesize {
                            cache.insert_file_size(*cid, filesize);
                        }
                    }
                    let mut out = Vec::new();
                    let mut offset = 0u64;
                    if let Some(inline) = data.data.as_deref() {
                        append_intersection(&mut out, inline, offset, start, end);
                        offset = offset.saturating_add(inline.len() as u64);
                    }

                    for (index, link) in node.links.iter().enumerate() {
                        let child = link_cid(link)?;
                        let child_size = data
                            .blocksizes
                            .get(index)
                            .copied()
                            .map(Ok)
                            .unwrap_or_else(|| file_size_cid(provider, &child, cache))?;
                        if child_size == 0 {
                            continue;
                        }
                        let child_end = offset.saturating_add(child_size - 1);
                        if ranges_intersect(offset, child_end, start, end) {
                            let range_start = start.saturating_sub(offset);
                            let range_end = end.min(child_end).saturating_sub(offset);
                            out.extend_from_slice(&read_file_cid_range(
                                provider,
                                &child,
                                range_start,
                                range_end,
                                cache,
                            )?);
                        }
                        offset = offset.saturating_add(child_size);
                        if offset > end {
                            break;
                        }
                    }
                    Ok(out)
                }
                DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
            }
        }
        codec => Err(UnixfsError::UnsupportedCodec(codec)),
    }
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

fn classify(
    provider: &dyn BlockProvider,
    cid: &Cid,
    cache: Option<&UnixfsPathCache>,
) -> Result<ResolvedNode> {
    if let Some(kind) = cache.and_then(|cache| cache.get_kind(cid)) {
        return Ok(ResolvedNode { cid: *cid, kind });
    }

    let block = provider
        .get_block(cid)
        .map_err(|err| UnixfsError::Provider(err.to_string()))?
        .ok_or(UnixfsError::NotFound(*cid))?;

    let kind = match block.codec() {
        CODEC_RAW => {
            if let Some(cache) = cache {
                cache.insert_file_size(*cid, block.data().len() as u64);
            }
            NodeKind::Raw
        }
        CODEC_DAG_PB => {
            let node = decode_pb_node(block.data())?;
            let data = decode_unixfs_data(&node)?;
            match data_type(&data)? {
                DataType::Raw | DataType::File => {
                    if let Some(filesize) = data.filesize {
                        if let Some(cache) = cache {
                            cache.insert_file_size(*cid, filesize);
                        }
                    }
                    NodeKind::File
                }
                DataType::Directory => {
                    if let Some(cache) = cache {
                        cache_directory_links_lossy(cache, *cid, &node.links);
                    }
                    NodeKind::Directory
                }
                DataType::HamtShard => NodeKind::HamtShard,
                other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
            }
        }
        codec => return Err(UnixfsError::UnsupportedCodec(codec)),
    };
    if let Some(cache) = cache {
        cache.insert_kind(*cid, kind);
    }

    Ok(ResolvedNode { cid: *cid, kind })
}

fn cache_directory_links_lossy(cache: &UnixfsPathCache, directory: Cid, links: &[PbLink]) {
    for link in links {
        let Some(name) = link.name.as_ref().filter(|name| !name.is_empty()) else {
            continue;
        };
        let Ok(cid) = link_cid(link) else {
            continue;
        };
        cache.insert_directory_link(directory, name.clone(), cid);
    }
}

fn cache_directory_entries(cache: &UnixfsPathCache, cid: Cid, entries: &[DirectoryEntry]) {
    cache.insert_directory_entries(cid, entries.to_vec());
    for entry in entries {
        cache.insert_directory_link(cid, entry.name.clone(), entry.cid);
    }
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

fn find_hamt_link(
    provider: &dyn BlockProvider,
    root: Cid,
    node: &PbNode,
    data: &UnixfsData,
    name: &str,
    cache: Option<&UnixfsPathCache>,
) -> Result<Option<Cid>> {
    validate_hamt(data)?;
    let mut pending = Vec::new();
    if let Some(cid) = scan_hamt_links(&node.links, name, &mut pending)? {
        if let Some(cache) = cache {
            cache.insert_directory_link(root, name.to_string(), cid);
        }
        return Ok(Some(cid));
    }

    let mut visited = 0usize;
    while let Some(cid) = pending.pop() {
        visited += 1;
        if visited > HAMT_MAX_SHARDS_VISITED {
            return Err(UnixfsError::InvalidDagPb(format!(
                "HAMT traversal exceeded {HAMT_MAX_SHARDS_VISITED} shards"
            )));
        }

        let block = provider
            .get_block(&cid)
            .map_err(|err| UnixfsError::Provider(err.to_string()))?
            .ok_or(UnixfsError::NotFound(cid))?;
        if block.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::NotDirectory);
        }

        let shard = decode_pb_node(block.data())?;
        let shard_data = decode_unixfs_data(&shard)?;
        if data_type(&shard_data)? != DataType::HamtShard {
            return Err(UnixfsError::InvalidDagPb(
                "HAMT bucket link did not resolve to a HAMT shard".into(),
            ));
        }
        validate_hamt(&shard_data)?;
        if let Some(cache) = cache {
            cache.insert_kind(cid, NodeKind::HamtShard);
        }
        if let Some(cid) = scan_hamt_links(&shard.links, name, &mut pending)? {
            if let Some(cache) = cache {
                cache.insert_directory_link(root, name.to_string(), cid);
            }
            return Ok(Some(cid));
        }
    }

    Ok(None)
}

fn validate_hamt(data: &UnixfsData) -> Result<()> {
    if data.hash_type != Some(HAMT_MURMUR3_X64_64) || data.fanout != Some(HAMT_FANOUT_256) {
        return Err(UnixfsError::InvalidDagPb(format!(
            "unsupported HAMT parameters hashType={:?} fanout={:?}",
            data.hash_type, data.fanout
        )));
    }
    if data.filesize.is_some() || !data.blocksizes.is_empty() {
        return Err(UnixfsError::InvalidDagPb(
            "HAMT shard carried file-only UnixFS fields".into(),
        ));
    }
    Ok(())
}

fn scan_hamt_links(links: &[PbLink], name: &str, pending: &mut Vec<Cid>) -> Result<Option<Cid>> {
    for link in links {
        let Some(link_name) = link.name.as_deref() else {
            continue;
        };
        let link_name = link_name.as_bytes();
        if link_name.len() == HAMT_LINK_PREFIX_LEN {
            pending.push(link_cid(link)?);
        } else if link_name.len() > HAMT_LINK_PREFIX_LEN
            && &link_name[HAMT_LINK_PREFIX_LEN..] == name.as_bytes()
        {
            return link_cid(link).map(Some);
        }
    }
    Ok(None)
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
    use freedom_ipfs_core::{cid_from_data, Block, Result as CoreResult, CODEC_DAG_PB, CODEC_RAW};
    use freedom_ipfs_store::SqliteBlockStore;
    use std::collections::HashMap;

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
        PbNode {
            data: Some(
                UnixfsData {
                    r#type: Some(DataType::HamtShard as i32),
                    data: Some(vec![0xff; (HAMT_FANOUT_256 / 8) as usize]),
                    filesize: None,
                    blocksizes: Vec::new(),
                    hash_type: Some(HAMT_MURMUR3_X64_64),
                    fanout: Some(HAMT_FANOUT_256),
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    fn link(name: &str, cid: &Cid) -> PbLink {
        PbLink {
            hash: Some(cid.to_bytes()),
            name: Some(name.to_string()),
            tsize: None,
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
    fn cached_path_resolution_reuses_directory_links_for_sibling_paths() {
        let alpha_data = b"alpha js";
        let alpha_cid = cid_from_data(CODEC_RAW, alpha_data);
        let beta_data = b"beta js";
        let beta_cid = cid_from_data(CODEC_RAW, beta_data);

        let assets_data = pb_directory(vec![
            link("alpha.js", &alpha_cid),
            link("beta.js", &beta_cid),
        ]);
        let assets_cid = cid_from_data(CODEC_DAG_PB, &assets_data);
        let root_data = pb_directory(vec![link("_nuxt", &assets_cid)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root_data);

        let mut provider = CountingProvider::default();
        provider.insert(alpha_cid, alpha_data);
        provider.insert(beta_cid, beta_data);
        provider.insert(assets_cid, &assets_data);
        provider.insert(root_cid, &root_data);

        let cache = UnixfsPathCache::new(64);

        assert_eq!(
            file_size_with_cache(&provider, &root_cid, "_nuxt/alpha.js", &cache).unwrap(),
            alpha_data.len() as u64
        );
        assert_eq!(
            file_size_with_cache(&provider, &root_cid, "_nuxt/beta.js", &cache).unwrap(),
            beta_data.len() as u64
        );

        assert_eq!(provider.calls(&root_cid), 1);
        assert_eq!(provider.calls(&assets_cid), 1);
        assert_eq!(provider.calls(&alpha_cid), 1);
        assert_eq!(provider.calls(&beta_cid), 1);
        assert!(cache.len() <= 64);
    }

    #[test]
    fn unixfs_path_cache_prunes_to_capacity() {
        let cache = UnixfsPathCache::new(2);
        let first = cid_from_data(CODEC_RAW, b"first");
        let second = cid_from_data(CODEC_RAW, b"second");
        let third = cid_from_data(CODEC_RAW, b"third");

        cache.insert_kind(first, NodeKind::Raw);
        cache.insert_kind(second, NodeKind::Raw);
        cache.insert_kind(third, NodeKind::Raw);

        let stats = cache.stats();
        assert_eq!(stats.capacity, 2);
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.inserts, 3);
        assert_eq!(stats.evictions, 1);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get_kind(&first), None);
        assert_eq!(cache.get_kind(&second), Some(NodeKind::Raw));
        assert_eq!(cache.get_kind(&third), Some(NodeKind::Raw));

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
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

    #[test]
    fn resolves_single_level_hamt_shard() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hamt index";
        let file_cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&file_cid, data).unwrap();

        let hamt_data = pb_hamt(vec![link("ABindex.html", &file_cid)]);
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

        let child_data = pb_hamt(vec![link("CDnested.txt", &file_cid)]);
        let child_cid = cid_from_data(CODEC_DAG_PB, &child_data);
        store.put_block(&child_cid, &child_data).unwrap();

        let root_data = pb_hamt(vec![link("AB", &child_cid)]);
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

    #[derive(Default)]
    struct CountingProvider {
        blocks: HashMap<Cid, Vec<u8>>,
        calls: Mutex<HashMap<Cid, usize>>,
    }

    impl CountingProvider {
        fn insert(&mut self, cid: Cid, data: &[u8]) {
            self.blocks.insert(cid, data.to_vec());
        }

        fn calls(&self, cid: &Cid) -> usize {
            self.calls
                .lock()
                .unwrap()
                .get(cid)
                .copied()
                .unwrap_or_default()
        }
    }

    impl BlockProvider for CountingProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            let Some(data) = self.blocks.get(cid) else {
                return Ok(None);
            };
            *self.calls.lock().unwrap().entry(*cid).or_default() += 1;
            Ok(Some(Block::unchecked(*cid, data.clone())))
        }
    }
}
