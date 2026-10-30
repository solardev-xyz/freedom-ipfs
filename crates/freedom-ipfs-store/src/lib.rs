use cid::Cid;
use freedom_ipfs_core::{
    block_data_range, encode_car_v1, parse_car_v1, verify_block, Block, BlockProvider, CarBlock,
    CoreError, Result as CoreResult,
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_HOT_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const HOT_CACHE_TOUCH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_NAME_CACHE_RECORDS: i64 = 128;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("core: {0}")]
    Core(#[from] CoreError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct SqliteBlockStore {
    conn: Arc<Mutex<Connection>>,
    max_bytes: u64,
    hot: Arc<Mutex<VerifiedHotCache>>,
    retained: Arc<Mutex<HashMap<Vec<u8>, usize>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedProviderRecord {
    pub id: Option<String>,
    pub addrs: Vec<String>,
}

impl SqliteBlockStore {
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self> {
        let conn = Connection::open(path)?;
        let max_bytes = if max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            max_bytes
        };
        let hot_cache = VerifiedHotCache::new(hot_cache_bytes(max_bytes));
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            max_bytes,
            hot: Arc::new(Mutex::new(hot_cache)),
            retained: Arc::new(Mutex::new(HashMap::new())),
        };
        store.init()?;
        Ok(store)
    }

    pub fn in_memory(max_bytes: u64) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let max_bytes = if max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            max_bytes
        };
        let hot_cache = VerifiedHotCache::new(hot_cache_bytes(max_bytes));
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            max_bytes,
            hot: Arc::new(Mutex::new(hot_cache)),
            retained: Arc::new(Mutex::new(HashMap::new())),
        };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn.lock().execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS blocks (
                cid BLOB PRIMARY KEY NOT NULL,
                codec INTEGER NOT NULL,
                size INTEGER NOT NULL,
                data BLOB NOT NULL,
                inserted_at INTEGER NOT NULL,
                last_accessed_at INTEGER NOT NULL
            );
            -- Eviction's order, oldest first: the scan reads it off this
            -- index and sorts nothing (#83). It replaces the index on
            -- last_accessed_at alone, which made each eviction sort a whole
            -- second's blocks.
            CREATE INDEX IF NOT EXISTS blocks_eviction
                ON blocks(last_accessed_at, inserted_at);
            DROP INDEX IF EXISTS blocks_last_accessed;
            -- The blocks' total size, kept by triggers, so the store's size
            -- is one row's read, not a scan of every block (#83). The
            -- triggers come first: a block written before the row exists is
            -- in the sum that makes it, and one written after is counted by
            -- them.
            CREATE TABLE IF NOT EXISTS block_bytes (
                id INTEGER PRIMARY KEY NOT NULL CHECK (id = 0),
                bytes INTEGER NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS block_bytes_insert AFTER INSERT ON blocks
            BEGIN
                UPDATE block_bytes SET bytes = bytes + NEW.size WHERE id = 0;
            END;
            CREATE TRIGGER IF NOT EXISTS block_bytes_delete AFTER DELETE ON blocks
            BEGIN
                UPDATE block_bytes SET bytes = bytes - OLD.size WHERE id = 0;
            END;
            CREATE TRIGGER IF NOT EXISTS block_bytes_update AFTER UPDATE OF size ON blocks
            BEGIN
                UPDATE block_bytes SET bytes = bytes - OLD.size + NEW.size WHERE id = 0;
            END;
            INSERT OR IGNORE INTO block_bytes(id, bytes)
                SELECT 0, COALESCE(SUM(size), 0) FROM blocks;
            CREATE TABLE IF NOT EXISTS bad_providers (
                peer_or_url TEXT PRIMARY KEY NOT NULL,
                reason TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS provider_cache (
                cid BLOB PRIMARY KEY NOT NULL,
                providers_json TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS name_cache (
                name TEXT PRIMARY KEY NOT NULL,
                resolved_target TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS name_cache_updated_at
                ON name_cache(updated_at);
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    /// Closes the database connection now, even while clones of this store
    /// are still alive (for example held by blocking work that outlived its
    /// runtime). The connection is swapped for an empty in-memory database, so
    /// later calls through any clone fail with an SQLite error instead of
    /// touching the file, and the file can be reopened right away. Waits at
    /// most `timeout` for a statement already in progress; returns `false`,
    /// leaving the connection open, if that does not finish in time.
    pub fn close(&self, timeout: Duration) -> bool {
        match self.conn.try_lock_for(timeout) {
            Some(conn) => self.close_locked(conn),
            None => false,
        }
    }

    /// Like [`close`](Self::close), but when the statement in progress is
    /// still running after `timeout`, hands the store to a background thread
    /// that closes it as soon as that statement releases the connection,
    /// instead of leaving it open until the last clone is dropped. Returns
    /// whether the store was closed within `timeout`.
    ///
    /// The background thread has no deadline: it blocks on the connection
    /// lock for exactly as long as the running statement holds it. A statement
    /// that never returns therefore parks one `freedom-ipfs-store-close`
    /// thread (and keeps the database open) for the rest of the process, one
    /// per call that timed out. That is the same lifetime the connection
    /// would have had without this call, plus one idle thread.
    pub fn close_or_close_later(self, timeout: Duration) -> bool {
        if self.close(timeout) {
            return true;
        }
        // If the thread cannot be spawned the store is dropped here and the
        // connection closes with its last clone, as before.
        let _ = std::thread::Builder::new()
            .name("freedom-ipfs-store-close".into())
            .spawn(move || {
                let conn = self.conn.lock();
                self.close_locked(conn);
            });
        false
    }

    fn close_locked(&self, mut conn: parking_lot::MutexGuard<'_, Connection>) -> bool {
        let Ok(closed) = Connection::open_in_memory() else {
            return false;
        };
        drop(std::mem::replace(&mut *conn, closed));
        *self.hot.lock() = VerifiedHotCache::new(0);
        true
    }

    pub fn put_block(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        verify_block(cid, data)?;
        let cid_bytes = block_key(cid);
        let now = now_secs();
        self.conn.lock().execute(
            r#"
            INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(cid) DO UPDATE SET
                codec = excluded.codec,
                size = excluded.size,
                data = excluded.data,
                last_accessed_at = excluded.last_accessed_at
            "#,
            params![
                &cid_bytes,
                cid.codec() as i64,
                data.len() as i64,
                data,
                now as i64
            ],
        )?;
        let inserted_block_evicted = self.evict_if_needed_tracking(&cid_bytes)?;
        if inserted_block_evicted {
            self.hot.lock().remove(&cid_bytes);
        } else {
            self.hot.lock().put_verified(cid_bytes, data.to_vec(), now);
        }
        Ok(())
    }

    pub fn put(&self, block: &Block) -> Result<()> {
        self.put_block(block.cid(), block.data())
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<Block>> {
        let cid_bytes = block_key(cid);
        let now = now_secs();
        if let Some(hit) = self.hot.lock().get_verified(&cid_bytes, now) {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(Block::unchecked(*cid, hit.data)));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                self.hot.lock().put_verified(cid_bytes, data.clone(), now);
                Ok(Some(Block::unchecked(*cid, data)))
            }
            None => Ok(None),
        }
    }

    pub fn get_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Option<Vec<u8>>> {
        let cid_bytes = block_key(cid);
        let now = now_secs();
        if let Some(hit) = self
            .hot
            .lock()
            .get_verified_range(&cid_bytes, now, start, end)
        {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(hit.data));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![&cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                let range = block_data_range(&data, start, end);
                self.hot.lock().put_verified(cid_bytes, data, now);
                Ok(Some(range))
            }
            None => Ok(None),
        }
    }

    pub fn import_car(&self, bytes: &[u8]) -> Result<Vec<Cid>> {
        let car = parse_car_v1(bytes)?;
        let mut imported = Vec::with_capacity(car.blocks.len());
        for block in car.blocks {
            self.put_block(&block.cid, &block.data)?;
            imported.push(block.cid);
        }
        Ok(imported)
    }

    pub fn export_car(&self) -> Result<Vec<u8>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT cid, data FROM blocks ORDER BY inserted_at ASC")?;
        let blocks = stmt
            .query_map([], |row| {
                let cid_bytes = row.get::<_, Vec<u8>>(0)?;
                let data = row.get::<_, Vec<u8>>(1)?;
                Ok((cid_bytes, data))
            })?
            .map(|row| {
                let (cid_bytes, data) = row?;
                let cid = Cid::read_bytes(&mut Cursor::new(cid_bytes))
                    .map_err(|err| CoreError::InvalidCid(err.to_string()))?;
                Ok(CarBlock { cid, data })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(encode_car_v1(&blocks))
    }

    pub fn put_provider_records(
        &self,
        cid: &Cid,
        providers: &[CachedProviderRecord],
        ttl: Duration,
    ) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        let providers_json = serde_json::to_string(providers)?;
        self.conn.lock().execute(
            r#"
            INSERT INTO provider_cache(cid, providers_json, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(cid) DO UPDATE SET
                providers_json = excluded.providers_json,
                expires_at = excluded.expires_at
            "#,
            params![provider_cache_key(cid), providers_json, expires_at as i64],
        )?;
        Ok(())
    }

    pub fn get_provider_records(&self, cid: &Cid) -> Result<Option<Vec<CachedProviderRecord>>> {
        let cid_bytes = provider_cache_key(cid);
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT providers_json, expires_at FROM provider_cache WHERE cid = ?1",
                params![cid_bytes],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((providers_json, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM provider_cache WHERE cid = ?1",
                params![provider_cache_key(cid)],
            )?;
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&providers_json)?))
    }

    pub fn put_name_record(&self, name: &str, resolved_target: &str, ttl: Duration) -> Result<()> {
        if ttl.is_zero() {
            return Ok(());
        }
        let now = now_secs();
        let expires_at = now.saturating_add(ttl.as_secs());
        self.conn.lock().execute(
            r#"
            INSERT INTO name_cache(name, resolved_target, expires_at, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(name) DO UPDATE SET
                resolved_target = excluded.resolved_target,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            "#,
            params![name, resolved_target, expires_at as i64, now as i64],
        )?;
        self.prune_name_cache()?;
        Ok(())
    }

    pub fn get_name_record(&self, name: &str) -> Result<Option<String>> {
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT resolved_target, expires_at FROM name_cache WHERE name = ?1",
                params![name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((resolved_target, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn
                .lock()
                .execute("DELETE FROM name_cache WHERE name = ?1", params![name])?;
            return Ok(None);
        }
        Ok(Some(resolved_target))
    }

    pub fn mark_bad_provider(&self, peer_or_url: &str, reason: &str, ttl: Duration) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        self.conn.lock().execute(
            r#"
            INSERT INTO bad_providers(peer_or_url, reason, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(peer_or_url) DO UPDATE SET
                reason = excluded.reason,
                expires_at = excluded.expires_at
            "#,
            params![peer_or_url, reason, expires_at as i64],
        )?;
        Ok(())
    }

    pub fn is_bad_provider(&self, peer_or_url: &str) -> Result<bool> {
        let expires_at = self
            .conn
            .lock()
            .query_row(
                "SELECT expires_at FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(expires_at) = expires_at else {
            return Ok(false);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
            )?;
            return Ok(false);
        }
        Ok(true)
    }

    /// The blocks' total size, as the `block_bytes` triggers keep it.
    pub fn total_bytes(&self) -> Result<u64> {
        total_bytes_in(&self.conn.lock())
    }

    pub fn block_count(&self) -> Result<u64> {
        let count = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| {
                row.get::<_, i64>(0)
            })?;
        Ok(count.max(0) as u64)
    }

    pub fn clear(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM blocks", [])?;
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        self.hot.lock().clear();
        Ok(())
    }

    pub fn clear_provider_metadata(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        Ok(())
    }

    pub fn trim_blocks_to(&self, max_bytes: u64) -> Result<()> {
        self.evict_until(max_bytes)
    }

    fn touch_at(&self, cid: &Cid, now: u64) -> Result<()> {
        self.conn.lock().execute(
            "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
            params![now as i64, block_key(cid)],
        )?;
        Ok(())
    }

    fn evict_if_needed_tracking(&self, tracked_cid_bytes: &[u8]) -> Result<bool> {
        self.evict_until_tracking(self.max_bytes, tracked_cid_bytes)
    }

    fn prune_name_cache(&self) -> Result<()> {
        let now = now_secs() as i64;
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM name_cache WHERE expires_at <= ?1",
            params![now],
        )?;
        conn.execute(
            r#"
            DELETE FROM name_cache
            WHERE name IN (
                SELECT name FROM name_cache
                ORDER BY updated_at DESC, name DESC
                LIMIT -1 OFFSET ?1
            )
            "#,
            params![MAX_NAME_CACHE_RECORDS],
        )?;
        Ok(())
    }

    fn evict_until(&self, max_bytes: u64) -> Result<()> {
        self.evict_until_tracking(max_bytes, &[]).map(|_| ())
    }

    /// Evicts the oldest blocks until the store holds `max_bytes` or less,
    /// or only retained blocks are left: oldest by last access, then by
    /// insertion, then by row (the order they were first stored in). It
    /// reads the total once, then walks the eviction index in batches of
    /// [`EVICTION_BATCH`], each after the last one's key, and deletes them in
    /// one transaction: it holds one batch at a time, however many blocks
    /// go. Each batch's retained check and deletes happen under the
    /// retained set's lock, so a `retain_block` that has returned is always
    /// seen. Whether `tracked_cid_bytes` was evicted is the answer.
    ///
    /// Lock order: the connection, then the retained set, then the hot
    /// cache. `retain_block` and `release_block` take the retained set
    /// alone, and nothing else takes it, so none waits on the connection
    /// while holding it.
    fn evict_until_tracking(&self, max_bytes: u64, tracked_cid_bytes: &[u8]) -> Result<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        let total = total_bytes_in(&tx)?;
        let Some(mut excess) = total.checked_sub(max_bytes).filter(|excess| *excess > 0) else {
            return Ok(false);
        };
        let mut tracked_evicted = false;
        {
            let mut next = tx.prepare(EVICTION_BATCH_QUERY)?;
            let mut delete = tx.prepare("DELETE FROM blocks WHERE rowid = ?1")?;
            let mut after = (i64::MIN, i64::MIN, i64::MIN);
            let mut batch: Vec<(Vec<u8>, u64, i64)> = Vec::with_capacity(EVICTION_BATCH);
            while excess > 0 {
                batch.clear();
                let mut rows =
                    next.query(params![after.0, after.1, after.2, EVICTION_BATCH as i64])?;
                while let Some(row) = rows.next()? {
                    let key = (row.get(2)?, row.get(3)?, row.get(4)?);
                    after = key;
                    batch.push((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?.max(0) as u64,
                        key.2,
                    ));
                }
                drop(rows);
                if batch.is_empty() {
                    break;
                }
                let retained = self.retained.lock();
                let mut hot = self.hot.lock();
                for (cid_bytes, size, rowid) in batch.drain(..) {
                    if excess == 0 {
                        break;
                    }
                    if retained.contains_key(&cid_bytes) {
                        continue;
                    }
                    delete.execute(params![rowid])?;
                    excess = excess.saturating_sub(size);
                    // The database is the source of truth: a block dropped
                    // here before a rollback is read from it again.
                    hot.remove(&cid_bytes);
                    tracked_evicted |=
                        !tracked_cid_bytes.is_empty() && cid_bytes == tracked_cid_bytes;
                }
            }
        }
        tx.commit()?;
        Ok(tracked_evicted)
    }

    fn retain_cid_bytes(&self, cid_bytes: Vec<u8>) {
        let mut retained = self.retained.lock();
        *retained.entry(cid_bytes).or_insert(0) += 1;
    }

    fn release_cid_bytes(&self, cid_bytes: &[u8]) {
        let mut retained = self.retained.lock();
        let Some(count) = retained.get_mut(cid_bytes) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            retained.remove(cid_bytes);
        }
    }
}

impl BlockProvider for SqliteBlockStore {
    fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
        self.get(cid)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
        self.get_range(cid, start, end)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn retain_block(&self, cid: &Cid) -> CoreResult<()> {
        self.retain_cid_bytes(block_key(cid));
        Ok(())
    }

    fn release_block(&self, cid: &Cid) {
        self.release_cid_bytes(&block_key(cid));
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn provider_cache_key(cid: &Cid) -> Vec<u8> {
    cid.hash().to_bytes()
}

/// The most blocks eviction reads and holds at once.
const EVICTION_BATCH: usize = 64;

/// The next [`EVICTION_BATCH`] blocks to evict, oldest first, after the
/// last batch's key: `(last_accessed_at, inserted_at, rowid)`. It reads
/// them off the `blocks_eviction` index, and sorts nothing.
const EVICTION_BATCH_QUERY: &str = "SELECT cid, size, last_accessed_at, inserted_at, rowid \
     FROM blocks WHERE (last_accessed_at, inserted_at, rowid) > (?1, ?2, ?3) \
     ORDER BY last_accessed_at ASC, inserted_at ASC, rowid ASC LIMIT ?4";

/// The blocks' total size on `conn`, as the `block_bytes` triggers keep it.
fn total_bytes_in(conn: &Connection) -> Result<u64> {
    let total = conn.query_row("SELECT bytes FROM block_bytes WHERE id = 0", [], |row| {
        row.get::<_, i64>(0)
    })?;
    Ok(total.max(0) as u64)
}

fn block_key(cid: &Cid) -> Vec<u8> {
    Cid::new_v1(cid.codec(), *cid.hash()).to_bytes()
}

fn hot_cache_bytes(max_bytes: u64) -> u64 {
    max_bytes.min(DEFAULT_HOT_CACHE_BYTES)
}

// Private cache for block bytes that were already verified on put or cold read.
struct VerifiedHotCache {
    max_bytes: u64,
    bytes: u64,
    clock: u64,
    entries: HashMap<Vec<u8>, VerifiedHotBlock>,
}

struct VerifiedHotBlock {
    data: Vec<u8>,
    last_accessed: u64,
    last_persistent_touch: u64,
}

struct VerifiedHotCacheHit {
    data: Vec<u8>,
    touch_persistent: bool,
}

impl VerifiedHotCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
        }
    }

    fn get_verified(&mut self, cid: &[u8], now: u64) -> Option<VerifiedHotCacheHit> {
        let entry = self.entries.get_mut(cid)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_accessed = self.clock;
        let touch_persistent =
            now.saturating_sub(entry.last_persistent_touch) >= HOT_CACHE_TOUCH_INTERVAL.as_secs();
        if touch_persistent {
            entry.last_persistent_touch = now;
        }
        Some(VerifiedHotCacheHit {
            data: entry.data.clone(),
            touch_persistent,
        })
    }

    fn get_verified_range(
        &mut self,
        cid: &[u8],
        now: u64,
        start: u64,
        end: u64,
    ) -> Option<VerifiedHotCacheHit> {
        let entry = self.entries.get_mut(cid)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_accessed = self.clock;
        let touch_persistent =
            now.saturating_sub(entry.last_persistent_touch) >= HOT_CACHE_TOUCH_INTERVAL.as_secs();
        if touch_persistent {
            entry.last_persistent_touch = now;
        }
        Some(VerifiedHotCacheHit {
            data: block_data_range(&entry.data, start, end),
            touch_persistent,
        })
    }

    fn put_verified(&mut self, cid: Vec<u8>, data: Vec<u8>, now: u64) {
        if self.max_bytes == 0 || data.len() as u64 > self.max_bytes {
            self.remove(&cid);
            return;
        }
        if let Some(existing) = self.entries.remove(&cid) {
            self.bytes = self.bytes.saturating_sub(existing.data.len() as u64);
        }
        self.clock = self.clock.saturating_add(1);
        self.bytes = self.bytes.saturating_add(data.len() as u64);
        self.entries.insert(
            cid,
            VerifiedHotBlock {
                data,
                last_accessed: self.clock,
                last_persistent_touch: now,
            },
        );
        self.evict_if_needed();
    }

    fn remove(&mut self, cid: &[u8]) {
        if let Some(block) = self.entries.remove(cid) {
            self.bytes = self.bytes.saturating_sub(block.data.len() as u64);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    fn evict_if_needed(&mut self) {
        while self.bytes > self.max_bytes {
            let Some(cid) = self
                .entries
                .iter()
                .min_by_key(|(_, block)| block.last_accessed)
                .map(|(cid, _)| cid.clone())
            else {
                self.bytes = 0;
                return;
            };
            self.remove(&cid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{cid_from_data, CODEC_DAG_PB, CODEC_RAW};

    /// Eviction's cost at scale (#83), measured by hand:
    ///
    /// `FREEDOM_IPFS_EVICTION_BLOCKS=262144 cargo test --release -p freedom-ipfs-store eviction_timing -- --ignored --nocapture`
    ///
    /// A file store of that many 1 KiB blocks, at its limit, every block
    /// last touched in the same second, or one second apart. Then ten puts,
    /// each evicting one block, and a trim of `FREEDOM_IPFS_EVICTION_TRIM`
    /// blocks (100 by default). It prints each eviction query's plan, and
    /// the times.
    #[test]
    #[ignore = "a measurement, run by hand"]
    fn eviction_timing() {
        let env = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        let blocks = env("FREEDOM_IPFS_EVICTION_BLOCKS", 262_144);
        let trim = env("FREEDOM_IPFS_EVICTION_TRIM", 100);
        let data = |i: usize| {
            let mut data = vec![0u8; 1024];
            data[..8].copy_from_slice(&(i as u64).to_le_bytes());
            data
        };
        for (layout, spread) in [("same second", false), ("one a second", true)] {
            let dir = tempfile::tempdir().unwrap();
            let store =
                SqliteBlockStore::open(dir.path().join("blocks.sqlite"), (blocks * 1024) as u64)
                    .unwrap();
            let started = std::time::Instant::now();
            {
                let mut conn = store.conn.lock();
                let tx = conn.transaction().unwrap();
                {
                    let mut insert = tx
                        .prepare(
                            "INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at) \
                             VALUES (?1, ?2, 1024, ?3, ?4, ?4)",
                        )
                        .unwrap();
                    for i in 0..blocks {
                        let data = data(i);
                        let cid = cid_from_data(CODEC_RAW, &data);
                        let at = 1_000_000 + if spread { i as i64 } else { 0 };
                        insert
                            .execute(params![block_key(&cid), CODEC_RAW as i64, data, at])
                            .unwrap();
                    }
                }
                tx.commit().unwrap();
            }
            let filled = started.elapsed();
            if !spread {
                let conn = store.conn.lock();
                let mut plan = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {EVICTION_BATCH_QUERY}"))
                    .unwrap();
                let mut rows = Vec::new();
                let mut query = plan
                    .query(params![i64::MIN, i64::MIN, i64::MIN, EVICTION_BATCH as i64])
                    .unwrap();
                while let Some(row) = query.next().unwrap() {
                    rows.push(row.get::<_, String>(3).unwrap());
                }
                println!("EVICTION plan: {rows:?}");
            }
            let started = std::time::Instant::now();
            for _ in 0..10 {
                store.total_bytes().unwrap();
            }
            let sum = started.elapsed() / 10;
            let started = std::time::Instant::now();
            for _ in 0..10 {
                let conn = store.conn.lock();
                let mut oldest = conn.prepare(EVICTION_BATCH_QUERY).unwrap();
                let mut rows = oldest
                    .query(params![i64::MIN, i64::MIN, i64::MIN, EVICTION_BATCH as i64])
                    .unwrap();
                rows.next().unwrap().unwrap();
            }
            let oldest = started.elapsed() / 10;
            println!("EVICTION {blocks} blocks, {layout}: total_bytes {sum:?}; the oldest evictable CID {oldest:?}");
            let started = std::time::Instant::now();
            for i in 0..10 {
                let data = data(blocks + i);
                store
                    .put_block(&cid_from_data(CODEC_RAW, &data), &data)
                    .unwrap();
            }
            let per_put = started.elapsed() / 10;
            let started = std::time::Instant::now();
            store
                .trim_blocks_to(((blocks - trim) * 1024) as u64)
                .unwrap();
            let trimmed = started.elapsed();
            assert_eq!(store.block_count().unwrap() as usize, blocks - trim);
            println!(
                "EVICTION {blocks} blocks, {layout}: filled in {filled:?}; a put that evicts one \
                 block {per_put:?}; a trim of {trim} blocks {trimmed:?}"
            );
        }
    }

    #[test]
    fn close_releases_the_file_while_clones_are_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.db");
        let store = SqliteBlockStore::open(&path, 0).unwrap();
        let data = b"closed store fixture".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        let leaked = store.clone();

        assert!(store.close(Duration::from_secs(1)));
        drop(store);

        // A clone that outlived close() gets errors, not the file or the hot cache.
        assert!(leaked.block_count().is_err());
        assert!(leaked.get(&cid).is_err());
        assert!(leaked.put_block(&cid, &data).is_err());

        let reopened = SqliteBlockStore::open(&path, 0).unwrap();
        assert_eq!(reopened.get(&cid).unwrap().unwrap().data(), data.as_slice());
    }

    #[test]
    fn close_gives_up_after_timeout_when_a_statement_holds_the_connection() {
        let store = SqliteBlockStore::in_memory(0).unwrap();
        let guard = store.conn.lock();
        assert!(!store.close(Duration::from_millis(20)));
        drop(guard);
        assert!(store.block_count().is_ok());
    }

    #[test]
    fn close_or_close_later_closes_once_the_statement_releases_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.db");
        let store = SqliteBlockStore::open(&path, 0).unwrap();
        let data = b"close later fixture".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        let leaked = store.clone();

        // Stands in for a statement that leaked blocking work is still running.
        let guard = leaked.conn.lock();
        assert!(!store.close_or_close_later(Duration::from_millis(20)));
        drop(guard);

        // Nothing but the background close touches the store from here on;
        // it must close the file even though `leaked` is never dropped.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while leaked.block_count().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "store was not closed after the statement released the connection"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(leaked.get(&cid).is_err());
        assert!(leaked.put_block(&cid, &data).is_err());
        let reopened = SqliteBlockStore::open(&path, 0).unwrap();
        assert_eq!(reopened.get(&cid).unwrap().unwrap().data(), data.as_slice());
    }

    #[test]
    fn stores_and_verifies_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cached block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let block = store.get(&cid).unwrap().unwrap();
        assert_eq!(block.data(), data);
        assert_eq!(block.cid(), &cid);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn rejected_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");

        assert!(store.put_block(&cid, b"invalid block").is_err());
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn evicted_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1).unwrap();
        let data = b"larger than cache budget";
        let cid = cid_from_data(CODEC_RAW, data);

        store.put_block(&cid, data).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn cold_read_verifies_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get(&cid).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn range_read_verifies_cold_block_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get_range(&cid, 0, 4).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn hot_cache_range_hit_returns_requested_bytes() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get_range(&cid, 2, 5).unwrap().unwrap(), b"2345");
        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn cold_range_read_populates_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cold range block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store.hot.lock().clear();

        assert_eq!(store.get_range(&cid, 5, 9).unwrap().unwrap(), b"range");

        assert!(store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn dag_pb_blocks_are_addressable_by_cidv0_and_cidv1() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"dag-pb alias block";
        let cidv1 = cid_from_data(CODEC_DAG_PB, data);
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();

        store.put_block(&cidv0, data).unwrap();

        assert_eq!(store.get(&cidv0).unwrap().unwrap().data(), data);
        let block = store.get(&cidv1).unwrap().unwrap();
        assert_eq!(block.cid(), &cidv1);
        assert_eq!(block.data(), data);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn cidv0_cidv1_aliases_share_retention_state() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cidv1 = cid_from_data(CODEC_DAG_PB, &first);
        let first_cidv0 = Cid::new_v0(*first_cidv1.hash()).unwrap();
        let second_cid = cid_from_data(CODEC_RAW, &second);

        store.put_block(&first_cidv0, &first).unwrap();
        store.retain_block(&first_cidv1).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cidv0).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cidv1);
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.get(&first_cidv0).unwrap().is_none());
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn evicts_lru_blocks_when_over_budget() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn clear_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache clear";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        assert!(store.get_name_record("example.test").unwrap().is_some());

        store.clear().unwrap();

        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn hot_cache_hit_skips_redundant_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch skip";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn hot_cache_hit_periodically_refreshes_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch refresh";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);
        store
            .hot
            .lock()
            .entries
            .get_mut(&block_key(&cid))
            .unwrap()
            .last_persistent_touch = 0;

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert!(block_last_accessed_at(&store, &cid) > 1);
    }

    #[test]
    fn trim_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache trim";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(store.get(&cid).unwrap().is_none());
    }

    #[test]
    fn trims_blocks_to_requested_budget() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        store.trim_blocks_to(20).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn retained_blocks_are_not_evicted_until_released() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let third = vec![3u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let third_cid = cid_from_data(CODEC_RAW, &third);

        store.put_block(&first_cid, &first).unwrap();
        store.retain_block(&first_cid).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.put_block(&third_cid, &third).unwrap();

        assert!(store.get(&first_cid).unwrap().is_none());
        assert_eq!(store.get(&third_cid).unwrap().unwrap().data(), third);
    }

    /// Rows straight into `blocks`, in this order, each 16 bytes with
    /// these timestamps: `(last_accessed_at, inserted_at)`.
    fn insert_blocks_at(store: &SqliteBlockStore, rows: &[(u8, i64, i64)]) -> Vec<Cid> {
        let conn = store.conn.lock();
        rows.iter()
            .map(|&(byte, last_accessed_at, inserted_at)| {
                let data = vec![byte; 16];
                let cid = cid_from_data(CODEC_RAW, &data);
                conn.execute(
                    "INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at) \
                     VALUES (?1, ?2, 16, ?3, ?4, ?5)",
                    params![
                        block_key(&cid),
                        CODEC_RAW as i64,
                        data,
                        inserted_at,
                        last_accessed_at
                    ],
                )
                .unwrap();
                cid
            })
            .collect()
    }

    /// Eviction takes the oldest by last access, then by insertion, then
    /// the first stored, whatever their CIDs, and passes over retained
    /// blocks: here every block shares one second of last access but two,
    /// and four share their insertion second too.
    #[test]
    fn eviction_with_tied_timestamps_takes_the_first_stored() {
        let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
        // Bytes chosen so the CIDs' order isn't the order stored in.
        let cids = insert_blocks_at(
            &store,
            &[
                (9, 100, 50), // 0: tied with 1, 2 and 3; first stored
                (3, 100, 50), // 1
                (7, 100, 50), // 2
                (1, 100, 50), // 3
                (5, 100, 40), // 4: inserted earlier: the oldest of all
                (8, 200, 10), // 5: touched later: the newest, though first inserted
                (2, 100, 60), // 6: inserted later than 0..3
            ],
        );
        assert_eq!(store.total_bytes().unwrap(), 7 * 16);
        store.retain_block(&cids[1]).unwrap();
        // Read straight from the table: a get would touch the block.
        let kept = |store: &SqliteBlockStore| -> Vec<usize> {
            let conn = store.conn.lock();
            (0..cids.len())
                .filter(|&i| {
                    conn.prepare("SELECT 1 FROM blocks WHERE cid = ?1")
                        .unwrap()
                        .exists(params![block_key(&cids[i])])
                        .unwrap()
                })
                .collect()
        };
        // Three evicted: 4, then 0, then 2 (1 is retained).
        store.trim_blocks_to(4 * 16).unwrap();
        assert_eq!(kept(&store), [1, 3, 5, 6]);
        // Two more: 3 (the last of the tie), then 6.
        store.trim_blocks_to(2 * 16).unwrap();
        assert_eq!(kept(&store), [1, 5]);
        // The retained block stays; 5 goes.
        store.trim_blocks_to(0).unwrap();
        assert_eq!(kept(&store), [1]);
        assert_eq!(store.total_bytes().unwrap(), 16);
    }

    /// Eviction holds one batch at a time, however many blocks it evicts:
    /// trimming 50,000 one-byte blocks peaks at no more heap than trimming
    /// 5,000, and at a few KiB. (Holding every evicted CID, it grew with
    /// the count: about 3 MB at 50,000.)
    #[test]
    fn eviction_holds_one_batch_whatever_it_evicts() {
        let peak_for = |blocks: usize| -> isize {
            let store = SqliteBlockStore::in_memory(1 << 30).unwrap();
            {
                let mut conn = store.conn.lock();
                let tx = conn.transaction().unwrap();
                {
                    let mut insert = tx
                        .prepare(
                            "INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at) \
                             VALUES (?1, ?2, 1, ?3, 1, 1)",
                        )
                        .unwrap();
                    for i in 0..blocks {
                        let data = (i as u64).to_le_bytes();
                        let cid = cid_from_data(CODEC_RAW, &data);
                        insert
                            .execute(params![block_key(&cid), CODEC_RAW as i64, &data[..]])
                            .unwrap();
                    }
                }
                tx.commit().unwrap();
            }
            let origin = heap::live();
            store.trim_blocks_to(0).unwrap();
            let peak = heap::peak() - origin;
            assert_eq!(store.block_count().unwrap(), 0);
            peak
        };
        let few = peak_for(5_000);
        let many = peak_for(50_000);
        assert!(
            many < 32 * 1024,
            "{many} bytes at the peak, trimming 50,000 blocks"
        );
        assert!(
            many <= few + 1024,
            "{few} bytes at the peak for 5,000 blocks, {many} for 50,000"
        );
    }

    /// A block retained while a trim waits for the connection is kept: the
    /// retained check and the deletes come after the wait, under the
    /// retained set's lock. (With the retained set read before the wait,
    /// the second block was deleted.)
    #[test]
    fn a_block_retained_while_a_trim_waits_is_kept() {
        let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
        let cids = insert_blocks_at(&store, &[(1, 100, 1), (2, 200, 1)]);
        let held = store.conn.lock();
        let trimming = {
            let store = store.clone();
            std::thread::spawn(move || store.trim_blocks_to(0))
        };
        std::thread::sleep(Duration::from_millis(100));
        store.retain_block(&cids[1]).unwrap();
        drop(held);
        trimming.join().unwrap().unwrap();
        let conn = store.conn.lock();
        let present = |cid: &Cid| {
            conn.prepare("SELECT 1 FROM blocks WHERE cid = ?1")
                .unwrap()
                .exists(params![block_key(cid)])
                .unwrap()
        };
        assert!(!present(&cids[0]), "the unretained block is evicted");
        assert!(
            present(&cids[1]),
            "the block retained during the wait is kept"
        );
    }

    /// The total the triggers keep is the blocks' sum after every kind of
    /// write: puts, a put of a stored block with other bytes, touches,
    /// eviction, and clear.
    #[test]
    fn the_kept_total_is_the_blocks_sum() {
        let store = SqliteBlockStore::in_memory(64).unwrap();
        let sum = |store: &SqliteBlockStore| -> u64 {
            store
                .conn
                .lock()
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blocks", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap() as u64
        };
        let mut cids = Vec::new();
        for byte in 0..6u8 {
            let data = vec![byte; 10 + byte as usize];
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            cids.push(cid);
            assert_eq!(store.total_bytes().unwrap(), sum(&store));
        }
        assert!(store.total_bytes().unwrap() <= 64);
        // The same block again, and a row updated in place with a new size.
        let data = vec![5u8; 15];
        store.put_block(&cids[5], &data).unwrap();
        store
            .conn
            .lock()
            .execute(
                "UPDATE blocks SET size = size + 3 WHERE cid = ?1",
                params![block_key(&cids[5])],
            )
            .unwrap();
        assert_eq!(store.total_bytes().unwrap(), sum(&store));
        store.touch_at(&cids[5], now_secs() + 10).unwrap();
        assert_eq!(store.total_bytes().unwrap(), sum(&store));
        store.trim_blocks_to(0).unwrap();
        assert_eq!((store.total_bytes().unwrap(), sum(&store)), (0, 0));
        store.put_block(&cids[0], &[0u8; 10]).unwrap();
        store.clear().unwrap();
        assert_eq!((store.total_bytes().unwrap(), sum(&store)), (0, 0));
    }

    /// The eviction scan reads its order off the index, with no sort: on a
    /// fresh store, and on one made before the index, whose single-column
    /// index is dropped and whose total is counted once when it opens.
    #[test]
    fn the_eviction_scan_sorts_nothing_and_old_stores_migrate() {
        // A seek into the index after the last batch's key, then its order:
        // no temporary B-tree.
        const EVICTION_PLAN: [&str; 1] =
            ["SEARCH blocks USING INDEX blocks_eviction ((last_accessed_at,inserted_at)>(?,?))"];
        let plan = |store: &SqliteBlockStore| -> Vec<String> {
            let conn = store.conn.lock();
            let mut plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {EVICTION_BATCH_QUERY}"))
                .unwrap();
            let mut rows = plan
                .query(params![i64::MIN, i64::MIN, i64::MIN, EVICTION_BATCH as i64])
                .unwrap();
            let mut steps = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                steps.push(row.get::<_, String>(3).unwrap());
            }
            steps
        };
        let indexes = |store: &SqliteBlockStore| -> Vec<String> {
            let conn = store.conn.lock();
            let mut names = conn
                .prepare(
                    "SELECT name FROM sqlite_master WHERE type = 'index' \
                     AND tbl_name = 'blocks' AND sql IS NOT NULL ORDER BY name",
                )
                .unwrap();
            let mut rows = names.query([]).unwrap();
            let mut found = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                found.push(row.get::<_, String>(0).unwrap());
            }
            found
        };
        let fresh = SqliteBlockStore::in_memory(1 << 20).unwrap();
        assert_eq!(plan(&fresh), EVICTION_PLAN);
        assert_eq!(indexes(&fresh), ["blocks_eviction"]);

        // A store as the code before #83 made it: the single-column index,
        // no total table, and blocks already in it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE blocks (
                    cid BLOB PRIMARY KEY NOT NULL,
                    codec INTEGER NOT NULL,
                    size INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    inserted_at INTEGER NOT NULL,
                    last_accessed_at INTEGER NOT NULL
                );
                CREATE INDEX blocks_last_accessed ON blocks(last_accessed_at);
                "#,
            )
            .unwrap();
            for byte in 0..3u8 {
                let data = vec![byte; 100];
                let cid = cid_from_data(CODEC_RAW, &data);
                conn.execute(
                    "INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at) \
                     VALUES (?1, ?2, 100, ?3, 1, 1)",
                    params![block_key(&cid), CODEC_RAW as i64, data],
                )
                .unwrap();
            }
        }
        let old = SqliteBlockStore::open(&path, 1 << 20).unwrap();
        assert_eq!(indexes(&old), ["blocks_eviction"]);
        assert_eq!(plan(&old), EVICTION_PLAN);
        assert_eq!(old.total_bytes().unwrap(), 300);
        drop(old);
        // Opened again, the total isn't counted twice.
        let again = SqliteBlockStore::open(&path, 1 << 20).unwrap();
        assert_eq!(again.total_bytes().unwrap(), 300);
        again.trim_blocks_to(150).unwrap();
        assert_eq!(again.total_bytes().unwrap(), 100);
    }

    /// Counts each thread's live heap bytes, and their peak, so a test can
    /// see what eviction holds on the heap.
    mod heap {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static LIVE: Cell<isize> = const { Cell::new(0) };
            static PEAK: Cell<isize> = const { Cell::new(0) };
        }

        struct Counting;

        fn add(bytes: isize) {
            let _ = LIVE.try_with(|live| {
                let now = live.get() + bytes;
                live.set(now);
                let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
            });
        }

        // SAFETY: every call is passed on to the system allocator as is.
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let ptr = unsafe { System.alloc(layout) };
                if !ptr.is_null() {
                    add(layout.size() as isize);
                }
                ptr
            }

            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                let ptr = unsafe { System.alloc_zeroed(layout) };
                if !ptr.is_null() {
                    add(layout.size() as isize);
                }
                ptr
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                unsafe { System.dealloc(ptr, layout) };
                add(-(layout.size() as isize));
            }

            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                let new = unsafe { System.realloc(ptr, layout, new_size) };
                if !new.is_null() {
                    add(new_size as isize - layout.size() as isize);
                }
                new
            }
        }

        #[global_allocator]
        static COUNTING: Counting = Counting;

        /// This thread's live heap bytes; the peak starts again from them.
        pub(super) fn live() -> isize {
            let live = LIVE.with(Cell::get);
            PEAK.with(|peak| peak.set(live));
            live
        }

        /// The most this thread held since [`live`] was last read.
        pub(super) fn peak() -> isize {
            PEAK.with(Cell::get)
        }
    }

    #[test]
    fn trim_skips_retained_blocks() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();
        store.retain_block(&first_cid).unwrap();

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
    }

    fn set_block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid, value: i64) {
        store
            .conn
            .lock()
            .execute(
                "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
                params![value, block_key(cid)],
            )
            .unwrap();
    }

    fn block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid) -> i64 {
        store
            .conn
            .lock()
            .query_row(
                "SELECT last_accessed_at FROM blocks WHERE cid = ?1",
                params![block_key(cid)],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn caches_name_records_with_ttl() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(
            store.get_name_record("example.test").unwrap().as_deref(),
            Some("/ipfs/bafkqaddwgevxmmraojswg33smq")
        );
        assert!(store.get_name_record("missing.test").unwrap().is_none());
    }

    #[test]
    fn skips_zero_ttl_name_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record("example.test", "/ipfs/bafyroot", Duration::ZERO)
            .unwrap();

        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn bounds_name_cache_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        for index in 0..(MAX_NAME_CACHE_RECORDS + 2) {
            store
                .put_name_record(
                    &format!("name-{index:03}.test"),
                    "/ipfs/bafyroot",
                    Duration::from_secs(60),
                )
                .unwrap();
        }

        let count = store
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM name_cache", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, MAX_NAME_CACHE_RECORDS);
    }

    #[test]
    fn exports_cache_as_importable_car() {
        let source = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"car export block";
        let cid = cid_from_data(CODEC_RAW, data);
        source.put_block(&cid, data).unwrap();

        let car = source.export_car().unwrap();
        let target = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let imported = target.import_car(&car).unwrap();

        assert_eq!(imported, vec![cid]);
        assert_eq!(target.get(&cid).unwrap().unwrap().data(), data);
    }

    #[test]
    fn caches_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider cache key");
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cid, &providers, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(providers));

        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: None,
                    addrs: vec![],
                }],
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn caches_empty_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"empty provider cache key");

        store
            .put_provider_records(&cid, &[], Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(Vec::new()));

        store
            .put_provider_records(&cid, &[], Duration::ZERO)
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn provider_cache_key_is_cid_representation_independent() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cidv1 = cid_from_data(freedom_ipfs_core::CODEC_DAG_PB, b"provider multihash key");
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cidv1, &providers, Duration::from_secs(60))
            .unwrap();

        assert_eq!(store.get_provider_records(&cidv0).unwrap(), Some(providers));
    }

    #[test]
    fn tracks_bad_providers_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();
        assert!(store.is_bad_provider("peer").unwrap());

        store
            .mark_bad_provider("peer", "timeout", Duration::ZERO)
            .unwrap();
        assert!(!store.is_bad_provider("peer").unwrap());
    }

    #[test]
    fn clears_provider_metadata_without_removing_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider metadata");
        store.put_block(&cid, b"provider metadata").unwrap();
        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: Some("peer".to_string()),
                    addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                }],
                Duration::from_secs(60),
            )
            .unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();

        store.clear_provider_metadata().unwrap();

        assert!(store.get(&cid).unwrap().is_some());
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
        assert!(!store.is_bad_provider("peer").unwrap());
    }
}
