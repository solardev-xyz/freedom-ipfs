//! Memory accounting by reservation (Freedom design 0007, "Budgets").
//!
//! A host that runs nodes under one memory budget gives them a
//! [`MemoryBudget`]. Every accounted allocation reserves its bytes from it
//! before it is made, and gives them back when it is freed. Nothing is
//! measured after the fact:
//!
//! - **[`MemoryBudget`]** is one counter. Reservations are atomic against
//!   it, so concurrent work can't together pass it. A reservation waits
//!   ([`MemoryBudget::reserve`]), waits up to a deadline
//!   ([`MemoryBudget::reserve_until`]) or never waits
//!   ([`MemoryBudget::try_reserve`]); one larger than the whole budget fails
//!   at once. A wait ends when its future is dropped.
//! - **[`Reservation`]** gives its bytes back when dropped.
//! - **[`ReservedVec`]** is a receive buffer that can never grow past its
//!   reservation. [`ReservedVec::into_charged`] trues it down: the
//!   allocation is first shrunk to the payload's exact size, freeing the
//!   larger backing, and only then the reservation.
//! - **[`ChargedBytes`]** is an allocation that carries its charges, shared
//!   by every clone and view of it. The charges go when its last owner drops
//!   it, after a panic or a cancel too. A small view keeps the whole
//!   allocation charged.
//! - **[`FixedPool`]** is a fixed reservation's account, such as a cache's
//!   capacity. A buffer its owner holds ([`FixedPool::hold`]) counts against
//!   it by its footprint ([`ChargedBytes::footprint`]: its backing capacity
//!   and its shared header) for the rest of the buffer's life, after the
//!   owner lets it go too, so the pool never covers more than its capacity.
//!   Once the owner's handles are the buffer's last, the owner takes over
//!   its dynamic charge, when the last other handle is dropped; until then
//!   the bytes count twice, which errs on the safe side. The fixed
//!   reservation a pool's capacity is reserved by can be tied to it
//!   ([`FixedPool::keep_lease`]): it then goes only with the last of the
//!   memory it stands for, a buffer a reader holds included. Memory that isn't
//!   one buffer, such as a cache's decoded entries and its maps, takes a
//!   [`PoolShare`] of the pool instead, sized by its owner
//!   ([`btree_map_bytes`] bounds a map's nodes).

use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why memory could not be reserved, or a buffer could not grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BudgetError {
    /// More than the whole budget: it could never be had.
    #[error("a reservation of {bytes} bytes exceeds the whole memory budget of {limit} bytes")]
    TooLarge { bytes: usize, limit: usize },
    /// The budget was full: at once for a reservation that doesn't wait, or
    /// still at its deadline.
    #[error("the memory budget is exhausted: {bytes} bytes could not be reserved")]
    Exhausted { bytes: usize },
    /// A buffer would grow past what it reserved.
    #[error("{bytes} bytes exceed the {reserved} bytes reserved for them")]
    ExceedsReservation { bytes: usize, reserved: usize },
}

/// A process-wide memory budget: one counter of bytes that every
/// reservation is taken from, atomically. Clones share it.
#[derive(Clone)]
pub struct MemoryBudget {
    inner: Arc<BudgetInner>,
}

struct BudgetInner {
    limit: usize,
    permits: Arc<Semaphore>,
}

impl fmt::Debug for MemoryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBudget")
            .field("limit", &self.limit())
            .field("used", &self.used())
            .finish()
    }
}

impl MemoryBudget {
    /// A budget of `limit` bytes (at most [`Semaphore::MAX_PERMITS`]).
    pub fn new(limit: usize) -> Self {
        let limit = limit.min(Semaphore::MAX_PERMITS);
        Self {
            inner: Arc::new(BudgetInner {
                limit,
                permits: Arc::new(Semaphore::new(limit)),
            }),
        }
    }

    /// The budget's size in bytes.
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    /// Bytes reserved now.
    pub fn used(&self) -> usize {
        self.inner
            .limit
            .saturating_sub(self.inner.permits.available_permits())
    }

    /// Bytes that can be reserved now.
    pub fn available(&self) -> usize {
        self.inner.permits.available_permits()
    }

    fn admissible(&self, bytes: usize) -> Result<u32, BudgetError> {
        let too_large = BudgetError::TooLarge {
            bytes,
            limit: self.inner.limit,
        };
        if bytes > self.inner.limit {
            return Err(too_large);
        }
        u32::try_from(bytes).map_err(|_| too_large)
    }

    /// Reserves `bytes`, waiting until they are free. A reservation larger
    /// than the whole budget fails at once; dropping the future ends the
    /// wait, taking nothing. Waiters are served in order, so a large
    /// reservation isn't starved by smaller ones.
    pub async fn reserve(&self, bytes: usize) -> Result<Reservation, BudgetError> {
        if bytes == 0 {
            return Ok(Reservation::empty());
        }
        let count = self.admissible(bytes)?;
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_many_owned(count)
            .await
            .map_err(|_| BudgetError::Exhausted { bytes })?;
        Ok(Reservation {
            permit: Some(permit),
        })
    }

    /// Reserves `bytes`, waiting at most until `deadline`:
    /// [`BudgetError::Exhausted`] if they aren't free by then.
    pub async fn reserve_until(
        &self,
        bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<Reservation, BudgetError> {
        match tokio::time::timeout_at(deadline, self.reserve(bytes)).await {
            Ok(reserved) => reserved,
            Err(_) => Err(BudgetError::Exhausted { bytes }),
        }
    }

    /// Reserves `bytes` if they are free now; never waits.
    pub fn try_reserve(&self, bytes: usize) -> Result<Reservation, BudgetError> {
        if bytes == 0 {
            return Ok(Reservation::empty());
        }
        let count = self.admissible(bytes)?;
        let permit = self
            .inner
            .permits
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| BudgetError::Exhausted { bytes })?;
        Ok(Reservation {
            permit: Some(permit),
        })
    }
}

/// Bytes reserved from a [`MemoryBudget`], given back when dropped.
#[must_use = "the bytes are reserved only while the reservation lives"]
pub struct Reservation {
    permit: Option<OwnedSemaphorePermit>,
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation")
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl Reservation {
    /// A reservation of nothing.
    pub fn empty() -> Self {
        Self { permit: None }
    }

    /// The bytes it holds.
    pub fn bytes(&self) -> usize {
        self.permit
            .as_ref()
            .map_or(0, OwnedSemaphorePermit::num_permits)
    }

    /// Takes `bytes` of it into a reservation of their own; `None`, taking
    /// nothing, if it holds fewer.
    pub fn split(&mut self, bytes: usize) -> Option<Reservation> {
        let held = self.bytes();
        if bytes > held {
            return None;
        }
        if bytes == 0 {
            return Some(Self::empty());
        }
        if bytes == held {
            return Some(Self {
                permit: self.permit.take(),
            });
        }
        let permit = self.permit.as_mut()?.split(bytes)?;
        Some(Self {
            permit: Some(permit),
        })
    }

    /// Adds `other` to it, if both are of the same budget; another budget's
    /// is given back.
    pub fn merge(&mut self, other: Reservation) {
        let Some(other) = other.permit else {
            return;
        };
        match self.permit.as_mut() {
            None => self.permit = Some(other),
            Some(permit) if Arc::ptr_eq(permit.semaphore(), other.semaphore()) => {
                permit.merge(other);
            }
            Some(_) => drop(other),
        }
    }

    /// Gives back all but `bytes` of it; nothing if it holds no more.
    pub fn shrink_to(&mut self, bytes: usize) {
        let held = self.bytes();
        if bytes >= held {
            return;
        }
        if bytes == 0 {
            self.permit = None;
            return;
        }
        if let Some(permit) = self.permit.as_mut() {
            drop(permit.split(held - bytes));
        }
    }
}

/// A receive buffer bound to its reservation: its allocation never grows
/// past the reserved bytes.
pub struct ReservedVec {
    data: Vec<u8>,
    reservation: Reservation,
}

impl fmt::Debug for ReservedVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservedVec")
            .field("len", &self.data.len())
            .field("capacity", &self.data.capacity())
            .field("reserved", &self.reservation.bytes())
            .finish()
    }
}

impl ReservedVec {
    /// An empty buffer that may grow to what `reservation` holds.
    pub fn new(reservation: Reservation) -> Self {
        Self {
            data: Vec::new(),
            reservation,
        }
    }

    /// `len` zeroed bytes in an allocation of exactly `len`, which must fit
    /// in `reservation`.
    pub fn zeroed(len: usize, reservation: Reservation) -> Result<Self, BudgetError> {
        if len > reservation.bytes() {
            return Err(BudgetError::ExceedsReservation {
                bytes: len,
                reserved: reservation.bytes(),
            });
        }
        Ok(Self {
            data: vec![0; len],
            reservation,
        })
    }

    /// The most bytes it may hold: what it reserved.
    pub fn limit(&self) -> usize {
        self.reservation.bytes()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The allocation's size now: never more than [`ReservedVec::limit`].
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Appends `bytes`, growing the allocation (by doubling, as a `Vec`
    /// does) but never past the reservation: an error, appending nothing,
    /// if the bytes wouldn't fit in it.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BudgetError> {
        let limit = self.limit();
        let needed = self.data.len().saturating_add(bytes.len());
        if needed > limit {
            return Err(BudgetError::ExceedsReservation {
                bytes: needed,
                reserved: limit,
            });
        }
        if needed > self.data.capacity() {
            let target = needed
                .max(self.data.capacity().saturating_mul(2))
                .min(limit);
            self.data.reserve_exact(target - self.data.len());
        }
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    /// Trues the buffer down and hands it on charged: the allocation is
    /// shrunk to the payload's exact size first, which frees the larger
    /// backing, and only then is the reservation shrunk to the new
    /// allocation, so the bytes are never undercharged.
    pub fn into_charged(self) -> ChargedBytes {
        let Self {
            mut data,
            mut reservation,
        } = self;
        data.shrink_to_fit();
        reservation.shrink_to(data.capacity());
        ChargedBytes::from_parts(data, Some(reservation))
    }
}

/// Bytes in an allocation that carries its memory charges: clones and views
/// share the allocation, and its charges go when the last of them is
/// dropped. A view keeps the whole allocation, and so its whole charge.
#[derive(Clone)]
pub struct ChargedBytes {
    buffer: Arc<Buffer>,
    start: usize,
    end: usize,
}

struct Buffer {
    data: Vec<u8>,
    charges: Mutex<Charges>,
}

#[derive(Default)]
struct Charges {
    /// The dynamic reservation that pays for the allocation, if any.
    dynamic: Option<Reservation>,
    /// The fixed pool that covers it for the rest of its life, if any.
    cover: Option<Cover>,
    /// How many handles the covering pool's owner holds ([`HeldBytes`]).
    holds: usize,
}

impl ChargedBytes {
    fn from_parts(data: Vec<u8>, dynamic: Option<Reservation>) -> Self {
        let end = data.len();
        Self {
            buffer: Arc::new(Buffer {
                data,
                charges: Mutex::new(Charges {
                    dynamic,
                    cover: None,
                    holds: 0,
                }),
            }),
            start: 0,
            end,
        }
    }

    /// Bytes no budget charges: those of an allocation the caller doesn't
    /// account, or one a fixed pool will cover.
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self::from_parts(data, None)
    }

    /// A copy of `bytes`, uncharged, in an allocation of their exact size.
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Self::from_vec(bytes.to_vec())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer.data[self.start..self.end]
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// A view of bytes `start..end` of this view (clamped to it), sharing
    /// the allocation and its charges.
    pub fn slice(&self, start: usize, end: usize) -> Self {
        let len = self.len();
        let end = end.min(len);
        let start = start.min(end);
        Self {
            buffer: self.buffer.clone(),
            start: self.start + start,
            end: self.start + end,
        }
    }

    /// The view of `subset`, if it lies within these bytes: as for a field
    /// a decoder handed out from them without copying.
    pub fn slice_ref(&self, subset: &[u8]) -> Option<Self> {
        let base = self.as_slice().as_ptr() as usize;
        let begin = subset.as_ptr() as usize;
        let offset = begin.checked_sub(base)?;
        let end = offset.checked_add(subset.len())?;
        (end <= self.len()).then(|| self.slice(offset, end))
    }

    /// The allocation's size, which is what its charges count.
    pub fn backing_capacity(&self) -> usize {
        self.buffer.data.capacity()
    }

    /// The heap an allocation takes besides its bytes' backing: the header
    /// its clones and views share, with its charges.
    pub const HEADER_BYTES: usize =
        std::mem::size_of::<Buffer>() + 2 * std::mem::size_of::<usize>();

    /// All the heap the allocation takes: its backing capacity and its
    /// header. What a [`FixedPool`] covers it by.
    pub fn footprint(&self) -> usize {
        self.backing_capacity().saturating_add(Self::HEADER_BYTES)
    }

    /// The bytes of dynamic reservation the allocation still holds.
    pub fn charged_bytes(&self) -> usize {
        self.charges()
            .dynamic
            .as_ref()
            .map_or(0, Reservation::bytes)
    }

    /// Whether a fixed pool covers the allocation.
    pub fn is_covered(&self) -> bool {
        self.charges().cover.is_some()
    }

    /// Whether this is the allocation's last owner: no other clone or view
    /// of it, and no [`bytes::Bytes`] made from one, is alive.
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.buffer) == 1
    }

    /// Frees the allocation and hands back the dynamic reservation that paid
    /// for it, for the caller's next allocation: only from its last handle,
    /// and only if no pool covers it. Otherwise the bytes come back.
    pub fn reclaim(self) -> Result<Reservation, ChargedBytes> {
        if !self.is_unique() {
            return Err(self);
        }
        let mut charges = self.charges();
        if charges.cover.is_some() {
            drop(charges);
            return Err(self);
        }
        let reservation = charges.dynamic.take().unwrap_or_else(Reservation::empty);
        drop(charges);
        // The last handle: this frees the allocation before the reservation
        // is handed on.
        drop(self);
        Ok(reservation)
    }

    /// The bytes as [`bytes::Bytes`], without copying: they keep the
    /// allocation, and its charges, until their last clone is dropped.
    pub fn to_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self.clone())
    }

    /// A copy of the bytes, which no budget charges.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    fn charges(&self) -> MutexGuard<'_, Charges> {
        self.buffer
            .charges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for ChargedBytes {
    /// A handle going: if the only handles left are those of the pool owner
    /// that covers the allocation, the owner takes over the dynamic charge.
    fn drop(&mut self) {
        let remaining = Arc::strong_count(&self.buffer).saturating_sub(1);
        if remaining == 0 {
            return;
        }
        // Never wait here; a sweep of the owner's catches a missed one.
        if let Ok(mut charges) = self.buffer.charges.try_lock() {
            if charges.cover.is_some() && charges.holds > 0 && charges.holds == remaining {
                charges.dynamic = None;
            }
        }
    }
}

impl Deref for ChargedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl PartialEq for ChargedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for ChargedBytes {}

impl PartialEq<[u8]> for ChargedBytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<&[u8]> for ChargedBytes {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for ChargedBytes {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for ChargedBytes {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == *other
    }
}

impl PartialEq<Vec<u8>> for ChargedBytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl fmt::Debug for ChargedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChargedBytes")
            .field("len", &self.len())
            .field("backing_capacity", &self.backing_capacity())
            .field("charged", &self.charged_bytes())
            .finish()
    }
}

impl From<Vec<u8>> for ChargedBytes {
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}

/// A fixed reservation's account: the buffers it covers count against its
/// capacity by their footprint ([`ChargedBytes::footprint`]), for the rest
/// of their lives. Clones share it.
#[derive(Clone)]
pub struct FixedPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    capacity: usize,
    used: AtomicUsize,
    /// The fixed reservation its capacity is reserved by, if its owner gave
    /// it one ([`FixedPool::keep_lease`]).
    lease: OnceLock<Arc<Reservation>>,
}

/// A pool's cover of one allocation, released when the allocation is freed.
struct Cover {
    pool: Arc<PoolInner>,
    bytes: usize,
}

impl Drop for Cover {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl fmt::Debug for FixedPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedPool")
            .field("capacity", &self.capacity())
            .field("used", &self.used())
            .finish()
    }
}

impl FixedPool {
    /// A pool of `capacity` bytes, which its owner reserves as fixed.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                capacity,
                used: AtomicUsize::new(0),
                lease: OnceLock::new(),
            }),
        }
    }

    /// A pool of no capacity, for a cache that is gone (a closed store's):
    /// it covers nothing, takes no lease, and allocates nothing, as every
    /// one of them is the same pool.
    pub fn none() -> Self {
        static NONE: OnceLock<FixedPool> = OnceLock::new();
        NONE.get_or_init(|| {
            let pool = FixedPool::new(0);
            // Taken already, by a lease of nothing: no other is ever kept.
            pool.keep_lease(Arc::new(Reservation::empty()));
            pool
        })
        .clone()
    }

    /// The heap a pool takes itself: its counter, in its `Arc`. A cache
    /// counts it with its own structures.
    pub const HEAP_BYTES: usize =
        std::mem::size_of::<PoolInner>() + 2 * std::mem::size_of::<usize>();

    /// Ties `lease`, the fixed reservation the pool's capacity is reserved
    /// by, to the memory it stands for: it is kept for as long as the pool,
    /// a clone or share of it, or any allocation it covers lives (a block a
    /// reader still holds after its cache is gone included), and goes with
    /// the last of them. A pool keeps the first lease it is given: whether
    /// it kept this one.
    pub fn keep_lease(&self, lease: Arc<Reservation>) -> bool {
        self.inner.lease.set(lease).is_ok()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// The footprint of every live allocation it covers, those its
    /// owner has let go of but others still hold included.
    pub fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }

    /// Whether this pool covers `bytes`' allocation.
    pub fn covers(&self, bytes: &ChargedBytes) -> bool {
        bytes
            .charges()
            .cover
            .as_ref()
            .is_some_and(|cover| Arc::ptr_eq(&cover.pool, &self.inner))
    }

    /// Holds `bytes` for the pool's owner, a cache: covers the allocation
    /// for the rest of its life if its footprint fits in what is left
    /// of the pool (or this pool covers it already). `None` if it doesn't
    /// fit, or another pool covers it.
    pub fn hold(&self, bytes: &ChargedBytes) -> Option<HeldBytes> {
        let mut charges = bytes.charges();
        match charges.cover.as_ref() {
            Some(cover) if !Arc::ptr_eq(&cover.pool, &self.inner) => return None,
            Some(_) => {}
            None => {
                let needed = bytes.footprint();
                let capacity = self.inner.capacity;
                self.inner
                    .used
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        used.checked_add(needed).filter(|total| *total <= capacity)
                    })
                    .ok()?;
                charges.cover = Some(Cover {
                    pool: self.inner.clone(),
                    bytes: needed,
                });
            }
        }
        charges.holds += 1;
        drop(charges);
        Some(HeldBytes {
            bytes: bytes.clone(),
        })
    }
}

impl FixedPool {
    /// Holds a copy of `len` bytes for the pool's owner: room for its
    /// footprint (`len` and a header) is taken in the pool first, and only
    /// then is the copy allocated, at exactly its size, so no copy is ever
    /// made that the pool doesn't cover.
    /// `None`, allocating nothing, if it doesn't fit; `copy` is what makes
    /// the copy, called only once the room is taken.
    pub fn hold_copy(&self, len: usize, copy: impl FnOnce() -> Vec<u8>) -> Option<HeldBytes> {
        let capacity = self.inner.capacity;
        let footprint = len.saturating_add(ChargedBytes::HEADER_BYTES);
        self.inner
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(footprint)
                    .filter(|total| *total <= capacity)
            })
            .ok()?;
        let room = Cover {
            pool: self.inner.clone(),
            bytes: footprint,
        };
        let mut data = copy();
        if data.capacity() > len {
            // Never more than the room taken: shrink to it (or give up).
            data.shrink_to_fit();
            if data.capacity() > len {
                return None;
            }
        }
        let bytes = ChargedBytes::from_vec(data);
        {
            let mut charges = bytes.charges();
            charges.cover = Some(room);
            charges.holds = 1;
        }
        Some(HeldBytes { bytes })
    }
}

/// A pool owner's handle on bytes its pool covers: what a cache keeps.
pub struct HeldBytes {
    bytes: ChargedBytes,
}

impl HeldBytes {
    /// The bytes, to hand out clones of.
    pub fn bytes(&self) -> &ChargedBytes {
        &self.bytes
    }

    /// Takes over the allocation's dynamic charge if the owner's handles are
    /// its last: the sweep behind the takeover on drop. Returns whether
    /// there was a charge to take over.
    pub fn take_over_if_only_held(&self) -> bool {
        let count = Arc::strong_count(&self.bytes.buffer);
        let mut charges = self.bytes.charges();
        if charges.holds < count {
            return false;
        }
        charges.dynamic.take().is_some()
    }
}

impl Drop for HeldBytes {
    fn drop(&mut self) {
        let mut charges = self.bytes.charges();
        charges.holds = charges.holds.saturating_sub(1);
    }
}

impl fmt::Debug for HeldBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("HeldBytes").field(&self.bytes).finish()
    }
}

impl FixedPool {
    /// A share of `bytes` of the pool, for memory its owner sizes itself:
    /// a cache's decoded entries and its maps, which are no one buffer.
    /// `None` if it doesn't fit in what is left of the pool. The share's
    /// bytes come back when it is dropped or shrunk.
    pub fn share(&self, bytes: usize) -> Option<PoolShare> {
        let mut share = PoolShare {
            cover: Cover {
                pool: self.inner.clone(),
                bytes: 0,
            },
        };
        share.try_grow(bytes).then_some(share)
    }
}

/// A share of a [`FixedPool`] whose owner sizes it: room is taken
/// ([`PoolShare::try_grow`]) before what it pays for is allocated, and
/// given back ([`PoolShare::shrink`]) once that is freed.
pub struct PoolShare {
    cover: Cover,
}

impl PoolShare {
    pub fn bytes(&self) -> usize {
        self.cover.bytes
    }

    /// Grows the share by `bytes` if the pool has room for them; otherwise
    /// takes nothing.
    pub fn try_grow(&mut self, bytes: usize) -> bool {
        let capacity = self.cover.pool.capacity;
        let taken = self
            .cover
            .pool
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= capacity)
            })
            .is_ok();
        if taken {
            self.cover.bytes += bytes;
        }
        taken
    }

    /// Gives `bytes` of the share back to the pool (at most all of it).
    pub fn shrink(&mut self, bytes: usize) {
        let bytes = bytes.min(self.cover.bytes);
        self.cover.bytes -= bytes;
        self.cover.pool.used.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Sizes the share to `bytes`: grows it if the pool has room (`false`,
    /// taking nothing, if it hasn't), or shrinks it.
    pub fn try_resize(&mut self, bytes: usize) -> bool {
        match bytes.checked_sub(self.cover.bytes) {
            Some(more) => self.try_grow(more),
            None => {
                self.shrink(self.cover.bytes - bytes);
                true
            }
        }
    }
}

impl fmt::Debug for PoolShare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PoolShare").field(&self.bytes()).finish()
    }
}

/// The most heap a `std` `BTreeMap<K, V>` of `len` entries holds: its
/// nodes, by `std`'s layout. A node holds at most 11 entries and, the root
/// excepted, at least 5 (insertion splits a full node in two of at least 5;
/// removal merges or refills a node that falls below 5), so `len` entries
/// take at most `1 + (len - 1) / 5` nodes; an emptied map may keep its
/// root, so even none take one. Each is at most an internal node: 11
/// `(K, V)` slots, 12 child pointers, its parent pointer, two `u16`s and
/// padding. What the keys and values own on the heap isn't in it. The bound
/// grows with `len` and never depends on the map's history, so room taken
/// for `len + 1` entries before an insertion covers it.
pub fn btree_map_bytes<K, V>(len: usize) -> usize {
    let nodes = 1 + len.saturating_sub(1) / 5;
    let word = std::mem::size_of::<usize>();
    let node = 11 * (std::mem::size_of::<K>() + std::mem::size_of::<V>())
        + 13 * word
        + 4
        + std::mem::align_of::<K>()
        + std::mem::align_of::<V>()
        + word;
    nodes.saturating_mul(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn concurrent_reservations_never_together_pass_the_budget() {
        let runtime = runtime();
        let budget = MemoryBudget::new(10_000);
        let held = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let over = Arc::new(AtomicBool::new(false));
        let record = {
            let (held, peak, over, limit) =
                (held.clone(), peak.clone(), over.clone(), budget.limit());
            move |bytes: usize| {
                let now = held.fetch_add(bytes, Ordering::SeqCst) + bytes;
                peak.fetch_max(now, Ordering::SeqCst);
                if now > limit {
                    over.store(true, Ordering::SeqCst);
                }
            }
        };
        // Waiting reservers on the runtime, non-waiting ones on threads.
        let tasks: Vec<_> = (0..16)
            .map(|n| {
                let (budget, held, record) = (budget.clone(), held.clone(), record.clone());
                runtime.spawn(async move {
                    for round in 0..200 {
                        let bytes = 1 + (n * 977 + round * 131) % 3_000;
                        let reservation = budget.reserve(bytes).await.unwrap();
                        record(reservation.bytes());
                        tokio::task::yield_now().await;
                        held.fetch_sub(reservation.bytes(), Ordering::SeqCst);
                        drop(reservation);
                    }
                })
            })
            .collect();
        let threads: Vec<_> = (0..4)
            .map(|n| {
                let (budget, held, record) = (budget.clone(), held.clone(), record.clone());
                std::thread::spawn(move || {
                    for round in 0..2_000 {
                        let bytes = 1 + (n * 409 + round * 61) % 4_000;
                        if let Ok(reservation) = budget.try_reserve(bytes) {
                            record(reservation.bytes());
                            std::thread::yield_now();
                            held.fetch_sub(reservation.bytes(), Ordering::SeqCst);
                            drop(reservation);
                        }
                    }
                })
            })
            .collect();
        for task in tasks {
            runtime.block_on(task).unwrap();
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(
            !over.load(Ordering::SeqCst),
            "peak {} over {}",
            peak.load(Ordering::SeqCst),
            budget.limit()
        );
        assert!(peak.load(Ordering::SeqCst) > 0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_reservation_larger_than_the_budget_fails_at_once() {
        let runtime = runtime();
        let budget = MemoryBudget::new(1_000);
        let _held = budget.try_reserve(600).unwrap();
        let too_large = BudgetError::TooLarge {
            bytes: 1_001,
            limit: 1_000,
        };
        assert_eq!(budget.try_reserve(1_001).unwrap_err(), too_large);
        // Even a waiting reservation: it returns at once, never waits.
        let reserved = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), budget.reserve(1_001)).await
        });
        assert_eq!(reserved.unwrap().unwrap_err(), too_large);
        // One that fits the budget, but not what is free, fails only when
        // it doesn't wait.
        assert_eq!(
            budget.try_reserve(500).unwrap_err(),
            BudgetError::Exhausted { bytes: 500 }
        );
        assert_eq!(budget.used(), 600);
    }

    #[test]
    fn a_wait_ends_when_dropped_or_at_its_deadline_taking_nothing() {
        let runtime = runtime();
        let budget = MemoryBudget::new(1_000);
        let held = budget.try_reserve(1_000).unwrap();
        runtime.block_on(async {
            // Dropped: the wait ends, and takes nothing.
            let waiting =
                tokio::time::timeout(Duration::from_millis(50), budget.reserve(400)).await;
            assert!(waiting.is_err(), "it waits while the budget is full");
            // At its deadline.
            let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
            assert_eq!(
                budget.reserve_until(400, deadline).await.unwrap_err(),
                BudgetError::Exhausted { bytes: 400 }
            );
            assert!(tokio::time::Instant::now() >= deadline);
        });
        assert_eq!(budget.used(), 1_000);
        drop(held);
        assert_eq!(budget.used(), 0);
        // Once free, a waiting reservation is served.
        let held = budget.try_reserve(1_000).unwrap();
        let waiter = {
            let budget = budget.clone();
            runtime.spawn(async move { budget.reserve(400).await.map(|r| r.bytes()) })
        };
        std::thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished());
        drop(held);
        assert_eq!(runtime.block_on(waiter).unwrap(), Ok(400));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn reservations_split_merge_and_come_back_from_freed_buffers() {
        let budget = MemoryBudget::new(10_000);
        let mut pool = budget.try_reserve(6_000).unwrap();
        let part = pool.split(2_500).unwrap();
        assert_eq!((pool.bytes(), part.bytes()), (3_500, 2_500));
        assert!(pool.split(3_501).is_none());
        assert_eq!(pool.bytes(), 3_500, "a failed split takes nothing");
        // A buffer paid by the part gives it back once freed.
        let buffer = ReservedVec::zeroed(2_500, part).unwrap().into_charged();
        let view = buffer.clone();
        let buffer = buffer.reclaim().unwrap_err();
        drop(view);
        let part = buffer.reclaim().unwrap();
        assert_eq!(part.bytes(), 2_500);
        pool.merge(part);
        assert_eq!(pool.bytes(), 6_000);
        assert_eq!(budget.used(), 6_000);
        // Another budget's reservation isn't merged: it is given back.
        let other = MemoryBudget::new(1_000);
        pool.merge(other.try_reserve(500).unwrap());
        assert_eq!((pool.bytes(), other.used()), (6_000, 0));
        let all = pool.split(6_000).unwrap();
        assert_eq!((pool.bytes(), all.bytes()), (0, 6_000));
    }

    #[test]
    fn a_receive_buffer_never_grows_past_its_reservation() {
        let budget = MemoryBudget::new(1 << 20);
        let mut buffer = ReservedVec::new(budget.try_reserve(10_000).unwrap());
        let chunk = [7u8; 1_500];
        for _ in 0..6 {
            buffer.extend_from_slice(&chunk).unwrap();
            assert!(buffer.capacity() <= 10_000, "{buffer:?}");
        }
        // 9,000 bytes; one more chunk would be 10,500.
        assert_eq!(
            buffer.extend_from_slice(&chunk).unwrap_err(),
            BudgetError::ExceedsReservation {
                bytes: 10_500,
                reserved: 10_000
            }
        );
        assert_eq!(buffer.len(), 9_000);
        assert!(buffer.capacity() <= 10_000);
        assert_eq!(budget.used(), 10_000);
        assert!(ReservedVec::zeroed(10_001, budget.try_reserve(10_000).unwrap()).is_err());
    }

    #[test]
    fn truing_down_releases_exactly_the_difference_after_the_backing_shrinks() {
        let budget = MemoryBudget::new(1 << 20);
        let mut buffer = ReservedVec::new(budget.try_reserve(64 * 1024).unwrap());
        buffer.extend_from_slice(&[1u8; 3_000]).unwrap();
        buffer.extend_from_slice(&[2u8; 1_000]).unwrap();
        assert!(buffer.capacity() > 4_000, "a doubled backing: {buffer:?}");
        assert_eq!(budget.used(), 64 * 1024);
        let charged = buffer.into_charged();
        // The allocation is the payload's exact size, and so is the charge.
        assert_eq!(charged.len(), 4_000);
        assert_eq!(charged.backing_capacity(), 4_000);
        assert_eq!(charged.charged_bytes(), 4_000);
        assert_eq!(budget.used(), 4_000);

        // A view can't be trued down: it keeps its whole allocation charged.
        let view = charged.slice(10, 20);
        drop(charged);
        assert_eq!(view.len(), 10);
        assert_eq!(view.charged_bytes(), 4_000);
        assert_eq!(budget.used(), 4_000);
        drop(view);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_charge_goes_with_the_last_owner_after_clones_and_a_panic() {
        let budget = MemoryBudget::new(1 << 20);
        let buffer = ReservedVec::zeroed(1_000, budget.try_reserve(1_000).unwrap()).unwrap();
        let charged = buffer.into_charged();
        let clone = charged.clone();
        let bytes = charged.to_bytes();
        let view = charged.slice(100, 200);
        drop(charged);
        drop(clone);
        assert_eq!(budget.used(), 1_000, "the bytes and the view still hold it");
        drop(bytes);
        assert_eq!(budget.used(), 1_000);
        // The last owner unwinds.
        let panicked = std::thread::spawn(move || {
            let _owner = view;
            panic!("an owner's thread panics");
        })
        .join();
        assert!(panicked.is_err());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_fixed_pool_covers_for_life_and_takes_over_only_from_the_last_owner() {
        let budget = MemoryBudget::new(1 << 20);
        const H: usize = ChargedBytes::HEADER_BYTES;
        let pool = FixedPool::new(10_000);
        let fetched = ReservedVec::zeroed(4_000, budget.try_reserve(4_000).unwrap())
            .unwrap()
            .into_charged();
        let reader = fetched.clone();
        let held = pool.hold(&fetched).unwrap();
        let again = pool.hold(&fetched).unwrap();
        // A buffer counts by its footprint: its backing and its header.
        assert_eq!(fetched.footprint(), 4_000 + H);
        assert_eq!(pool.used(), 4_000 + H, "held twice, covered once");
        drop(again);
        // While others hold it, it counts twice.
        assert!(!held.take_over_if_only_held());
        drop(fetched);
        assert_eq!(budget.used(), 4_000);
        // The last other handle goes: the owner takes the charge over.
        drop(reader);
        assert_eq!(budget.used(), 0);
        assert_eq!(pool.used(), 4_000 + H);
        // Handed out again and let go by the owner, it stays covered until
        // it is freed.
        let reader = held.bytes().clone();
        drop(held);
        assert_eq!(pool.used(), 4_000 + H);
        drop(reader);
        assert_eq!(pool.used(), 0);

        // The sweep takes over too, once the owner's handles are the last.
        let fetched = ReservedVec::zeroed(1_000, budget.try_reserve(1_000).unwrap())
            .unwrap()
            .into_charged();
        let held = pool.hold(&fetched).unwrap();
        assert!(!held.take_over_if_only_held());
        let buffer = fetched.buffer.clone();
        let lock = buffer.charges.lock().unwrap();
        // A drop that finds the charges busy leaves the charge in place...
        let dropped = std::thread::spawn(move || drop(fetched));
        dropped.join().unwrap();
        drop(lock);
        drop(buffer);
        assert_eq!(budget.used(), 1_000);
        // ...and the sweep takes it over.
        assert!(held.take_over_if_only_held());
        assert_eq!(budget.used(), 0);
        drop(held);

        // A copy is made only once its room is taken.
        let mut copies = 0;
        let full = FixedPool::new(100 + H);
        let _filled = full
            .hold_copy(100, || {
                copies += 1;
                vec![1; 100]
            })
            .unwrap();
        assert!(full
            .hold_copy(1, || {
                copies += 1;
                vec![2]
            })
            .is_none());
        assert_eq!((copies, full.used()), (1, 100 + H));

        // What doesn't fit isn't held; another pool's buffer isn't either.
        let big = ChargedBytes::from_vec(vec![0; 10_001 - H]);
        assert!(pool.hold(&big).is_none());
        let other = FixedPool::new(10_000);
        let small = ChargedBytes::from_vec(vec![0; 100]);
        let _other_held = other.hold(&small).unwrap();
        assert!(pool.hold(&small).is_none());
        assert!(small.is_covered() && !pool.covers(&small) && other.covers(&small));
    }

    #[test]
    fn a_pools_lease_goes_only_with_the_last_of_its_memory() {
        let budget = MemoryBudget::new(1 << 20);
        let lease = Arc::new(budget.try_reserve(10_000).unwrap());
        let pool = FixedPool::new(10_000);
        assert!(pool.keep_lease(lease.clone()));
        assert!(!pool.keep_lease(Arc::new(budget.try_reserve(1).unwrap())));
        assert_eq!(budget.used(), 10_000, "a second lease isn't kept");
        let share = pool.share(100).unwrap();
        let held = pool.hold(&ChargedBytes::from_vec(vec![1; 1_000])).unwrap();
        let reader = held.bytes().clone();
        // The owner lets go of the lease, the pool and its cache's handle.
        drop((lease, pool, held));
        assert_eq!(budget.used(), 10_000, "a share still stands for it");
        drop(share);
        assert_eq!(
            budget.used(),
            10_000,
            "a reader still holds a covered buffer"
        );
        drop(reader);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn one_lease_wins_a_race_and_its_shares_and_views_release_it_once() {
        use std::sync::Barrier;
        let budget = MemoryBudget::new(8 * 4096);
        let pool = FixedPool::new(4096);
        let share = pool.share(0).unwrap();
        let data = ChargedBytes::from_vec(vec![7; 100]);
        let held = pool.hold(&data).unwrap();
        let view = held.bytes().to_bytes();
        drop(data);
        let barrier = Arc::new(Barrier::new(4));
        let racers: Vec<_> = (0..4)
            .map(|_| {
                let (budget, pool, barrier) = (budget.clone(), pool.clone(), barrier.clone());
                std::thread::spawn(move || {
                    let lease = Arc::new(budget.try_reserve(4096).unwrap());
                    barrier.wait();
                    pool.keep_lease(lease)
                })
            })
            .collect();
        let winners = racers
            .into_iter()
            .map(|racer| usize::from(racer.join().unwrap()))
            .sum::<usize>();
        assert_eq!(winners, 1);
        assert_eq!(budget.used(), 4096, "the losing leases go at once");
        drop((held, pool));
        assert_eq!(budget.used(), 4096);
        drop(view);
        assert_eq!(budget.used(), 4096, "an empty share still owns the pool");
        drop(share);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn the_pool_of_nothing_holds_nothing_and_keeps_no_lease() {
        let budget = MemoryBudget::new(1 << 20);
        let none = FixedPool::none();
        assert!(
            Arc::ptr_eq(&none.inner, &FixedPool::none().inner),
            "one for all"
        );
        assert!(!none.keep_lease(Arc::new(budget.try_reserve(100).unwrap())));
        assert_eq!(budget.used(), 0);
        assert!(none.hold(&ChargedBytes::from_vec(vec![1])).is_none());
        assert!(none.share(1).is_none());
        let empty = none.share(0).unwrap();
        assert_eq!((none.capacity(), none.used(), empty.bytes()), (0, 0, 0));
    }

    #[test]
    fn a_pool_share_takes_room_first_and_gives_it_back() {
        let pool = FixedPool::new(1_000);
        let mut share = pool.share(600).unwrap();
        assert!(pool.share(401).is_none(), "nothing past the capacity");
        let held = pool
            .hold(&ChargedBytes::from_vec(
                vec![0; 300 - ChargedBytes::HEADER_BYTES],
            ))
            .unwrap();
        assert_eq!(pool.used(), 900);
        assert!(!share.try_grow(101));
        assert_eq!(
            (share.bytes(), pool.used()),
            (600, 900),
            "a refusal takes nothing"
        );
        assert!(share.try_resize(700));
        assert!(!share.try_resize(701));
        share.shrink(200);
        assert_eq!((share.bytes(), pool.used()), (500, 800));
        share.shrink(10_000);
        assert_eq!((share.bytes(), pool.used()), (0, 300));
        assert!(share.try_resize(400));
        drop(share);
        assert_eq!(pool.used(), 300);
        drop(held);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn a_view_of_a_decoded_field_is_found_in_its_buffer() {
        let charged = ChargedBytes::from_vec((0..100u8).collect());
        let bytes = charged.to_bytes();
        let field = bytes.slice(10..30);
        let view = charged.slice_ref(&field).unwrap();
        assert_eq!(view.as_slice(), &field[..]);
        assert!(charged.slice_ref(&[1, 2, 3]).is_none());
    }
}
