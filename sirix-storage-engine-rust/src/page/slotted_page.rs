//! Slotted page implementation matching SirixDB's `KeyValueLeafPage`.
//!
//! On-disk format (V0 - immutable):
//! ```text
//! Offset   Size       Field
//! ──────── ───────── ─────────────────────────
//! 0        8         recordPageKey (i64)
//! 8        4         revision (i32)
//! 12       2         populatedCount (u16)
//! 14       4         heapEnd (i32)
//! 18       4         heapUsed (i32)
//! 22       1         indexType (u8)
//! 23       1         flags (u8)
//! 24       8         reserved
//! 32       128       slotBitmap (16 × u64)
//! 160      ---       (end of on-disk header+bitmap)
//! ```
//!
//! Runtime layout extends the on-disk format:
//! ```text
//! 160      128       preservationBitmap (runtime only)
//! 288      8192      slotDirectory (1024 × 8 bytes)
//! 8480     ...       heap (bump-allocated records)
//! ```

use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use crate::constants::BITMAP_SIZE;
use crate::constants::BITMAP_WORDS;
use crate::constants::DIR_ENTRY_SIZE;
use crate::constants::DISK_HEADER_BITMAP_SIZE;
use crate::constants::FLAG_DEWEY_IDS_STORED;
use crate::constants::HEADER_SIZE;
use crate::constants::HEAP_START;
use crate::constants::INITIAL_PAGE_SIZE;
use crate::constants::MAX_RECORD_SIZE;
use crate::constants::PRESERVATION_BITMAP_SIZE;
use crate::constants::SLOT_COUNT;
use crate::constants::STATE_CLOSED_BIT;
use crate::constants::STATE_HOT_BIT;
use crate::error::Result;
use crate::error::StorageError;
use crate::types::IndexType;

/// Slot directory entry: packed into 8 bytes.
///
/// ```text
/// Bytes 0-3: heap_offset (u32) - offset from HEAP_START
/// Bytes 4-6: data_length (u24) - record size in heap
/// Byte 7:    node_kind_id (u8) - NodeKind ordinal
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct SlotEntry {
    pub heap_offset: u32,
    pub data_length: u32, // only lower 24 bits used on disk
    pub node_kind_id: u8,
}

impl SlotEntry {
    /// Pack into 8 bytes for the directory.
    #[inline]
    pub fn pack(&self) -> [u8; DIR_ENTRY_SIZE] {
        let mut buf = [0u8; DIR_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&self.heap_offset.to_le_bytes());
        // Pack data_length (24 bits) + node_kind_id (8 bits) into bytes 4..8
        let packed = (self.data_length & 0x00FF_FFFF) | ((self.node_kind_id as u32) << 24);
        buf[4..8].copy_from_slice(&packed.to_le_bytes());
        buf
    }

    /// Unpack from 8 bytes.
    #[inline]
    pub fn unpack(buf: &[u8; DIR_ENTRY_SIZE]) -> Self {
        let heap_offset = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let packed = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let data_length = packed & 0x00FF_FFFF;
        let node_kind_id = (packed >> 24) as u8;
        Self {
            heap_offset,
            data_length,
            node_kind_id,
        }
    }

    /// Returns true if this entry is empty (unoccupied).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data_length == 0
    }
}

/// High-performance slotted page with bump-allocated heap.
///
/// This is the Rust equivalent of Java's `KeyValueLeafPage`.
/// Uses a flat byte buffer for the entire page to maximize cache locality.
pub struct SlottedPage {
    /// The flat backing buffer: header + bitmap + preservation_bitmap + directory + heap.
    data: Vec<u8>,

    /// Record page key (unique identifier).
    record_page_key: i64,

    /// Revision this page belongs to.
    revision: i32,

    /// Index type (document, changed_nodes, etc.).
    index_type: IndexType,

    /// Current heap write position (offset from start of `data`).
    heap_end: usize,

    /// Total bytes used in the heap (may differ from heap_end after compaction).
    heap_used: usize,

    /// Number of occupied slots.
    populated_count: u16,

    /// Highest occupied slot index (for fast iteration bounds).
    last_slot_index: i32,

    /// Flags (dewey IDs stored, FSST table, etc.).
    flags: u8,

    /// Whether DeweyIDs are stored.
    dewey_ids_stored: bool,

    /// HFT-style packed state flags (hot, orphaned, closed).
    state_flags: AtomicU32,

    /// Guard count for LeanStore-style eviction prevention.
    guard_count: AtomicU32,

    /// Version counter for frame-reuse detection.
    version: AtomicU32,
}

impl SlottedPage {
    /// Create a new empty slotted page.
    pub fn new(record_page_key: i64, revision: i32, index_type: IndexType) -> Self {
        let mut data = vec![0u8; INITIAL_PAGE_SIZE];

        // Write header fields into the buffer
        data[0..8].copy_from_slice(&record_page_key.to_le_bytes());
        data[8..12].copy_from_slice(&revision.to_le_bytes());
        // populated_count at offset 12 = 0 (already zeroed)
        // heapEnd at offset 14 = HEAP_START
        let heap_start = HEAP_START as i32;
        data[14..18].copy_from_slice(&heap_start.to_le_bytes());
        // heapUsed at offset 18 = 0
        data[22] = index_type as u8;
        // flags at offset 23 = 0

        Self {
            data,
            record_page_key,
            revision,
            index_type,
            heap_end: HEAP_START,
            heap_used: 0,
            populated_count: 0,
            last_slot_index: -1,
            flags: 0,
            dewey_ids_stored: false,
            state_flags: AtomicU32::new(0),
            guard_count: AtomicU32::new(0),
            version: AtomicU32::new(0),
        }
    }

    /// Record page key.
    #[inline]
    pub fn record_page_key(&self) -> i64 {
        self.record_page_key
    }

    /// Revision number.
    #[inline]
    pub fn revision(&self) -> i32 {
        self.revision
    }

    /// Index type.
    #[inline]
    pub fn index_type(&self) -> IndexType {
        self.index_type
    }

    /// Number of occupied slots.
    #[inline]
    pub fn populated_count(&self) -> u16 {
        self.populated_count
    }

    /// Highest occupied slot index, or -1 if empty.
    #[inline]
    pub fn last_slot_index(&self) -> i32 {
        self.last_slot_index
    }

    /// Total heap bytes used.
    #[inline]
    pub fn heap_used(&self) -> usize {
        self.heap_used
    }

    /// Available heap space.
    #[inline]
    pub fn heap_available(&self) -> usize {
        self.data.len() - self.heap_end
    }

    /// Check if a slot is occupied via the bitmap.
    #[inline]
    pub fn is_slot_occupied(&self, slot: usize) -> bool {
        if slot >= SLOT_COUNT {
            return false;
        }
        let word_idx = slot / 64;
        let bit_idx = slot % 64;
        let bitmap_offset = HEADER_SIZE + word_idx * 8;
        let word = u64::from_le_bytes(
            self.data[bitmap_offset..bitmap_offset + 8]
                .try_into()
                .unwrap(),
        );
        (word >> bit_idx) & 1 == 1
    }

    /// Set a bit in the slot bitmap.
    #[inline]
    fn set_bitmap_bit(&mut self, slot: usize) {
        let word_idx = slot / 64;
        let bit_idx = slot % 64;
        let bitmap_offset = HEADER_SIZE + word_idx * 8;
        let mut word = u64::from_le_bytes(
            self.data[bitmap_offset..bitmap_offset + 8]
                .try_into()
                .unwrap(),
        );
        word |= 1u64 << bit_idx;
        self.data[bitmap_offset..bitmap_offset + 8].copy_from_slice(&word.to_le_bytes());
    }

    /// Clear a bit in the slot bitmap.
    #[inline]
    fn clear_bitmap_bit(&mut self, slot: usize) {
        let word_idx = slot / 64;
        let bit_idx = slot % 64;
        let bitmap_offset = HEADER_SIZE + word_idx * 8;
        let mut word = u64::from_le_bytes(
            self.data[bitmap_offset..bitmap_offset + 8]
                .try_into()
                .unwrap(),
        );
        word &= !(1u64 << bit_idx);
        self.data[bitmap_offset..bitmap_offset + 8].copy_from_slice(&word.to_le_bytes());
    }

    /// Get the directory offset for a given slot index.
    #[inline]
    fn dir_offset(slot: usize) -> usize {
        DISK_HEADER_BITMAP_SIZE + PRESERVATION_BITMAP_SIZE + slot * DIR_ENTRY_SIZE
    }

    /// Read a slot directory entry.
    #[inline]
    pub fn get_slot_entry(&self, slot: usize) -> SlotEntry {
        let off = Self::dir_offset(slot);
        let buf: &[u8; DIR_ENTRY_SIZE] = self.data[off..off + DIR_ENTRY_SIZE].try_into().unwrap();
        SlotEntry::unpack(buf)
    }

    /// Write a slot directory entry.
    #[inline]
    fn set_slot_entry(&mut self, slot: usize, entry: &SlotEntry) {
        let off = Self::dir_offset(slot);
        let packed = entry.pack();
        self.data[off..off + DIR_ENTRY_SIZE].copy_from_slice(&packed);
    }

    /// Insert a record into the given slot.
    ///
    /// The `record_data` should contain the raw serialized record bytes
    /// (node_kind byte + field offset table + data).
    ///
    /// Returns the heap offset where the record was placed.
    pub fn insert_record(
        &mut self,
        slot: usize,
        node_kind_id: u8,
        record_data: &[u8],
    ) -> Result<u32> {
        if slot >= SLOT_COUNT {
            return Err(StorageError::SlotOutOfRange {
                slot,
                max: SLOT_COUNT - 1,
            });
        }
        if record_data.len() > MAX_RECORD_SIZE {
            return Err(StorageError::RecordTooLarge {
                size: record_data.len(),
                max: MAX_RECORD_SIZE,
            });
        }
        if self.is_closed() {
            return Err(StorageError::PageClosed {
                page_key: self.record_page_key,
            });
        }

        let data_len = record_data.len();

        // Ensure we have enough space, growing if needed
        while self.heap_end + data_len > self.data.len() {
            self.grow();
        }

        // Bump-allocate on the heap
        let heap_offset = (self.heap_end - HEAP_START) as u32;
        self.data[self.heap_end..self.heap_end + data_len].copy_from_slice(record_data);
        self.heap_end += data_len;
        self.heap_used += data_len;

        // Update directory entry
        let entry = SlotEntry {
            heap_offset,
            data_length: data_len as u32,
            node_kind_id,
        };
        self.set_slot_entry(slot, &entry);

        // Update bitmap
        if !self.is_slot_occupied(slot) {
            self.set_bitmap_bit(slot);
            self.populated_count += 1;
        }

        // Update last_slot_index
        if slot as i32 > self.last_slot_index {
            self.last_slot_index = slot as i32;
        }

        // Sync header fields
        self.sync_header();

        Ok(heap_offset)
    }

    /// Read a record from the given slot. Returns `(node_kind_id, data_slice)`.
    #[inline]
    pub fn read_record(&self, slot: usize) -> Result<(u8, &[u8])> {
        if slot >= SLOT_COUNT {
            return Err(StorageError::SlotOutOfRange {
                slot,
                max: SLOT_COUNT - 1,
            });
        }
        if !self.is_slot_occupied(slot) {
            return Err(StorageError::SlotNotOccupied {
                page_key: self.record_page_key,
                slot,
            });
        }
        if self.is_closed() {
            return Err(StorageError::PageClosed {
                page_key: self.record_page_key,
            });
        }

        let entry = self.get_slot_entry(slot);
        let start = HEAP_START + entry.heap_offset as usize;
        let end = start + entry.data_length as usize;
        Ok((entry.node_kind_id, &self.data[start..end]))
    }

    /// Delete a record from the given slot.
    /// Note: this does NOT compact the heap (lazy deletion).
    pub fn delete_record(&mut self, slot: usize) -> Result<()> {
        if slot >= SLOT_COUNT {
            return Err(StorageError::SlotOutOfRange {
                slot,
                max: SLOT_COUNT - 1,
            });
        }
        if !self.is_slot_occupied(slot) {
            return Err(StorageError::SlotNotOccupied {
                page_key: self.record_page_key,
                slot,
            });
        }

        let entry = self.get_slot_entry(slot);
        self.heap_used -= entry.data_length as usize;

        // Clear directory entry
        self.set_slot_entry(slot, &SlotEntry::default());

        // Clear bitmap bit
        self.clear_bitmap_bit(slot);
        self.populated_count -= 1;

        // Update last_slot_index if we deleted the last slot
        if slot as i32 == self.last_slot_index {
            self.last_slot_index = self.find_last_occupied_slot();
        }

        self.sync_header();
        Ok(())
    }

    /// Find the highest occupied slot index by scanning the bitmap from the top.
    fn find_last_occupied_slot(&self) -> i32 {
        for word_idx in (0..BITMAP_WORDS).rev() {
            let bitmap_offset = HEADER_SIZE + word_idx * 8;
            let word = u64::from_le_bytes(
                self.data[bitmap_offset..bitmap_offset + 8]
                    .try_into()
                    .unwrap(),
            );
            if word != 0 {
                let bit = 63 - word.leading_zeros() as usize;
                return (word_idx * 64 + bit) as i32;
            }
        }
        -1
    }

    /// Iterate over all occupied slots, calling `f(slot_index, node_kind_id, data)`.
    pub fn for_each_record<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(usize, u8, &[u8]) -> Result<()>,
    {
        if self.populated_count == 0 {
            return Ok(());
        }

        let upper = (self.last_slot_index + 1) as usize;
        // Scan bitmap word by word for efficiency
        let words_needed = (upper + 63) / 64;
        for word_idx in 0..words_needed {
            let bitmap_offset = HEADER_SIZE + word_idx * 8;
            let mut word = u64::from_le_bytes(
                self.data[bitmap_offset..bitmap_offset + 8]
                    .try_into()
                    .unwrap(),
            );
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;
                if slot >= upper {
                    break;
                }
                let (kind, data) = self.read_record(slot)?;
                f(slot, kind, data)?;
                word &= word - 1; // clear lowest set bit
            }
        }
        Ok(())
    }

    /// Double the page buffer size.
    fn grow(&mut self) {
        let new_size = self.data.len() * 2;
        self.data.resize(new_size, 0);
    }

    /// Sync in-memory header fields back to the buffer.
    fn sync_header(&mut self) {
        self.data[0..8].copy_from_slice(&self.record_page_key.to_le_bytes());
        self.data[8..12].copy_from_slice(&self.revision.to_le_bytes());
        self.data[12..14].copy_from_slice(&self.populated_count.to_le_bytes());
        let heap_end_i32 = self.heap_end as i32;
        self.data[14..18].copy_from_slice(&heap_end_i32.to_le_bytes());
        let heap_used_i32 = self.heap_used as i32;
        self.data[18..22].copy_from_slice(&heap_used_i32.to_le_bytes());
        self.data[22] = self.index_type as u8;
        self.data[23] = self.flags;
    }

    /// Serialize the on-disk format into the provided buffer.
    ///
    /// On-disk format writes:
    /// 1. Header + bitmap (160 bytes, unchanged)
    /// 2. Compact directory entries: populated_count × 4 bytes each
    /// 3. Heap size (4 bytes)
    /// 4. Heap data (contiguous)
    pub fn serialize_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // Reserve approximate space
        let approx_size =
            DISK_HEADER_BITMAP_SIZE + (self.populated_count as usize) * 4 + 4 + self.heap_used;
        buf.reserve(approx_size);

        // 1. Header + bitmap (on-disk portion only)
        buf.extend_from_slice(&self.data[..DISK_HEADER_BITMAP_SIZE]);

        // 2. Compact directory: for each set bit, write (dataLength << 8 | nodeKindId) as u32
        if self.populated_count > 0 {
            let upper = (self.last_slot_index + 1) as usize;
            let words_needed = (upper + 63) / 64;
            for word_idx in 0..words_needed {
                let bitmap_offset = HEADER_SIZE + word_idx * 8;
                let mut word = u64::from_le_bytes(
                    self.data[bitmap_offset..bitmap_offset + 8]
                        .try_into()
                        .unwrap(),
                );
                while word != 0 {
                    let bit = word.trailing_zeros() as usize;
                    let slot = word_idx * 64 + bit;
                    if slot >= upper {
                        break;
                    }
                    let entry = self.get_slot_entry(slot);
                    let compact = (entry.data_length << 8) | (entry.node_kind_id as u32);
                    buf.extend_from_slice(&compact.to_le_bytes());
                    word &= word - 1;
                }
            }
        }

        // 3. Heap size
        let heap_data_len = if self.heap_used == 0 {
            1usize // minimum 1 byte (matches Java behavior)
        } else {
            self.heap_end - HEAP_START
        };
        buf.extend_from_slice(&(heap_data_len as u32).to_le_bytes());

        // 4. Heap data
        if self.heap_used == 0 {
            buf.push(0); // padding byte
        } else {
            buf.extend_from_slice(&self.data[HEAP_START..HEAP_START + heap_data_len]);
        }

        Ok(())
    }

    /// Deserialize from the on-disk format.
    pub fn deserialize_from(data: &[u8]) -> Result<Self> {
        if data.len() < DISK_HEADER_BITMAP_SIZE {
            return Err(StorageError::PageCorruption(
                "data too short for page header".into(),
            ));
        }

        // Parse header
        let record_page_key = i64::from_le_bytes(data[0..8].try_into().unwrap());
        let revision = i32::from_le_bytes(data[8..12].try_into().unwrap());
        let populated_count = u16::from_le_bytes(data[12..14].try_into().unwrap());
        let _heap_end_stored = i32::from_le_bytes(data[14..18].try_into().unwrap());
        let _heap_used_stored = i32::from_le_bytes(data[18..22].try_into().unwrap());
        let index_type_byte = data[22];
        let flags = data[23];

        let index_type = IndexType::from_byte(index_type_byte).ok_or_else(|| {
            StorageError::PageCorruption(format!("unknown index type: {index_type_byte}"))
        })?;

        // Allocate full runtime page
        let mut page_data = vec![0u8; INITIAL_PAGE_SIZE];

        // Copy header + bitmap
        page_data[..DISK_HEADER_BITMAP_SIZE].copy_from_slice(&data[..DISK_HEADER_BITMAP_SIZE]);

        let mut cursor = DISK_HEADER_BITMAP_SIZE;

        // Read compact directory entries and rebuild full directory
        // Scan bitmap to know which slots have entries
        let mut slot_entries = Vec::with_capacity(populated_count as usize);
        let mut last_slot_index: i32 = -1;

        for word_idx in 0..BITMAP_WORDS {
            let bitmap_offset = HEADER_SIZE + word_idx * 8;
            let mut word = u64::from_le_bytes(
                page_data[bitmap_offset..bitmap_offset + 8]
                    .try_into()
                    .unwrap(),
            );
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;

                // Read compact entry from serialized data
                if cursor + 4 > data.len() {
                    return Err(StorageError::PageCorruption(
                        "truncated directory entry".into(),
                    ));
                }
                let compact = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                cursor += 4;

                let data_length = compact >> 8;
                let node_kind_id = (compact & 0xFF) as u8;

                slot_entries.push((slot, data_length, node_kind_id));
                if slot as i32 > last_slot_index {
                    last_slot_index = slot as i32;
                }

                word &= word - 1;
            }
        }

        // Read heap size
        if cursor + 4 > data.len() {
            return Err(StorageError::PageCorruption("truncated heap size".into()));
        }
        let heap_data_len =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        // Read heap data
        if cursor + heap_data_len > data.len() {
            return Err(StorageError::PageCorruption("truncated heap data".into()));
        }

        // Ensure page_data is large enough
        let needed = HEAP_START + heap_data_len;
        if needed > page_data.len() {
            page_data.resize(needed.next_power_of_two().max(INITIAL_PAGE_SIZE), 0);
        }

        page_data[HEAP_START..HEAP_START + heap_data_len]
            .copy_from_slice(&data[cursor..cursor + heap_data_len]);

        // Rebuild directory entries with heap offsets
        let mut heap_offset = 0u32;
        let mut total_heap_used = 0usize;
        for &(slot, data_length, node_kind_id) in &slot_entries {
            let entry = SlotEntry {
                heap_offset,
                data_length,
                node_kind_id,
            };
            let dir_off =
                DISK_HEADER_BITMAP_SIZE + PRESERVATION_BITMAP_SIZE + slot * DIR_ENTRY_SIZE;
            let packed = entry.pack();
            page_data[dir_off..dir_off + DIR_ENTRY_SIZE].copy_from_slice(&packed);
            heap_offset += data_length;
            total_heap_used += data_length as usize;
        }

        let dewey_ids_stored = (flags & FLAG_DEWEY_IDS_STORED) != 0;

        Ok(Self {
            data: page_data,
            record_page_key,
            revision,
            index_type,
            heap_end: HEAP_START + heap_data_len,
            heap_used: total_heap_used,
            populated_count,
            last_slot_index,
            flags,
            dewey_ids_stored,
            state_flags: AtomicU32::new(0),
            guard_count: AtomicU32::new(0),
            version: AtomicU32::new(0),
        })
    }

    /// Set the DeweyID stored flag.
    pub fn set_dewey_ids_stored(&mut self, stored: bool) {
        self.dewey_ids_stored = stored;
        if stored {
            self.flags |= FLAG_DEWEY_IDS_STORED;
        } else {
            self.flags &= !FLAG_DEWEY_IDS_STORED;
        }
        self.sync_header();
    }

    /// Whether DeweyIDs are stored.
    #[inline]
    pub fn dewey_ids_stored(&self) -> bool {
        self.dewey_ids_stored
    }

    // === LeanStore Guard Pattern ===

    /// Acquire a guard (prevent eviction).
    #[inline]
    pub fn acquire_guard(&self) {
        self.guard_count.fetch_add(1, Ordering::Acquire);
    }

    /// Release a guard.
    #[inline]
    pub fn release_guard(&self) {
        self.guard_count.fetch_sub(1, Ordering::Release);
    }

    /// Current guard count.
    #[inline]
    pub fn guard_count(&self) -> u32 {
        self.guard_count.load(Ordering::Relaxed)
    }

    /// Current version (for frame-reuse detection).
    #[inline]
    pub fn version(&self) -> u32 {
        self.version.load(Ordering::Relaxed)
    }

    /// Increment version (called when frame is reused).
    #[inline]
    pub fn increment_version(&self) {
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Mark page as hot (for clock-based eviction).
    #[inline]
    pub fn set_hot(&self) {
        self.state_flags.fetch_or(STATE_HOT_BIT, Ordering::Relaxed);
    }

    /// Clear hot bit. Returns true if it was previously set.
    #[inline]
    pub fn clear_hot(&self) -> bool {
        let prev = self.state_flags.fetch_and(!STATE_HOT_BIT, Ordering::Relaxed);
        (prev & STATE_HOT_BIT) != 0
    }

    /// Check if page is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        (self.state_flags.load(Ordering::Relaxed) & STATE_CLOSED_BIT) != 0
    }

    /// Close the page, releasing resources.
    pub fn close(&mut self) {
        self.state_flags
            .fetch_or(STATE_CLOSED_BIT, Ordering::Release);
        // Shrink the backing buffer to reclaim memory
        self.data = Vec::new();
    }

    /// Raw access to the underlying page buffer (for zero-copy I/O).
    #[inline]
    pub fn raw_data(&self) -> &[u8] {
        &self.data
    }

    /// Raw access to bitmap region.
    #[inline]
    pub fn bitmap_region(&self) -> &[u8] {
        &self.data[HEADER_SIZE..HEADER_SIZE + BITMAP_SIZE]
    }

    /// Raw access to heap region.
    #[inline]
    pub fn heap_region(&self) -> &[u8] {
        &self.data[HEAP_START..self.heap_end]
    }

    /// Size of the page in memory.
    #[inline]
    pub fn memory_size(&self) -> usize {
        self.data.len()
    }

    // === Versioning Support ===

    /// Returns an array of all occupied slot indices, using bitmap-driven O(k) iteration.
    ///
    /// This is the Rust equivalent of Java's `populatedSlots()`.
    pub fn populated_slots(&self) -> Vec<usize> {
        if self.populated_count == 0 {
            return Vec::new();
        }
        let mut slots = Vec::with_capacity(self.populated_count as usize);
        let upper = (self.last_slot_index + 1) as usize;
        let words_needed = (upper + 63) / 64;
        for word_idx in 0..words_needed {
            let bitmap_offset = HEADER_SIZE + word_idx * 8;
            let mut word = u64::from_le_bytes(
                self.data[bitmap_offset..bitmap_offset + 8]
                    .try_into()
                    .unwrap(),
            );
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;
                if slot >= upper {
                    break;
                }
                slots.push(slot);
                word &= word - 1;
            }
        }
        slots
    }

    /// Returns a copy of the slot bitmap as raw u64 words.
    ///
    /// The bitmap has BITMAP_WORDS (16) entries, each covering 64 slots.
    /// Bit `(words[slot >> 6] >> (slot & 63)) & 1` indicates slot occupancy.
    #[inline]
    pub fn slot_bitmap(&self) -> [u64; BITMAP_WORDS] {
        let mut bmp = [0u64; BITMAP_WORDS];
        for i in 0..BITMAP_WORDS {
            let off = HEADER_SIZE + i * 8;
            bmp[i] = u64::from_le_bytes(self.data[off..off + 8].try_into().unwrap());
        }
        bmp
    }

    /// Mark a slot for preservation (lazy copy from complete page at commit time).
    ///
    /// Sets the bit in the preservation bitmap without actually copying data.
    /// At commit time, if the slot is still unoccupied, the record will be
    /// copied from the complete page.
    #[inline]
    pub fn mark_slot_for_preservation(&mut self, slot: usize) {
        if slot >= SLOT_COUNT {
            return;
        }
        let word_idx = slot / 64;
        let bit_idx = slot % 64;
        let off = DISK_HEADER_BITMAP_SIZE + word_idx * 8;
        let mut word = u64::from_le_bytes(self.data[off..off + 8].try_into().unwrap());
        word |= 1u64 << bit_idx;
        self.data[off..off + 8].copy_from_slice(&word.to_le_bytes());
    }

    /// Check if a slot is marked for preservation.
    #[inline]
    pub fn is_marked_for_preservation(&self, slot: usize) -> bool {
        if slot >= SLOT_COUNT {
            return false;
        }
        let word_idx = slot / 64;
        let bit_idx = slot % 64;
        let off = DISK_HEADER_BITMAP_SIZE + word_idx * 8;
        let word = u64::from_le_bytes(self.data[off..off + 8].try_into().unwrap());
        (word >> bit_idx) & 1 == 1
    }

    /// Copy a slot (directory entry + heap data) from another page into this page.
    ///
    /// This is the core primitive for versioning page combination. It copies
    /// the record data from `src` slot into `self` at the same slot index.
    pub fn copy_slot_from(&mut self, src: &SlottedPage, slot: usize) -> Result<()> {
        if slot >= SLOT_COUNT {
            return Err(StorageError::SlotOutOfRange { slot, max: SLOT_COUNT - 1 });
        }
        if !src.is_slot_occupied(slot) {
            return Ok(()); // nothing to copy
        }
        let entry = src.get_slot_entry(slot);
        let data_start = HEAP_START + entry.heap_offset as usize;
        let data_end = data_start + entry.data_length as usize;
        if data_end > src.data.len() {
            return Err(StorageError::PageCorruption("source slot data out of bounds".into()));
        }
        let data = &src.data[data_start..data_end];
        self.insert_record(slot, entry.node_kind_id, data)?;
        Ok(())
    }

    /// Create a new empty page with the same key, revision, and index type.
    #[inline]
    pub fn new_instance(&self) -> Self {
        Self::new(self.record_page_key, self.revision, self.index_type)
    }

    /// Create a new empty page with the same key and index type but a different revision.
    #[inline]
    pub fn new_instance_for_revision(&self, revision: i32) -> Self {
        Self::new(self.record_page_key, revision, self.index_type)
    }

    /// Resolve preservation marks by copying records from the complete page.
    ///
    /// For each slot marked in the preservation bitmap that is not yet occupied
    /// in `self`, copies the record from `complete`. Called at commit time
    /// for non-Full versioning strategies.
    pub fn resolve_preservation(&mut self, complete: &SlottedPage) -> Result<()> {
        for word_idx in 0..BITMAP_WORDS {
            let pres_off = DISK_HEADER_BITMAP_SIZE + word_idx * 8;
            let pres_word = u64::from_le_bytes(
                self.data[pres_off..pres_off + 8].try_into().unwrap(),
            );
            if pres_word == 0 {
                continue;
            }
            let bitmap_off = HEADER_SIZE + word_idx * 8;
            let occupied_word = u64::from_le_bytes(
                self.data[bitmap_off..bitmap_off + 8].try_into().unwrap(),
            );
            // Slots marked for preservation but not yet occupied
            let mut need_copy = pres_word & !occupied_word;
            while need_copy != 0 {
                let bit = need_copy.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;
                self.copy_slot_from(complete, slot)?;
                need_copy &= need_copy - 1;
            }
        }
        Ok(())
    }
}

impl Drop for SlottedPage {
    fn drop(&mut self) {
        if !self.is_closed() && !self.data.is_empty() {
            // Page dropped without explicit close - this is a leak indicator
            // In production, we'd log this. For now, just clean up.
            self.state_flags
                .fetch_or(STATE_CLOSED_BIT, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_entry_pack_unpack() {
        let entry = SlotEntry {
            heap_offset: 1234,
            data_length: 567,
            node_kind_id: 42,
        };
        let packed = entry.pack();
        let unpacked = SlotEntry::unpack(&packed);
        assert_eq!(entry.heap_offset, unpacked.heap_offset);
        assert_eq!(entry.data_length, unpacked.data_length);
        assert_eq!(entry.node_kind_id, unpacked.node_kind_id);
    }

    #[test]
    fn test_new_page_is_empty() {
        let page = SlottedPage::new(1, 0, IndexType::Document);
        assert_eq!(page.populated_count(), 0);
        assert_eq!(page.last_slot_index(), -1);
        assert_eq!(page.heap_used(), 0);
        assert!(!page.is_slot_occupied(0));
    }

    #[test]
    fn test_insert_and_read_record() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        let data = b"hello world";
        page.insert_record(0, 1, data).unwrap();

        assert_eq!(page.populated_count(), 1);
        assert!(page.is_slot_occupied(0));
        assert_eq!(page.last_slot_index(), 0);

        let (kind, read_data) = page.read_record(0).unwrap();
        assert_eq!(kind, 1);
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_insert_multiple_slots() {
        let mut page = SlottedPage::new(42, 1, IndexType::Document);

        for i in 0..10 {
            let data = format!("record_{i}");
            page.insert_record(i * 100, (i as u8) + 1, data.as_bytes())
                .unwrap();
        }

        assert_eq!(page.populated_count(), 10);
        assert_eq!(page.last_slot_index(), 900);

        for i in 0..10 {
            let expected = format!("record_{i}");
            let (kind, data) = page.read_record(i * 100).unwrap();
            assert_eq!(kind, (i as u8) + 1);
            assert_eq!(data, expected.as_bytes());
        }
    }

    #[test]
    fn test_delete_record() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        page.insert_record(5, 1, b"test").unwrap();
        page.insert_record(10, 2, b"test2").unwrap();

        assert_eq!(page.populated_count(), 2);
        assert_eq!(page.last_slot_index(), 10);

        page.delete_record(10).unwrap();
        assert_eq!(page.populated_count(), 1);
        assert_eq!(page.last_slot_index(), 5);
        assert!(!page.is_slot_occupied(10));
    }

    #[test]
    fn test_for_each_record() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        page.insert_record(0, 1, b"a").unwrap();
        page.insert_record(5, 2, b"bb").unwrap();
        page.insert_record(100, 3, b"ccc").unwrap();

        let mut found = Vec::new();
        page.for_each_record(|slot, kind, data| {
            found.push((slot, kind, data.to_vec()));
            Ok(())
        })
        .unwrap();

        assert_eq!(found.len(), 3);
        assert_eq!(found[0], (0, 1, b"a".to_vec()));
        assert_eq!(found[1], (5, 2, b"bb".to_vec()));
        assert_eq!(found[2], (100, 3, b"ccc".to_vec()));
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut page = SlottedPage::new(42, 7, IndexType::ChangedNodes);
        page.insert_record(0, 1, b"hello").unwrap();
        page.insert_record(3, 2, b"world").unwrap();
        page.insert_record(100, 5, b"!!!!").unwrap();

        let mut buf = Vec::new();
        page.serialize_to(&mut buf).unwrap();

        let restored = SlottedPage::deserialize_from(&buf).unwrap();
        assert_eq!(restored.record_page_key(), 42);
        assert_eq!(restored.revision(), 7);
        assert_eq!(restored.populated_count(), 3);
        assert_eq!(restored.index_type(), IndexType::ChangedNodes);

        let (kind, data) = restored.read_record(0).unwrap();
        assert_eq!(kind, 1);
        assert_eq!(data, b"hello");

        let (kind, data) = restored.read_record(3).unwrap();
        assert_eq!(kind, 2);
        assert_eq!(data, b"world");

        let (kind, data) = restored.read_record(100).unwrap();
        assert_eq!(kind, 5);
        assert_eq!(data, b"!!!!");
    }

    #[test]
    fn test_slot_out_of_range() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        let result = page.insert_record(SLOT_COUNT, 1, b"data");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_too_large() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        let large_data = vec![0u8; MAX_RECORD_SIZE + 1];
        let result = page.insert_record(0, 1, &large_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_guard_pattern() {
        let page = SlottedPage::new(1, 0, IndexType::Document);
        assert_eq!(page.guard_count(), 0);

        page.acquire_guard();
        assert_eq!(page.guard_count(), 1);

        page.acquire_guard();
        assert_eq!(page.guard_count(), 2);

        page.release_guard();
        assert_eq!(page.guard_count(), 1);

        page.release_guard();
        assert_eq!(page.guard_count(), 0);
    }

    #[test]
    fn test_hot_bit() {
        let page = SlottedPage::new(1, 0, IndexType::Document);
        page.set_hot();
        assert!(page.clear_hot());
        assert!(!page.clear_hot());
    }

    #[test]
    fn test_empty_page_serialization() {
        let page = SlottedPage::new(99, 0, IndexType::Document);
        let mut buf = Vec::new();
        page.serialize_to(&mut buf).unwrap();

        let restored = SlottedPage::deserialize_from(&buf).unwrap();
        assert_eq!(restored.populated_count(), 0);
        assert_eq!(restored.record_page_key(), 99);
    }
}
