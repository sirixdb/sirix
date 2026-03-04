//! Memory-mapped file I/O backend using `memmap2`.
//!
//! Equivalent to Java's `MMFileReader` / `MMFileWriter`.
//! Uses mmap for zero-copy reads and efficient writes via mapped regions.
//!
//! HFT optimizations:
//! - Dirty flag avoids redundant mmap refresh on every read
//! - Bitmask alignment (replaces modulo)
//! - Zero-copy read path via mmap slices
//! - mem::take instead of drain().collect() to eliminate allocation

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use memmap2::Mmap;
use memmap2::MmapMut;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::compression::ByteHandlerPipeline;
use crate::constants::DATA_FILENAME;
use crate::constants::FIRST_BEACON;
use crate::constants::REVISIONS_FILENAME;
use crate::error::Result;
use crate::error::StorageError;
use crate::io::reader::RevisionFileData;
use crate::io::reader::StorageReader;
use crate::io::writer::FLUSH_SIZE;
use crate::io::writer::PAGE_FRAGMENT_BYTE_ALIGN;
use crate::io::writer::StorageWriter;
use crate::io::writer::UBER_PAGE_BYTE_ALIGN;
use crate::page::page_reference::PageReference;
use crate::page::serialization::DeserializedPage;
use crate::page::serialization::deserialize_page;
use crate::page::serialization::serialize_uber_page;
use crate::revision::revision_index::RevisionIndex;
use crate::revision::uber_page::UberPage;
use crate::types::SerializationType;

/// Minimum mapped region size for writes: 64 MB.
const MIN_MAPPED_SIZE: u64 = 64 * 1024 * 1024;

/// Shared state for mmap-based I/O.
struct MmapShared {
    /// Read-only mmap of the data file (refreshed on remap).
    data_mmap: RwLock<Option<Mmap>>,
    /// Read-only mmap of the revisions file.
    revisions_mmap: RwLock<Option<Mmap>>,
    /// Data file handle.
    data_file: Mutex<File>,
    /// Revisions file handle.
    revisions_file: Mutex<File>,
    /// Compression pipeline.
    byte_handler: ByteHandlerPipeline,
    /// In-memory revision index.
    revision_index: Mutex<RevisionIndex>,
    /// Dirty flag: set when data file is modified, cleared after mmap refresh.
    /// Prevents redundant mmap remaps on read-only access.
    data_dirty: AtomicBool,
    /// Dirty flag for revisions file.
    revisions_dirty: AtomicBool,
}

impl MmapShared {
    /// Refresh the data file mmap only if the data has been modified.
    #[inline]
    fn refresh_data_mmap_if_dirty(&self) -> Result<()> {
        if !self.data_dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        self.refresh_data_mmap()
    }

    /// Force refresh the data file mmap.
    fn refresh_data_mmap(&self) -> Result<()> {
        let file = self.data_file.lock();
        let len = file.metadata()?.len();
        if len == 0 {
            *self.data_mmap.write() = None;
        } else {
            let mmap = unsafe { Mmap::map(&*file)? };
            *self.data_mmap.write() = Some(mmap);
        }
        self.data_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Refresh the revisions file mmap only if dirty.
    #[inline]
    fn refresh_revisions_mmap_if_dirty(&self) -> Result<()> {
        if !self.revisions_dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        self.refresh_revisions_mmap()
    }

    /// Force refresh the revisions file mmap.
    fn refresh_revisions_mmap(&self) -> Result<()> {
        let file = self.revisions_file.lock();
        let len = file.metadata()?.len();
        if len == 0 {
            *self.revisions_mmap.write() = None;
        } else {
            let mmap = unsafe { Mmap::map(&*file)? };
            *self.revisions_mmap.write() = Some(mmap);
        }
        self.revisions_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// Mark data file as dirty (needs mmap refresh before reads).
    #[inline]
    fn mark_data_dirty(&self) {
        self.data_dirty.store(true, Ordering::Release);
    }

    /// Mark revisions file as dirty (used by revision write path).
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    fn mark_revisions_dirty(&self) {
        self.revisions_dirty.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// MMFileReader
// ---------------------------------------------------------------------------

/// Memory-mapped file reader.
///
/// Uses mmap for zero-copy reads. The data file is mapped read-only and
/// page reads are simple slice operations without syscall overhead.
pub struct MMFileReader {
    shared: Arc<MmapShared>,
}

impl MMFileReader {
    /// Open a reader for the given resource path.
    pub fn open(resource_path: &Path, byte_handler: ByteHandlerPipeline) -> Result<Self> {
        let shared = open_mmap_shared(resource_path, byte_handler)?;
        Ok(Self { shared: Arc::new(shared) })
    }

    /// Create from existing shared state.
    fn from_shared(shared: Arc<MmapShared>) -> Self {
        Self { shared }
    }

    /// Read a slice from the data mmap.
    /// Returns a copy of the data (required since we release the mmap read lock).
    fn read_data_slice(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        let guard = self.shared.data_mmap.read();
        let mmap = guard.as_ref().ok_or_else(|| {
            StorageError::PageCorruption("data file not mapped (empty)".into())
        })?;

        if offset + len > mmap.len() {
            return Err(StorageError::PageCorruption(format!(
                "read past end of data file: offset={}, len={}, file_size={}",
                offset, len, mmap.len()
            )));
        }

        Ok(mmap[offset..offset + len].to_vec())
    }

    /// Read an i32 from the data mmap at the given byte offset.
    /// Reads 4 bytes directly without intermediate allocation.
    #[inline]
    fn read_data_i32(&self, offset: usize) -> Result<i32> {
        let guard = self.shared.data_mmap.read();
        let mmap = guard.as_ref().ok_or_else(|| {
            StorageError::PageCorruption("data file not mapped (empty)".into())
        })?;

        if offset + 4 > mmap.len() {
            return Err(StorageError::PageCorruption(format!(
                "read past end of data file: offset={}, len=4, file_size={}",
                offset, mmap.len()
            )));
        }

        Ok(i32::from_le_bytes(mmap[offset..offset + 4].try_into().unwrap()))
    }
}

impl StorageReader for MMFileReader {
    fn read_uber_page_reference(&self) -> Result<PageReference> {
        Ok(PageReference::with_key(FIRST_BEACON as i64))
    }

    fn read_page(&self, reference: &PageReference) -> Result<DeserializedPage> {
        if !reference.is_persisted() {
            return Err(StorageError::PageCorruption(
                "cannot read non-persisted page reference".into(),
            ));
        }

        let offset = reference.key() as usize;

        // Read page size (4 bytes LE) - no intermediate allocation
        let data_length = self.read_data_i32(offset)? as usize;
        if data_length == 0 {
            return Err(StorageError::PageCorruption("zero-length page".into()));
        }

        // Read compressed data
        let compressed = self.read_data_slice(offset + 4, data_length)?;

        // Decompress
        let decompressed = self.shared.byte_handler.decompress(&compressed)?;

        // Deserialize
        deserialize_page(&decompressed, SerializationType::Data)
    }

    fn read_uber_page(&self) -> Result<UberPage> {
        let reference = self.read_uber_page_reference()?;
        let offset = reference.key() as usize;

        let guard = self.shared.data_mmap.read();
        match guard.as_ref() {
            None => Ok(UberPage::new_bootstrap()),
            Some(mmap) => {
                if offset + 4 > mmap.len() {
                    return Ok(UberPage::new_bootstrap());
                }
                let size = i32::from_le_bytes(
                    mmap[offset..offset + 4].try_into().unwrap(),
                ) as usize;
                if size == 0 || offset + 4 + size > mmap.len() {
                    return Ok(UberPage::new_bootstrap());
                }
                drop(guard);

                let data = self.read_data_slice(offset + 4, size)?;
                let decompressed = self.shared.byte_handler.decompress(&data)?;
                if decompressed.len() < 2 {
                    return Ok(UberPage::new_bootstrap());
                }
                UberPage::deserialize(&decompressed[2..])
            }
        }
    }

    fn read_revision_index(&self) -> Result<RevisionIndex> {
        let idx = self.shared.revision_index.lock();
        Ok((*idx).clone())
    }

    fn get_revision_offset(&self, revision: i32) -> Result<(i64, i64)> {
        let idx = self.shared.revision_index.lock();
        let offset = idx.get_offset(revision as usize)?;
        let timestamp = idx.get_timestamp(revision as usize)?;
        Ok((offset, timestamp))
    }

    fn get_revision_file_data(&self, revision: i32) -> Result<RevisionFileData> {
        let guard = self.shared.revisions_mmap.read();
        let mmap = guard.as_ref().ok_or_else(|| {
            StorageError::RevisionNotFound(revision)
        })?;

        let file_offset = FIRST_BEACON as usize + (revision as usize) * 16;
        if file_offset + 16 > mmap.len() {
            return Err(StorageError::RevisionNotFound(revision));
        }

        let offset = i64::from_le_bytes(
            mmap[file_offset..file_offset + 8].try_into().unwrap(),
        );
        let timestamp = i64::from_le_bytes(
            mmap[file_offset + 8..file_offset + 16].try_into().unwrap(),
        );
        Ok(RevisionFileData { offset, timestamp })
    }

    fn read_revision_root_page_commit_timestamp(&self, revision: i32) -> Result<i64> {
        let data = self.get_revision_file_data(revision)?;
        Ok(data.timestamp)
    }
}

// ---------------------------------------------------------------------------
// MMFileWriter
// ---------------------------------------------------------------------------

/// Memory-mapped file writer.
///
/// Uses mmap for writes. Maps a writable region of the data file and
/// writes directly into it, extending the mapping as needed.
pub struct MMFileWriter {
    shared: Arc<MmapShared>,
    /// Current writable mmap region.
    mapped_region: Option<MmapMut>,
    /// File offset where the mapped region starts.
    mapped_offset: u64,
    /// Current write position within the mapped region.
    write_pos_in_region: u64,
    /// Overall write position in the file.
    write_position: u64,
    /// Fallback write buffer for small writes.
    write_buf: Vec<u8>,
}

impl MMFileWriter {
    /// Open a writer for the given resource path.
    pub fn open(resource_path: &Path, byte_handler: ByteHandlerPipeline) -> Result<Self> {
        let shared = open_mmap_shared(resource_path, byte_handler)?;

        let write_position = {
            let file = shared.data_file.lock();
            let len = file.metadata()?.len();
            if len == 0 {
                let start = FIRST_BEACON;
                // Bitmask alignment
                start + (PAGE_FRAGMENT_BYTE_ALIGN as u64
                    - (start & (PAGE_FRAGMENT_BYTE_ALIGN as u64 - 1)))
            } else {
                len
            }
        };

        Ok(Self {
            shared: Arc::new(shared),
            mapped_region: None,
            mapped_offset: 0,
            write_pos_in_region: 0,
            write_position,
            write_buf: Vec::with_capacity(FLUSH_SIZE),
        })
    }

    /// Get a reader backed by the same shared state.
    pub fn reader(&self) -> MMFileReader {
        MMFileReader::from_shared(Arc::clone(&self.shared))
    }

    /// Ensure a writable mmap covers the given range.
    fn ensure_mapped(&mut self, required_offset: u64, required_size: u64) -> Result<()> {
        let required_end = required_offset + required_size;

        // Check if current mapping covers the range
        if let Some(ref region) = self.mapped_region {
            if required_offset >= self.mapped_offset
                && required_end <= self.mapped_offset + region.len() as u64
            {
                return Ok(());
            }
        }

        // Force sync and drop old mapping
        if let Some(ref region) = self.mapped_region {
            region.flush()?;
        }
        self.mapped_region = None;

        // Extend file if needed
        let file = self.shared.data_file.lock();
        let file_size = file.metadata()?.len();
        if required_end > file_size {
            let new_size = required_end.max(file_size + MIN_MAPPED_SIZE);
            file.set_len(new_size)?;
        }

        // Create new mapping (maps entire file)
        self.mapped_offset = 0;

        let mmap = unsafe {
            MmapMut::map_mut(&*file)?
        };

        self.mapped_region = Some(mmap);
        self.write_pos_in_region = 0;

        Ok(())
    }

    /// Write data to the mapped region.
    fn write_to_mapped(&mut self, file_offset: u64, data: &[u8]) -> Result<()> {
        self.ensure_mapped(file_offset, data.len() as u64)?;

        if let Some(ref mut region) = self.mapped_region {
            let region_offset = file_offset as usize;
            if region_offset + data.len() <= region.len() {
                region[region_offset..region_offset + data.len()]
                    .copy_from_slice(data);
            } else {
                let mut file = self.shared.data_file.lock();
                file.seek(SeekFrom::Start(file_offset))?;
                file.write_all(data)?;
            }
        }

        // Mark data as dirty so reads know to refresh mmap
        self.shared.mark_data_dirty();

        Ok(())
    }

    /// Flush the write buffer using mem::take to avoid drain().collect() allocation.
    fn flush_write_buffer(&mut self) -> Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        let buf_len = self.write_buf.len() as u64;
        let write_offset = self.write_position - buf_len;
        // Use mem::take to avoid drain().collect() heap allocation
        let data = std::mem::take(&mut self.write_buf);
        self.write_to_mapped(write_offset, &data)?;
        // Restore the buffer (now empty) with its capacity preserved
        self.write_buf = data;
        self.write_buf.clear();

        Ok(())
    }
}

impl StorageReader for MMFileWriter {
    fn read_uber_page_reference(&self) -> Result<PageReference> {
        self.reader().read_uber_page_reference()
    }

    fn read_page(&self, reference: &PageReference) -> Result<DeserializedPage> {
        // Only refresh if data has been modified
        self.shared.refresh_data_mmap_if_dirty()?;
        self.reader().read_page(reference)
    }

    fn read_uber_page(&self) -> Result<UberPage> {
        self.shared.refresh_data_mmap_if_dirty()?;
        self.reader().read_uber_page()
    }

    fn read_revision_index(&self) -> Result<RevisionIndex> {
        self.reader().read_revision_index()
    }

    fn get_revision_offset(&self, revision: i32) -> Result<(i64, i64)> {
        self.reader().get_revision_offset(revision)
    }

    fn get_revision_file_data(&self, revision: i32) -> Result<RevisionFileData> {
        self.shared.refresh_revisions_mmap_if_dirty()?;
        self.reader().get_revision_file_data(revision)
    }

    fn read_revision_root_page_commit_timestamp(&self, revision: i32) -> Result<i64> {
        self.reader().read_revision_root_page_commit_timestamp(revision)
    }
}

impl StorageWriter for MMFileWriter {
    fn write_page(
        &mut self,
        reference: &mut PageReference,
        serialized_data: &[u8],
    ) -> Result<i64> {
        // Compress
        let compressed = self.shared.byte_handler.compress(serialized_data)?;

        // Alignment using bitmask
        let padding = {
            let mask = PAGE_FRAGMENT_BYTE_ALIGN as u64 - 1;
            let rem = self.write_position & mask;
            if rem == 0 { 0 } else { PAGE_FRAGMENT_BYTE_ALIGN as u64 - rem }
        };
        if padding > 0 {
            // Zero-fill padding in write buffer so flush offset calculation is correct
            static ZERO_PAD: [u8; 512] = [0u8; 512];
            self.write_buf.extend_from_slice(&ZERO_PAD[..padding as usize]);
            self.write_position += padding;
        }

        let offset = self.write_position;

        // Write size + data
        let size = compressed.len() as u32;
        self.write_buf.extend_from_slice(&size.to_le_bytes());
        self.write_buf.extend_from_slice(&compressed);
        self.write_position += 4 + compressed.len() as u64;

        // Update reference
        reference.set_key(offset as i64);
        let hash = xxhash_rust::xxh3::xxh3_64(&compressed);
        reference.set_hash_value(hash);

        if self.write_buf.len() >= FLUSH_SIZE {
            self.flush_write_buffer()?;
        }

        Ok(offset as i64)
    }

    fn write_uber_page_reference(
        &mut self,
        reference: &mut PageReference,
        uber_page: &UberPage,
    ) -> Result<()> {
        self.flush_write_buffer()?;

        let mut page_data = Vec::new();
        serialize_uber_page(uber_page, &mut page_data)?;

        let compressed = self.shared.byte_handler.compress(&page_data)?;
        let size = compressed.len() as u32;

        let padding = UBER_PAGE_BYTE_ALIGN
            - ((compressed.len() + 4) % UBER_PAGE_BYTE_ALIGN);

        let mut block = Vec::with_capacity(4 + compressed.len() + padding);
        block.extend_from_slice(&size.to_le_bytes());
        block.extend_from_slice(&compressed);
        if padding > 0 && padding < UBER_PAGE_BYTE_ALIGN {
            block.resize(block.len() + padding, 0);
        }

        // Write at FIRST_BEACON
        self.write_to_mapped(FIRST_BEACON, &block)?;
        // Mirror copy
        self.write_to_mapped(FIRST_BEACON / 2, &block)?;

        if let Some(ref region) = self.mapped_region {
            region.flush()?;
        }

        reference.set_key(FIRST_BEACON as i64);
        let hash = xxhash_rust::xxh3::xxh3_64(&compressed);
        reference.set_hash_value(hash);

        // Ensure write_position accounts for uber page block
        let uber_end = FIRST_BEACON + block.len() as u64;
        if uber_end > self.write_position {
            self.write_position = uber_end;
        }

        // Force refresh data mmap for reads
        self.shared.refresh_data_mmap()?;

        Ok(())
    }

    fn write_revision_data(
        &mut self,
        timestamp_nanos: i64,
        data_file_offset: i64,
    ) -> Result<()> {
        // Append to in-memory index
        let mut idx = self.shared.revision_index.lock();
        idx.append(timestamp_nanos, data_file_offset)?;

        // Serialize and write entire index to revisions file
        let mut buf = Vec::with_capacity(4 + idx.len() * 16);
        idx.serialize(&mut buf);
        drop(idx);

        {
            let mut rev_file = self.shared.revisions_file.lock();
            rev_file.seek(SeekFrom::Start(0))?;
            rev_file.write_all(&buf)?;
            rev_file.set_len(buf.len() as u64)?;
            rev_file.sync_all()?;
        }

        self.shared.mark_revisions_dirty();

        Ok(())
    }

    fn truncate_to(&mut self, revision: i32) -> Result<()> {
        if revision < 0 {
            return self.truncate();
        }

        let idx = self.shared.revision_index.lock();
        let offset = idx.get_offset(revision as usize)?;
        drop(idx);

        self.mapped_region = None;

        let file = self.shared.data_file.lock();
        file.set_len(offset as u64)?;
        drop(file);

        self.write_position = offset as u64;
        self.write_buf.clear();
        self.shared.refresh_data_mmap()?;

        Ok(())
    }

    fn truncate(&mut self) -> Result<()> {
        self.mapped_region = None;

        {
            let file = self.shared.data_file.lock();
            file.set_len(0)?;
        }
        {
            let file = self.shared.revisions_file.lock();
            file.set_len(0)?;
        }

        self.write_position = FIRST_BEACON
            + (PAGE_FRAGMENT_BYTE_ALIGN as u64
                - (FIRST_BEACON & (PAGE_FRAGMENT_BYTE_ALIGN as u64 - 1)));
        self.write_buf.clear();
        *self.shared.revision_index.lock() = RevisionIndex::new();
        self.shared.refresh_data_mmap()?;
        self.shared.refresh_revisions_mmap()?;

        Ok(())
    }

    fn force_all(&mut self) -> Result<()> {
        self.flush_write_buffer()?;

        if let Some(ref region) = self.mapped_region {
            region.flush()?;
        }

        // Drop the mutable mapping before truncating to avoid conflict
        self.mapped_region = None;

        {
            let file = self.shared.data_file.lock();
            // Trim file to actual write position (ensure_mapped may have extended it)
            let file_len = file.metadata()?.len();
            if file_len > self.write_position && self.write_position > 0 {
                file.set_len(self.write_position)?;
            }
            file.sync_all()?;
        }
        {
            let file = self.shared.revisions_file.lock();
            file.sync_all()?;
        }

        // Refresh mmaps for reads
        self.shared.refresh_data_mmap()?;
        self.shared.refresh_revisions_mmap()?;

        Ok(())
    }

    fn flush_if_needed(&mut self) -> Result<()> {
        if self.write_buf.len() >= FLUSH_SIZE {
            self.flush_write_buffer()?;
        }
        Ok(())
    }

    fn flush_buffered_writes(&mut self) -> Result<()> {
        self.flush_write_buffer()
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn open_mmap_shared(
    resource_path: &Path,
    byte_handler: ByteHandlerPipeline,
) -> Result<MmapShared> {
    std::fs::create_dir_all(resource_path)?;

    let data_path = resource_path.join(DATA_FILENAME);
    let revisions_path = resource_path.join(REVISIONS_FILENAME);

    let data_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&data_path)?;

    let revisions_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&revisions_path)?;

    let data_mmap = if data_file.metadata()?.len() > 0 {
        Some(unsafe { Mmap::map(&data_file)? })
    } else {
        None
    };

    let revisions_mmap = if revisions_file.metadata()?.len() > 0 {
        Some(unsafe { Mmap::map(&revisions_file)? })
    } else {
        None
    };

    let revision_index = if let Some(ref mmap) = revisions_mmap {
        RevisionIndex::deserialize(mmap)?
    } else {
        RevisionIndex::new()
    };

    Ok(MmapShared {
        data_mmap: RwLock::new(data_mmap),
        revisions_mmap: RwLock::new(revisions_mmap),
        data_file: Mutex::new(data_file),
        revisions_file: Mutex::new(revisions_file),
        byte_handler,
        revision_index: Mutex::new(revision_index),
        data_dirty: AtomicBool::new(false),
        revisions_dirty: AtomicBool::new(false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmap_reader_open() {
        let dir = tempfile::tempdir().unwrap();
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let uber_ref = reader.read_uber_page_reference().unwrap();
        assert_eq!(uber_ref.key(), FIRST_BEACON as i64);
    }

    #[test]
    fn test_mmap_writer_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let page_data = b"test mmap page data";
        let mut reference = PageReference::new();
        let offset = writer.write_page(&mut reference, page_data).unwrap();

        assert!(offset > 0);
        assert!(reference.is_persisted());
        writer.force_all().unwrap();
    }

    #[test]
    fn test_mmap_uber_page_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let uber = UberPage::new(5, PageReference::with_key(8192));
        let mut ref_ = PageReference::new();
        writer.write_uber_page_reference(&mut ref_, &uber).unwrap();
        writer.force_all().unwrap();

        let restored = writer.read_uber_page().unwrap();
        assert_eq!(restored.revision_count(), 5);
    }

    #[test]
    fn test_mmap_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut ref_ = PageReference::new();
        writer.write_page(&mut ref_, b"mmap data").unwrap();
        writer.force_all().unwrap();

        writer.truncate().unwrap();
    }

    // --- Additional behavioral and coverage tests ---

    #[test]
    fn test_dirty_flag_avoids_unnecessary_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let shared = open_mmap_shared(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let shared = Arc::new(shared);

        // Initially not dirty
        assert!(!shared.data_dirty.load(Ordering::Relaxed));

        // Refresh when not dirty should be a no-op (fast path)
        shared.refresh_data_mmap_if_dirty().unwrap();

        // Mark dirty
        shared.mark_data_dirty();
        assert!(shared.data_dirty.load(Ordering::Relaxed));

        // Refresh should clear dirty flag
        shared.refresh_data_mmap_if_dirty().unwrap();
        assert!(!shared.data_dirty.load(Ordering::Relaxed));
    }

    #[test]
    fn test_multiple_writes_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut refs = Vec::new();
        for i in 0..10 {
            let data = format!("mmap page data {}", i);
            let mut reference = PageReference::new();
            writer.write_page(&mut reference, data.as_bytes()).unwrap();
            refs.push(reference);
        }
        writer.force_all().unwrap();

        // All offsets should be unique and persisted
        for (i, r) in refs.iter().enumerate() {
            assert!(r.is_persisted(), "ref {} should be persisted", i);
            assert!(r.key() > 0);
        }

        let offsets: Vec<i64> = refs.iter().map(|r| r.key()).collect();
        for i in 0..offsets.len() {
            for j in i + 1..offsets.len() {
                assert_ne!(offsets[i], offsets[j]);
            }
        }
    }

    #[test]
    fn test_mmap_hash_computed() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut ref1 = PageReference::new();
        let mut ref2 = PageReference::new();
        writer.write_page(&mut ref1, b"data one").unwrap();
        writer.write_page(&mut ref2, b"data two").unwrap();

        assert!(ref1.hash_bytes().is_some());
        assert!(ref2.hash_bytes().is_some());
        assert_ne!(ref1.hash_bytes(), ref2.hash_bytes());
    }

    #[test]
    fn test_mmap_flush_if_needed() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut ref_ = PageReference::new();
        writer.write_page(&mut ref_, b"small data").unwrap();
        writer.flush_if_needed().unwrap();
    }

    #[test]
    fn test_mmap_flush_buffered_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut ref_ = PageReference::new();
        writer.write_page(&mut ref_, b"buffered mmap data").unwrap();
        writer.flush_buffered_writes().unwrap();
    }

    #[test]
    fn test_mmap_writer_reopen() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
            let mut ref_ = PageReference::new();
            writer.write_page(&mut ref_, b"first mmap write").unwrap();
            writer.force_all().unwrap();
        }

        {
            let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
            let mut ref_ = PageReference::new();
            let offset = writer.write_page(&mut ref_, b"second mmap write").unwrap();
            assert!(offset > FIRST_BEACON as i64);
            writer.force_all().unwrap();
        }
    }

    #[test]
    fn test_mmap_empty_file_bootstrap() {
        let dir = tempfile::tempdir().unwrap();
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let uber = reader.read_uber_page().unwrap();
        assert_eq!(uber.revision_count(), 0);
    }

    #[test]
    fn test_mmap_reader_data_slice_bounds_check() {
        let dir = tempfile::tempdir().unwrap();
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        // Reading from empty file should fail
        let result = reader.read_data_slice(0, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_reader_i32_bounds_check() {
        let dir = tempfile::tempdir().unwrap();
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let result = reader.read_data_i32(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_read_non_persisted_reference() {
        let dir = tempfile::tempdir().unwrap();
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let reference = PageReference::new(); // not persisted
        let result = reader.read_page(&reference);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_lz4_compression() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

        let data = b"compressible mmap data compressible mmap data compressible";
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, data).unwrap();
        writer.force_all().unwrap();

        assert!(reference.is_persisted());
    }

    #[test]
    fn test_mmap_large_page() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let large_data = vec![0xABu8; FLUSH_SIZE + 1000];
        let mut ref_ = PageReference::new();
        writer.write_page(&mut ref_, &large_data).unwrap();
        writer.force_all().unwrap();

        assert!(ref_.is_persisted());
    }

    #[test]
    fn test_revisions_dirty_flag() {
        let dir = tempfile::tempdir().unwrap();
        let shared = open_mmap_shared(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let shared = Arc::new(shared);

        assert!(!shared.revisions_dirty.load(Ordering::Relaxed));
        shared.mark_revisions_dirty();
        assert!(shared.revisions_dirty.load(Ordering::Relaxed));
        shared.refresh_revisions_mmap_if_dirty().unwrap();
        assert!(!shared.revisions_dirty.load(Ordering::Relaxed));
    }
}
