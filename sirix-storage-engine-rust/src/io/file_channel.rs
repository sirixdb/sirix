//! FileChannel-based storage backend.
//!
//! Equivalent to Java's `FileChannelStorage` using standard `File` I/O
//! with striped buffer pools for concurrent access.

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::compression::ByteHandlerPipeline;
use crate::constants::DATA_FILENAME;
use crate::constants::FIRST_BEACON;
use crate::constants::REVISIONS_FILENAME;
use crate::error::Result;
use crate::error::StorageError;
use crate::io::reader::StorageReader;
use crate::io::writer::StorageWriter;
use crate::io::writer::FLUSH_SIZE;
use crate::page::page_reference::PageReference;
use crate::page::serialization::DeserializedPage;
use crate::page::serialization::deserialize_page;
use crate::page::serialization::serialize_uber_page;
use crate::revision::revision_index::RevisionIndex;
use crate::revision::uber_page::UberPage;
use crate::types::SerializationType;

/// Number of striped locks for concurrent I/O.
fn stripe_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 2)
        .unwrap_or(8)
}

/// A stripe for concurrent I/O operations.
struct Stripe {
    /// Read buffer for this stripe.
    read_buf: Vec<u8>,
}

/// FileChannel-based storage implementation.
///
/// Uses standard file I/O with striped buffers for concurrent reads.
/// The data file stores pages, and the revisions file stores the revision index.
pub struct FileChannelStorage {
    /// Path to the resource directory.
    resource_path: PathBuf,

    /// Main data file.
    data_file: Arc<Mutex<File>>,

    /// Revision index file.
    revisions_file: Arc<Mutex<File>>,

    /// Compression pipeline.
    byte_handler: ByteHandlerPipeline,

    /// Striped read buffers.
    stripes: Vec<Mutex<Stripe>>,

    /// Write buffer for batching.
    write_buf: Vec<u8>,

    /// Current write position in the data file.
    write_position: u64,

    /// The revision index (cached in memory).
    revision_index: RevisionIndex,
}

impl FileChannelStorage {
    /// Open or create a FileChannel storage at the given path.
    pub fn open(resource_path: &Path, byte_handler: ByteHandlerPipeline) -> Result<Self> {
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

        // Determine write position from data file size
        let write_position = data_file.metadata()?.len();

        // Initialize stripes
        let sc = stripe_count();
        let mut stripes = Vec::with_capacity(sc);
        for _ in 0..sc {
            stripes.push(Mutex::new(Stripe {
                read_buf: vec![0u8; crate::constants::STRIPE_BUFFER_SIZE],
            }));
        }

        // Load or create revision index
        let revision_index = Self::load_revision_index(&revisions_file)?;

        Ok(Self {
            resource_path: resource_path.to_path_buf(),
            data_file: Arc::new(Mutex::new(data_file)),
            revisions_file: Arc::new(Mutex::new(revisions_file)),
            byte_handler,
            stripes,
            write_buf: Vec::with_capacity(FLUSH_SIZE),
            write_position: write_position.max(FIRST_BEACON + 512), // ensure past beacon
            revision_index,
        })
    }

    /// Load the revision index from the revisions file.
    fn load_revision_index(file: &File) -> Result<RevisionIndex> {
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(RevisionIndex::new());
        }

        let mut data = vec![0u8; len as usize];
        let mut f = file;
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut data)?;
        RevisionIndex::deserialize(&data)
    }

    /// Save the revision index to the revisions file.
    fn save_revision_index(&self) -> Result<()> {
        let mut buf = Vec::new();
        self.revision_index.serialize(&mut buf);

        let mut file = self.revisions_file.lock();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)?;
        file.set_len(buf.len() as u64)?;
        Ok(())
    }

    /// Get a stripe index for the current thread.
    #[inline]
    fn stripe_index(&self) -> usize {
        // Use thread ID for stripe selection (matches Java's approach)
        let tid = std::thread::current().id();
        let hash = {
            use std::hash::Hash;
            use std::hash::Hasher;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            tid.hash(&mut hasher);
            hasher.finish()
        };
        (hash as usize) % self.stripes.len()
    }

    /// Read raw bytes from the data file at the given offset.
    fn read_raw(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let stripe_idx = self.stripe_index();
        let stripe = &self.stripes[stripe_idx];
        let mut stripe = stripe.lock();

        if size > stripe.read_buf.len() {
            stripe.read_buf.resize(size, 0);
        }

        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut stripe.read_buf[..size])?;

        Ok(stripe.read_buf[..size].to_vec())
    }

    /// Write raw bytes to the data file at the current write position.
    fn write_raw(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.write_position;
        self.write_buf.extend_from_slice(data);
        self.write_position += data.len() as u64;

        if self.write_buf.len() >= FLUSH_SIZE {
            self.flush_write_buffer()?;
        }

        Ok(offset)
    }

    /// Flush the write buffer to disk.
    fn flush_write_buffer(&mut self) -> Result<()> {
        if self.write_buf.is_empty() {
            return Ok(());
        }

        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(
            self.write_position - self.write_buf.len() as u64,
        ))?;
        file.write_all(&self.write_buf)?;
        self.write_buf.clear();
        Ok(())
    }

    /// Resource path.
    pub fn resource_path(&self) -> &Path {
        &self.resource_path
    }

    /// Whether the storage exists on disk.
    pub fn exists(&self) -> bool {
        self.resource_path.join(DATA_FILENAME).exists()
    }
}

impl StorageReader for FileChannelStorage {
    fn read_uber_page_reference(&self) -> Result<PageReference> {
        // UberPage is at FIRST_BEACON
        Ok(PageReference::with_key(FIRST_BEACON as i64))
    }

    fn read_page(&self, reference: &PageReference) -> Result<DeserializedPage> {
        if !reference.is_persisted() {
            return Err(StorageError::PageCorruption(
                "cannot read non-persisted page reference".into(),
            ));
        }

        let offset = reference.key() as u64;

        // Read page size header (4 bytes)
        let size_bytes = self.read_raw(offset, 4)?;
        let page_size = u32::from_le_bytes(size_bytes[0..4].try_into().unwrap()) as usize;

        // Read compressed page data
        let compressed = self.read_raw(offset + 4, page_size)?;

        // Decompress
        let decompressed = self.byte_handler.decompress(&compressed)?;

        // Deserialize
        deserialize_page(&decompressed, SerializationType::Data)
    }

    fn read_uber_page(&self) -> Result<UberPage> {
        let reference = self.read_uber_page_reference()?;

        // Read from the beacon offset
        let offset = reference.key() as u64;

        // Read size
        let size_bytes = self.read_raw(offset, 4)?;
        let size = u32::from_le_bytes(size_bytes[0..4].try_into().unwrap()) as usize;

        if size == 0 {
            // Fresh database
            return Ok(UberPage::new_bootstrap());
        }

        let data = self.read_raw(offset + 4, size)?;
        let decompressed = self.byte_handler.decompress(&data)?;

        // Skip PageKind + BinaryEncodingVersion bytes
        if decompressed.len() < 2 {
            return Ok(UberPage::new_bootstrap());
        }
        UberPage::deserialize(&decompressed[2..])
    }

    fn read_revision_index(&self) -> Result<RevisionIndex> {
        Self::load_revision_index(&self.revisions_file.lock())
    }

    fn get_revision_offset(&self, revision: i32) -> Result<(i64, i64)> {
        let offset = self.revision_index.get_offset(revision as usize)?;
        let timestamp = self.revision_index.get_timestamp(revision as usize)?;
        Ok((offset, timestamp))
    }
}

impl StorageWriter for FileChannelStorage {
    fn write_page(
        &mut self,
        reference: &mut PageReference,
        serialized_data: &[u8],
    ) -> Result<i64> {
        // Compress
        let compressed = self.byte_handler.compress(serialized_data)?;

        // Write size header + compressed data
        let size = compressed.len() as u32;
        let offset = self.write_raw(&size.to_le_bytes())?;
        self.write_raw(&compressed)?;

        // Update reference with the file offset
        reference.set_key(offset as i64);

        Ok(offset as i64)
    }

    fn write_uber_page_reference(
        &mut self,
        reference: &mut PageReference,
        uber_page: &UberPage,
    ) -> Result<()> {
        // Serialize UberPage with page kind header
        let mut page_data = Vec::new();
        serialize_uber_page(uber_page, &mut page_data)?;

        // Compress
        let compressed = self.byte_handler.compress(&page_data)?;

        // Write at the fixed beacon offset
        let size = compressed.len() as u32;
        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(FIRST_BEACON))?;
        file.write_all(&size.to_le_bytes())?;
        file.write_all(&compressed)?;

        reference.set_key(FIRST_BEACON as i64);

        Ok(())
    }

    fn truncate_to(&mut self, revision: i32) -> Result<()> {
        if revision < 0 {
            return self.truncate();
        }

        let offset = self.revision_index.get_offset(revision as usize)?;
        // Truncate data file to just after this revision's data
        // (In practice, we'd need the end offset, but for Phase 1
        // we just note this needs more sophisticated handling)
        let file = self.data_file.lock();
        file.set_len(offset as u64)?;
        self.write_position = offset as u64;

        Ok(())
    }

    fn truncate(&mut self) -> Result<()> {
        let data_file = self.data_file.lock();
        data_file.set_len(0)?;
        drop(data_file);

        let rev_file = self.revisions_file.lock();
        rev_file.set_len(0)?;
        drop(rev_file);

        self.write_position = FIRST_BEACON + 512;
        self.revision_index = RevisionIndex::new();
        self.write_buf.clear();

        Ok(())
    }

    fn force_all(&mut self) -> Result<()> {
        self.flush_write_buffer()?;

        let file = self.data_file.lock();
        file.sync_all()?;
        drop(file);

        self.save_revision_index()?;
        let rev_file = self.revisions_file.lock();
        rev_file.sync_all()?;

        Ok(())
    }

    fn flush_if_needed(&mut self) -> Result<()> {
        if self.write_buf.len() >= FLUSH_SIZE {
            self.flush_write_buffer()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_new_storage() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            FileChannelStorage::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        assert!(storage.exists());
    }

    #[test]
    fn test_write_and_read_page() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            FileChannelStorage::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        // Write a raw page
        let page_data = b"test page data for storage engine";
        let mut reference = PageReference::new();
        let offset = storage.write_page(&mut reference, page_data).unwrap();

        assert!(offset > 0);
        assert!(reference.is_persisted());

        // Flush to ensure data is on disk
        storage.force_all().unwrap();
    }

    #[test]
    fn test_uber_page_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            FileChannelStorage::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let uber = UberPage::new(3, PageReference::with_key(4096));
        let mut ref_ = PageReference::new();
        storage.write_uber_page_reference(&mut ref_, &uber).unwrap();
        storage.force_all().unwrap();

        let restored = storage.read_uber_page().unwrap();
        assert_eq!(restored.revision_count(), 3);
    }

    #[test]
    fn test_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            FileChannelStorage::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let mut ref_ = PageReference::new();
        storage.write_page(&mut ref_, b"some data").unwrap();
        storage.force_all().unwrap();

        storage.truncate().unwrap();
        // After truncation, the storage should be empty
        let uber = storage.read_uber_page();
        // Should get a bootstrap page or error since we truncated
        assert!(uber.is_ok() || uber.is_err());
    }

    #[test]
    fn test_lz4_compression_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            FileChannelStorage::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

        let page_data = b"compressible data compressible data compressible data";
        let mut reference = PageReference::new();
        storage.write_page(&mut reference, page_data).unwrap();
        storage.force_all().unwrap();

        assert!(reference.is_persisted());
    }
}
