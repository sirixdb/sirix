//! Storage writer trait.

use crate::error::Result;
use crate::io::reader::StorageReader;
use crate::page::page_reference::PageReference;
use crate::revision::uber_page::UberPage;

/// Alignment constants for writes.
pub const UBER_PAGE_BYTE_ALIGN: usize = 512;
pub const REVISION_ROOT_PAGE_BYTE_ALIGN: usize = 256;
pub const PAGE_FRAGMENT_BYTE_ALIGN: usize = 8;

/// Buffered writer flush threshold.
pub const FLUSH_SIZE: usize = 64_000;

/// Trait for writing pages to storage.
///
/// Extends `StorageReader` since writers can also read.
/// Equivalent to Java's `Writer` interface.
pub trait StorageWriter: StorageReader {
    /// Write a serialized page to storage, updating the PageReference with the file offset.
    ///
    /// The `serialized_data` is the compressed page bytes ready for I/O.
    /// Returns the file offset where the page was written.
    fn write_page(
        &mut self,
        reference: &mut PageReference,
        serialized_data: &[u8],
    ) -> Result<i64>;

    /// Write the UberPage reference at the fixed beacon offset.
    fn write_uber_page_reference(
        &mut self,
        reference: &mut PageReference,
        uber_page: &UberPage,
    ) -> Result<()>;

    /// Persist a revision entry (timestamp + data file offset) to the revisions file.
    ///
    /// Appends the entry to the in-memory RevisionIndex and serializes the
    /// entire index to the revisions file. Called during commit after the
    /// revision root page has been written to the data file.
    fn write_revision_data(
        &mut self,
        timestamp_nanos: i64,
        data_file_offset: i64,
    ) -> Result<()>;

    /// Truncate the storage to the given revision.
    fn truncate_to(&mut self, revision: i32) -> Result<()>;

    /// Truncate the entire storage (delete all data).
    fn truncate(&mut self) -> Result<()>;

    /// Force all buffered data to disk (fsync barrier).
    fn force_all(&mut self) -> Result<()>;

    /// Flush the write buffer if it exceeds the threshold.
    fn flush_if_needed(&mut self) -> Result<()>;

    /// Flush all buffered writes to storage (without fsync).
    fn flush_buffered_writes(&mut self) -> Result<()>;
}
