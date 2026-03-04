//! Storage reader trait.

use crate::error::Result;
use crate::page::page_reference::PageReference;
use crate::page::serialization::DeserializedPage;
use crate::revision::revision_index::RevisionIndex;
use crate::revision::uber_page::UberPage;

/// Revision file data: (data file offset, timestamp in epoch millis).
#[derive(Debug, Clone, Copy)]
pub struct RevisionFileData {
    /// File offset of the RevisionRootPage in the data file.
    pub offset: i64,
    /// Commit timestamp in epoch milliseconds.
    pub timestamp: i64,
}

/// Trait for reading pages from storage.
///
/// Equivalent to Java's `Reader` interface.
pub trait StorageReader: Send + Sync {
    /// Read the UberPage reference (root entry point of the database).
    fn read_uber_page_reference(&self) -> Result<PageReference>;

    /// Read and deserialize a page at the given file offset.
    fn read_page(&self, reference: &PageReference) -> Result<DeserializedPage>;

    /// Read the UberPage itself.
    fn read_uber_page(&self) -> Result<UberPage>;

    /// Read the revision index (timestamp → offset mapping).
    fn read_revision_index(&self) -> Result<RevisionIndex>;

    /// Get the file offset and timestamp for a specific revision.
    fn get_revision_offset(&self, revision: i32) -> Result<(i64, i64)>;

    /// Get the revision file data for a specific revision.
    fn get_revision_file_data(&self, revision: i32) -> Result<RevisionFileData>;

    /// Read the revision root page commit timestamp (epoch millis).
    fn read_revision_root_page_commit_timestamp(&self, revision: i32) -> Result<i64>;
}
