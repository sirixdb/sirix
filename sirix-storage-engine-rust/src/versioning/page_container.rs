//! PageContainer: holds a complete page (for reads) and a modified page (for writes).
//!
//! Equivalent to Java's `io.sirix.cache.PageContainer`.
//! The complete page is the fully reconstructed view for readers.
//! The modified page is the COW copy that accumulates writes for the current transaction.

use crate::page::SlottedPage;

/// Container holding the complete and modified views of a page.
///
/// - `complete`: Fully reconstructed page (all fragments merged). Used for reads.
/// - `modified`: Copy-on-write page for the current transaction. Contains only
///   changed slots for non-Full strategies, or a full copy for Full versioning.
///
/// At commit time, only `modified` is serialized and written to storage.
pub struct PageContainer {
    /// The fully reconstructed page for reading.
    complete: SlottedPage,
    /// The page being modified by the current transaction.
    modified: SlottedPage,
}

impl PageContainer {
    /// Create a new container with the given complete and modified pages.
    #[inline]
    pub fn new(complete: SlottedPage, modified: SlottedPage) -> Self {
        Self { complete, modified }
    }

    /// Create a container where both complete and modified point to the same data.
    /// Used by Full versioning where every revision stores the complete page.
    pub fn new_single(page: SlottedPage) -> Self {
        // For Full versioning, the modified page IS the complete page.
        // We create a fresh instance and copy all slots.
        let mut clone = page.new_instance();
        let slots = page.populated_slots();
        for slot in &slots {
            // Ignore errors for slots that can't be copied (shouldn't happen)
            let _ = clone.copy_slot_from(&page, *slot);
        }
        Self {
            complete: page,
            modified: clone,
        }
    }

    /// Access the complete (read-only) page.
    #[inline]
    pub fn complete(&self) -> &SlottedPage {
        &self.complete
    }

    /// Access the modified (writable) page.
    #[inline]
    pub fn modified(&self) -> &SlottedPage {
        &self.modified
    }

    /// Mutable access to the modified page.
    #[inline]
    pub fn modified_mut(&mut self) -> &mut SlottedPage {
        &mut self.modified
    }

    /// Mutable access to the complete page.
    #[inline]
    pub fn complete_mut(&mut self) -> &mut SlottedPage {
        &mut self.complete
    }

    /// Consume the container, returning (complete, modified).
    #[inline]
    pub fn into_parts(self) -> (SlottedPage, SlottedPage) {
        (self.complete, self.modified)
    }
}
