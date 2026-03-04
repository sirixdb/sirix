//! UberPage - the root entry point for each database.
//!
//! Located at `FIRST_BEACON` (1024 bytes) in the data file.
//! Contains the revision count and a reference to the latest RevisionRootPage.

use crate::error::Result;
use crate::page::page_reference::PageReference;

/// The root page of a SirixDB database file.
///
/// Written at a fixed, aligned offset (`FIRST_BEACON`) to enable
/// atomic updates and crash recovery.
pub struct UberPage {
    /// Total number of revisions in the database.
    revision_count: i32,

    /// Whether this is a fresh/bootstrap database.
    is_bootstrap: bool,

    /// Reference to the latest `RevisionRootPage`.
    root_page_reference: PageReference,
}

impl UberPage {
    /// Create a new UberPage for a fresh (bootstrap) database.
    pub fn new_bootstrap() -> Self {
        Self {
            revision_count: 0,
            is_bootstrap: true,
            root_page_reference: PageReference::new(),
        }
    }

    /// Create a UberPage with the given revision count.
    pub fn new(revision_count: i32, root_page_reference: PageReference) -> Self {
        Self {
            revision_count,
            is_bootstrap: false,
            root_page_reference,
        }
    }

    /// Total number of revisions.
    #[inline]
    pub fn revision_count(&self) -> i32 {
        self.revision_count
    }

    /// Increment the revision count.
    #[inline]
    pub fn increment_revision_count(&mut self) {
        self.revision_count += 1;
        self.is_bootstrap = false;
    }

    /// Whether this is a fresh database.
    #[inline]
    pub fn is_bootstrap(&self) -> bool {
        self.is_bootstrap
    }

    /// Reference to the latest RevisionRootPage.
    #[inline]
    pub fn root_page_reference(&self) -> &PageReference {
        &self.root_page_reference
    }

    /// Mutable reference to the root page reference.
    #[inline]
    pub fn root_page_reference_mut(&mut self) -> &mut PageReference {
        &mut self.root_page_reference
    }

    /// Serialize the UberPage.
    ///
    /// Format:
    /// - revision_count (i32, 4 bytes)
    /// - is_bootstrap (u8, 1 byte)
    /// - root_page_key (i64, 8 bytes)
    /// - root_page_fragment_count (u32, 4 bytes)
    /// - fragments: (revision i32 + offset i64) × count
    pub fn serialize(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(&self.revision_count.to_le_bytes());
        buf.push(if self.is_bootstrap { 1 } else { 0 });
        buf.extend_from_slice(&self.root_page_reference.key().to_le_bytes());

        let fragments = self.root_page_reference.page_fragments();
        buf.extend_from_slice(&(fragments.len() as u32).to_le_bytes());
        for frag in fragments {
            buf.extend_from_slice(&frag.revision.to_le_bytes());
            buf.extend_from_slice(&frag.offset.to_le_bytes());
        }

        Ok(())
    }

    /// Deserialize an UberPage.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 13 {
            return Err(crate::error::StorageError::PageCorruption(
                "uber page too short".into(),
            ));
        }

        let revision_count = i32::from_le_bytes(data[0..4].try_into().unwrap());
        let is_bootstrap = data[4] != 0;
        let root_key = i64::from_le_bytes(data[5..13].try_into().unwrap());

        let mut root_ref = PageReference::with_key(root_key);

        let mut cursor = 13;
        if cursor + 4 <= data.len() {
            let frag_count =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            for _ in 0..frag_count {
                if cursor + 12 > data.len() {
                    break;
                }
                let revision = i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                cursor += 4;
                let offset = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                root_ref.add_page_fragment(crate::types::PageFragmentKey { revision, offset });
            }
        }

        Ok(Self {
            revision_count,
            is_bootstrap,
            root_page_reference: root_ref,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap() {
        let page = UberPage::new_bootstrap();
        assert!(page.is_bootstrap());
        assert_eq!(page.revision_count(), 0);
        assert!(!page.root_page_reference().is_persisted());
    }

    #[test]
    fn test_increment() {
        let mut page = UberPage::new_bootstrap();
        page.increment_revision_count();
        assert_eq!(page.revision_count(), 1);
        assert!(!page.is_bootstrap());
    }

    #[test]
    fn test_serialize_deserialize() {
        let page = UberPage::new(5, PageReference::with_key(2048));

        let mut buf = Vec::new();
        page.serialize(&mut buf).unwrap();

        let restored = UberPage::deserialize(&buf).unwrap();
        assert_eq!(restored.revision_count(), 5);
        assert!(!restored.is_bootstrap());
        assert_eq!(restored.root_page_reference().key(), 2048);
    }
}
