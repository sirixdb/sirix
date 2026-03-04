//! RevisionRootPage - per-revision metadata page.
//!
//! Each committed revision has exactly one RevisionRootPage that records:
//! - Max node keys for each index
//! - References to index indirect pages
//! - Commit metadata (message, timestamp, user)

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::error::Result;
use crate::error::StorageError;
use crate::page::page_reference::PageReference;
use crate::types::IndexType;
use crate::types::SerializationType;

/// Index offsets within the RevisionRootPage reference array.
pub const INDIRECT_DOCUMENT_INDEX_OFFSET: usize = 0;
pub const INDIRECT_CHANGED_NODES_INDEX_OFFSET: usize = 1;
pub const INDIRECT_RECORD_TO_REVISIONS_INDEX_OFFSET: usize = 2;
pub const PATH_SUMMARY_OFFSET: usize = 3;
pub const NAME_OFFSET: usize = 4;
pub const CAS_OFFSET: usize = 5;
pub const PATH_OFFSET: usize = 6;
pub const DEWEY_ID_OFFSET: usize = 7;

/// Total number of reference slots.
const REFERENCE_COUNT: usize = 8;

/// Per-revision metadata page.
pub struct RevisionRootPage {
    /// Child page references for each index type.
    references: Vec<PageReference>,

    /// Maximum node key in the document index.
    max_node_key_document: i64,

    /// Maximum node key in the changed-nodes index.
    max_node_key_changed_nodes: i64,

    /// Maximum node key in the record-to-revisions index.
    max_node_key_record_to_revisions: i64,

    /// Current max depth of the document index indirect page tree.
    max_level_document: i32,

    /// Current max depth of the changed-nodes index indirect page tree.
    max_level_changed_nodes: i32,

    /// Current max depth of the record-to-revisions index indirect page tree.
    max_level_record_to_revisions: i32,

    /// Commit message (optional, may be empty).
    commit_message: String,

    /// Revision timestamp (nanos since UNIX epoch).
    revision_timestamp_nanos: i64,

    /// User who created this revision.
    user_name: String,
}

impl RevisionRootPage {
    /// Create a new empty RevisionRootPage.
    pub fn new() -> Self {
        let mut references = Vec::with_capacity(REFERENCE_COUNT);
        for _ in 0..REFERENCE_COUNT {
            references.push(PageReference::new());
        }

        Self {
            references,
            max_node_key_document: 0,
            max_node_key_changed_nodes: 0,
            max_node_key_record_to_revisions: 0,
            max_level_document: 0,
            max_level_changed_nodes: 0,
            max_level_record_to_revisions: 0,
            commit_message: String::new(),
            revision_timestamp_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64,
            user_name: String::new(),
        }
    }

    // === Accessors ===

    #[inline]
    pub fn max_node_key_document(&self) -> i64 {
        self.max_node_key_document
    }

    #[inline]
    pub fn set_max_node_key_document(&mut self, key: i64) {
        self.max_node_key_document = key;
    }

    #[inline]
    pub fn max_node_key_changed_nodes(&self) -> i64 {
        self.max_node_key_changed_nodes
    }

    #[inline]
    pub fn set_max_node_key_changed_nodes(&mut self, key: i64) {
        self.max_node_key_changed_nodes = key;
    }

    #[inline]
    pub fn max_node_key_record_to_revisions(&self) -> i64 {
        self.max_node_key_record_to_revisions
    }

    #[inline]
    pub fn set_max_node_key_record_to_revisions(&mut self, key: i64) {
        self.max_node_key_record_to_revisions = key;
    }

    #[inline]
    pub fn max_level_document(&self) -> i32 {
        self.max_level_document
    }

    #[inline]
    pub fn set_max_level_document(&mut self, level: i32) {
        self.max_level_document = level;
    }

    #[inline]
    pub fn max_level_changed_nodes(&self) -> i32 {
        self.max_level_changed_nodes
    }

    #[inline]
    pub fn set_max_level_changed_nodes(&mut self, level: i32) {
        self.max_level_changed_nodes = level;
    }

    #[inline]
    pub fn max_level_record_to_revisions(&self) -> i32 {
        self.max_level_record_to_revisions
    }

    #[inline]
    pub fn set_max_level_record_to_revisions(&mut self, level: i32) {
        self.max_level_record_to_revisions = level;
    }

    #[inline]
    pub fn commit_message(&self) -> &str {
        &self.commit_message
    }

    #[inline]
    pub fn set_commit_message(&mut self, msg: String) {
        self.commit_message = msg;
    }

    #[inline]
    pub fn revision_timestamp_nanos(&self) -> i64 {
        self.revision_timestamp_nanos
    }

    #[inline]
    pub fn set_revision_timestamp_nanos(&mut self, ts: i64) {
        self.revision_timestamp_nanos = ts;
    }

    #[inline]
    pub fn user_name(&self) -> &str {
        &self.user_name
    }

    #[inline]
    pub fn set_user_name(&mut self, name: String) {
        self.user_name = name;
    }

    /// Get a reference at the given index offset.
    #[inline]
    pub fn get_reference(&self, offset: usize) -> &PageReference {
        &self.references[offset]
    }

    /// Get a mutable reference at the given index offset.
    #[inline]
    pub fn get_reference_mut(&mut self, offset: usize) -> &mut PageReference {
        &mut self.references[offset]
    }

    /// Fix up IDs on all references.
    pub fn fixup_ids(&mut self, database_id: i64, resource_id: i64) {
        for r in &mut self.references {
            r.fixup_ids(database_id, resource_id);
        }
    }

    // === Index-type-aware accessors ===

    /// Map an IndexType to the corresponding reference offset in this page.
    #[inline]
    pub fn index_offset(index_type: IndexType) -> usize {
        match index_type {
            IndexType::Document => INDIRECT_DOCUMENT_INDEX_OFFSET,
            IndexType::ChangedNodes => INDIRECT_CHANGED_NODES_INDEX_OFFSET,
            IndexType::RecordToRevisions => INDIRECT_RECORD_TO_REVISIONS_INDEX_OFFSET,
            IndexType::PathSummary => PATH_SUMMARY_OFFSET,
            IndexType::Name => NAME_OFFSET,
            IndexType::Cas => CAS_OFFSET,
            IndexType::Path => PATH_OFFSET,
            IndexType::DeweyId => DEWEY_ID_OFFSET,
        }
    }

    /// Get the root reference for a given index type.
    #[inline]
    pub fn index_reference(&self, index_type: IndexType) -> &PageReference {
        &self.references[Self::index_offset(index_type)]
    }

    /// Get mutable root reference for a given index type.
    #[inline]
    pub fn index_reference_mut(&mut self, index_type: IndexType) -> &mut PageReference {
        let offset = Self::index_offset(index_type);
        &mut self.references[offset]
    }

    /// Get the current max node key for a given index type.
    /// Returns 0 for indices that don't track max node key (PathSummary, Name, Cas, Path, DeweyId).
    #[inline]
    pub fn max_node_key(&self, index_type: IndexType) -> i64 {
        match index_type {
            IndexType::Document => self.max_node_key_document,
            IndexType::ChangedNodes => self.max_node_key_changed_nodes,
            IndexType::RecordToRevisions => self.max_node_key_record_to_revisions,
            _ => 0,
        }
    }

    /// Set the max node key for a given index type.
    #[inline]
    pub fn set_max_node_key(&mut self, index_type: IndexType, key: i64) {
        match index_type {
            IndexType::Document => self.max_node_key_document = key,
            IndexType::ChangedNodes => self.max_node_key_changed_nodes = key,
            IndexType::RecordToRevisions => self.max_node_key_record_to_revisions = key,
            _ => {}
        }
    }

    /// Get the current max indirect page tree level for a given index type.
    /// Returns 0 for indices that don't track tree level.
    #[inline]
    pub fn max_level(&self, index_type: IndexType) -> i32 {
        match index_type {
            IndexType::Document => self.max_level_document,
            IndexType::ChangedNodes => self.max_level_changed_nodes,
            IndexType::RecordToRevisions => self.max_level_record_to_revisions,
            _ => 0,
        }
    }

    /// Set the max indirect page tree level for a given index type.
    #[inline]
    pub fn set_max_level(&mut self, index_type: IndexType, level: i32) {
        match index_type {
            IndexType::Document => self.max_level_document = level,
            IndexType::ChangedNodes => self.max_level_changed_nodes = level,
            IndexType::RecordToRevisions => self.max_level_record_to_revisions = level,
            _ => {}
        }
    }

    /// Serialize the RevisionRootPage.
    pub fn serialize(&self, buf: &mut Vec<u8>, _ser_type: SerializationType) -> Result<()> {
        // Max node keys (3 × i64 = 24 bytes)
        buf.extend_from_slice(&self.max_node_key_document.to_le_bytes());
        buf.extend_from_slice(&self.max_node_key_changed_nodes.to_le_bytes());
        buf.extend_from_slice(&self.max_node_key_record_to_revisions.to_le_bytes());

        // Max levels (3 × i32 = 12 bytes)
        buf.extend_from_slice(&self.max_level_document.to_le_bytes());
        buf.extend_from_slice(&self.max_level_changed_nodes.to_le_bytes());
        buf.extend_from_slice(&self.max_level_record_to_revisions.to_le_bytes());

        // Timestamp (i64 = 8 bytes)
        buf.extend_from_slice(&self.revision_timestamp_nanos.to_le_bytes());

        // Commit message (length-prefixed string)
        let msg_bytes = self.commit_message.as_bytes();
        buf.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(msg_bytes);

        // User name (length-prefixed string)
        let user_bytes = self.user_name.as_bytes();
        buf.extend_from_slice(&(user_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(user_bytes);

        // References (REFERENCE_COUNT entries)
        for r in &self.references {
            buf.extend_from_slice(&r.key().to_le_bytes());
            let frags = r.page_fragments();
            buf.extend_from_slice(&(frags.len() as u32).to_le_bytes());
            for frag in frags {
                buf.extend_from_slice(&frag.revision.to_le_bytes());
                buf.extend_from_slice(&frag.offset.to_le_bytes());
            }
        }

        Ok(())
    }

    /// Deserialize a RevisionRootPage.
    pub fn deserialize(data: &[u8], _ser_type: SerializationType) -> Result<Self> {
        let min_size = 24 + 12 + 8 + 4 + 4; // keys + levels + ts + msg_len + user_len
        if data.len() < min_size {
            return Err(StorageError::PageCorruption(
                "revision root page too short".into(),
            ));
        }

        let mut cursor = 0;

        let max_node_key_document = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let max_node_key_changed_nodes =
            i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let max_node_key_record_to_revisions =
            i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let max_level_document = i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let max_level_changed_nodes =
            i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let max_level_record_to_revisions =
            i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;

        let revision_timestamp_nanos =
            i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        // Commit message
        let msg_len =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let commit_message = if msg_len > 0 {
            String::from_utf8_lossy(&data[cursor..cursor + msg_len]).into_owned()
        } else {
            String::new()
        };
        cursor += msg_len;

        // User name
        let user_len =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let user_name = if user_len > 0 {
            String::from_utf8_lossy(&data[cursor..cursor + user_len]).into_owned()
        } else {
            String::new()
        };
        cursor += user_len;

        // References
        let mut references = Vec::with_capacity(REFERENCE_COUNT);
        for _ in 0..REFERENCE_COUNT {
            if cursor + 8 > data.len() {
                references.push(PageReference::new());
                continue;
            }
            let key = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            let mut r = PageReference::with_key(key);

            if cursor + 4 <= data.len() {
                let frag_count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;

                for _ in 0..frag_count {
                    if cursor + 12 > data.len() {
                        break;
                    }
                    let rev = i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                    cursor += 4;
                    let off = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    r.add_page_fragment(crate::types::PageFragmentKey {
                        revision: rev,
                        offset: off,
                    });
                }
            }

            references.push(r);
        }

        Ok(Self {
            references,
            max_node_key_document,
            max_node_key_changed_nodes,
            max_node_key_record_to_revisions,
            max_level_document,
            max_level_changed_nodes,
            max_level_record_to_revisions,
            commit_message,
            revision_timestamp_nanos,
            user_name,
        })
    }
}

impl Default for RevisionRootPage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = RevisionRootPage::new();
        assert_eq!(page.max_node_key_document(), 0);
        assert!(page.commit_message().is_empty());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut page = RevisionRootPage::new();
        page.set_max_node_key_document(100);
        page.set_max_node_key_changed_nodes(50);
        page.set_max_level_document(3);
        page.set_commit_message("test commit".into());
        page.set_user_name("alice".into());
        page.set_revision_timestamp_nanos(1234567890);
        page.get_reference_mut(INDIRECT_DOCUMENT_INDEX_OFFSET)
            .set_key(8192);

        let mut buf = Vec::new();
        page.serialize(&mut buf, SerializationType::Data).unwrap();

        let restored = RevisionRootPage::deserialize(&buf, SerializationType::Data).unwrap();
        assert_eq!(restored.max_node_key_document(), 100);
        assert_eq!(restored.max_node_key_changed_nodes(), 50);
        assert_eq!(restored.max_level_document(), 3);
        assert_eq!(restored.commit_message(), "test commit");
        assert_eq!(restored.user_name(), "alice");
        assert_eq!(restored.revision_timestamp_nanos(), 1234567890);
        assert_eq!(
            restored
                .get_reference(INDIRECT_DOCUMENT_INDEX_OFFSET)
                .key(),
            8192
        );
    }
}
