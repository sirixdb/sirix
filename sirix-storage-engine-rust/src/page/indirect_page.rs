//! Indirect page - a B+ tree-like fanout page holding references to child pages.
//!
//! Each indirect page holds up to 1024 (`INP_REFERENCE_COUNT`) references.
//! The multi-level tree supports up to 2^70 logical keys.

use crate::constants::INP_REFERENCE_COUNT;
use crate::error::Result;
use crate::page::page_reference::PageReference;
use crate::types::SerializationType;

/// An indirect page in the page tree, holding `INP_REFERENCE_COUNT` child references.
///
/// Equivalent to Java's `IndirectPage`.
pub struct IndirectPage {
    /// Child page references. Lazily initialized entries start as `None`.
    references: Vec<Option<PageReference>>,
}

impl IndirectPage {
    /// Create a new indirect page with no references.
    pub fn new() -> Self {
        let mut references = Vec::with_capacity(INP_REFERENCE_COUNT);
        references.resize_with(INP_REFERENCE_COUNT, || None);
        Self { references }
    }

    /// Get a reference at the given offset.
    #[inline]
    pub fn get_reference(&self, offset: usize) -> Option<&PageReference> {
        self.references.get(offset).and_then(|r| r.as_ref())
    }

    /// Get or create a reference at the given offset.
    /// If the slot is empty, creates a new default `PageReference`.
    #[inline]
    pub fn get_or_create_reference(&mut self, offset: usize) -> &mut PageReference {
        if self.references[offset].is_none() {
            self.references[offset] = Some(PageReference::new());
        }
        self.references[offset].as_mut().unwrap()
    }

    /// Set a reference at the given offset.
    #[inline]
    pub fn set_reference(&mut self, offset: usize, reference: PageReference) {
        self.references[offset] = Some(reference);
    }

    /// Returns the number of non-null references.
    pub fn reference_count(&self) -> usize {
        self.references.iter().filter(|r| r.is_some()).count()
    }

    /// Fix up database and resource IDs on all references.
    pub fn fixup_ids(&mut self, database_id: i64, resource_id: i64) {
        for r in self.references.iter_mut().flatten() {
            r.fixup_ids(database_id, resource_id);
        }
    }

    /// Serialize the indirect page.
    ///
    /// Format: For each of the 1024 slots:
    /// - 1 byte: presence flag (0 = absent, 1 = present)
    /// - If present: 8 bytes key (i64) + 4 bytes fragment count + fragments
    pub fn serialize(&self, buf: &mut Vec<u8>, _ser_type: SerializationType) -> Result<()> {
        // Write a compact bitmap of which references are present
        // Then only write the present references
        let mut bitmap = [0u64; INP_REFERENCE_COUNT / 64];
        for (i, r) in self.references.iter().enumerate() {
            if r.is_some() {
                bitmap[i / 64] |= 1u64 << (i % 64);
            }
        }

        // Write bitmap (128 bytes)
        for word in &bitmap {
            buf.extend_from_slice(&word.to_le_bytes());
        }

        // Write present references
        for r in self.references.iter().flatten() {
            buf.extend_from_slice(&r.key().to_le_bytes());

            // Write page fragments
            let fragments = r.page_fragments();
            buf.extend_from_slice(&(fragments.len() as u32).to_le_bytes());
            for frag in fragments {
                buf.extend_from_slice(&frag.revision.to_le_bytes());
                buf.extend_from_slice(&frag.offset.to_le_bytes());
            }

            // Write hash if present
            match r.hash_bytes() {
                Some(hash) => {
                    buf.extend_from_slice(&(hash.len() as u32).to_le_bytes());
                    buf.extend_from_slice(hash);
                }
                None => {
                    buf.extend_from_slice(&0u32.to_le_bytes());
                }
            }
        }

        Ok(())
    }

    /// Deserialize an indirect page.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let mut cursor: usize;
        let mut references: Vec<Option<PageReference>> = Vec::with_capacity(INP_REFERENCE_COUNT);
        references.resize_with(INP_REFERENCE_COUNT, || None);

        // Read bitmap (128 bytes = 16 u64 words)
        let bitmap_bytes = INP_REFERENCE_COUNT / 64 * 8;
        if data.len() < bitmap_bytes {
            return Err(crate::error::StorageError::PageCorruption(
                "indirect page bitmap truncated".into(),
            ));
        }

        let mut bitmap = [0u64; INP_REFERENCE_COUNT / 64];
        for (i, word) in bitmap.iter_mut().enumerate() {
            let off = i * 8;
            *word = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        }
        cursor = bitmap_bytes;

        // Read present references
        for (word_idx, &word) in bitmap.iter().enumerate() {
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as usize;
                let slot = word_idx * 64 + bit;

                // Read key
                let key = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;

                let mut r = PageReference::with_key(key);

                // Read page fragments
                let frag_count =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;

                for _ in 0..frag_count {
                    let revision =
                        i32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                    cursor += 4;
                    let offset = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    r.add_page_fragment(crate::types::PageFragmentKey { revision, offset });
                }

                // Read hash
                let hash_len =
                    u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                if hash_len > 0 {
                    r.set_hash(data[cursor..cursor + hash_len].to_vec());
                    cursor += hash_len;
                }

                references[slot] = Some(r);
                w &= w - 1;
            }
        }

        Ok(Self { references })
    }
}

impl Default for IndirectPage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page_empty() {
        let page = IndirectPage::new();
        assert_eq!(page.reference_count(), 0);
        assert!(page.get_reference(0).is_none());
    }

    #[test]
    fn test_get_or_create() {
        let mut page = IndirectPage::new();
        let r = page.get_or_create_reference(42);
        r.set_key(999);

        assert_eq!(page.reference_count(), 1);
        assert_eq!(page.get_reference(42).unwrap().key(), 999);
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut page = IndirectPage::new();
        page.set_reference(0, PageReference::with_key(100));
        page.set_reference(500, PageReference::with_key(200));
        page.set_reference(1023, PageReference::with_key(300));

        let mut buf = Vec::new();
        page.serialize(&mut buf, SerializationType::Data).unwrap();

        let restored = IndirectPage::deserialize(&buf).unwrap();
        assert_eq!(restored.reference_count(), 3);
        assert_eq!(restored.get_reference(0).unwrap().key(), 100);
        assert_eq!(restored.get_reference(500).unwrap().key(), 200);
        assert_eq!(restored.get_reference(1023).unwrap().key(), 300);
    }
}
