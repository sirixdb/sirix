//! Page serialization and deserialization dispatch.
//!
//! Handles reading/writing the PageKind discriminator byte and BinaryEncodingVersion,
//! then delegates to the appropriate page type's serializer.

use crate::error::Result;
use crate::error::StorageError;
use crate::page::indirect_page::IndirectPage;
use crate::page::slotted_page::SlottedPage;
use crate::revision::revision_root_page::RevisionRootPage;
use crate::revision::uber_page::UberPage;
use crate::types::BinaryEncodingVersion;
use crate::types::PageKind;
use crate::types::SerializationType;

/// A deserialized page of any kind.
pub enum DeserializedPage {
    KeyValueLeaf(SlottedPage),
    Indirect(IndirectPage),
    Uber(UberPage),
    RevisionRoot(RevisionRootPage),
}

/// Serialize a page with its kind discriminator and encoding version.
pub fn serialize_page(
    kind: PageKind,
    page_data: &[u8],
    buf: &mut Vec<u8>,
) -> Result<()> {
    buf.push(kind as u8);
    buf.push(BinaryEncodingVersion::V0 as u8);
    buf.extend_from_slice(page_data);
    Ok(())
}

/// Deserialize a page, reading the kind discriminator and encoding version first.
pub fn deserialize_page(
    data: &[u8],
    ser_type: SerializationType,
) -> Result<DeserializedPage> {
    if data.len() < 2 {
        return Err(StorageError::PageCorruption(
            "data too short for page header".into(),
        ));
    }

    let kind = PageKind::from_byte(data[0])
        .ok_or(StorageError::UnknownPageKind(data[0]))?;
    let _version = BinaryEncodingVersion::from_byte(data[1])
        .ok_or(StorageError::UnknownEncodingVersion(data[1]))?;

    let payload = &data[2..];

    match kind {
        PageKind::KeyValueLeaf => {
            let page = SlottedPage::deserialize_from(payload)?;
            Ok(DeserializedPage::KeyValueLeaf(page))
        }
        PageKind::Indirect => {
            let page = IndirectPage::deserialize(payload)?;
            Ok(DeserializedPage::Indirect(page))
        }
        PageKind::Uber => {
            let page = UberPage::deserialize(payload)?;
            Ok(DeserializedPage::Uber(page))
        }
        PageKind::RevisionRoot => {
            let page = RevisionRootPage::deserialize(payload, ser_type)?;
            Ok(DeserializedPage::RevisionRoot(page))
        }
        PageKind::Name => {
            // Name pages are metadata-only; for Phase 1, treat as indirect
            let page = IndirectPage::deserialize(payload)?;
            Ok(DeserializedPage::Indirect(page))
        }
    }
}

/// Serialize a `SlottedPage` with the full page header.
pub fn serialize_slotted_page(page: &SlottedPage, buf: &mut Vec<u8>) -> Result<()> {
    buf.push(PageKind::KeyValueLeaf as u8);
    buf.push(BinaryEncodingVersion::V0 as u8);
    page.serialize_to(buf)
}

/// Serialize an `IndirectPage` with the full page header.
pub fn serialize_indirect_page(
    page: &IndirectPage,
    buf: &mut Vec<u8>,
    ser_type: SerializationType,
) -> Result<()> {
    buf.push(PageKind::Indirect as u8);
    buf.push(BinaryEncodingVersion::V0 as u8);
    page.serialize(&mut *buf, ser_type)
}

/// Serialize an `UberPage` with the full page header.
pub fn serialize_uber_page(page: &UberPage, buf: &mut Vec<u8>) -> Result<()> {
    buf.push(PageKind::Uber as u8);
    buf.push(BinaryEncodingVersion::V0 as u8);
    page.serialize(buf)
}

/// Serialize a `RevisionRootPage` with the full page header.
pub fn serialize_revision_root_page(
    page: &RevisionRootPage,
    buf: &mut Vec<u8>,
    ser_type: SerializationType,
) -> Result<()> {
    buf.push(PageKind::RevisionRoot as u8);
    buf.push(BinaryEncodingVersion::V0 as u8);
    page.serialize(buf, ser_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IndexType;

    #[test]
    fn test_slotted_page_full_roundtrip() {
        let mut page = SlottedPage::new(1, 0, IndexType::Document);
        page.insert_record(0, 1, b"test_data").unwrap();

        let mut buf = Vec::new();
        serialize_slotted_page(&page, &mut buf).unwrap();

        let result = deserialize_page(&buf, SerializationType::Data).unwrap();
        match result {
            DeserializedPage::KeyValueLeaf(restored) => {
                assert_eq!(restored.record_page_key(), 1);
                let (kind, data) = restored.read_record(0).unwrap();
                assert_eq!(kind, 1);
                assert_eq!(data, b"test_data");
            }
            _ => panic!("expected KeyValueLeaf"),
        }
    }

    #[test]
    fn test_indirect_page_full_roundtrip() {
        use crate::page::page_reference::PageReference;

        let mut page = IndirectPage::new();
        page.set_reference(0, PageReference::with_key(42));

        let mut buf = Vec::new();
        serialize_indirect_page(&page, &mut buf, SerializationType::Data).unwrap();

        let result = deserialize_page(&buf, SerializationType::Data).unwrap();
        match result {
            DeserializedPage::Indirect(restored) => {
                assert_eq!(restored.get_reference(0).unwrap().key(), 42);
            }
            _ => panic!("expected Indirect"),
        }
    }

    #[test]
    fn test_unknown_page_kind() {
        let data = [255u8, 0];
        let result = deserialize_page(&data, SerializationType::Data);
        assert!(result.is_err());
    }
}
