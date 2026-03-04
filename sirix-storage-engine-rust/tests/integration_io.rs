//! Integration tests: end-to-end write → flush → read → verify content.
//!
//! These tests exercise the full I/O pipeline for both FileChannel and mmap backends:
//! - Serialize a SlottedPage with records
//! - Write via write_page (which compresses)
//! - Flush to disk
//! - Read back via read_page (which decompresses + deserializes)
//! - Verify the deserialized page content matches what was written

use std::sync::Arc;

use sirix_storage_engine::compression::ByteHandlerPipeline;
use sirix_storage_engine::io::FileChannelReader;
use sirix_storage_engine::io::FileChannelWriter;
use sirix_storage_engine::io::MMFileReader;
use sirix_storage_engine::io::MMFileWriter;
use sirix_storage_engine::io::reader::StorageReader;
use sirix_storage_engine::io::writer::StorageWriter;
use sirix_storage_engine::page::page_reference::PageReference;
use sirix_storage_engine::page::serialization::DeserializedPage;
use sirix_storage_engine::page::serialization::serialize_slotted_page;
use sirix_storage_engine::page::slotted_page::SlottedPage;
use sirix_storage_engine::revision::uber_page::UberPage;
use sirix_storage_engine::types::IndexType;

/// Helper: create a SlottedPage with some records inserted.
fn create_test_page(key: i64, records: &[(usize, u8, &[u8])]) -> SlottedPage {
    let mut page = SlottedPage::new(key, 0, IndexType::Document);
    for &(slot, kind, data) in records {
        page.insert_record(slot, kind, data).unwrap();
    }
    page
}

/// Helper: serialize a SlottedPage to bytes for write_page.
fn serialize_page(page: &SlottedPage) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_slotted_page(page, &mut buf).unwrap();
    buf
}

/// Helper: extract the SlottedPage from a DeserializedPage.
fn unwrap_kvl(page: DeserializedPage) -> SlottedPage {
    match page {
        DeserializedPage::KeyValueLeaf(p) => p,
        other => panic!("expected KeyValueLeaf, got {:?}", std::mem::discriminant(&other)),
    }
}

// ============================================================================
// FileChannel integration tests
// ============================================================================

#[test]
fn test_file_channel_write_read_verify_single_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Create a page with records
    let page = create_test_page(42, &[
        (0, 1, b"hello world"),
        (5, 2, b"record at slot 5"),
        (100, 3, b"sparse slot"),
    ]);
    let serialized = serialize_page(&page);

    // Write, flush, read back
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    assert!(reference.is_persisted());

    // Read back and verify
    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);

    assert_eq!(read_page.record_page_key(), 42);
    assert_eq!(read_page.populated_count(), 3);

    let (kind0, data0) = read_page.read_record(0).unwrap();
    assert_eq!(kind0, 1);
    assert_eq!(data0, b"hello world");

    let (kind5, data5) = read_page.read_record(5).unwrap();
    assert_eq!(kind5, 2);
    assert_eq!(data5, b"record at slot 5");

    let (kind100, data100) = read_page.read_record(100).unwrap();
    assert_eq!(kind100, 3);
    assert_eq!(data100, b"sparse slot");
}

#[test]
fn test_file_channel_write_close_reopen_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = PageReference::new();

    // Write phase
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let page = create_test_page(7, &[
            (0, 10, b"persistent data survives close"),
        ]);
        let serialized = serialize_page(&page);
        writer.write_page(&mut reference, &serialized).unwrap();
        writer.force_all().unwrap();
    }
    // Writer is dropped (closed)

    // Read phase with fresh reader
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let deserialized = reader.read_page(&reference).unwrap();
        let read_page = unwrap_kvl(deserialized);

        assert_eq!(read_page.record_page_key(), 7);
        let (kind, data) = read_page.read_record(0).unwrap();
        assert_eq!(kind, 10);
        assert_eq!(data, b"persistent data survives close");
    }
}

#[test]
fn test_file_channel_multiple_pages_write_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    let mut refs = Vec::new();
    for i in 0..20 {
        let page = create_test_page(i, &[
            (0, 1, format!("page {} record 0", i).as_bytes()),
            (1, 2, format!("page {} record 1", i).as_bytes()),
        ]);
        let serialized = serialize_page(&page);
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialized).unwrap();
        refs.push(reference);
    }
    writer.force_all().unwrap();

    // Read all pages back and verify each one
    for (i, reference) in refs.iter().enumerate() {
        let deserialized = writer.read_page(reference).unwrap();
        let read_page = unwrap_kvl(deserialized);

        assert_eq!(read_page.record_page_key(), i as i64);
        let (_, data0) = read_page.read_record(0).unwrap();
        assert_eq!(data0, format!("page {} record 0", i).as_bytes());
        let (_, data1) = read_page.read_record(1).unwrap();
        assert_eq!(data1, format!("page {} record 1", i).as_bytes());
    }
}

#[test]
fn test_file_channel_lz4_write_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Compressible data within MAX_RECORD_SIZE (500)
    let big_data = vec![0xABu8; 400];
    let page = create_test_page(99, &[(0, 1, &big_data)]);
    let serialized = serialize_page(&page);

    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    // Read back through decompression pipeline
    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);

    assert_eq!(read_page.record_page_key(), 99);
    let (kind, data) = read_page.read_record(0).unwrap();
    assert_eq!(kind, 1);
    assert_eq!(data, &big_data[..]);
}

#[test]
fn test_file_channel_uber_page_close_reopen_verify() {
    let dir = tempfile::tempdir().unwrap();

    // Write uber page
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let uber = UberPage::new(10, PageReference::with_key(4096));
        let mut ref_ = PageReference::new();
        writer.write_uber_page_reference(&mut ref_, &uber).unwrap();
        writer.force_all().unwrap();
    }

    // Reopen and verify
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let restored = reader.read_uber_page().unwrap();
        assert_eq!(restored.revision_count(), 10);
    }
}

#[test]
fn test_file_channel_concurrent_write_then_concurrent_read() {
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Write 50 pages sequentially (writer is not Sync)
    let mut refs = Vec::new();
    for i in 0..50 {
        let page = create_test_page(i, &[(0, 1, format!("data-{}", i).as_bytes())]);
        let serialized = serialize_page(&page);
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialized).unwrap();
        refs.push(reference);
    }
    writer.force_all().unwrap();

    // Read concurrently from multiple threads
    let reader = Arc::new(writer.reader());
    let refs = Arc::new(refs);
    let mut handles = Vec::new();

    for t in 0..4 {
        let reader = Arc::clone(&reader);
        let refs = Arc::clone(&refs);
        handles.push(thread::spawn(move || {
            for i in (t..50).step_by(4) {
                let deserialized = reader.read_page(&refs[i]).unwrap();
                let read_page = unwrap_kvl(deserialized);
                assert_eq!(read_page.record_page_key(), i as i64);
                let (_, data) = read_page.read_record(0).unwrap();
                assert_eq!(data, format!("data-{}", i).as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Mmap integration tests
// ============================================================================

#[test]
fn test_mmap_write_read_verify_single_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    let page = create_test_page(42, &[
        (0, 1, b"mmap hello"),
        (3, 2, b"mmap record 3"),
    ]);
    let serialized = serialize_page(&page);

    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);

    assert_eq!(read_page.record_page_key(), 42);
    let (kind0, data0) = read_page.read_record(0).unwrap();
    assert_eq!(kind0, 1);
    assert_eq!(data0, b"mmap hello");
    let (kind3, data3) = read_page.read_record(3).unwrap();
    assert_eq!(kind3, 2);
    assert_eq!(data3, b"mmap record 3");
}

#[test]
fn test_mmap_write_close_reopen_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = PageReference::new();

    {
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let page = create_test_page(13, &[(0, 5, b"mmap persistent data")]);
        let serialized = serialize_page(&page);
        writer.write_page(&mut reference, &serialized).unwrap();
        writer.force_all().unwrap();
    }

    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let deserialized = reader.read_page(&reference).unwrap();
        let read_page = unwrap_kvl(deserialized);

        assert_eq!(read_page.record_page_key(), 13);
        let (kind, data) = read_page.read_record(0).unwrap();
        assert_eq!(kind, 5);
        assert_eq!(data, b"mmap persistent data");
    }
}

#[test]
fn test_mmap_multiple_pages_write_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    let mut refs = Vec::new();
    for i in 0..20 {
        let page = create_test_page(i, &[
            (0, 1, format!("mmap page {} data", i).as_bytes()),
        ]);
        let serialized = serialize_page(&page);
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialized).unwrap();
        refs.push(reference);
    }
    writer.force_all().unwrap();

    for (i, reference) in refs.iter().enumerate() {
        let deserialized = writer.read_page(reference).unwrap();
        let read_page = unwrap_kvl(deserialized);
        assert_eq!(read_page.record_page_key(), i as i64);
        let (_, data) = read_page.read_record(0).unwrap();
        assert_eq!(data, format!("mmap page {} data", i).as_bytes());
    }
}

#[test]
fn test_mmap_lz4_write_read_verify() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    let big_data = vec![0xCDu8; 400];
    let page = create_test_page(50, &[(0, 1, &big_data)]);
    let serialized = serialize_page(&page);

    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);

    assert_eq!(read_page.record_page_key(), 50);
    let (kind, data) = read_page.read_record(0).unwrap();
    assert_eq!(kind, 1);
    assert_eq!(data, &big_data[..]);
}

#[test]
fn test_mmap_uber_page_close_reopen_verify() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let uber = UberPage::new(7, PageReference::with_key(2048));
        let mut ref_ = PageReference::new();
        writer.write_uber_page_reference(&mut ref_, &uber).unwrap();
        writer.force_all().unwrap();
    }

    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let restored = reader.read_uber_page().unwrap();
        assert_eq!(restored.revision_count(), 7);
    }
}

// ============================================================================
// Cross-backend tests
// ============================================================================

#[test]
fn test_file_channel_write_mmap_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = PageReference::new();

    // Write with FileChannel
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let page = create_test_page(77, &[(0, 1, b"cross-backend test data")]);
        let serialized = serialize_page(&page);
        writer.write_page(&mut reference, &serialized).unwrap();
        writer.force_all().unwrap();
    }

    // Read with mmap
    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let deserialized = reader.read_page(&reference).unwrap();
        let read_page = unwrap_kvl(deserialized);

        assert_eq!(read_page.record_page_key(), 77);
        let (_, data) = read_page.read_record(0).unwrap();
        assert_eq!(data, b"cross-backend test data");
    }
}

#[test]
fn test_mmap_write_file_channel_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = PageReference::new();

    // Write with mmap
    {
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let page = create_test_page(88, &[(0, 1, b"mmap to file_channel")]);
        let serialized = serialize_page(&page);
        writer.write_page(&mut reference, &serialized).unwrap();
        writer.force_all().unwrap();
    }

    // Read with FileChannel
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let deserialized = reader.read_page(&reference).unwrap();
        let read_page = unwrap_kvl(deserialized);

        assert_eq!(read_page.record_page_key(), 88);
        let (_, data) = read_page.read_record(0).unwrap();
        assert_eq!(data, b"mmap to file_channel");
    }
}

// ============================================================================
// Large page / stress tests
// ============================================================================

#[test]
fn test_file_channel_many_records_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Fill a page with many records
    let mut page = SlottedPage::new(1000, 0, IndexType::Document);
    for slot in 0..100 {
        let data = format!("record-{:04}", slot);
        page.insert_record(slot, (slot % 255) as u8, data.as_bytes()).unwrap();
    }

    let serialized = serialize_page(&page);
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    // Read back and verify all 100 records
    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);
    assert_eq!(read_page.record_page_key(), 1000);
    assert_eq!(read_page.populated_count(), 100);

    for slot in 0..100 {
        let expected_data = format!("record-{:04}", slot);
        let (kind, data) = read_page.read_record(slot).unwrap();
        assert_eq!(kind, (slot % 255) as u8);
        assert_eq!(data, expected_data.as_bytes(), "slot {} data mismatch", slot);
    }
}

#[test]
fn test_mmap_many_records_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    let mut page = SlottedPage::new(2000, 0, IndexType::Document);
    for slot in 0..100 {
        let data = format!("mmap-rec-{:04}", slot);
        page.insert_record(slot, (slot % 255) as u8, data.as_bytes()).unwrap();
    }

    let serialized = serialize_page(&page);
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    let deserialized = writer.read_page(&reference).unwrap();
    let read_page = unwrap_kvl(deserialized);
    assert_eq!(read_page.record_page_key(), 2000);
    assert_eq!(read_page.populated_count(), 100);

    for slot in 0..100 {
        let expected_data = format!("mmap-rec-{:04}", slot);
        let (kind, data) = read_page.read_record(slot).unwrap();
        assert_eq!(kind, (slot % 255) as u8);
        assert_eq!(data, expected_data.as_bytes(), "slot {} data mismatch", slot);
    }
}

// ============================================================================
// Revision data persistence tests
// ============================================================================

#[test]
fn test_file_channel_revision_data_write_and_reopen() {
    let dir = tempfile::tempdir().unwrap();

    // Write pages for two revisions and persist revision data
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        // Revision 0: write a page, record its offset
        let page0 = create_test_page(0, &[(0, 1, b"rev0 data")]);
        let mut ref0 = PageReference::new();
        let offset0 = writer.write_page(&mut ref0, &serialize_page(&page0)).unwrap();
        writer.write_revision_data(1000, offset0).unwrap();

        // Revision 1: write another page
        let page1 = create_test_page(1, &[(0, 1, b"rev1 data")]);
        let mut ref1 = PageReference::new();
        let offset1 = writer.write_page(&mut ref1, &serialize_page(&page1)).unwrap();
        writer.write_revision_data(2000, offset1).unwrap();

        writer.force_all().unwrap();
    }

    // Reopen and verify revision index survived
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let idx = reader.read_revision_index().unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get_timestamp(0).unwrap(), 1000);
        assert_eq!(idx.get_timestamp(1).unwrap(), 2000);

        // Offsets should point to valid page data
        let (offset, ts) = reader.get_revision_offset(0).unwrap();
        assert!(offset > 0);
        assert_eq!(ts, 1000);

        // Read the page at the stored offset
        let ref0 = PageReference::with_key(offset);
        let deserialized = reader.read_page(&ref0).unwrap();
        let read_page = unwrap_kvl(deserialized);
        assert_eq!(read_page.record_page_key(), 0);
        let (_, data) = read_page.read_record(0).unwrap();
        assert_eq!(data, b"rev0 data");
    }
}

#[test]
fn test_mmap_revision_data_write_and_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        let page0 = create_test_page(10, &[(0, 1, b"mmap rev0")]);
        let mut ref0 = PageReference::new();
        let offset0 = writer.write_page(&mut ref0, &serialize_page(&page0)).unwrap();
        writer.write_revision_data(5000, offset0).unwrap();

        let page1 = create_test_page(11, &[(0, 1, b"mmap rev1")]);
        let mut ref1 = PageReference::new();
        let offset1 = writer.write_page(&mut ref1, &serialize_page(&page1)).unwrap();
        writer.write_revision_data(6000, offset1).unwrap();

        writer.force_all().unwrap();
    }

    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let idx = reader.read_revision_index().unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get_timestamp(0).unwrap(), 5000);
        assert_eq!(idx.get_timestamp(1).unwrap(), 6000);

        let (offset, _) = reader.get_revision_offset(1).unwrap();
        let ref1 = PageReference::with_key(offset);
        let deserialized = reader.read_page(&ref1).unwrap();
        let read_page = unwrap_kvl(deserialized);
        assert_eq!(read_page.record_page_key(), 11);
        let (_, data) = read_page.read_record(0).unwrap();
        assert_eq!(data, b"mmap rev1");
    }
}

#[test]
fn test_file_channel_full_commit_flow() {
    use sirix_storage_engine::page::serialization::serialize_revision_root_page;
    use sirix_storage_engine::revision::revision_root_page::RevisionRootPage;
    use sirix_storage_engine::types::SerializationType;

    let dir = tempfile::tempdir().unwrap();

    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        // 1. Write data page
        let page = create_test_page(0, &[(0, 1, b"document node")]);
        let mut data_ref = PageReference::new();
        writer.write_page(&mut data_ref, &serialize_page(&page)).unwrap();

        // 2. Write revision root page
        let mut rev_root = RevisionRootPage::new();
        rev_root.set_revision_timestamp_nanos(1_000_000);
        rev_root.set_commit_message("initial commit".into());
        let mut rev_buf = Vec::new();
        serialize_revision_root_page(&rev_root, &mut rev_buf, SerializationType::Data).unwrap();
        let mut rev_ref = PageReference::new();
        let rev_offset = writer.write_page(&mut rev_ref, &rev_buf).unwrap();

        // 3. Write uber page pointing to revision root
        let uber = UberPage::new(1, rev_ref.clone());
        let mut uber_ref = PageReference::new();
        writer.write_uber_page_reference(&mut uber_ref, &uber).unwrap();

        // 4. Persist revision data
        writer.write_revision_data(1_000_000, rev_offset).unwrap();

        writer.force_all().unwrap();
    }

    // Reopen and verify full chain
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        // Uber page
        let uber = reader.read_uber_page().unwrap();
        assert_eq!(uber.revision_count(), 1);

        // Revision index
        let idx = reader.read_revision_index().unwrap();
        assert_eq!(idx.len(), 1);
        assert_eq!(idx.get_timestamp(0).unwrap(), 1_000_000);
    }
}

#[test]
fn test_file_channel_write_truncate_write_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Write first page
    let page1 = create_test_page(1, &[(0, 1, b"before truncate")]);
    let mut ref1 = PageReference::new();
    writer.write_page(&mut ref1, &serialize_page(&page1)).unwrap();
    writer.force_all().unwrap();

    // Truncate
    writer.truncate().unwrap();

    // Write new page after truncate
    let page2 = create_test_page(2, &[(0, 1, b"after truncate")]);
    let mut ref2 = PageReference::new();
    writer.write_page(&mut ref2, &serialize_page(&page2)).unwrap();
    writer.force_all().unwrap();

    // Old reference should fail to read (data was truncated)
    // New reference should succeed
    let deserialized = writer.read_page(&ref2).unwrap();
    let read_page = unwrap_kvl(deserialized);
    assert_eq!(read_page.record_page_key(), 2);
    let (_, data) = read_page.read_record(0).unwrap();
    assert_eq!(data, b"after truncate");
}
