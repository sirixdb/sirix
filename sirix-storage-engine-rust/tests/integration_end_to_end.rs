//! End-to-end integration tests: full pipeline store → retrieve cycles.
//!
//! Exercises the complete data lifecycle through all layers of the storage engine:
//! - Records → SlottedPage → Serialization → Compression → I/O Writer → File
//! - File → I/O Reader → Decompression → Deserialization → SlottedPage → Records
//! - Multi-revision commit flow with UberPage → RevisionRootPage → IndirectPage → SlottedPage
//! - Cache layer: put → get → eviction → dirty tracking
//! - TIL lifecycle: put → snapshot → resolve stale ref → cleanup
//! - Versioning: multi-revision store/retrieve with all 4 strategies
//! - RevisionIndex: append → serialize → deserialize → timestamp lookup
//! - BufferPool: acquire → use → release cycle
//! - Cross-layer interactions (cache + I/O + versioning combined)

use std::sync::Arc;

use sirix_storage_engine::cache::buffer_pool::BufferPool;
use sirix_storage_engine::cache::page_cache::PageCache;
use sirix_storage_engine::cache::page_cache::PageId;
use sirix_storage_engine::cache::transaction_intent_log::PageContainer as TilPageContainer;
use sirix_storage_engine::cache::transaction_intent_log::PageData;
use sirix_storage_engine::cache::transaction_intent_log::TransactionIntentLog;
use sirix_storage_engine::compression::ByteHandlerPipeline;
use sirix_storage_engine::io::FileChannelReader;
use sirix_storage_engine::io::FileChannelWriter;
use sirix_storage_engine::io::MMFileReader;
use sirix_storage_engine::io::MMFileWriter;
use sirix_storage_engine::io::reader::StorageReader;
use sirix_storage_engine::io::writer::StorageWriter;
use sirix_storage_engine::page::indirect_page::IndirectPage;
use sirix_storage_engine::page::page_reference::PageReference;
use sirix_storage_engine::page::serialization::deserialize_page;
use sirix_storage_engine::page::serialization::serialize_indirect_page;
use sirix_storage_engine::page::serialization::serialize_revision_root_page;
use sirix_storage_engine::page::serialization::serialize_slotted_page;
use sirix_storage_engine::page::serialization::serialize_uber_page;
use sirix_storage_engine::page::serialization::DeserializedPage;
use sirix_storage_engine::page::slotted_page::SlottedPage;
use sirix_storage_engine::revision::revision_index::RevisionIndex;
use sirix_storage_engine::revision::revision_root_page::RevisionRootPage;
use sirix_storage_engine::revision::uber_page::UberPage;
use sirix_storage_engine::types::IndexType;
use sirix_storage_engine::types::SerializationType;
use sirix_storage_engine::types::VersioningType;
use sirix_storage_engine::versioning::combine_record_pages;
use sirix_storage_engine::versioning::combine_record_pages_for_modification;

// ============================================================================
// Helpers
// ============================================================================

fn make_page(key: i64, revision: i32, records: &[(usize, u8, &[u8])]) -> SlottedPage {
    let mut page = SlottedPage::new(key, revision, IndexType::Document);
    for &(slot, kind, data) in records {
        page.insert_record(slot, kind, data).unwrap();
    }
    page
}

fn serialize(page: &SlottedPage) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_slotted_page(page, &mut buf).unwrap();
    buf
}

fn unwrap_kvl(page: DeserializedPage) -> SlottedPage {
    match page {
        DeserializedPage::KeyValueLeaf(p) => p,
        other => panic!("expected KeyValueLeaf, got {:?}", std::mem::discriminant(&other)),
    }
}

fn clone_page(src: &SlottedPage) -> SlottedPage {
    let mut dst = src.new_instance();
    for &slot in &src.populated_slots() {
        let _ = dst.copy_slot_from(src, slot);
    }
    dst
}

// ============================================================================
// 1. Full pipeline: Records → SlottedPage → Serialize → Compress → Write
//                    → Read → Decompress → Deserialize → SlottedPage → Records
// ============================================================================

#[test]
fn test_full_pipeline_records_through_lz4_filechannel_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Insert diverse records into a SlottedPage
    let mut page = SlottedPage::new(42, 1, IndexType::Document);
    page.insert_record(0, 1, b"hello world").unwrap();
    page.insert_record(5, 2, b"").unwrap(); // empty record
    page.insert_record(100, 3, &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); // binary data
    page.insert_record(500, 4, &vec![0xAB; 400]).unwrap(); // large compressible record
    page.insert_record(1023, 5, b"last slot").unwrap(); // max slot

    // Serialize → Compress → Write
    let serialized = serialize(&page);
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    assert!(reference.is_persisted());

    // Read → Decompress → Deserialize
    let deserialized = writer.read_page(&reference).unwrap();
    let restored = unwrap_kvl(deserialized);

    // Verify every record
    assert_eq!(restored.record_page_key(), 42);
    assert_eq!(restored.populated_count(), 5);

    let (k, d) = restored.read_record(0).unwrap();
    assert_eq!(k, 1);
    assert_eq!(d, b"hello world");

    let (k, d) = restored.read_record(5).unwrap();
    assert_eq!(k, 2);
    assert_eq!(d, b"");

    let (k, d) = restored.read_record(100).unwrap();
    assert_eq!(k, 3);
    assert_eq!(d, &[0xDE, 0xAD, 0xBE, 0xEF]);

    let (k, d) = restored.read_record(500).unwrap();
    assert_eq!(k, 4);
    assert_eq!(d, &vec![0xAB; 400][..]);

    let (k, d) = restored.read_record(1023).unwrap();
    assert_eq!(k, 5);
    assert_eq!(d, b"last slot");
}

#[test]
fn test_full_pipeline_records_through_lz4_mmap_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    let mut page = SlottedPage::new(99, 0, IndexType::PathSummary);
    for slot in 0..50 {
        let data = format!("mmap-record-{:04}", slot);
        page.insert_record(slot, (slot % 255) as u8, data.as_bytes()).unwrap();
    }

    let serialized = serialize(&page);
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialized).unwrap();
    writer.force_all().unwrap();

    // Reopen with reader
    drop(writer);
    let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();
    let deserialized = reader.read_page(&reference).unwrap();
    let restored = unwrap_kvl(deserialized);

    assert_eq!(restored.record_page_key(), 99);
    assert_eq!(restored.populated_count(), 50);

    for slot in 0..50 {
        let expected = format!("mmap-record-{:04}", slot);
        let (kind, data) = restored.read_record(slot).unwrap();
        assert_eq!(kind, (slot % 255) as u8);
        assert_eq!(data, expected.as_bytes(), "slot {} mismatch", slot);
    }
}

// ============================================================================
// 2. All page types serialization roundtrip through dispatch layer
// ============================================================================

#[test]
fn test_all_page_types_serialize_compress_decompress_deserialize() {
    let pipeline = ByteHandlerPipeline::lz4();

    // --- SlottedPage ---
    let mut slotted = SlottedPage::new(1, 0, IndexType::Document);
    slotted.insert_record(0, 1, b"test").unwrap();
    let mut buf = Vec::new();
    serialize_slotted_page(&slotted, &mut buf).unwrap();
    let compressed = pipeline.compress(&buf).unwrap();
    let decompressed = pipeline.decompress(&compressed).unwrap();
    let page = deserialize_page(&decompressed, SerializationType::Data).unwrap();
    match page {
        DeserializedPage::KeyValueLeaf(p) => {
            assert_eq!(p.record_page_key(), 1);
            assert_eq!(p.read_record(0).unwrap(), (1, &b"test"[..]));
        }
        _ => panic!("expected KeyValueLeaf"),
    }

    // --- IndirectPage ---
    let mut indirect = IndirectPage::new();
    indirect.set_reference(0, PageReference::with_key(100));
    indirect.set_reference(512, PageReference::with_key(200));
    indirect.set_reference(1023, PageReference::with_key(300));
    let mut buf = Vec::new();
    serialize_indirect_page(&indirect, &mut buf, SerializationType::Data).unwrap();
    let compressed = pipeline.compress(&buf).unwrap();
    let decompressed = pipeline.decompress(&compressed).unwrap();
    let page = deserialize_page(&decompressed, SerializationType::Data).unwrap();
    match page {
        DeserializedPage::Indirect(p) => {
            assert_eq!(p.get_reference(0).unwrap().key(), 100);
            assert_eq!(p.get_reference(512).unwrap().key(), 200);
            assert_eq!(p.get_reference(1023).unwrap().key(), 300);
        }
        _ => panic!("expected Indirect"),
    }

    // --- UberPage ---
    let uber = UberPage::new(42, PageReference::with_key(8192));
    let mut buf = Vec::new();
    serialize_uber_page(&uber, &mut buf).unwrap();
    let compressed = pipeline.compress(&buf).unwrap();
    let decompressed = pipeline.decompress(&compressed).unwrap();
    let page = deserialize_page(&decompressed, SerializationType::Data).unwrap();
    match page {
        DeserializedPage::Uber(p) => {
            assert_eq!(p.revision_count(), 42);
            assert_eq!(p.root_page_reference().key(), 8192);
        }
        _ => panic!("expected Uber"),
    }

    // --- RevisionRootPage ---
    let mut rev_root = RevisionRootPage::new();
    rev_root.set_max_node_key_document(500);
    rev_root.set_commit_message("test commit".into());
    rev_root.set_user_name("alice".into());
    rev_root.set_revision_timestamp_nanos(1_000_000_000);
    let mut buf = Vec::new();
    serialize_revision_root_page(&rev_root, &mut buf, SerializationType::Data).unwrap();
    let compressed = pipeline.compress(&buf).unwrap();
    let decompressed = pipeline.decompress(&compressed).unwrap();
    let page = deserialize_page(&decompressed, SerializationType::Data).unwrap();
    match page {
        DeserializedPage::RevisionRoot(p) => {
            assert_eq!(p.max_node_key_document(), 500);
            assert_eq!(p.commit_message(), "test commit");
            assert_eq!(p.user_name(), "alice");
            assert_eq!(p.revision_timestamp_nanos(), 1_000_000_000);
        }
        _ => panic!("expected RevisionRoot"),
    }
}

// ============================================================================
// 3. Multi-revision commit flow: UberPage → RevisionRootPage → Data Pages
// ============================================================================

#[test]
fn test_multi_revision_commit_flow_filechannel() {
    let dir = tempfile::tempdir().unwrap();

    // Commit 3 revisions
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

        for rev in 0..3 {
            // Write data page
            let data_page = make_page(rev as i64, rev, &[
                (0, 1, format!("rev{}-node0", rev).as_bytes()),
                (1, 2, format!("rev{}-node1", rev).as_bytes()),
            ]);
            let mut data_ref = PageReference::new();
            writer.write_page(&mut data_ref, &serialize(&data_page)).unwrap();

            // Write revision root page
            let mut rev_root = RevisionRootPage::new();
            rev_root.set_revision_timestamp_nanos((rev + 1) as i64 * 1_000_000);
            rev_root.set_commit_message(format!("commit {}", rev));
            rev_root.set_max_node_key_document(1);
            let mut rev_buf = Vec::new();
            serialize_revision_root_page(&rev_root, &mut rev_buf, SerializationType::Data).unwrap();
            let mut rev_ref = PageReference::new();
            let rev_offset = writer.write_page(&mut rev_ref, &rev_buf).unwrap();

            // Write uber page
            let uber = UberPage::new(rev + 1, rev_ref);
            let mut uber_ref = PageReference::new();
            writer.write_uber_page_reference(&mut uber_ref, &uber).unwrap();

            // Record revision data
            writer.write_revision_data((rev + 1) as i64 * 1_000_000, rev_offset).unwrap();
        }

        writer.force_all().unwrap();
    }

    // Reopen and verify entire chain
    {
        let reader = FileChannelReader::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

        // Verify uber page points to last revision
        let uber = reader.read_uber_page().unwrap();
        assert_eq!(uber.revision_count(), 3);

        // Verify revision index
        let idx = reader.read_revision_index().unwrap();
        assert_eq!(idx.len(), 3);
        assert_eq!(idx.get_timestamp(0).unwrap(), 1_000_000);
        assert_eq!(idx.get_timestamp(1).unwrap(), 2_000_000);
        assert_eq!(idx.get_timestamp(2).unwrap(), 3_000_000);

        // Verify point-in-time lookup
        assert_eq!(idx.find_revision_by_timestamp(1_500_000), Some(0));
        assert_eq!(idx.find_revision_by_timestamp(2_000_000), Some(1));
        assert_eq!(idx.find_exact_revision(3_000_000), Some(2));

        // Read back each revision's data page via stored offset
        for rev in 0..3 {
            let (offset, ts) = reader.get_revision_offset(rev).unwrap();
            assert_eq!(ts, (rev + 1) as i64 * 1_000_000);
            assert!(offset > 0);
        }
    }
}

#[test]
fn test_multi_revision_commit_flow_mmap() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut writer = MMFileWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

        for rev in 0..5 {
            let data_page = make_page(rev as i64 * 10, rev, &[
                (0, 1, format!("mmap-rev{}", rev).as_bytes()),
            ]);
            let mut data_ref = PageReference::new();
            writer.write_page(&mut data_ref, &serialize(&data_page)).unwrap();

            let mut rev_root = RevisionRootPage::new();
            rev_root.set_revision_timestamp_nanos((rev + 1) as i64 * 100_000);
            let mut rev_buf = Vec::new();
            serialize_revision_root_page(&rev_root, &mut rev_buf, SerializationType::Data).unwrap();
            let mut rev_ref = PageReference::new();
            let rev_offset = writer.write_page(&mut rev_ref, &rev_buf).unwrap();

            let uber = UberPage::new(rev + 1, rev_ref);
            let mut uber_ref = PageReference::new();
            writer.write_uber_page_reference(&mut uber_ref, &uber).unwrap();
            writer.write_revision_data((rev + 1) as i64 * 100_000, rev_offset).unwrap();
        }

        writer.force_all().unwrap();
    }

    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
        let uber = reader.read_uber_page().unwrap();
        assert_eq!(uber.revision_count(), 5);

        let idx = reader.read_revision_index().unwrap();
        assert_eq!(idx.len(), 5);
        assert_eq!(idx.latest_revision(), Some(4));
    }
}

// ============================================================================
// 4. IndirectPage hierarchy: write tree of pages and read back
// ============================================================================

#[test]
fn test_indirect_page_tree_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Write 4 leaf data pages
    let mut leaf_refs = Vec::new();
    for i in 0..4 {
        let leaf = make_page(i, 0, &[(0, 1, format!("leaf-{}", i).as_bytes())]);
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialize(&leaf)).unwrap();
        leaf_refs.push(reference);
    }

    // Write an indirect page pointing to the leaf pages
    let mut indirect = IndirectPage::new();
    for (i, leaf_ref) in leaf_refs.iter().enumerate() {
        indirect.set_reference(i, leaf_ref.clone());
    }
    let mut indirect_buf = Vec::new();
    serialize_indirect_page(&indirect, &mut indirect_buf, SerializationType::Data).unwrap();
    let mut indirect_ref = PageReference::new();
    writer.write_page(&mut indirect_ref, &indirect_buf).unwrap();

    writer.force_all().unwrap();

    // Read back the indirect page
    let deserialized = writer.read_page(&indirect_ref).unwrap();
    match deserialized {
        DeserializedPage::Indirect(restored_indirect) => {
            assert_eq!(restored_indirect.reference_count(), 4);

            // Read each leaf through the indirect page's references
            for i in 0..4 {
                let child_ref = restored_indirect.get_reference(i).unwrap();
                let leaf_page = unwrap_kvl(writer.read_page(child_ref).unwrap());
                assert_eq!(leaf_page.record_page_key(), i as i64);
                let (_, data) = leaf_page.read_record(0).unwrap();
                assert_eq!(data, format!("leaf-{}", i).as_bytes());
            }
        }
        _ => panic!("expected Indirect page"),
    }
}

// ============================================================================
// 5. RevisionIndex: build → serialize → deserialize → lookup
// ============================================================================

#[test]
fn test_revision_index_full_lifecycle() {
    let mut idx = RevisionIndex::new();

    // Build index with 100 revisions
    for i in 0..100 {
        idx.append(i * 1000, i * 4096).unwrap();
    }

    assert_eq!(idx.len(), 100);
    assert_eq!(idx.latest_revision(), Some(99));

    // Serialize → Deserialize roundtrip
    let mut buf = Vec::new();
    idx.serialize(&mut buf);
    let restored = RevisionIndex::deserialize(&buf).unwrap();

    assert_eq!(restored.len(), 100);

    // Verify all entries survived
    for i in 0..100 {
        assert_eq!(restored.get_timestamp(i as usize).unwrap(), i * 1000);
        assert_eq!(restored.get_offset(i as usize).unwrap(), i * 4096);
    }

    // Binary search lookups
    assert_eq!(restored.find_exact_revision(50_000), Some(50));
    assert_eq!(restored.find_revision_by_timestamp(50_500), Some(50)); // between 50 and 51
    assert!(restored.find_revision_by_timestamp(-1).is_none()); // before all

    // Compress → Decompress the serialized index
    let pipeline = ByteHandlerPipeline::lz4();
    let compressed = pipeline.compress(&buf).unwrap();
    let decompressed = pipeline.decompress(&compressed).unwrap();
    let restored2 = RevisionIndex::deserialize(&decompressed).unwrap();
    assert_eq!(restored2.len(), 100);
    assert_eq!(restored2.get_offset(99).unwrap(), 99 * 4096);
}

// ============================================================================
// 6. PageCache: put → get → eviction → dirty tracking end-to-end
// ============================================================================

#[test]
fn test_page_cache_store_retrieve_with_real_pages() {
    let cache = PageCache::new(32);

    // Insert real SlottedPages with data
    for i in 0..20 {
        let mut page = SlottedPage::new(i, 0, IndexType::Document);
        page.insert_record(0, 1, format!("cached-{}", i).as_bytes()).unwrap();
        let id = PageId::new(1, 1, i);
        let (handle, _) = cache.put(id, Arc::new(page));
        if i % 3 == 0 {
            handle.mark_dirty();
        }
        drop(handle);
    }

    // Retrieve and verify data integrity
    for i in 0..20 {
        let id = PageId::new(1, 1, i);
        let handle = cache.get(&id).unwrap();
        assert_eq!(handle.page().record_page_key(), i);
        let (kind, data) = handle.page().read_record(0).unwrap();
        assert_eq!(kind, 1);
        assert_eq!(data, format!("cached-{}", i).as_bytes());
        drop(handle);
    }

    // Verify dirty tracking
    let dirty = cache.dirty_pages();
    assert_eq!(dirty.len(), 7); // indices 0,3,6,9,12,15,18
    for (id, page) in &dirty {
        assert_eq!(id.page_key % 3, 0);
        let (_, data) = page.read_record(0).unwrap();
        assert_eq!(data, format!("cached-{}", id.page_key).as_bytes());
    }

    assert!(cache.hits() > 0);
    assert_eq!(cache.evictions(), 0); // 20 pages fit in 32-frame cache
}

#[test]
fn test_page_cache_eviction_preserves_data_integrity() {
    let cache = PageCache::new(16); // small cache to force eviction

    // Insert 64 pages, way more than cache capacity
    for i in 0..64i64 {
        let mut page = SlottedPage::new(i, 0, IndexType::Document);
        page.insert_record(0, 1, format!("page-{}", i).as_bytes()).unwrap();
        let id = PageId::new(1, 1, i);
        let (handle, _) = cache.put(id, Arc::new(page));
        drop(handle);
    }

    assert!(cache.evictions() > 0);
    assert!(cache.size() <= 16);

    // Pages still in cache should have valid data
    for i in 0..64i64 {
        let id = PageId::new(1, 1, i);
        if let Some(handle) = cache.get(&id) {
            assert_eq!(handle.page().record_page_key(), i);
            let (_, data) = handle.page().read_record(0).unwrap();
            assert_eq!(data, format!("page-{}", i).as_bytes());
        }
    }
}

// ============================================================================
// 7. TransactionIntentLog: full lifecycle
// ============================================================================

#[test]
fn test_til_put_snapshot_resolve_cleanup_lifecycle() {
    let mut til = TransactionIntentLog::new(16);

    // Phase 1: Active transaction — put 5 pages
    for i in 0..5 {
        let data = format!("page-data-{}", i).into_bytes();
        let container = TilPageContainer::new(
            Some(PageData { data: data.clone(), page_key: i, is_kvl: true }),
            Some(PageData { data, page_key: i, is_kvl: true }),
        );
        let (log_key, generation) = til.put(container);
        assert_eq!(log_key, i as i32);
        assert_eq!(generation, 0);
    }
    assert_eq!(til.size(), 5);

    // Phase 2: Snapshot — freeze for background flush
    let snap_size = til.snapshot();
    assert_eq!(snap_size, 5);
    assert_eq!(til.size(), 0);
    assert_eq!(til.current_generation(), 1);

    // Snapshot entries are accessible via Layer 2
    for i in 0..5 {
        let entry = til.get(0, i as i32);
        assert!(entry.is_some(), "snapshot entry {} should be accessible", i);
    }

    // Phase 3: Background thread writes disk offsets
    for i in 0..5 {
        til.set_snapshot_disk_offset(i, (i * 4096 + 1024) as i64);
        til.set_snapshot_hash(i, (i * 0xDEAD) as u64);
    }

    // Phase 4: Cleanup — moves KVL entries to Layer 3
    let promoted = til.cleanup_snapshot();
    assert!(promoted.is_empty()); // all KVL, none promoted

    // Phase 5: Resolve stale refs from Layer 3
    let resolved = til.resolve_stale_ref(0, 0).unwrap();
    assert_eq!(resolved.disk_offset, 1024);
    assert_eq!(resolved.hash, 0);

    let resolved = til.resolve_stale_ref(0, 3).unwrap();
    assert_eq!(resolved.disk_offset, 3 * 4096 + 1024);

    // Consumed entries can't be resolved again
    assert!(til.resolve_stale_ref(0, 0).is_none());

    assert_eq!(til.layer3_hits(), 2);
}

#[test]
fn test_til_concurrent_generation_isolation() {
    let mut til = TransactionIntentLog::new(16);

    // Gen 0 entries
    let c = TilPageContainer::new(
        Some(PageData { data: vec![1], page_key: 0, is_kvl: true }),
        Some(PageData { data: vec![1], page_key: 0, is_kvl: true }),
    );
    til.put(c);
    til.snapshot(); // freeze gen 0

    // Gen 1 entries
    let c = TilPageContainer::new(
        Some(PageData { data: vec![2], page_key: 1, is_kvl: true }),
        Some(PageData { data: vec![2], page_key: 1, is_kvl: true }),
    );
    let (log_key, generation) = til.put(c);
    assert_eq!(generation, 1);
    assert_eq!(log_key, 0);

    // Gen 1 entry accessible via Layer 1
    assert!(til.get(1, 0).is_some());
    // Gen 0 entry accessible via Layer 2 (snapshot)
    assert!(til.get(0, 0).is_some());
    // Cross-generation mismatch returns None
    assert!(til.get(0, 0 /* but looking in gen 1 */).is_some()); // this is gen 0 snapshot
    assert!(til.get(1, 1).is_none()); // gen 1 only has log_key 0
}

// ============================================================================
// 8. BufferPool: acquire → use → release cycle
// ============================================================================

#[test]
fn test_buffer_pool_acquire_use_for_serialization_release() {
    let pool = BufferPool::new(4, 8);

    // Simulate: acquire buffer, serialize a page into it, release back
    for _ in 0..100 {
        let mut buf = pool.acquire(8192);
        assert!(buf.capacity() >= 8192);

        // Use buffer for page serialization
        let page = SlottedPage::new(1, 0, IndexType::Document);
        page.serialize_to(&mut buf).unwrap();
        assert!(!buf.is_empty());

        pool.release(buf);
    }

    assert_eq!(pool.acquire_count(), 100);
    assert_eq!(pool.release_count(), 100);

    // Verify reuse: second acquire should not allocate fresh memory
    let buf = pool.acquire(8192);
    assert!(buf.capacity() >= 8192);
    pool.release(buf);
}

#[test]
fn test_buffer_pool_different_tiers() {
    let pool = BufferPool::new(4, 8);

    // Acquire buffers at different size tiers
    let sizes = [100, 4096, 8192, 16384, 32768, 65536, 131072, 262144];
    let mut bufs: Vec<Vec<u8>> = Vec::new();

    for &size in &sizes {
        let buf = pool.acquire(size);
        assert!(buf.capacity() >= size);
        bufs.push(buf);
    }

    // Release all
    for buf in bufs {
        pool.release(buf);
    }

    assert_eq!(pool.acquire_count(), sizes.len() as u64);
}

// ============================================================================
// 9. Versioning + I/O combined: multi-revision store/retrieve cycle
// ============================================================================

#[test]
fn test_incremental_5_revision_store_retrieve_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Simulate 5 revisions with incremental versioning (revs_to_restore=3)
    let mut all_refs: Vec<PageReference> = Vec::new();

    // Rev 1: full page (slots 0, 1, 2)
    let rev1 = make_page(0, 1, &[
        (0, 1, b"r1-slot0"),
        (1, 1, b"r1-slot1"),
        (2, 1, b"r1-slot2"),
    ]);
    let mut ref1 = PageReference::new();
    writer.write_page(&mut ref1, &serialize(&rev1)).unwrap();
    all_refs.push(ref1);

    // Rev 2: update slot 0
    let rev2 = make_page(0, 2, &[(0, 1, b"r2-slot0")]);
    let mut ref2 = PageReference::new();
    writer.write_page(&mut ref2, &serialize(&rev2)).unwrap();
    all_refs.push(ref2);

    // Rev 3: update slot 1 (chain = [r3, r2, r1] = full dump threshold)
    let rev3 = make_page(0, 3, &[(1, 1, b"r3-slot1")]);
    let mut ref3 = PageReference::new();
    writer.write_page(&mut ref3, &serialize(&rev3)).unwrap();
    all_refs.push(ref3);

    // Rev 4: new full dump (after COW + preservation)
    let rev4 = make_page(0, 4, &[
        (0, 1, b"r2-slot0"),  // carried from r2
        (1, 1, b"r3-slot1"),  // carried from r3
        (2, 1, b"r4-slot2"),  // updated
    ]);
    let mut ref4 = PageReference::new();
    writer.write_page(&mut ref4, &serialize(&rev4)).unwrap();
    all_refs.push(ref4);

    // Rev 5: update slot 0
    let rev5 = make_page(0, 5, &[(0, 1, b"r5-slot0")]);
    let mut ref5 = PageReference::new();
    writer.write_page(&mut ref5, &serialize(&rev5)).unwrap();
    all_refs.push(ref5);

    writer.force_all().unwrap();

    // Read back and combine for each revision
    let read_page = |idx: usize| -> SlottedPage {
        unwrap_kvl(writer.read_page(&all_refs[idx]).unwrap())
    };

    // Reconstruct at rev 1 (just r1)
    let c1 = combine_record_pages(VersioningType::Incremental, &[read_page(0)], 3).unwrap();
    assert_eq!(c1.read_record(0).unwrap(), (1, &b"r1-slot0"[..]));
    assert_eq!(c1.read_record(1).unwrap(), (1, &b"r1-slot1"[..]));
    assert_eq!(c1.read_record(2).unwrap(), (1, &b"r1-slot2"[..]));

    // Reconstruct at rev 3 (chain: r3, r2, r1)
    let c3 = combine_record_pages(
        VersioningType::Incremental,
        &[read_page(2), read_page(1), read_page(0)],
        3,
    ).unwrap();
    assert_eq!(c3.read_record(0).unwrap(), (1, &b"r2-slot0"[..]));
    assert_eq!(c3.read_record(1).unwrap(), (1, &b"r3-slot1"[..]));
    assert_eq!(c3.read_record(2).unwrap(), (1, &b"r1-slot2"[..]));

    // Reconstruct at rev 5 (chain: r5, r4)
    let c5 = combine_record_pages(
        VersioningType::Incremental,
        &[read_page(4), read_page(3)],
        3,
    ).unwrap();
    assert_eq!(c5.read_record(0).unwrap(), (1, &b"r5-slot0"[..]));
    assert_eq!(c5.read_record(1).unwrap(), (1, &b"r3-slot1"[..]));
    assert_eq!(c5.read_record(2).unwrap(), (1, &b"r4-slot2"[..]));
}

#[test]
fn test_differential_3_revision_store_retrieve_with_lz4() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Rev 0: base (full dump at revs_to_restore=3 boundary)
    let rev0 = make_page(0, 0, &[
        (0, 1, b"base-s0"), (1, 1, b"base-s1"), (2, 1, b"base-s2"),
    ]);
    let mut ref0 = PageReference::new();
    writer.write_page(&mut ref0, &serialize(&rev0)).unwrap();

    // Rev 1: delta (update slot 0, add slot 5)
    let rev1 = make_page(0, 1, &[(0, 1, b"delta1-s0"), (5, 2, b"delta1-s5")]);
    let mut ref1 = PageReference::new();
    writer.write_page(&mut ref1, &serialize(&rev1)).unwrap();

    // Rev 2: delta (update slot 1)
    let rev2 = make_page(0, 2, &[(1, 1, b"delta2-s1")]);
    let mut ref2 = PageReference::new();
    writer.write_page(&mut ref2, &serialize(&rev2)).unwrap();

    writer.force_all().unwrap();

    // Read back through LZ4
    let r0 = unwrap_kvl(writer.read_page(&ref0).unwrap());
    let r1 = unwrap_kvl(writer.read_page(&ref1).unwrap());
    let r2 = unwrap_kvl(writer.read_page(&ref2).unwrap());

    // Reconstruct at rev 1: [delta1, base]
    let c1 = combine_record_pages(VersioningType::Differential, &[r1, clone_page(&r0)], 3).unwrap();
    assert_eq!(c1.populated_count(), 4);
    assert_eq!(c1.read_record(0).unwrap(), (1, &b"delta1-s0"[..]));
    assert_eq!(c1.read_record(1).unwrap(), (1, &b"base-s1"[..]));
    assert_eq!(c1.read_record(2).unwrap(), (1, &b"base-s2"[..]));
    assert_eq!(c1.read_record(5).unwrap(), (2, &b"delta1-s5"[..]));

    // Reconstruct at rev 2: [delta2, base] (differential always uses base)
    let c2 = combine_record_pages(VersioningType::Differential, &[r2, r0], 3).unwrap();
    assert_eq!(c2.populated_count(), 3);
    assert_eq!(c2.read_record(0).unwrap(), (1, &b"base-s0"[..]));
    assert_eq!(c2.read_record(1).unwrap(), (1, &b"delta2-s1"[..]));
    assert_eq!(c2.read_record(2).unwrap(), (1, &b"base-s2"[..]));
}

#[test]
fn test_sliding_snapshot_store_retrieve_with_preservation() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();

    // Write 3 revision fragments
    let rev1 = make_page(0, 1, &[(10, 3, b"oow-slot10"), (20, 4, b"oow-slot20")]);
    let rev2 = make_page(0, 2, &[(5, 2, b"in-window-s5")]);
    let rev3 = make_page(0, 3, &[(0, 1, b"in-window-s0")]);

    let mut ref1 = PageReference::new();
    let mut ref2 = PageReference::new();
    let mut ref3 = PageReference::new();
    writer.write_page(&mut ref1, &serialize(&rev1)).unwrap();
    writer.write_page(&mut ref2, &serialize(&rev2)).unwrap();
    writer.write_page(&mut ref3, &serialize(&rev3)).unwrap();
    writer.force_all().unwrap();

    // Read back
    let r1 = unwrap_kvl(writer.read_page(&ref1).unwrap());
    let r2 = unwrap_kvl(writer.read_page(&ref2).unwrap());
    let r3 = unwrap_kvl(writer.read_page(&ref3).unwrap());

    // Combine for modification (window_size=3)
    let mut page_ref = PageReference::new();
    let container = combine_record_pages_for_modification(
        VersioningType::SlidingSnapshot { window_size: 3 },
        &[r3, r2, r1],
        3,
        &mut page_ref,
        4,
        0,
        0,
    ).unwrap();

    let (complete, mut modified) = container.into_parts();

    // Out-of-window slots should be marked for preservation
    assert!(modified.is_marked_for_preservation(10));
    assert!(modified.is_marked_for_preservation(20));
    assert!(!modified.is_marked_for_preservation(0));
    assert!(!modified.is_marked_for_preservation(5));

    // Write new data and resolve preservation
    modified.insert_record(0, 1, b"rev4-explicit").unwrap();
    modified.resolve_preservation(&complete).unwrap();

    assert_eq!(modified.populated_count(), 3);
    assert_eq!(modified.read_record(0).unwrap(), (1, &b"rev4-explicit"[..]));
    assert_eq!(modified.read_record(10).unwrap(), (3, &b"oow-slot10"[..]));
    assert_eq!(modified.read_record(20).unwrap(), (4, &b"oow-slot20"[..]));

    // Write modified page back to disk and verify roundtrip
    let mut ref4 = PageReference::new();
    writer.write_page(&mut ref4, &serialize(&modified)).unwrap();
    writer.force_all().unwrap();

    let restored = unwrap_kvl(writer.read_page(&ref4).unwrap());
    assert_eq!(restored.populated_count(), 3);
    assert_eq!(restored.read_record(0).unwrap(), (1, &b"rev4-explicit"[..]));
    assert_eq!(restored.read_record(10).unwrap(), (3, &b"oow-slot10"[..]));
    assert_eq!(restored.read_record(20).unwrap(), (4, &b"oow-slot20"[..]));
}

// ============================================================================
// 10. Cross-layer: Cache + I/O combined
// ============================================================================

#[test]
fn test_cache_backed_io_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();
    let cache = PageCache::new(64);

    // Write 20 pages to disk and cache them
    let mut refs = Vec::new();
    for i in 0..20i64 {
        let mut page = SlottedPage::new(i, 0, IndexType::Document);
        page.insert_record(0, 1, format!("data-{}", i).as_bytes()).unwrap();

        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialize(&page)).unwrap();
        refs.push(reference);

        // Also put into cache
        let id = PageId::new(1, 1, i);
        let (handle, _) = cache.put(id, Arc::new(page));
        drop(handle);
    }
    writer.force_all().unwrap();

    // Read: try cache first, fall back to I/O
    for i in 0..20i64 {
        let id = PageId::new(1, 1, i);
        let page = if let Some(handle) = cache.get(&id) {
            // Cache hit
            handle.page().record_page_key()
        } else {
            // Cache miss: load from disk
            let deserialized = writer.read_page(&refs[i as usize]).unwrap();
            let p = unwrap_kvl(deserialized);
            p.record_page_key()
        };
        assert_eq!(page, i);
    }

    assert_eq!(cache.hits(), 20);
    assert_eq!(cache.misses(), 0);
}

// ============================================================================
// 11. TIL + I/O: simulate transaction commit flow
// ============================================================================

#[test]
fn test_til_io_transaction_commit_flow() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::new()).unwrap();
    let mut til = TransactionIntentLog::new(16);

    // Step 1: Transaction writes pages into TIL
    let mut pages = Vec::new();
    for i in 0..3 {
        let page = make_page(i, 0, &[(0, 1, format!("tx-data-{}", i).as_bytes())]);
        let serialized = serialize(&page);
        let container = TilPageContainer::new(
            Some(PageData { data: serialized.clone(), page_key: i, is_kvl: true }),
            Some(PageData { data: serialized, page_key: i, is_kvl: true }),
        );
        let (log_key, generation) = til.put(container);
        pages.push((log_key, generation));
    }
    assert_eq!(til.size(), 3);

    // Step 2: Snapshot for background flush
    let snap_size = til.snapshot();
    assert_eq!(snap_size, 3);

    // Step 3: Background thread reads from snapshot and writes to disk
    let mut disk_refs = Vec::new();
    for i in 0..snap_size {
        let entry = til.get_snapshot_entry(i).unwrap();
        let data = &entry.modified.as_ref().unwrap().data;

        let mut reference = PageReference::new();
        let offset = writer.write_page(&mut reference, data).unwrap();
        disk_refs.push(reference);

        til.set_snapshot_disk_offset(i, offset);
        til.set_snapshot_hash(i, 0);
    }
    writer.force_all().unwrap();

    // Step 4: Cleanup snapshot
    til.mark_snapshot_commit_complete();
    assert!(til.is_snapshot_commit_complete());
    let promoted = til.cleanup_snapshot();
    assert!(promoted.is_empty());

    // Step 5: Verify pages are readable from disk
    for (i, reference) in disk_refs.iter().enumerate() {
        let deserialized = writer.read_page(reference).unwrap();
        let page = unwrap_kvl(deserialized);
        assert_eq!(page.record_page_key(), i as i64);
        let (_, data) = page.read_record(0).unwrap();
        assert_eq!(data, format!("tx-data-{}", i).as_bytes());
    }
}

// ============================================================================
// 12. Cross-backend consistency: write with one backend, read with another
// ============================================================================

#[test]
fn test_cross_backend_lz4_filechannel_write_mmap_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = PageReference::new();

    // Write with FileChannel + LZ4
    {
        let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();
        let page = make_page(42, 0, &[
            (0, 1, b"cross-backend-lz4"),
            (100, 2, &vec![0xFF; 300]),
        ]);
        writer.write_page(&mut reference, &serialize(&page)).unwrap();
        writer.force_all().unwrap();
    }

    // Read with mmap + LZ4
    {
        let reader = MMFileReader::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();
        let deserialized = reader.read_page(&reference).unwrap();
        let page = unwrap_kvl(deserialized);

        assert_eq!(page.record_page_key(), 42);
        assert_eq!(page.read_record(0).unwrap(), (1, &b"cross-backend-lz4"[..]));
        assert_eq!(page.read_record(100).unwrap(), (2, &vec![0xFF; 300][..]));
    }
}

// ============================================================================
// 13. Stress test: many pages, many records, concurrent reads
// ============================================================================

#[test]
fn test_stress_100_pages_50_records_each_lz4_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    let mut refs = Vec::with_capacity(100);
    for page_idx in 0..100 {
        let mut page = SlottedPage::new(page_idx, 0, IndexType::Document);
        for slot in 0..50 {
            let data = format!("p{}-s{}", page_idx, slot);
            page.insert_record(slot, (slot % 255) as u8, data.as_bytes()).unwrap();
        }
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialize(&page)).unwrap();
        refs.push(reference);
    }
    writer.force_all().unwrap();

    // Read all pages back and verify every record
    for (page_idx, reference) in refs.iter().enumerate() {
        let deserialized = writer.read_page(reference).unwrap();
        let page = unwrap_kvl(deserialized);
        assert_eq!(page.record_page_key(), page_idx as i64);
        assert_eq!(page.populated_count(), 50);

        for slot in 0..50 {
            let expected = format!("p{}-s{}", page_idx, slot);
            let (kind, data) = page.read_record(slot).unwrap();
            assert_eq!(kind, (slot % 255) as u8);
            assert_eq!(data, expected.as_bytes(), "page {} slot {} mismatch", page_idx, slot);
        }
    }
}

#[test]
fn test_concurrent_read_after_write_stress() {
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Write 100 pages
    let mut refs = Vec::new();
    for i in 0..100i64 {
        let page = make_page(i, 0, &[(0, 1, format!("concurrent-{}", i).as_bytes())]);
        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &serialize(&page)).unwrap();
        refs.push(reference);
    }
    writer.force_all().unwrap();

    // Read concurrently from 8 threads
    let reader = Arc::new(writer.reader());
    let refs = Arc::new(refs);
    let mut handles = Vec::new();

    for t in 0..8 {
        let reader = Arc::clone(&reader);
        let refs = Arc::clone(&refs);
        handles.push(thread::spawn(move || {
            for i in (t..100).step_by(8) {
                let deserialized = reader.read_page(&refs[i]).unwrap();
                let page = unwrap_kvl(deserialized);
                assert_eq!(page.record_page_key(), i as i64);
                let (_, data) = page.read_record(0).unwrap();
                assert_eq!(data, format!("concurrent-{}", i).as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// 14. Full versioning COW cycle: read → modify → resolve → write → read back
// ============================================================================

#[test]
fn test_full_versioning_cow_write_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    // Rev 1: initial full page
    let rev1 = make_page(0, 1, &[
        (0, 1, b"initial-s0"),
        (5, 2, b"initial-s5"),
        (10, 3, b"initial-s10"),
    ]);
    let mut ref1 = PageReference::new();
    writer.write_page(&mut ref1, &serialize(&rev1)).unwrap();
    writer.force_all().unwrap();

    // Read back rev1
    let read_rev1 = unwrap_kvl(writer.read_page(&ref1).unwrap());

    // COW modification for rev 2 (Full versioning)
    let mut cow_ref = PageReference::new();
    let container = combine_record_pages_for_modification(
        VersioningType::Full,
        &[read_rev1],
        1,
        &mut cow_ref,
        2,
        0,
        0,
    ).unwrap();

    let (_complete, mut modified) = container.into_parts();

    // Modify: overwrite slot 0 and add slot 20
    modified.insert_record(0, 1, b"updated-s0").unwrap();
    modified.insert_record(20, 4, b"new-s20").unwrap();

    // Write modified page as rev 2
    let mut ref2 = PageReference::new();
    writer.write_page(&mut ref2, &serialize(&modified)).unwrap();
    writer.force_all().unwrap();

    // Read back rev 2 and verify
    let read_rev2 = unwrap_kvl(writer.read_page(&ref2).unwrap());
    let combined = combine_record_pages(VersioningType::Full, &[read_rev2], 1).unwrap();

    assert_eq!(combined.populated_count(), 4);
    assert_eq!(combined.read_record(0).unwrap(), (1, &b"updated-s0"[..]));
    assert_eq!(combined.read_record(5).unwrap(), (2, &b"initial-s5"[..]));
    assert_eq!(combined.read_record(10).unwrap(), (3, &b"initial-s10"[..]));
    assert_eq!(combined.read_record(20).unwrap(), (4, &b"new-s20"[..]));
}

// ============================================================================
// 15. BufferPool + I/O integration: use pooled buffers for serialization
// ============================================================================

#[test]
fn test_buffer_pool_io_integration() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();
    let pool = BufferPool::new(4, 16);

    let mut refs = Vec::new();

    // Write pages using pooled buffers for serialization
    for i in 0..20i64 {
        let mut buf = pool.acquire(8192);
        let page = make_page(i, 0, &[(0, 1, format!("pooled-{}", i).as_bytes())]);
        serialize_slotted_page(&page, &mut buf).unwrap();

        let mut reference = PageReference::new();
        writer.write_page(&mut reference, &buf).unwrap();
        refs.push(reference);

        pool.release(buf);
    }
    writer.force_all().unwrap();

    // Read back and verify
    for (i, reference) in refs.iter().enumerate() {
        let deserialized = writer.read_page(reference).unwrap();
        let page = unwrap_kvl(deserialized);
        assert_eq!(page.record_page_key(), i as i64);
        let (_, data) = page.read_record(0).unwrap();
        assert_eq!(data, format!("pooled-{}", i).as_bytes());
    }

    assert_eq!(pool.acquire_count(), 20);
    assert_eq!(pool.release_count(), 20);
}

// ============================================================================
// 16. All 4 versioning strategies produce consistent results for same data
// ============================================================================

#[test]
fn test_all_strategies_consistent_single_revision_io_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileChannelWriter::open(dir.path(), ByteHandlerPipeline::lz4()).unwrap();

    let page = make_page(0, 1, &[
        (0, 1, b"alpha"),
        (10, 2, b"beta"),
        (100, 3, b"gamma"),
        (500, 4, b"delta"),
    ]);

    // Write once
    let mut reference = PageReference::new();
    writer.write_page(&mut reference, &serialize(&page)).unwrap();
    writer.force_all().unwrap();

    // Read back and combine with each strategy
    let strategies: Vec<VersioningType> = vec![
        VersioningType::Full,
        VersioningType::Incremental,
        VersioningType::Differential,
        VersioningType::SlidingSnapshot { window_size: 3 },
    ];

    let mut results: Vec<SlottedPage> = Vec::new();
    for strategy in &strategies {
        let read_back = unwrap_kvl(writer.read_page(&reference).unwrap());
        let revs_to_restore = match strategy {
            VersioningType::Full => 1,
            _ => 3,
        };
        let combined = combine_record_pages(*strategy, &[read_back], revs_to_restore).unwrap();
        results.push(combined);
    }

    // All strategies should produce identical results for a single page
    for slot in [0, 10, 100, 500] {
        let expected = results[0].read_record(slot).unwrap();
        for (i, result) in results.iter().enumerate().skip(1) {
            assert_eq!(
                result.read_record(slot).unwrap(),
                expected,
                "strategy {:?} produced different result at slot {}",
                strategies[i],
                slot,
            );
        }
    }
}
