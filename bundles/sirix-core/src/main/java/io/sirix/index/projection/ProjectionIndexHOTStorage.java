/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.HOTBulkSlotLoader;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.node.LE;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.function.Consumer;

/**
 * HOT-backed persistent storage for projection-index leaf payloads in the <b>segment-slot
 * layout</b> (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3, §3, §4).
 *
 * <h2>Storage contract</h2>
 *
 * <ul>
 * <li><b>Slot keys</b> — every segment gets its OWN HOT slot under a composite key
 * {@code (rowGroupId << 16) | slotKind}, serialized by {@code PathKeySerializer} (sign-flipped
 * 8-byte BE, so unsigned byte comparison preserves signed-long order). {@code slotKind 0} is the
 * row group's zone-map descriptor, {@code slotKind
 *       columnSegmentId + 1} that segment's bytes — so one row group's slots are key-adjacent and a
 * range scan reads them in one descent. A fresh build assigns contiguous ids from 1; incremental
 * splits and deletes may later reuse ids or leave physical gaps. Document order is therefore the
 * explicit order stored with the fence chunks, and
 * {@link #readAllRowGroupsFromColumnSegmentSlots(StorageEngineReader, int, int, int[])} validates
 * that exact ordered descriptor set rather than inferring position from numeric id order. A
 * missing, duplicate, or extra descriptor fails loudly instead of silently mislabelling following
 * rows.</li>
 * <li><b>Descriptor slot value = RowGroupDescriptor (PIXD)</b> — a zone-map-only directory of the
 * leaf's semantic segments (KEYS, per-column BODY/DICT), each entry carrying columnSegmentId,
 * byteLen and an XXH3-64 content hash. It holds no segment bytes: the hashes are what a later
 * assembly verifies its slots against.</li>
 * <li><b>Segment slot value</b> — BARE (no marker, no second hash: the descriptor entry already
 * carries byteLen + hash). Payloads no larger than {@link #BLOB_INLINE_MAX} sit inline behind a
 * discriminator byte; larger ones become a lone reference byte plus one CoW-versioned
 * {@link OverflowPage}, hung off the owning HOT leaf's side map under {@code (slotKey << 16) | 0} —
 * references follow their owning slot across arbitrary split cascades.</li>
 * <li><b>Blob slots (PIXB)</b> — opaque payloads (the PIXM metadata bytes, the fence chunks) stored
 * via {@link #putBlob}: the value is a small marker with byteLen + hash, the payload is inline or
 * one page, and reads are length/hash-verified ({@link #verifyBlob}).</li>
 * <li><b>Assembly</b> — {@link #getRowGroupFromColumnSegmentSlots} /
 * {@link #readRowGroupFromColumnSegmentSlots} / {@link #readAllRowGroupsFromColumnSegmentSlots}
 * reassemble the raw leaf form from the descriptor's segment slots;
 * {@code ProjectionIndexColumnSegmentCodec} verifies each segment's hash so torn or internally
 * inconsistent stores fail loudly instead of misparsing.</li>
 * <li><b>Tombstone vs live-empty</b> — a zero-length slot value is a tombstone (absent leaf,
 * skipped by enumeration); a live EMPTY leaf is a descriptor whose segments encode zero rows and
 * still round-trips. An unchanged segment is carried forward by reference (equal byteLen + hash →
 * no write at all), which is the SLIDING_SNAPSHOT containment no-op asserted on durable offsets by
 * {@link #segmentPageOffset}.</li>
 * </ul>
 *
 * <h2>Historical failure families (regression-guarded)</h2>
 *
 * Two pre-redesign bug families remain guarded by tests: <b>grow-overwrite</b> (larger re-puts
 * silently dropped values that no longer fit — all writes now funnel through the single incremental
 * HOT driver) and <b>stale-swizzle use-after-close</b> (CoW'd references resolving a closed
 * {@link HOTLeafPage} — {@link PageReference#getPage()} treats a closed leaf as a cache miss). See
 * {@code ProjectionPersistForceRebuildTest} (sirix-query).
 */
public final class ProjectionIndexHOTStorage extends AbstractHOTIndexWriter<Long> {

  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(ProjectionIndexHOTStorage.class));

  /**
   * Shared upper bound for the segment-slot row-group id space. Composite row-group slots use
   * {@code rowGroupId << 16}; bounding the id at 2^24 makes the exact maximum composite key
   * {@code (2^24 << 16) | 0xffff = 2^40 + 65535}, safely below the fence (2^42) and Bloom-chunk
   * (2^43) namespaces.
   */
  public static final int MAX_ROW_GROUPS = 1 << 24;

  /** Diagnostic switch shared with the executor's {@code sirix.projDiag}. */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

  /** Zero-length slot value marking a tombstoned slot (HOT has no per-entry delete). */
  private static final byte[] TOMBSTONE = new byte[0];

  /** 8-byte scratch for encoding slot keys. */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[8]);

  private final PathKeySerializer keySerializer = PathKeySerializer.INSTANCE;

  /** Whether fresh side refs may enter the bounded pre-publication append pipeline. */
  private final boolean stageFreshSidePages;

  /**
   * Entry-count cap for bulk slot accumulation ({@code docs/HOT_BULK_BUILD.md} §2: at 8 M entries the
   * transient footprint — arena plus the not-yet-spill-eligible built pages — stays ≈1.6 GB).
   */
  private static final int BULK_SLOT_MAX_ENTRIES = 8_000_000;

  /** Arena-byte cap for bulk slot accumulation (payload bytes). */
  private static final long BULK_SLOT_MAX_ARENA_BYTES = 512L << 20;

  /**
   * Active bulk slot accumulator, or {@code null} on the ordinary per-entry path. Engaged only on a
   * VIRGIN tree ({@link #beginBulkSlotAccumulation}); while active, every slot write funnels into it
   * and point reads of accumulated keys are served from it (read-through), so the accumulator and the
   * empty tree partition the key space. See {@link HOTBulkSlotLoader}.
   */
  private @Nullable HOTBulkSlotLoader bulkSlotLoader;

  /** Witness counter: DISTINCT entries materialized by bulk splices on this storage (tests). */
  private int bulkSplicedEntryCount;

  /** Side-page payload byte budget for deferred attaches during bulk slot accumulation. */
  private static final long BULK_SIDE_PENDING_MAX_BYTES = 512L << 20;

  /**
   * Deferred side-page attaches while bulk slot accumulation is active, keyed by side-map refKey
   * ({@code (ownerSlotKey << 16) | columnSegmentId}), insertion-ordered, last-writer-wins. Payload
   * arrays are RETAINED, not copied — the exact ownership contract of the immediate
   * {@code new OverflowPage(bytes)} attach they stand in for.
   */
  private final Long2ObjectLinkedOpenHashMap<byte[]> pendingSideAttaches = new Long2ObjectLinkedOpenHashMap<>();

  /** Total payload bytes retained in {@link #pendingSideAttaches}. */
  private long pendingSideBytes;

  /** Deterministic pre-splice fault seam; non-null only in the bulk-finalization atomicity test. */
  private static volatile @Nullable Runnable bulkFinalizeBeforeSpliceTestHook;

  public ProjectionIndexHOTStorage(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
    this(storageEngineWriter, indexNumber, false);
  }

  private ProjectionIndexHOTStorage(final StorageEngineWriter storageEngineWriter, final int indexNumber,
      final boolean stageFreshSidePages) {
    super(storageEngineWriter, IndexType.PROJECTION, indexNumber);
    this.stageFreshSidePages = stageFreshSidePages;
    initializeProjectionIndex();
  }

  /**
   * Create storage for an append-oriented bulk build.
   *
   * <p>
   * Only genuinely fresh side-map keys are staged. This factory is accepted only by the virgin-tree
   * initializer; populated trees use the ordinary incremental storage instance.
   * </p>
   */
  public static ProjectionIndexHOTStorage forBulkBuild(final StorageEngineWriter storageEngineWriter,
      final int indexNumber) {
    return new ProjectionIndexHOTStorage(storageEngineWriter, indexNumber, true);
  }

  private void initializeProjectionIndex() {
    final ProjectionIndexPage projPage = prepareWritableProjectionIndexPage();
    final PageReference existingRef = projPage.getIndirectPageReference(indexNumber);
    final boolean exists = !existingRef.isVirginStructuralPlaceholder();
    if (!exists) {
      projPage.createProjectionIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
    }
    rootReference = projPage.getIndirectPageReference(indexNumber);
  }

  /** The writer's private CoW copy of the projection container page (task #57 discipline). */
  private ProjectionIndexPage prepareWritableProjectionIndexPage() {
    return storageEngineWriter.prepareSecondaryIndexPage(IndexType.PROJECTION);
  }

  /**
   * Enforce the only legal bulk-build boundary: a naturally virgin projection tree.
   *
   * <p>
   * This method never clears or replaces data. Existing projections are updated exclusively by
   * incremental maintenance; callers that want to run an initializer over a populated definition
   * receive a loud refusal instead of an implicit full rebuild.
   * </p>
   */
  void requireVirginTreeForInitialBuild() {
    if (bulkSlotLoader != null) {
      throw new IllegalStateException(
          "Projection index " + indexNumber + " already has an active virgin-tree accumulator");
    }
    if (!isEmptyTree()) {
      throw new IllegalStateException("Projection index " + indexNumber
          + " is not virgin; full reset/rebuild is forbidden, use incremental maintenance");
    }
  }

  /**
   * Engage bulk slot accumulation for a FRESH build: subsequent slot writes are collected in a
   * {@link HOTBulkSlotLoader} and materialized in ONE canonical {@code HOTBulkBuilder} pass (9–14×
   * the per-entry path at 1 M–10 M slots — {@code docs/HOT_BULK_BUILD.md} §2) instead of paying a
   * descent per slot. A no-op unless the tree is VIRGIN — that is the positive witness the
   * read-through contract rests on: while accumulating, a key is either in the loader or it was never
   * written, so point reads serve accumulated keys from the loader and everything else from the
   * (empty) tree.
   *
   * <p>
   * Self-defending behavior keeps every other contract intact: a capacity trip
   * ({@link HOTBulkSlotLoader#tryAdd} refusing) splices the accumulated prefix — the tree is still
   * virgin at that moment, so the splice is legal — and falls back to the per-entry path; side-page
   * attaches against accumulated owner slots ({@link #putSegmentPage}) are DEFERRED (payloads
   * retained under the side budget, served read-through, attached through the production path right
   * after the splice).
   * </p>
   */
  void beginBulkSlotAccumulation() {
    if (bulkSlotLoader != null) {
      throw new IllegalStateException(
          "Projection index " + indexNumber + " already has an active virgin-tree accumulator");
    }
    requireVirginTreeForInitialBuild();
    bulkSlotLoader = new HOTBulkSlotLoader(BULK_SLOT_MAX_ENTRIES, BULK_SLOT_MAX_ARENA_BYTES);
  }

  /**
   * Materialize everything accumulated since {@link #beginBulkSlotAccumulation} as this index's tree
   * (production {@code spliceBulkBuiltRoot}: empty-tree guard, canonical build, fresh-subtree TIL
   * registration) and leave accumulation mode. A no-op when accumulation is not active. A failure at
   * any point is fail-closed: the accumulator and deferred side payloads are released and the page
   * transaction becomes rollback-only, because neither a discarded pre-publication prefix nor a
   * partially attached post-publication tree is safe to commit.
   */
  void finalizeBulkSlotAccumulation() {
    final HOTBulkSlotLoader loader = bulkSlotLoader;
    if (loader == null) {
      return;
    }
    bulkSlotLoader = null;
    try {
      final Runnable beforeSpliceTestHook = bulkFinalizeBeforeSpliceTestHook;
      if (beforeSpliceTestHook != null) {
        beforeSpliceTestHook.run();
      }
      final int splicedEntryCount = loader.spliceInto(this);
      attachPendingSidePages();
      bulkSplicedEntryCount += splicedEntryCount;
    } catch (final RuntimeException | Error failure) {
      failBulkFinalization(loader, failure);
      throw failure;
    }
  }

  /** Poison the transaction and release every heap-owned remainder without replacing the cause. */
  private void failBulkFinalization(final HOTBulkSlotLoader loader, final Throwable failure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(failure);
    } catch (final RuntimeException | Error poisonFailure) {
      addSuppressedSafely(failure, poisonFailure);
    }
    try {
      loader.clear();
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(failure, cleanupFailure);
    }
    try {
      pendingSideAttaches.clear();
    } catch (final RuntimeException | Error cleanupFailure) {
      addSuppressedSafely(failure, cleanupFailure);
    } finally {
      pendingSideBytes = 0;
    }
  }

  private static void addSuppressedSafely(final Throwable failure, final Throwable suppressed) {
    if (failure == suppressed) {
      return;
    }
    try {
      failure.addSuppressed(suppressed);
    } catch (final RuntimeException | Error ignored) {
      // Preserve the authoritative bulk-finalization failure even under secondary VM pressure.
    }
  }

  /**
   * Attach every deferred side page against the freshly spliced leaves, through the production
   * {@link #putSegmentPage} path (owner-residency check, replace semantics, append-pipeline staging).
   * Runs with the loader already disabled, so the attaches hit real pages.
   */
  private void attachPendingSidePages() {
    if (pendingSideAttaches.isEmpty()) {
      pendingSideBytes = 0;
      return;
    }
    final long[] refKeys = pendingSideAttaches.keySet().toLongArray();
    for (final long refKey : refKeys) {
      final byte[] payload = pendingSideAttaches.get(refKey);
      putSegmentPage(HOTLeafPage.overflowPageRefOwnerSlot(refKey), (int) (refKey & 0xFFFFL), payload);
    }
    pendingSideAttaches.clear();
    pendingSideBytes = 0;
  }

  /**
   * Move an active bulk-slot accumulation from {@code source} onto this storage — the epoch-rebind
   * transplant. A post-pass build rides the writer's async-flush epochs
   * ({@code ProjectionIndexBuilder.BulkBuildEpoch#rebind} constructs a fresh storage per epoch); the
   * accumulator holds only un-materialized slot writes for the SAME still-virgin tree, so it is
   * tree-state-independent and moves wholesale: loader, deferred side attaches (insertion order
   * preserved — this map is empty on a freshly bound storage), byte accounting and the splice witness
   * counter. A no-op when {@code source} is this storage or holds no accumulation.
   *
   * @throws IllegalStateException if BOTH storages accumulate — two accumulators over one tree would
   *         fork the read-through truth
   */
  void adoptBulkSlotAccumulation(final ProjectionIndexHOTStorage source) {
    if (source == this || source == null || source.bulkSlotLoader == null) {
      return;
    }
    if (bulkSlotLoader != null) {
      throw new IllegalStateException(
          "cannot adopt a bulk slot accumulation into a storage that is already accumulating (indexNumber="
              + indexNumber + ')');
    }
    bulkSlotLoader = source.bulkSlotLoader;
    source.bulkSlotLoader = null;
    pendingSideAttaches.putAll(source.pendingSideAttaches);
    source.pendingSideAttaches.clear();
    pendingSideBytes += source.pendingSideBytes;
    source.pendingSideBytes = 0;
    bulkSplicedEntryCount += source.bulkSplicedEntryCount;
    source.bulkSplicedEntryCount = 0;
  }

  /** Whether bulk slot accumulation is currently active (test/diagnostic). */
  boolean isBulkAccumulating() {
    return bulkSlotLoader != null;
  }

  /** DISTINCT entries materialized by bulk splices on this storage so far (test/diagnostic). */
  int bulkSplicedEntryCount() {
    return bulkSplicedEntryCount;
  }

  /** Whether a bulk accumulator still owns deferred side payloads (test/diagnostic). */
  boolean hasPendingBulkSideAttaches() {
    return !pendingSideAttaches.isEmpty() || pendingSideBytes != 0;
  }

  static void setBulkFinalizeBeforeSpliceTestHook(final @Nullable Runnable hook) {
    bulkFinalizeBeforeSpliceTestHook = hook;
  }

  // ==================== blob slots (PIXB container) ====================
  //
  // The hashed-payload container shared by the two slot families that are NOT column segments: the
  // PIXM shape metadata at slot 0, each row group's zone-map RowGroupDescriptor (PIXD) at
  // slotKind 0, and the fence chunks at/above CHUNK_SLOT_BASE. Payload inline when small, else one
  // OverflowPage; byteLen + XXH3 in the marker either way, because nothing else backs a blob's
  // integrity. A column segment is NOT a blob — see SEG_KIND_INLINE below.

  /** Blob marker magic for slot values that reference one opaque segment ("PIXB" LE). */
  private static final int BLOB_MAGIC = 0x42584950;
  private static final byte BLOB_VERSION = 0;
  private static final int BLOB_MARKER_BYTES = 4 + 1 + 4 + 8;
  private static final int BLOB_SEGMENT_ID = 0;

  /**
   * High bit of a blob marker's length field marking the payload as INLINE (bytes in the slot value's
   * trailing region, right after the marker) rather than REFERENCED (bytes in a side-map
   * {@link OverflowPage}). A blob is capped at {@link RowGroupDescriptor#MAX_SEGMENT_BYTES} (16 MB ≪
   * 2^31) so the true length never touches the sign bit.
   */
  private static final int BLOB_INLINE_FLAG = 0x8000_0000;

  /**
   * Write-side threshold: a payload of at most this many bytes is stored inline in the slot value (no
   * page, no random read to resolve it), larger ones spill to an {@link OverflowPage} as before. The
   * reader keys off the stored {@link #BLOB_INLINE_FLAG} alone, so this bound can change without
   * breaking already-written blobs. Sized to inline the small PIXM shape metadata (slot 0, a few
   * hundred bytes). A <em>full</em> 8 KiB fence chunk stays referenced — inlining that would bloat
   * the HOT leaf pages that hold the fence slots; a small partial tail chunk (≤ 32 leaves = ≤ 512 B)
   * does inline, which is harmless and even saves it a page.
   */
  static final int INLINE_SEGMENT_MAX_BYTES = 512;
  private static final int BLOB_INLINE_MAX = INLINE_SEGMENT_MAX_BYTES;

  /**
   * Segment-slot layout discriminator (slot value's leading byte): a segment slot stores its bytes
   * INLINE (byte {@code 0x00} then the raw segment bytes) or REFERENCED (byte {@code 0x01}, bytes in
   * a side-map {@link OverflowPage}). Unlike the blob container, a segment slot carries NO magic,
   * version, or hash — the segment's byteLen + XXH3 content hash live in its descriptor entry and are
   * re-checked by {@code verifyColumnSegment} at assembly, so a second on-disk hash would be pure
   * redundancy. The leading byte also keeps a 0-byte inline segment's slot value non-empty (an empty
   * value is a tombstone). Same {@link #BLOB_INLINE_MAX} inline threshold as the blob container.
   */
  private static final byte SEG_KIND_INLINE = 0;
  private static final byte SEG_KIND_REF = 1;

  /**
   * The lone slot value of a REFERENCED bare segment (bytes live in the page). Immutable + shared —
   * {@code writeSlotValue} copies it into the slot heap, so one instance serves every referenced
   * write.
   */
  private static final byte[] SEG_REF_VALUE = {SEG_KIND_REF};

  // ==================================================================================
  // Segment ⇔ slot layout — one HOT slot per segment. The only storage layout there is.
  //
  // Composite slot key = (rowGroupId << 16) | slotKind:
  // slotKind 0 → the zone-map DESCRIPTOR (rowCount, fences, kinds, per-seg entry array)
  // slotKind columnSegmentId+1 → segment `columnSegmentId`'s bytes
  // The DESCRIPTOR slot is a hashed blob (putBlob container) — nothing else backs its integrity.
  // SEGMENT slots are BARE (putColumnSegmentSlot): a 1-byte inline/referenced discriminator plus
  // either the
  // raw bytes (inline) or a side-map OverflowPage (referenced), and NO magic/version/hash — the
  // segment's byteLen + XXH3 hash live in the descriptor entry and are re-checked by
  // verifyColumnSegment, so
  // a second on-disk hash would be pure redundancy. A range scan groups slots by the high
  // (rowGroupId)
  // bits; countRows and zone-map pruning read slotKind 0 alone; an aggregate over column c reads only
  // that column's segment slots and skips the rest. See scratchpad SEGMENT_SLOT_DESIGN.md.
  // ==================================================================================

  /** slotKind 0 — the zone-map descriptor slot for {@code rowGroupId}. */
  static long rowGroupDescriptorSlotKey(final long rowGroupId) {
    checkRowGroupId(rowGroupId);
    return rowGroupId << 16;
  }

  /**
   * slotKind {@code columnSegmentId+1} — the slot holding segment {@code columnSegmentId}'s bytes for
   * {@code rowGroupId}.
   */
  static long columnSegmentSlotKey(final long rowGroupId, final int columnSegmentId) {
    checkRowGroupId(rowGroupId);
    // columnSegmentId+1 must fit the 16-bit slotKind; columnSegmentId==0xFFFF would alias the NEXT
    // leaf's descriptor slot.
    if (columnSegmentId < 0 || columnSegmentId >= HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID) {
      throw new IllegalArgumentException(
          "columnSegmentId out of range for segment-slot key (columnSegmentId+1 must fit the 16-bit slotKind): "
              + columnSegmentId);
    }
    return (rowGroupId << 16) | (columnSegmentId + 1);
  }

  private static void checkRowGroupId(final long rowGroupId) {
    if (rowGroupId < 1 || rowGroupId > MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("rowGroupId out of range [1, " + MAX_ROW_GROUPS + "]: " + rowGroupId);
    }
  }

  /**
   * Write one logical leaf in the segment-slot layout: the zone-map descriptor at slotKind 0 (a
   * hashed blob — it has no descriptor-entry to back its integrity) and each segment at its own BARE
   * slot (inline discriminator + raw bytes, or a referenced {@link OverflowPage}; NO redundant blob
   * marker/hash, see {@link #putColumnSegmentSlot}). Segment slots present before but absent now are
   * tombstoned (real deletes). Per-segment carry-forward makes an unchanged segment a true no-op —
   * its slot value and page carry forward untouched — preserving the §6.3 CoW sharing at slot
   * granularity; entry {@code i} of the descriptor describes {@code segments[i]} (both ascending by
   * columnSegmentId), so the (byteLen, contentHash) compare needs no second hash pass over the bytes.
   */
  /**
   * Slot key of column {@code column}'s PBMF fingerprint manifest. Lives in the {@code rowGroupId==0}
   * key space (keys {@code 16 + column}, all {@code < 2^16}) beside the metadata blob at key 0 —
   * every row-group walker already skips the whole space, so the manifest is invisible to them.
   */
  public static long bloomBlockSlotKey(final int column) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("column out of Bloom-manifest key range: " + column);
    }
    return 16L + column;
  }

  public boolean putRowGroupAsColumnSegmentSlots(final long rowGroupId,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    return putRowGroupAsColumnSegmentSlots(rowGroupId, encoded, null, true);
  }

  /** Physical writes performed by one descriptor-and-columns patch. */
  record ColumnPatchResult(boolean changed, int segmentsWritten, int segmentsTombstoned) {
    private static final ColumnPatchResult UNCHANGED = new ColumnPatchResult(false, 0, 0);
  }

  /**
   * Publish a sorted batch of independently encoded columns while carrying every other segment slot
   * forward without reading its bytes. The prior descriptor is checked against the writer-visible
   * slot and validated once; the final descriptor is published exactly once. Only the union of the
   * replacement columns is eligible for tombstones or segment writes.
   */
  ColumnPatchResult putColumnPatches(final long rowGroupId, final byte[] priorDescriptor,
      final byte[] patchedDescriptor, final ProjectionIndexColumnSegmentCodec.EncodedColumn[] encodedColumns,
      final int encodedColumnCount, final long[] changedColumnWords) {
    checkRowGroupId(rowGroupId);
    if (priorDescriptor == null || patchedDescriptor == null || encodedColumns == null || changedColumnWords == null) {
      throw new IllegalArgumentException(
          "prior descriptor, patched descriptor, encoded columns, and changed columns are required");
    }
    if (encodedColumnCount <= 0 || encodedColumnCount > encodedColumns.length) {
      throw new IllegalArgumentException("encodedColumnCount out of range: " + encodedColumnCount);
    }
    final byte[] current = getVerifiedRowGroupDescriptor(rowGroupId);
    if (current == null || !Arrays.equals(current, priorDescriptor)) {
      throw new IllegalStateException("projection row group " + rowGroupId + " changed during its columns patch");
    }
    final byte[] next = patchedDescriptor;
    validateColumnPatchDescriptor(priorDescriptor, next, encodedColumns, encodedColumnCount, changedColumnWords);
    if (Arrays.equals(priorDescriptor, next)) {
      return ColumnPatchResult.UNCHANGED;
    }

    // From the first owned-slot mutation onward this is one logical publication. Individual slot
    // primitives guard their own marker/reference transitions, but a later pre-mutation rejection
    // (for example a pending descriptor side page) must also prevent an earlier tombstone or segment
    // write in this batch from being committed by a caller that catches the exception.
    try {
      int tombstoned = 0;
      final int priorCount = RowGroupDescriptor.columnSegmentCount(priorDescriptor);
      for (int i = 0; i < priorCount; i++) {
        final int id = RowGroupDescriptor.entryColumnSegmentId(priorDescriptor, i);
        final int owner = ProjectionIndexColumnSegmentCodec.columnOfColumnSegment(id);
        final int encodedIndex = encodedColumnIndex(encodedColumns, encodedColumnCount, owner);
        if (encodedIndex >= 0 && Arrays.binarySearch(encodedColumns[encodedIndex].columnSegmentIds(), id) < 0) {
          tombstoneBlobSlot(columnSegmentSlotKey(rowGroupId, id));
          tombstoned++;
        }
      }

      putBlob(rowGroupDescriptorSlotKey(rowGroupId), next);
      int written = 0;
      for (int encodedIndex = 0; encodedIndex < encodedColumnCount; encodedIndex++) {
        final ProjectionIndexColumnSegmentCodec.EncodedColumn encodedColumn = encodedColumns[encodedIndex];
        final int[] ids = encodedColumn.columnSegmentIds();
        final byte[][] segments = encodedColumn.segments();
        for (int i = 0; i < ids.length; i++) {
          final int priorEntry = RowGroupDescriptor.entryIndexOf(priorDescriptor, ids[i]);
          final int nextEntry = RowGroupDescriptor.entryIndexOf(next, ids[i]);
          final boolean unchanged = priorEntry >= 0
              && RowGroupDescriptor.entryByteLen(priorDescriptor, priorEntry) == RowGroupDescriptor.entryByteLen(next,
                  nextEntry)
              && RowGroupDescriptor.entryContentHash(priorDescriptor,
                  priorEntry) == RowGroupDescriptor.entryContentHash(next, nextEntry);
          if (!unchanged) {
            putColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, ids[i]), segments[i]);
            written++;
          }
        }
      }
      return new ColumnPatchResult(true, written, tombstoned);
    } catch (final RuntimeException | Error failure) {
      poisonMutation(failure);
      throw failure;
    }
  }

  private static void validateColumnPatchDescriptor(final byte[] prior, final byte[] next,
      final ProjectionIndexColumnSegmentCodec.EncodedColumn[] encodedColumns, final int encodedColumnCount,
      final long[] changedColumnWords) {
    // The writer-visible current descriptor was already validated before byte-equality established
    // that it is this prior descriptor. Do not repeat the full semantic scan here.
    RowGroupDescriptor.validate(next);
    if (RowGroupDescriptor.rowCount(prior) != RowGroupDescriptor.rowCount(next)
        || RowGroupDescriptor.firstRecordKey(prior) != RowGroupDescriptor.firstRecordKey(next)
        || RowGroupDescriptor.lastRecordKey(prior) != RowGroupDescriptor.lastRecordKey(next)
        || RowGroupDescriptor.columnCount(prior) != RowGroupDescriptor.columnCount(next)) {
      throw new IllegalStateException("column patch changed row-group keys or shape");
    }
    for (int c = 0; c < RowGroupDescriptor.columnCount(prior); c++) {
      if (RowGroupDescriptor.kind(prior, c) != RowGroupDescriptor.kind(next, c)) {
        throw new IllegalStateException("column patch changed persisted kind " + c);
      }
    }
    final int columnCount = RowGroupDescriptor.columnCount(prior);
    if (changedColumnWords.length < (columnCount + Long.SIZE - 1) >>> 6) {
      throw new IllegalArgumentException("changed-column bitmap is too short for " + columnCount + " columns");
    }
    validateChangedColumnBitmap(changedColumnWords, columnCount);

    int previousColumn = -1;
    for (int encodedIndex = 0; encodedIndex < encodedColumnCount; encodedIndex++) {
      final ProjectionIndexColumnSegmentCodec.EncodedColumn encodedColumn = encodedColumns[encodedIndex];
      if (encodedColumn == null) {
        throw new IllegalArgumentException("encoded column " + encodedIndex + " is required");
      }
      final int column = encodedColumn.column();
      if (column <= previousColumn || column >= columnCount) {
        throw new IllegalArgumentException("encoded columns must be unique and ascending within the descriptor");
      }
      if (encodedColumn.rowCount() != RowGroupDescriptor.rowCount(next)
          || encodedColumn.columnKind() != RowGroupDescriptor.kind(next, column)) {
        throw new IllegalArgumentException("encoded column " + column + " shape does not match descriptor");
      }
      final int[] ids = encodedColumn.columnSegmentIds();
      final byte[][] segments = encodedColumn.segments();
      final long[] hashes = encodedColumn.contentHashes();
      if (ids.length == 0 || segments.length != ids.length || hashes.length != ids.length
          || encodedColumn.entryFlags().length != ids.length || encodedColumn.entryMins().length != ids.length
          || encodedColumn.entryMaxs().length != ids.length) {
        throw new IllegalArgumentException("encoded column " + column + " arrays are not aligned");
      }
      int previousId = -1;
      for (int i = 0; i < ids.length; i++) {
        final int id = ids[i];
        if (id <= previousId || ProjectionIndexColumnSegmentCodec.columnOfColumnSegment(id) != column) {
          throw new IllegalArgumentException("invalid encoded segment id " + id + " for column " + column);
        }
        final int entry = RowGroupDescriptor.entryIndexOf(next, id);
        if (entry < 0 || hashes[i] != RowGroupDescriptor.entryContentHash(next, entry)
            || encodedColumn.entryFlags()[i] != RowGroupDescriptor.entryColFlags(next, entry)
            || encodedColumn.entryMins()[i] != RowGroupDescriptor.entryMin(next, entry)
            || encodedColumn.entryMaxs()[i] != RowGroupDescriptor.entryMax(next, entry)) {
          throw new IllegalStateException("encoded segment " + id + " does not match its patched descriptor entry");
        }
        ProjectionIndexColumnSegmentCodec.validateColumnSegmentShape(next, segments[i], id,
            ProjectionIndexColumnSegmentCodec.expectedSegmentKind(id), entry);
        previousId = id;
      }
      previousColumn = column;
    }
    for (int column = 0; column < columnCount; column++) {
      if (columnSelected(changedColumnWords, column)
          && encodedColumnIndex(encodedColumns, encodedColumnCount, column) < 0) {
        throw new IllegalStateException("changed-column bitmap names unsupplied column " + column);
      }
    }
    validateColumnScopedChanges(next, prior, changedColumnWords, false);

    for (int i = 0; i < RowGroupDescriptor.columnSegmentCount(next); i++) {
      final int id = RowGroupDescriptor.entryColumnSegmentId(next, i);
      final int owner = ProjectionIndexColumnSegmentCodec.columnOfColumnSegment(id);
      final int encodedIndex = encodedColumnIndex(encodedColumns, encodedColumnCount, owner);
      if (encodedIndex >= 0) {
        if (Arrays.binarySearch(encodedColumns[encodedIndex].columnSegmentIds(), id) < 0) {
          throw new IllegalStateException("patched descriptor contains unsupplied segment " + id);
        }
      }
    }
  }

  private static int encodedColumnIndex(final ProjectionIndexColumnSegmentCodec.EncodedColumn[] encodedColumns,
      final int encodedColumnCount, final int column) {
    int low = 0;
    int high = encodedColumnCount - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int candidate = encodedColumns[middle].column();
      if (candidate == column) {
        return middle;
      }
      if (candidate < column) {
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    return -1;
  }

  private static boolean columnSelected(final long[] changedColumnWords, final int column) {
    final int word = column >>> 6;
    return word < changedColumnWords.length && (changedColumnWords[word] & (1L << (column & 63))) != 0L;
  }

  private static void validateChangedColumnBitmap(final long[] changedColumnWords, final int columnCount) {
    final int requiredWords = (columnCount + Long.SIZE - 1) >>> 6;
    if (requiredWords > 0 && (columnCount & 63) != 0) {
      final long highBits = changedColumnWords[requiredWords - 1] & (-1L << (columnCount & 63));
      if (highBits != 0L) {
        throw new IllegalArgumentException("changed-column bitmap names a column outside the descriptor");
      }
    }
    for (int word = requiredWords; word < changedColumnWords.length; word++) {
      if (changedColumnWords[word] != 0L) {
        throw new IllegalArgumentException("changed-column bitmap has non-zero trailing words");
      }
    }
  }

  boolean putRowGroupAsColumnSegmentSlots(final long rowGroupId,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final long @Nullable [] changedColumnWords,
      final boolean keysChanged) {
    checkRowGroupId(rowGroupId);
    if (encoded == null) {
      throw new IllegalArgumentException("encoded leaf must not be null — use tombstoneRowGroupAsColumnSegmentSlots");
    }
    final byte[] descriptor = encoded.descriptor();
    RowGroupDescriptor.validate(descriptor);
    final int[] columnSegmentIds = encoded.columnSegmentIds();
    final byte[][] segments = encoded.segments();
    validateEncodedRowGroupShape(descriptor, columnSegmentIds, segments);

    // Diff the prior and new columnSegmentId sets so a shrunk leaf (dropped DICT, fewer columns)
    // tombstones every segment slot that vanished. This read is deliberately strict: without a
    // readable prior descriptor there is no authoritative list of owned side slots, so overwriting
    // it could strand durable pages. There is no reset/rebuild mutation mode to clean those up.
    final byte[] prior = getPriorBlobForMutation(rowGroupDescriptorSlotKey(rowGroupId));
    final boolean priorIsDescriptor = prior != null && RowGroupDescriptor.isDescriptor(prior);
    if (prior != null && !priorIsDescriptor) {
      throw poisonMalformedPriorDescriptor(rowGroupId, "missing descriptor magic");
    }
    if (priorIsDescriptor) {
      try {
        RowGroupDescriptor.validate(prior);
      } catch (final RuntimeException | Error failure) {
        poisonMutation(failure);
        throw failure;
      }
    }
    if (changedColumnWords != null && !priorIsDescriptor) {
      throw new IllegalStateException("cannot validate a column-scoped projection update without a prior descriptor");
    }
    if (priorIsDescriptor && Arrays.equals(prior, descriptor)) {
      return false;
    }
    if (changedColumnWords != null) {
      validateColumnScopedChanges(descriptor, prior, changedColumnWords, keysChanged);
    }

    // ORDER: tombstone vanished slots before overwriting the descriptor, then publish the new
    // descriptor before its replacement segments. The whole row-group publication is fail-closed:
    // even a later primitive that rejects before touching its own slot poisons the transaction,
    // because an earlier primitive in this sequence may already have changed another owned slot.
    try {
      if (priorIsDescriptor) {
        tombstoneVanishedColumnSegmentSlots(rowGroupId, descriptor, prior);
      }
      // Descriptor before its segments so the row group's leading slot is never headless.
      putBlob(rowGroupDescriptorSlotKey(rowGroupId), descriptor);
      writeChangedColumnSegmentSlots(rowGroupId, descriptor, columnSegmentIds, segments, priorIsDescriptor
          ? prior
          : null);
      return true;
    } catch (final RuntimeException | Error failure) {
      poisonMutation(failure);
      throw failure;
    }
  }

  /**
   * Allocation-free alignment gate for an encoded row group. The codec owns the hash calculation;
   * this boundary verifies that the arrays it publishes still name the exact descriptor entries and
   * lengths before any slot is mutated.
   */
  private static void validateEncodedRowGroupShape(final byte[] descriptor, final int[] columnSegmentIds,
      final byte[][] segments) {
    final int count = RowGroupDescriptor.columnSegmentCount(descriptor);
    if (columnSegmentIds == null || segments == null || columnSegmentIds.length != count || segments.length != count) {
      throw new IllegalArgumentException("encoded row-group descriptor and segment arrays are not index-aligned");
    }
    for (int i = 0; i < count; i++) {
      final byte[] segment = segments[i];
      if (segment == null || columnSegmentIds[i] != RowGroupDescriptor.entryColumnSegmentId(descriptor, i)
          || segment.length != RowGroupDescriptor.entryByteLen(descriptor, i)) {
        throw new IllegalArgumentException("encoded row-group segment does not match descriptor entry " + i);
      }
      ProjectionIndexColumnSegmentCodec.validateColumnSegmentShape(descriptor, segment, columnSegmentIds[i],
          ProjectionIndexColumnSegmentCodec.expectedSegmentKind(columnSegmentIds[i]), i);
    }
  }

  private static void validateColumnScopedChanges(final byte[] descriptor, final byte[] prior,
      final long[] changedColumnWords, final boolean keysChanged) {
    final int priorCount = RowGroupDescriptor.columnSegmentCount(prior);
    final int nextCount = RowGroupDescriptor.columnSegmentCount(descriptor);
    int priorIndex = 0;
    int nextIndex = 0;
    while (priorIndex < priorCount || nextIndex < nextCount) {
      final int priorId = priorIndex < priorCount
          ? RowGroupDescriptor.entryColumnSegmentId(prior, priorIndex)
          : Integer.MAX_VALUE;
      final int nextId = nextIndex < nextCount
          ? RowGroupDescriptor.entryColumnSegmentId(descriptor, nextIndex)
          : Integer.MAX_VALUE;
      final int changedId;
      final boolean changed;
      if (priorId == nextId) {
        changedId = priorId;
        changed =
            RowGroupDescriptor.entryByteLen(prior, priorIndex) != RowGroupDescriptor.entryByteLen(descriptor, nextIndex)
                || RowGroupDescriptor.entryContentHash(prior,
                    priorIndex) != RowGroupDescriptor.entryContentHash(descriptor, nextIndex)
                || RowGroupDescriptor.entryColFlags(prior, priorIndex) != RowGroupDescriptor.entryColFlags(descriptor,
                    nextIndex)
                || RowGroupDescriptor.entryMin(prior, priorIndex) != RowGroupDescriptor.entryMin(descriptor, nextIndex)
                || RowGroupDescriptor.entryMax(prior, priorIndex) != RowGroupDescriptor.entryMax(descriptor, nextIndex);
        priorIndex++;
        nextIndex++;
      } else if (priorId < nextId) {
        changedId = priorId;
        changed = true;
        priorIndex++;
      } else {
        changedId = nextId;
        changed = true;
        nextIndex++;
      }
      if (changed && !columnSegmentMayChange(changedId, changedColumnWords, keysChanged)) {
        throw new IllegalStateException("projection update changed unmarked column segment " + changedId);
      }
    }
  }

  private static boolean columnSegmentMayChange(final int columnSegmentId, final long[] changedColumnWords,
      final boolean keysChanged) {
    final int column = ProjectionIndexColumnSegmentCodec.columnOfColumnSegment(columnSegmentId);
    if (column < 0) {
      return keysChanged;
    }
    final int word = column >>> 6;
    return word < changedColumnWords.length && (changedColumnWords[word] & (1L << (column & 63))) != 0L;
  }

  /**
   * Write each segment whose bytes actually changed. An unchanged segment (same byteLen + contentHash
   * as the prior descriptor's entry) needs no write at all — its bare slot value and page survive the
   * leaf's CoW copy, which is the §6.3 sharing this layout exists to preserve.
   *
   * <p>
   * Entry {@code i} of {@code descriptor} describes {@code segments[i]}, and both id lists ascend, so
   * one monotonic cursor over the prior entries makes the per-segment carry-forward test O(1)
   * amortized rather than a binary search — and the compare needs no second hash pass over the bytes.
   *
   * @param prior the prior descriptor, or {@code null} when there was none (every segment is new)
   */
  private void writeChangedColumnSegmentSlots(final long rowGroupId, final byte[] descriptor,
      final int[] columnSegmentIds, final byte[][] segments, final byte @Nullable [] prior) {
    final int priorSegCount = prior == null
        ? 0
        : RowGroupDescriptor.columnSegmentCount(prior);
    int priorCursor = 0;
    for (int i = 0; i < columnSegmentIds.length; i++) {
      final int columnSegmentId = columnSegmentIds[i];
      if (prior != null) {
        while (priorCursor < priorSegCount
            && RowGroupDescriptor.entryColumnSegmentId(prior, priorCursor) < columnSegmentId) {
          priorCursor++;
        }
        if (priorCursor < priorSegCount
            && RowGroupDescriptor.entryColumnSegmentId(prior, priorCursor) == columnSegmentId
            && RowGroupDescriptor.entryByteLen(prior, priorCursor) == RowGroupDescriptor.entryByteLen(descriptor, i)
            && RowGroupDescriptor.entryContentHash(prior,
                priorCursor) == RowGroupDescriptor.entryContentHash(descriptor, i)) {
          continue;
        }
      }
      putColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, columnSegmentId), segments[i]);
    }
  }

  /**
   * Tombstone the segment slots whose id vanished from the new descriptor — a shrunk row group
   * (dropped DICT, fewer columns) would otherwise leave them readable but unreferenced. Both id lists
   * ascend, so a second monotonic cursor over the NEW descriptor replaces a per-prior lookup.
   */
  private void tombstoneVanishedColumnSegmentSlots(final long rowGroupId, final byte[] descriptor, final byte[] prior) {
    final int priorSegCount = RowGroupDescriptor.columnSegmentCount(prior);
    final int newSegCount = RowGroupDescriptor.columnSegmentCount(descriptor);
    int newCursor = 0;
    for (int i = 0; i < priorSegCount; i++) {
      final int priorSegId = RowGroupDescriptor.entryColumnSegmentId(prior, i);
      while (newCursor < newSegCount && RowGroupDescriptor.entryColumnSegmentId(descriptor, newCursor) < priorSegId) {
        newCursor++;
      }
      final boolean present =
          newCursor < newSegCount && RowGroupDescriptor.entryColumnSegmentId(descriptor, newCursor) == priorSegId;
      if (!present) {
        tombstoneBlobSlot(columnSegmentSlotKey(rowGroupId, priorSegId));
      }
    }
  }

  /**
   * Write one segment's bytes into its own BARE slot — no blob marker/hash. Small payloads (≤
   * {@link #BLOB_INLINE_MAX}) go inline (leading {@link #SEG_KIND_INLINE} byte + raw bytes, no page);
   * larger ones go referenced (a lone {@link #SEG_KIND_REF} byte in the slot value + one
   * {@link OverflowPage}). A referenced value writes its slot BEFORE the page
   * ({@link #putSegmentPage}'s owner-slot-residency precondition). A referenced→inline shrink fully
   * allocates the inline value, then removes the old resolved ref before installing it; a pending
   * bulk ref fails before either structure changes. An inline→referenced growth's prior (page-less)
   * slot just gets overwritten.
   */
  public void putColumnSegmentSlot(final long slotKey, final byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("segment bytes must not be null — use tombstoneRowGroupAsColumnSegmentSlots");
    }
    // Validate the side-map key precondition before any write (mirrors putBlob).
    HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID);
    if (bytes.length <= BLOB_INLINE_MAX) {
      final byte[] value = new byte[1 + bytes.length];
      value[0] = SEG_KIND_INLINE;
      System.arraycopy(bytes, 0, value, 1, bytes.length);
      // Check/remove first: a pending bulk ref is append-only and must fail before the inline value
      // replaces its owner marker. The value is fully allocated above, so OOME cannot strand a
      // resolved ref between removal and the owner-slot update.
      boolean sideReferenceRemoved = false;
      try {
        removeSegmentPage(slotKey, BLOB_SEGMENT_ID); // no-op unless the prior segment was referenced
        sideReferenceRemoved = true;
        writeSlotValue(slotKey, value);
      } catch (final RuntimeException | Error failure) {
        if (sideReferenceRemoved) {
          poisonMutation(failure);
        }
        throw failure;
      }
    } else {
      boolean ownerMarkerWritten = false;
      try {
        writeSlotValue(slotKey, SEG_REF_VALUE);
        ownerMarkerWritten = true;
        putSegmentPage(slotKey, BLOB_SEGMENT_ID, bytes);
      } catch (final RuntimeException | Error failure) {
        if (ownerMarkerWritten) {
          poisonMutation(failure);
        }
        throw failure;
      }
    }
  }

  /**
   * Extract a bare segment slot value's inline payload, or signal "referenced" via {@code null}. The
   * caller resolves the page for a referenced slot. Throws on a malformed discriminator.
   */
  private static byte @Nullable [] inlineColumnSegmentPayload(final byte[] value, final long slotKey) {
    final byte kind = value[0];
    if (kind == SEG_KIND_INLINE) {
      return Arrays.copyOfRange(value, 1, value.length);
    }
    if (kind == SEG_KIND_REF) {
      return null;
    }
    throw new IllegalStateException(
        "segment slot " + slotKey + " has an unknown discriminator " + kind + " — not a bare segment slot");
  }

  /** Writer-side (same-transaction) bare segment read; {@code null} when absent/tombstoned. */
  private byte @Nullable [] getColumnSegmentSlot(final long slotKey) {
    final byte[] value = readSlotValueForWrite(slotKey);
    if (value == null || value.length == 0) {
      return null;
    }
    final byte[] inline = inlineColumnSegmentPayload(value, slotKey);
    final byte[] segment = inline != null
        ? inline
        : getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID);
    return segment;
  }

  /** Reader-side (committed) bare segment read; {@code null} when absent/tombstoned. */
  static byte @Nullable [] readColumnSegmentSlot(final StorageEngineReader reader, final int indexNumber,
      final long slotKey) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      final long refKey = HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID);
      for (int attempt = 0; attempt <= HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
        final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, slotKey, keyBuf);
        if (leaf == null) {
          return null;
        }
        byte @Nullable [] value;
        PageReference ref = null;
        try {
          final int idx = leaf.findEntry(keyBuf);
          value = idx < 0
              ? null
              : leaf.copyStoredValue(idx);
          if (value != null && value.length != 0 && value[0] == SEG_KIND_REF) {
            ref = leaf.getPageReference(refKey);
          }
        } catch (final RuntimeException failure) {
          if (trieReader.validateCurrentLeaf()) {
            throw failure;
          }
          continue;
        }
        if (!trieReader.validateCurrentLeaf()) {
          continue;
        }
        if (value == null || value.length == 0) {
          return null;
        }
        final byte[] inline = inlineColumnSegmentPayload(value, slotKey);
        if (inline != null) {
          return inline;
        }
        if (ref == null) {
          return null;
        }
        final OverflowPage page = reader.readSideOverflowPage(ref);
        return page == null
            ? null
            : page.getDataBytes();
      }
      throw HOTTrieReader.stampRetriesExhausted("readColumnSegmentSlot(slot " + slotKey + ")");
    }
  }

  /**
   * Tombstone a whole row group in the segment-slot layout: drop every segment slot named by the
   * descriptor, then the descriptor slot itself.
   */
  public void tombstoneRowGroupAsColumnSegmentSlots(final long rowGroupId) {
    checkRowGroupId(rowGroupId);
    // A delete needs the authoritative descriptor before it can remove the owned segment slots.
    // Treating an unreadable descriptor as absent would tombstone only the owner and strand its side
    // pages, so corruption poisons the transaction instead.
    final byte[] descriptor = getPriorBlobForMutation(rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor != null) {
      if (!RowGroupDescriptor.isDescriptor(descriptor)) {
        throw poisonMalformedPriorDescriptor(rowGroupId, "missing descriptor magic");
      }
      try {
        RowGroupDescriptor.validate(descriptor);
      } catch (final RuntimeException | Error failure) {
        poisonMutation(failure);
        throw failure;
      }
      try {
        final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
        for (int i = 0; i < columnSegmentCount; i++) {
          tombstoneBlobSlot(columnSegmentSlotKey(rowGroupId, RowGroupDescriptor.entryColumnSegmentId(descriptor, i)));
        }
        tombstoneBlobSlot(rowGroupDescriptorSlotKey(rowGroupId));
      } catch (final RuntimeException | Error failure) {
        poisonMutation(failure);
        throw failure;
      }
      return;
    }
    tombstoneBlobSlot(rowGroupDescriptorSlotKey(rowGroupId));
  }

  /** Remove a blob slot: drop its (possible) referenced page, then zero the slot value. */
  private void tombstoneBlobSlot(final long slotKey) {
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior == null) {
      return;
    }
    boolean sideReferenceRemoved = false;
    try {
      removeSegmentPage(slotKey, BLOB_SEGMENT_ID); // no-op when the blob was inline (no page)
      sideReferenceRemoved = true;
      if (prior.length > 0) {
        writeSlotValue(slotKey, TOMBSTONE);
      }
    } catch (final RuntimeException | Error failure) {
      if (sideReferenceRemoved) {
        poisonMutation(failure);
      }
      throw failure;
    }
  }

  /**
   * Reader-side (committed) assembly of a leaf from its segment slots — byte-identical to the raw
   * scan form. {@code null} when the descriptor slot is absent or tombstoned.
   */
  public static byte @Nullable [] readRowGroupFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final long rowGroupId) {
    final byte[] descriptor = readBlob(reader, indexNumber, rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null) {
      return null;
    }
    return ProjectionIndexColumnSegmentCodec.assembleRaw(descriptor, columnSegmentId -> readColumnSegmentSlot(reader,
        indexNumber, columnSegmentSlotKey(rowGroupId, columnSegmentId)));
  }

  /** Writer-side (same-transaction) assembly from segment slots; {@code null} if absent. */
  public byte @Nullable [] getRowGroupFromColumnSegmentSlots(final long rowGroupId) {
    final byte[] descriptor = getBlob(rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null) {
      return null;
    }
    return ProjectionIndexColumnSegmentCodec.assembleRaw(descriptor,
        columnSegmentId -> getColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, columnSegmentId)));
  }

  byte @Nullable [] getVerifiedColumnSegment(final long rowGroupId, final int columnSegmentId,
      final byte expectedKind) {
    checkRowGroupId(rowGroupId);
    final byte[] descriptor = getVerifiedRowGroupDescriptor(rowGroupId);
    return descriptor == null
        ? null
        : getVerifiedColumnSegment(rowGroupId, descriptor, columnSegmentId, expectedKind);
  }

  /**
   * Read and structurally validate one writer-visible zone-map descriptor without fetching segments.
   */
  byte @Nullable [] getVerifiedRowGroupDescriptor(final long rowGroupId) {
    checkRowGroupId(rowGroupId);
    final byte[] descriptor = getBlob(rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null) {
      return null;
    }
    RowGroupDescriptor.validate(descriptor);
    return descriptor;
  }

  /** Resolve one segment against a descriptor the caller already fetched. */
  byte @Nullable [] getVerifiedColumnSegment(final long rowGroupId, final byte[] descriptor, final int columnSegmentId,
      final byte expectedKind) {
    checkRowGroupId(rowGroupId);
    if (descriptor == null) {
      throw new IllegalArgumentException("descriptor is required");
    }
    final int entry = RowGroupDescriptor.entryIndexOf(descriptor, columnSegmentId);
    if (entry < 0) {
      return null;
    }
    final byte[] segment = getColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, columnSegmentId));
    ProjectionIndexColumnSegmentCodec.verifyColumnSegment(descriptor, segment, columnSegmentId, expectedKind, entry);
    return segment;
  }

  /**
   * Descriptor-only row count for the segment-slot layout: reads slotKind 0 alone, touching no
   * segment slots. {@code -1} when the descriptor is absent — the count/pruning path never pays for
   * segment I/O.
   */
  public static long readRowCountFromColumnSegmentSlots(final StorageEngineReader reader, final int indexNumber,
      final long rowGroupId) {
    final byte[] descriptor = readBlob(reader, indexNumber, rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null) {
      return -1L;
    }
    RowGroupDescriptor.validate(descriptor);
    return RowGroupDescriptor.rowCount(descriptor);
  }

  /**
   * One live leaf's assembly state: its zone-map descriptor plus a columnSegmentId→payload table
   * sized exactly from the descriptor's entry count. Positions are filled in the post-walk resolution
   * pass.
   */
  private static final class ColumnSegmentSlotRowGroupAccum {
    private byte[] descriptor;
    private int[] columnSegmentIds;
    private byte[][] payloads;
    private int seenSegmentCount;
  }

  /**
   * A slot captured during the walk. Its payload source is exactly one of: {@code inlineValue} (a
   * DESCRIPTOR's whole blob value, verified on resolution; or a SEGMENT's already-stripped raw
   * payload), {@code resolved} (an uncommitted/swizzled ref read in-walk — the descriptor verified, a
   * segment raw), or {@code marker}+{@code offset} (a committed reference batch-resolved after the
   * walk; {@code marker} is non-null only for descriptors, which still hash-verify).
   */
  record RawBlobSlot(long rowGroupId, int slotKind, byte[] inlineValue, byte[] resolved, byte[] marker, long offset,
      long slotKey) {
  }

  /**
   * Deferred, content-verified blob locations captured by one HOT range scan. Referenced payloads
   * retain only their durable offset plus the PIXB marker's exact length/hash; inline payloads are
   * verified and retained directly. Arrays are index-aligned with {@code firstSlotKey + i}.
   */
  static final class BlobLocators {
    private final long firstSlotKey;
    private final long[] offsets;
    private final int[] lengths;
    private final long[] hashes;
    private final byte[][] inlinePayloads;

    private BlobLocators(final long firstSlotKey, final int count) {
      this.firstSlotKey = firstSlotKey;
      offsets = new long[count];
      Arrays.fill(offsets, Constants.NULL_ID_LONG);
      lengths = new int[count];
      Arrays.fill(lengths, -1);
      hashes = new long[count];
      inlinePayloads = new byte[count][];
    }

    int size() {
      return offsets.length;
    }

    long slotKey(final int index) {
      Objects.checkIndex(index, offsets.length);
      return firstSlotKey + index;
    }

    long offset(final int index) {
      return offsets[index];
    }

    int length(final int index) {
      return lengths[index];
    }

    long hash(final int index) {
      return hashes[index];
    }

    byte @Nullable [] inlinePayload(final int index) {
      return inlinePayloads[index];
    }

    long retainedBytes() {
      // Primitive arrays plus one object-reference array. Charge references pessimistically at
      // eight bytes so the cache weight remains safe with or without compressed oops.
      long bytes = 64L + (long) offsets.length * (Long.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES);
      for (final byte[] inline : inlinePayloads) {
        if (inline != null) {
          bytes += inline.length;
        }
      }
      return bytes;
    }

    private void put(final int index, final BlobLocatorCapture capture) {
      offsets[index] = capture.offset;
      lengths[index] = capture.length;
      hashes[index] = capture.hash;
      inlinePayloads[index] = capture.inlinePayload;
    }
  }

  /** Reused while a range cursor optimistically reads one blob slot. */
  private static final class BlobLocatorCapture {
    private boolean skip;
    private long offset;
    private int length;
    private long hash;
    private byte @Nullable [] inlinePayload;

    private void reset() {
      skip = false;
      offset = Constants.NULL_ID_LONG;
      length = -1;
      hash = 0L;
      inlinePayload = null;
    }
  }

  /**
   * Capture {@code count} adjacent blob slots without reading referenced payload pages. One bounded
   * HOT range walk replaces a root descent per slot; malformed/missing entries remain empty locators
   * so derived accelerations can fail open locally.
   */
  static BlobLocators collectBlobLocators(final StorageEngineReader reader, final int indexNumber,
      final long firstSlotKey, final int count) {
    // The two callers are the <=13,104 manifest family and one <=65,536 Bloom-chunk family. Keep
    // this generic primitive bounded at that proven ceiling so damaged metadata can never request a
    // corpus-sized/object-sized allocation here.
    final int maxLocatorCount = ProjectionIndexHOTStorage.MAX_ROW_GROUPS / ProjectionBloomChunks.CHUNK_LEAVES;
    if (firstSlotKey < 0) {
      throw new IllegalArgumentException("blob locator range must not enter the negative record-locator namespace");
    }
    if (count < 0 || count > maxLocatorCount) {
      throw new IllegalArgumentException("count out of range [0, " + maxLocatorCount + "]: " + count);
    }
    final BlobLocators out = new BlobLocators(firstSlotKey, count);
    if (count == 0) {
      return out;
    }
    final long lastSlotKey;
    try {
      lastSlotKey = Math.addExact(firstSlotKey, count - 1L);
    } catch (final ArithmeticException overflow) {
      throw new IllegalArgumentException("blob locator key range overflows long", overflow);
    }
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return out;
    }
    final BlobLocatorCapture capture = new BlobLocatorCapture();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
        HOTRangeCursor cursor = trieReader.range(rootRef, slotKeyBytes(firstSlotKey), slotKeyBytes(lastSlotKey))) {
      int tornRounds = 0;
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIndex = cursor.currentEntryIndex();
        long slotKey = firstSlotKey;
        RuntimeException stableFailure = null;
        capture.reset();
        try {
          slotKey = leaf.decodeKey8BE(entryIndex) ^ 0x8000_0000_0000_0000L;
          readBlobLocatorSlot(leaf, entryIndex, slotKey, capture);
        } catch (final RuntimeException failure) {
          stableFailure = failure;
        }
        if (!cursor.validateLeaf()) {
          cursor.recoverTorn(++tornRounds);
          continue;
        }
        tornRounds = 0;
        // Stable corruption is absence for a derived Bloom acceleration. The authoritative row-group
        // segment remains available through the per-leaf chain, so throwing here would lose a whole
        // projection merely because an optional locator is damaged.
        if (stableFailure == null && !capture.skip && slotKey >= firstSlotKey && slotKey <= lastSlotKey) {
          out.put((int) (slotKey - firstSlotKey), capture);
        }
        cursor.advance();
      }
    }
    return out;
  }

  /** Decode one PIXB marker directly from a HOT leaf without materializing referenced payload. */
  private static void readBlobLocatorSlot(final HOTLeafPage leaf, final int entryIndex, final long slotKey,
      final BlobLocatorCapture capture) {
    final long valueRef = leaf.valueRef(entryIndex);
    final int valueSize = Math.max(HOTLeafPage.refLength(valueRef), 0);
    if (valueSize == 0) {
      capture.skip = true;
      return;
    }
    if (valueSize < BLOB_MARKER_BYTES || leaf.refIntLEAt(valueRef, 0) != BLOB_MAGIC
        || leaf.refByteAt(valueRef, Integer.BYTES) != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int encodedLength = leaf.refIntLEAt(valueRef, 5);
    final boolean inline = (encodedLength & BLOB_INLINE_FLAG) != 0;
    final int length = encodedLength & ~BLOB_INLINE_FLAG;
    if (length < 0) {
      throw new IllegalStateException("Blob slot " + slotKey + " has negative payload length " + length);
    }
    capture.length = length;
    capture.hash = leaf.refLongLEAt(valueRef, 9);
    if (inline) {
      capture.inlinePayload = verifyInlineBlobFromSlot(leaf, valueRef, valueSize, slotKey);
      return;
    }
    if (valueSize != BLOB_MARKER_BYTES) {
      throw new IllegalStateException(
          "Referenced blob slot " + slotKey + " has marker length " + valueSize + " instead of " + BLOB_MARKER_BYTES);
    }
    capture.offset = resolvedSegmentPageOffset(leaf, slotKey);
    if (capture.offset == Constants.NULL_ID_LONG) {
      capture.skip = true;
    }
  }

  /**
   * A referenced SEGMENT awaiting one batched page read. No marker: a segment's integrity is the
   * descriptor entry's byteLen + contentHash, re-checked by {@code verifyColumnSegment} at assembly.
   */
  private record PendingSegRef(byte[][] target, int idx, long offset, long slotKey) {
  }

  /**
   * Reader-side enumeration of ALL live leaves in the segment-slot layout, emitted in the explicit
   * physical document order, each assembled byte-identically to
   * {@link #readRowGroupFromColumnSegmentSlots}.
   *
   * <p>
   * HFT read path (P3, replacing the per-slot point-read version): ONE trie range scan captures every
   * blob slot (vs {@code O(rowGroupCount×segments)} independent {@link #readBlob} root-to-leaf
   * descents before); referenced blobs are then resolved by TWO coalesced
   * {@link #readSegmentBytesBatch} calls (descriptors, then segments), and assembly fans out across
   * the common pool for large stores.
   *
   * <p>
   * The walk makes NO assumption about slot VISIT order — like {@link #readAllRowGroups} (whose
   * topology order "can diverge from key order after splits") it collects everything first and
   * resolves positions afterward, so a leaf's segment slot seen before its descriptor is fine.
   * Loud-on-gap and loud-on-orphan both fall out of validating the descriptor key set against the
   * exact live physical ids named by that order — checked before the committed segment BATCH — and a
   * segment for a non-existent leaf, or with no matching descriptor entry, throws unambiguously. (An
   * uncommitted blob is read in-walk, so its page read precedes validation, but still throws just as
   * loudly.)
   *
   * <p>
   * Serves both committed and uncommitted (writer, this-transaction) reads, like the descriptor
   * path's {@link #readAllRowGroups}: a referenced blob whose durable offset is not yet resolved (a
   * swizzled, unflushed page) is read in-walk through its live reference, while committed references
   * take the coalesced batch — so a same-transaction build-then-query still serves from the store.
   */
  /**
   * ONE trie range scan over a projection sub-tree, returning the row groups' DESCRIPTOR slots in the
   * supplied logical order with the physical key set already validated against that order.
   *
   * <p>
   * This is the shared front half of every full read. Doing it as a range scan rather than
   * {@code rowGroupCount} independent {@link #readBlob} descents is the whole point: a descent is a
   * root-to-leaf walk plus a fresh {@link HOTTrieReader}, so the point-read shape costs
   * {@code O(rowGroupCount × height)} page touches where the scan costs one pass.
   *
   * <p>
   * Slots are captured unresolved — a referenced blob keeps only its 17-byte marker and durable
   * offset — so the caller can resolve them in ONE coalesced batch instead of a random page read
   * each. Nothing here reads a segment page.
   *
   * <p>
   * The walk makes NO assumption about visit order (topology order can diverge from key order after
   * splits): everything is collected first and positions resolved afterwards.
   *
   * @param segmentSlotsOut collects the row groups' SEGMENT slots (slotKind >= 1) for a caller that
   *        goes on to assemble; {@code null} skips capturing them entirely, which is what makes a
   *        descriptor-tier count cheap
   */
  private static RawBlobSlot[] orderedDescriptorSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount, final int[] physicalOrder, final @Nullable ArrayList<RawBlobSlot> segmentSlotsOut) {
    final Long2ObjectRBTreeMap<RawBlobSlot> descriptors = new Long2ObjectRBTreeMap<>();
    collectSlotsRange(reader, indexNumber, rowGroupCount, 1, MAX_ROW_GROUPS, descriptors, segmentSlotsOut);
    return drainOrderedDescriptors(descriptors, rowGroupCount, indexNumber, physicalOrder);
  }

  /**
   * The slot walk over ONE row-group id range, {@code [fromRowGroup, toRowGroup]} inclusive — the
   * unit a PARALLEL materialization partitions across per-thread readers (each range touches
   * mostly-disjoint trie leaves, so the page decodes the cursor forces run concurrently into the
   * shared buffer manager). Negative bounds = the historical UNBOUNDED walk.
   */
  static void collectSlotsRange(final StorageEngineReader reader, final int indexNumber, final int rowGroupCount,
      final long fromRowGroup, final long toRowGroup, final Long2ObjectRBTreeMap<RawBlobSlot> descriptors,
      final @Nullable ArrayList<RawBlobSlot> segmentSlotsOut) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      if (rowGroupCount != 0) {
        throw new IllegalStateException("segment-slot sub-tree missing but metadata declares " + rowGroupCount
            + " leaves (indexNumber=" + indexNumber + ")");
      }
      return;
    }
    // UNBOUNDED, not [0x00..8, 0xFF..8]: both name the whole trie, but explicit bounds make the
    // cursor lex-compare every key against the upper bound on every step — a comparison that,
    // against an all-0xFF bound, can never once be true. Passing null lets the cursor skip the
    // bound check and start at the leftmost leaf directly (measured: 0.687 -> 0.659 ms over 977
    // row groups, 3 forks x 10 iterations). A key longer than 8 bytes, which the bounded form
    // would have quietly ended the scan on, now reaches decodeKey8BE and fails loudly — the
    // better outcome for a store that must contain only 8-byte slot keys. RANGED calls pay the
    // bound compare; they exist to be run in parallel, where it is noise.
    final byte[] fromKey = fromRowGroup < 0
        ? null
        : slotKeyBytes(fromRowGroup << 16);
    final byte[] toKey = toRowGroup < 0
        ? null
        : slotKeyBytes((toRowGroup << 16) | 0xFFFFL);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
        HOTRangeCursor cursor = trieReader.range(rootRef, fromKey, toKey)) {
      // The walk reads UNPINNED leaves under optimistic stamps: each slot's read batch — key
      // decode, value slice, capture copy — is validated before its outcome is committed to the
      // collections; a torn batch re-evaluates the SAME slot on a refreshed leaf copy.
      int tornRounds = 0;
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIdx = cursor.currentEntryIndex();
        long rowGroupId = 0;
        RawBlobSlot descriptorSlot = null;
        RawBlobSlot segmentSlot = null;
        try {
          final long slotKey = leaf.decodeKey8BE(entryIdx) ^ 0x8000_0000_0000_0000L;
          rowGroupId = slotKey >>> 16;
          final MemorySegment valueSlice = cursor.currentValueSlice();
          final int valueSize = valueSlice == null
              ? 0
              : (int) valueSlice.byteSize();
          if (valueSize > leaf.slotCapacity()) {
            // A length no slot can hold must not drive the capture copies below: torn (validation
            // fails — retried) or genuine corruption (it holds — the throw escapes).
            throw new IllegalStateException("segment-slot value of " + valueSize + " bytes exceeds the slot capacity");
          }
          // The leaf slots must be told apart from the two other blob families by KEY: the metadata
          // (PIXM) blob at slot 0, and the fence chunk blobs at slotKey >= CHUNK_SLOT_BASE (2^42, far
          // above any rowGroupId<<16). Both are skipped; tombstones are zero-length. The DESCRIPTOR
          // slot (slotKind 0) is a hashed blob; SEGMENT slots (slotKind >= 1) are BARE (discriminator
          // byte).
          if (valueSize != 0 && slotKey > 0 && rowGroupId != 0 && slotKey < ProjectionIndexFences.CHUNK_SLOT_BASE) {
            final int slotKind = (int) (slotKey & 0xFFFF);
            if (slotKind == 0) {
              descriptorSlot =
                  captureDescriptorSlot(reader, leaf, valueSlice, valueSize, rowGroupId, slotKey, indexNumber);
            } else if (segmentSlotsOut != null) {
              segmentSlot = captureColumnSegmentSlot(reader, leaf, valueSlice, valueSize, rowGroupId, slotKind, slotKey,
                  indexNumber);
            }
          }
        } catch (RuntimeException e) {
          if (cursor.validateLeaf()) {
            throw e; // stable bytes — genuine corruption, not a torn read
          }
          cursor.recoverTorn(++tornRounds);
          continue;
        }
        if (!cursor.validateLeaf()) {
          cursor.recoverTorn(++tornRounds);
          continue;
        }
        tornRounds = 0;
        if (descriptorSlot != null) {
          if (descriptors.put(rowGroupId, descriptorSlot) != null) {
            throw new IllegalStateException(
                "segment-slot leaf " + rowGroupId + " has two descriptor slots (indexNumber=" + indexNumber + ")");
          }
        } else if (segmentSlot != null) {
          segmentSlotsOut.add(segmentSlot);
        }
        cursor.advance();
      }
    }
  }

  /** BE-encoded trie key of one slot key (the walk's decode, inverted). */
  private static byte[] slotKeyBytes(final long slotKey) {
    final long flipped = slotKey ^ 0x8000_0000_0000_0000L;
    final byte[] key = new byte[8];
    for (int i = 0; i < 8; i++) {
      key[i] = (byte) (flipped >>> (56 - 8 * i));
    }
    return key;
  }

  /** Validation + positional drain shared by the serial walk and the parallel partitions' merge. */
  static RawBlobSlot[] drainOrderedDescriptors(final Long2ObjectRBTreeMap<RawBlobSlot> descriptors,
      final int rowGroupCount, final int indexNumber) {
    return drainOrderedDescriptors(descriptors, rowGroupCount, indexNumber, identityPhysicalOrder(rowGroupCount));
  }

  static RawBlobSlot[] drainOrderedDescriptors(final Long2ObjectRBTreeMap<RawBlobSlot> descriptors,
      final int rowGroupCount, final int indexNumber, final int[] physicalOrder) {
    if (descriptors.isEmpty() && rowGroupCount == 0) {
      return NO_DESCRIPTOR_SLOTS;
    }
    // Validate the descriptor key set against the explicit live physical order BEFORE any page I/O:
    // a size mismatch is a gap/truncation/leaked orphan; a key absent from that order is unexpected.
    if (descriptors.size() != rowGroupCount) {
      throw new IllegalStateException(
          "segment-slot store has " + descriptors.size() + " live descriptors but metadata declares " + rowGroupCount
              + " (indexNumber=" + indexNumber + ") — truncated, stale, or leaked orphan");
    }
    final int[] logicalByPhysical = logicalSlotsByPhysical(physicalOrder, rowGroupCount);
    final RawBlobSlot[] descArr = new RawBlobSlot[rowGroupCount];
    for (final Long2ObjectMap.Entry<RawBlobSlot> e : descriptors.long2ObjectEntrySet()) {
      final long slot = e.getLongKey();
      if (slot < 1 || slot >= logicalByPhysical.length || logicalByPhysical[(int) slot] < 0) {
        if (DIAG) {
          System.err.println(describeDescriptorKeySet(descriptors, rowGroupCount));
        }
        throw new IllegalStateException(
            "segment-slot store names unexpected physical leaf " + slot + " (indexNumber=" + indexNumber + ")");
      }
      descArr[logicalByPhysical[(int) slot]] = e.getValue();
    }
    return descArr;
  }

  private static int[] identityPhysicalOrder(final int rowGroupCount) {
    final int[] order = new int[rowGroupCount];
    for (int index = 0; index < rowGroupCount; index++) {
      order[index] = index + 1;
    }
    return order;
  }

  private static int[] persistedPhysicalOrder(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount) {
    if (readBlob(reader, indexNumber, ProjectionIndexFences.ORDER_HEADER_SLOT) == null) {
      return identityPhysicalOrder(rowGroupCount);
    }
    return ProjectionIndexFences.readPhysicalOrder(reader, indexNumber, rowGroupCount);
  }

  private static int[] logicalSlotsByPhysical(final int[] physicalOrder, final int rowGroupCount) {
    if (physicalOrder == null || physicalOrder.length != rowGroupCount) {
      throw new IllegalArgumentException("physical row-group order must cover exactly " + rowGroupCount + " leaves");
    }
    final int physicalUpperBound = physicalSlotUpperBound(physicalOrder);
    final int[] logicalByPhysical = new int[physicalUpperBound + 1];
    Arrays.fill(logicalByPhysical, -1);
    for (int logical = 0; logical < rowGroupCount; logical++) {
      final int physical = physicalOrder[logical];
      if (physical < 1 || physical > MAX_ROW_GROUPS || logicalByPhysical[physical] >= 0) {
        throw new IllegalStateException("physical row-group order is not a permutation at leaf " + physical);
      }
      logicalByPhysical[physical] = logical;
    }
    return logicalByPhysical;
  }

  /** Largest live physical row-group id named by an order vector (zero for an empty index). */
  static int physicalSlotUpperBound(final int[] physicalOrder) {
    if (physicalOrder == null) {
      throw new NullPointerException("physicalOrder is required");
    }
    int maximum = 0;
    for (final int physical : physicalOrder) {
      if (physical < 1 || physical > MAX_ROW_GROUPS) {
        throw new IllegalStateException("physical row-group slot out of range: " + physical);
      }
      maximum = Math.max(maximum, physical);
    }
    return maximum;
  }

  /**
   * The descriptor keys as contiguous ranges — what a contiguity break needs in order to be read as
   * "leaves 340..400 are missing" rather than as one offending number.
   */
  private static String describeDescriptorKeySet(final Long2ObjectRBTreeMap<RawBlobSlot> descriptors,
      final int rowGroupCount) {
    final StringBuilder keys = new StringBuilder("[cat] descriptor key set: ");
    long lo = -1;
    long prev = -1;
    for (final Long2ObjectMap.Entry<RawBlobSlot> k : descriptors.long2ObjectEntrySet()) {
      final long v = k.getLongKey();
      if (lo < 0) {
        lo = v;
      } else if (v != prev + 1) {
        keys.append(lo).append("..").append(prev).append(' ');
        lo = v;
      }
      prev = v;
    }
    return keys.append(lo)
               .append("..")
               .append(prev)
               .append("  (count=")
               .append(descriptors.size())
               .append(", metadata declares ")
               .append(rowGroupCount)
               .append(')')
               .toString();
  }

  private static final RawBlobSlot[] NO_DESCRIPTOR_SLOTS = new RawBlobSlot[0];

  public static List<byte[]> readAllRowGroupsFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final int rowGroupCount) {
    return readAllRowGroupsFromColumnSegmentSlots(reader, indexNumber, rowGroupCount,
        persistedPhysicalOrder(reader, indexNumber, rowGroupCount));
  }

  public static List<byte[]> readAllRowGroupsFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final int rowGroupCount, final int[] physicalOrder) {
    final ArrayList<RawBlobSlot> segmentSlots = new ArrayList<>();
    // Phases 1-2 — one walk, then validate descriptors against the explicit live physical order.
    final RawBlobSlot[] descArr =
        orderedDescriptorSlots(reader, indexNumber, rowGroupCount, physicalOrder, segmentSlots);
    return assembleRowGroupsFromSlots(reader, indexNumber, rowGroupCount, descArr, segmentSlots, physicalOrder);
  }

  /**
   * Phases 3-5 over an already-collected slot set — the shared tail of the serial walk above and the
   * catalog's PARALLEL collection (partitioned {@link #collectSlotsRange} calls merged by the
   * caller). Batch I/O (descriptor resolution, committed segment refs) runs on the ONE reader passed
   * here; the per-leaf assembly fans out as before.
   */
  static List<byte[]> assembleRowGroupsFromSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount, final RawBlobSlot[] descArr, final ArrayList<RawBlobSlot> segmentSlots) {
    return assembleRowGroupsFromSlots(reader, indexNumber, rowGroupCount, descArr, segmentSlots,
        identityPhysicalOrder(rowGroupCount), null);
  }

  static List<byte[]> assembleRowGroupsFromSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount, final RawBlobSlot[] descArr, final ArrayList<RawBlobSlot> segmentSlots,
      final int[] physicalOrder) {
    return assembleRowGroupsFromSlots(reader, indexNumber, rowGroupCount, descArr, segmentSlots, physicalOrder, null);
  }

  /**
   * {@code parallelReaders}: pre-opened extra readers for PARTITIONED committed-segment resolution
   * (the caller owns their transactions). {@code null} keeps the single-reader batch.
   */
  static List<byte[]> assembleRowGroupsFromSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount, final RawBlobSlot[] descArr, final ArrayList<RawBlobSlot> segmentSlots,
      final StorageEngineReader @Nullable [] parallelReaders) {
    return assembleRowGroupsFromSlots(reader, indexNumber, rowGroupCount, descArr, segmentSlots,
        identityPhysicalOrder(rowGroupCount), parallelReaders);
  }

  static List<byte[]> assembleRowGroupsFromSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount, final RawBlobSlot[] descArr, final ArrayList<RawBlobSlot> segmentSlots,
      final int[] physicalOrder, final StorageEngineReader @Nullable [] parallelReaders) {
    final long p3 = DIAG
        ? System.nanoTime()
        : 0L;
    // Phase 3 — resolve descriptors (referenced ones in one batch), then size each leaf's accum.
    // The descriptor array is already logical-order positional; physical segment ids map through
    // the persisted fence order below.
    final ColumnSegmentSlotRowGroupAccum[] ordered = new ColumnSegmentSlotRowGroupAccum[rowGroupCount];
    resolveDescriptors(reader, descArr, ordered);
    final int[] logicalByPhysical = logicalSlotsByPhysical(physicalOrder, rowGroupCount);
    // Phase 4 — resolve segment positions (order-agnostic) and fill; referenced ones in one batch.
    final ArrayList<PendingSegRef> pendingSeg = new ArrayList<>();
    for (final RawBlobSlot s : segmentSlots) {
      // A segment naming a leaf past rowGroupCount is a leaked orphan (rowGroupId is an unsigned >>>16 of
      // a
      // non-zero, sub-CHUNK_SLOT_BASE key, so it is always >= 1). Caught before this segment's I/O.
      if (s.rowGroupId() >= logicalByPhysical.length || logicalByPhysical[(int) s.rowGroupId()] < 0) {
        throw new IllegalStateException("segment-slot segment slot " + s.slotKey() + " names leaf " + s.rowGroupId()
            + " outside the live physical order (leaked orphan, indexNumber=" + indexNumber + ")");
      }
      final ColumnSegmentSlotRowGroupAccum accum = ordered[logicalByPhysical[(int) s.rowGroupId()]];
      final int columnSegmentId = s.slotKind() - 1;
      final int pos = indexOf(accum.columnSegmentIds, columnSegmentId);
      if (pos < 0) {
        throw new IllegalStateException("segment-slot leaf " + s.rowGroupId() + " segment " + columnSegmentId
            + " has no descriptor entry (headless or corrupt store, indexNumber=" + indexNumber + ")");
      }
      accum.seenSegmentCount++;
      // Bare segment payloads carry their integrity in the descriptor entry (byteLen + contentHash),
      // re-checked by assembleRaw's verifyColumnSegment — no marker, no re-hash. A segment's inlineValue
      // is
      // already the raw payload (the walk stripped the 1-byte discriminator); referenced ones batch.
      if (s.resolved() != null) {
        accum.payloads[pos] = s.resolved();
      } else if (s.inlineValue() != null) {
        accum.payloads[pos] = s.inlineValue();
      } else if (columnSegmentId < ProjectionIndexColumnSegmentCodec.DICT_HASH_SEGMENT_BASE) {
        pendingSeg.add(new PendingSegRef(accum.payloads, pos, s.offset(), s.slotKey()));
      }
      // A referenced DICT_HASHES segment is deliberately NOT fetched here: the raw scan form
      // reassembles from KEYS/BODY/DICT alone, so its bytes would be pages read and thrown away —
      // and on a high-cardinality string column that is the largest chain in the leaf after the
      // dictionary itself. Its descriptor entry is still validated above; assembleRaw never asks
      // for the id, and the column-sliced fill fetches the chain on its own when a fold needs it.
    }
    for (int logicalSlot = 0; logicalSlot < ordered.length; logicalSlot++) {
      final ColumnSegmentSlotRowGroupAccum accum = ordered[logicalSlot];
      if (accum.seenSegmentCount != accum.columnSegmentIds.length) {
        throw new IllegalStateException("segment-slot leaf " + physicalOrder[logicalSlot] + " exposes "
            + accum.seenSegmentCount + " live segment slots but its descriptor declares "
            + accum.columnSegmentIds.length + " (indexNumber=" + indexNumber + ")");
      }
    }
    final long p4 = DIAG
        ? System.nanoTime()
        : 0L;
    if (parallelReaders != null && parallelReaders.length > 1 && pendingSeg.size() >= 1024) {
      resolvePendingParallel(parallelReaders, pendingSeg);
    } else {
      resolvePending(reader, pendingSeg);
    }
    final long p5 = DIAG
        ? System.nanoTime()
        : 0L;
    // Phase 5 — assemble each (independent) leaf; fan out for large stores.
    final byte[][] assembled = new byte[ordered.length][];
    assembleColumnSegmentSlotRowGroups(ordered, assembled, ordered.length >= PARALLEL_ASSEMBLE_MIN);
    if (DIAG) {
      System.err.println("[hot] assemble phases: descriptors+fill=" + (p4 - p3) / 1_000_000 + "ms resolvePending="
          + (p5 - p4) / 1_000_000 + "ms (" + pendingSeg.size() + " refs) assemble="
          + (System.nanoTime() - p5) / 1_000_000 + "ms");
    }
    final ArrayList<byte[]> out = new ArrayList<>(assembled.length);
    Collections.addAll(out, assembled);
    return out;
  }

  /**
   * Capture a DESCRIPTOR slot (slotKind 0, a hashed blob) during the walk: inline blobs keep the
   * whole small value (verified on resolution); referenced blobs keep only the 17-byte marker +
   * durable offset, or — for an unresolved (uncommitted, swizzled) page — read + verify it in-walk
   * while the leaf/ref is valid.
   */
  private static RawBlobSlot captureDescriptorSlot(final StorageEngineReader reader, final HOTLeafPage leaf,
      final MemorySegment valueSlice, final int valueSize, final long rowGroupId, final long slotKey,
      final int indexNumber) {
    if (valueSize < BLOB_MARKER_BYTES || valueSlice.get(LE.INT, 0) != BLOB_MAGIC) {
      throw new IllegalStateException("projection descriptor slot " + slotKey + " is not a canonical PIXB blob marker ("
          + valueSize + " bytes, indexNumber=" + indexNumber + ")");
    }
    final boolean inline = (valueSlice.get(LE.INT, 5) & BLOB_INLINE_FLAG) != 0;
    if (inline) {
      final byte[] value = new byte[valueSize];
      MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, value, 0, valueSize);
      return new RawBlobSlot(rowGroupId, 0, value, null, null, Constants.NULL_ID_LONG, slotKey);
    }
    final byte[] marker = new byte[BLOB_MARKER_BYTES];
    MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, marker, 0, BLOB_MARKER_BYTES);
    final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    final long offset = ref == null
        ? Constants.NULL_ID_LONG
        : ref.getKey();
    if (offset != Constants.NULL_ID_LONG) {
      return new RawBlobSlot(rowGroupId, 0, null, null, marker, offset, slotKey);
    }
    // Unresolved (uncommitted, this-transaction) or absent — resolve in-walk. The descriptor slot is
    // the ONLY content-integrity check on itself (RowGroupDescriptor.validate is structural), so
    // verify.
    final OverflowPage page = ref == null
        ? null
        : reader.readSideOverflowPage(ref);
    final byte[] resolved = verifyBlob(marker, page == null
        ? null
        : page.getDataBytes(), slotKey);
    return new RawBlobSlot(rowGroupId, 0, null, resolved, null, Constants.NULL_ID_LONG, slotKey);
  }

  /**
   * Capture a BARE SEGMENT slot (slotKind >= 1) during the walk: the leading discriminator byte marks
   * it inline (the rest of the value IS the raw payload) or referenced (bytes in a page — kept as a
   * durable offset for the coalesced batch, or read in-walk when unresolved). No marker, no hash;
   * assembleRaw's verifyColumnSegment is the sole integrity check against the descriptor entry.
   */
  private static RawBlobSlot captureColumnSegmentSlot(final StorageEngineReader reader, final HOTLeafPage leaf,
      final MemorySegment valueSlice, final int valueSize, final long rowGroupId, final int slotKind,
      final long slotKey, final int indexNumber) {
    final byte kind = valueSlice.get(ValueLayout.JAVA_BYTE, 0);
    if (kind == SEG_KIND_INLINE) {
      final byte[] payload = new byte[valueSize - 1];
      MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 1, payload, 0, valueSize - 1);
      return new RawBlobSlot(rowGroupId, slotKind, payload, null, null, Constants.NULL_ID_LONG, slotKey);
    }
    if (kind != SEG_KIND_REF) {
      throw new IllegalStateException("projection segment slot " + slotKey + " has noncanonical discriminator " + kind
          + " (" + valueSize + " bytes, indexNumber=" + indexNumber + ")");
    }
    final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    final long offset = ref == null
        ? Constants.NULL_ID_LONG
        : ref.getKey();
    if (offset != Constants.NULL_ID_LONG) {
      return new RawBlobSlot(rowGroupId, slotKind, null, null, null, offset, slotKey);
    }
    // Unresolved (uncommitted, this-transaction) or absent — resolve in-walk while the ref is valid.
    final OverflowPage page = ref == null
        ? null
        : reader.readSideOverflowPage(ref);
    final byte[] rawBytes = page == null
        ? null
        : page.getDataBytes();
    if (rawBytes == null) {
      throw new IllegalStateException("segment-slot uncommitted segment at slot " + slotKey
          + " has no page bytes (indexNumber=" + indexNumber + ")");
    }
    return new RawBlobSlot(rowGroupId, slotKind, null, rawBytes, null, Constants.NULL_ID_LONG, slotKey);
  }

  /**
   * Resolve every descriptor (referenced ones through ONE coalesced batch), verify + validate it, and
   * size its leaf's accum into {@code ordered[i]}. {@code descArr[i]} is rowGroupId {@code i+1}'s
   * descriptor slot; the referenced-collection and build passes iterate it in the same order so the
   * batch result stays aligned. The descriptor slot keeps its full content-hash verify — it is the
   * only integrity check on the descriptor itself (its {@code validate} is structural).
   */
  private static void resolveDescriptors(final StorageEngineReader reader, final RawBlobSlot[] descArr,
      final ColumnSegmentSlotRowGroupAccum[] ordered) {
    final byte[][] payloads = resolveDescriptorPayloads(reader, descArr);
    for (int i = 0; i < descArr.length; i++) {
      final byte[] descriptor = payloads[i];
      RowGroupDescriptor.validate(descriptor);
      final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
      final ColumnSegmentSlotRowGroupAccum accum = new ColumnSegmentSlotRowGroupAccum();
      accum.descriptor = descriptor;
      accum.columnSegmentIds = new int[columnSegmentCount];
      accum.payloads = new byte[columnSegmentCount][];
      for (int s = 0; s < columnSegmentCount; s++) {
        accum.columnSegmentIds[s] = RowGroupDescriptor.entryColumnSegmentId(descriptor, s);
      }
      ordered[i] = accum;
    }
  }

  /**
   * One coalesced page read for a batch of referenced SEGMENTS. No blob-hash re-verify: the raw page
   * bytes flow straight to the accum, and {@code assembleRaw}'s {@code verifyColumnSegment} checks
   * byteLen + contentHash against the descriptor entry (a genuinely-missing page yields {@code null}
   * → a loud "Missing segment bytes" there). {@code indexNumber} is unused — kept out of the hot
   * loop.
   */
  /**
   * {@link #resolvePending} partitioned across pre-opened readers. The refs sort by OFFSET first and
   * each reader takes a CONTIGUOUS run of the sorted order, so per-thread reads keep the coalescing
   * locality the single-reader batch had — cold storage sees T mostly-sequential streams instead of
   * one.
   */
  private static void resolvePendingParallel(final StorageEngineReader[] readers,
      final ArrayList<PendingSegRef> pending) {
    if (pending.isEmpty()) {
      return;
    }
    pending.sort(java.util.Comparator.comparingLong(PendingSegRef::offset));
    final int lanes = Math.min(readers.length, Math.max(1, pending.size() / 512));
    if (lanes <= 1) {
      resolvePending(readers[0], pending);
      return;
    }
    final int chunk = (pending.size() + lanes - 1) / lanes;
    ForkJoinPool.commonPool().invoke(new RecursiveAction() {
      @Override
      protected void compute() {
        final RecursiveAction[] subs = new RecursiveAction[lanes];
        for (int l = 0; l < lanes; l++) {
          final int lane = l;
          subs[l] = new RecursiveAction() {
            @Override
            protected void compute() {
              final int from = lane * chunk;
              final int to = Math.min(from + chunk, pending.size());
              if (from >= to) {
                return;
              }
              final long[] offsets = new long[to - from];
              for (int i = from; i < to; i++) {
                offsets[i - from] = pending.get(i).offset();
              }
              final byte[][] pages = readSegmentBytesBatch(readers[lane], offsets);
              for (int i = from; i < to; i++) {
                final PendingSegRef p = pending.get(i);
                p.target()[p.idx()] = pages[i - from];
              }
            }
          };
        }
        invokeAll(subs);
      }
    });
  }

  private static void resolvePending(final StorageEngineReader reader, final ArrayList<PendingSegRef> pending) {
    if (pending.isEmpty()) {
      return;
    }
    final long[] offsets = new long[pending.size()];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = pending.get(i).offset();
    }
    final byte[][] pages = readSegmentBytesBatch(reader, offsets);
    for (int i = 0; i < pending.size(); i++) {
      final PendingSegRef p = pending.get(i);
      p.target()[p.idx()] = pages[i];
    }
  }

  /**
   * Index of {@code columnSegmentId} in the ascending-id table, or {@code -1} when absent.
   *
   * <p>
   * Binary search, not a scan: the table is filled in descriptor-entry order and
   * {@link RowGroupDescriptor#serialize} enforces strictly ascending ids. This is the resolver
   * {@code assembleRaw} calls once per segment, so a linear scan made a row-group assembly
   * O(segments²) — negligible while the column cap was 84, but the dominant cost of a full read at
   * the widened cap.
   * </p>
   */
  private static int indexOf(final int[] columnSegmentIds, final int columnSegmentId) {
    final int pos = Arrays.binarySearch(columnSegmentIds, columnSegmentId);
    return pos >= 0
        ? pos
        : -1;
  }

  /** Assemble each accumulated leaf (independent per leaf); fans out for large stores. */
  private static void assembleColumnSegmentSlotRowGroups(final ColumnSegmentSlotRowGroupAccum[] ordered,
      final byte[][] out, final boolean parallel) {
    if (!parallel) {
      for (int i = 0; i < ordered.length; i++) {
        out[i] = assembleColumnSegmentSlotRowGroup(ordered[i]);
      }
      return;
    }
    final int n = ordered.length;
    ForkJoinPool.commonPool().invoke(new RecursiveAction() {
      @Override
      protected void compute() {
        final int workers = Math.min(n, Runtime.getRuntime().availableProcessors());
        final int chunk = (n + workers - 1) / workers;
        final RecursiveAction[] subs = new RecursiveAction[workers];
        for (int w = 0; w < workers; w++) {
          final int lo = w * chunk;
          final int hi = Math.min(n, lo + chunk);
          subs[w] = new RecursiveAction() {
            @Override
            protected void compute() {
              for (int i = lo; i < hi; i++) {
                out[i] = assembleColumnSegmentSlotRowGroup(ordered[i]);
              }
            }
          };
        }
        invokeAll(subs);
      }
    });
  }

  /** Assemble one leaf's raw bytes from its resolved columnSegmentId→payload table. */
  private static byte[] assembleColumnSegmentSlotRowGroup(final ColumnSegmentSlotRowGroupAccum accum) {
    return ProjectionIndexColumnSegmentCodec.assembleRaw(accum.descriptor, columnSegmentId -> {
      final int pos = indexOf(accum.columnSegmentIds, columnSegmentId);
      return pos < 0
          ? null
          : accum.payloads[pos];
    });
  }

  /**
   * Descriptor-tier row count for the segment-slot layout: sums {@code rowCount} across the
   * descriptor slots (slotKind 0) of all live physical leaves, reading NO segment slots.
   *
   * <p>
   * This serves counting queries WITHOUT hydrating, so it has to be cheap in page touches, not just
   * in bytes: it takes the same single {@link #orderedDescriptorSlots} range scan the full read does,
   * and resolves whatever descriptors spilled past the inline threshold in ONE coalesced batch.
   * Per-row-group {@link #readRowCountFromColumnSegmentSlots} probes would be {@code rowGroupCount}
   * root-to-leaf descents plus a random page read each — at 100k row groups that is a planning-path
   * cost the count tier exists to avoid.
   *
   * <p>
   * Loud on a gap, a duplicate, or a leaked orphan (the scan sees EVERY live descriptor, so an orphan
   * anywhere above the count is caught, not just the next slot), so the count can never disagree with
   * a full hydrate.
   */
  public static long sumRowsFromColumnSegmentSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount) {
    final RawBlobSlot[] descArr = orderedDescriptorSlots(reader, indexNumber, rowGroupCount,
        persistedPhysicalOrder(reader, indexNumber, rowGroupCount), null);
    long total = 0;
    for (final byte[] descriptor : resolveDescriptorPayloads(reader, descArr)) {
      // isDescriptor only guarantees the 4-byte magic; rowCount reads a 4-byte field at offset 5.
      RowGroupDescriptor.validate(descriptor);
      total += RowGroupDescriptor.rowCount(descriptor);
    }
    return total;
  }

  /**
   * Descriptor bytes for each captured slot, positional. Inline values verify in place; referenced
   * ones go out as ONE coalesced batch, which is the difference between a sequential read and
   * {@code n} random ones.
   */
  private static byte[][] resolveDescriptorPayloads(final StorageEngineReader reader, final RawBlobSlot[] descArr) {
    int referencedCount = 0;
    for (final RawBlobSlot ds : descArr) {
      if (ds.marker() != null) {
        referencedCount++;
      }
    }
    byte[][] refPages = null;
    if (referencedCount > 0) {
      final long[] offsets = new long[referencedCount];
      int oi = 0;
      for (final RawBlobSlot ds : descArr) {
        if (ds.marker() != null) {
          offsets[oi++] = ds.offset();
        }
      }
      refPages = readSegmentBytesBatch(reader, offsets);
    }
    final byte[][] payloads = new byte[descArr.length][];
    int ri = 0;
    for (int i = 0; i < descArr.length; i++) {
      final RawBlobSlot ds = descArr[i];
      payloads[i] = ds.resolved() != null
          ? ds.resolved()
          : ds.inlineValue() != null
              ? verifyInlineBlob(ds.inlineValue(), ds.slotKey())
              : verifyBlob(ds.marker(), refPages[ri++], ds.slotKey());
    }
    return payloads;
  }

  /**
   * Tombstone one blob slot: validate its current PIXB carrier, remove its side reference, then write
   * the zero-length slot value. A truly absent slot is a free no-op. Malformed ownership state fails
   * closed; tombstoning only the owner would strand the side page.
   */
  public void tombstoneBlob(final long slotKey) {
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior == null) {
      return;
    }
    if (prior.length > 0) {
      verifyPriorBlobForMutation(slotKey, prior);
      // Referenced blob: remove the page. Inline blob: strict verification above proved no side ref,
      // so this is a cheap no-op.
      boolean sideReferenceRemoved = false;
      try {
        removeSegmentPage(slotKey, BLOB_SEGMENT_ID);
        sideReferenceRemoved = true;
        writeSlotValue(slotKey, TOMBSTONE);
      } catch (final RuntimeException | Error failure) {
        if (sideReferenceRemoved) {
          poisonMutation(failure);
        }
        throw failure;
      }
    }
  }

  /** Minimum deferred leaves before phase-2 assembly fans out to the common pool. */
  private static final int PARALLEL_ASSEMBLE_MIN = 64;

  /**
   * One live leaf's directory — descriptor plus resolved segment page offsets, WITHOUT any segment
   * fetch or assembly (P5b stage 2): the construction input of the segment-lazy handle.
   * {@code columnSegmentIds}/{@code columnSegmentOffsets} are parallel, ascending-id.
   *
   * <p>
   * {@code inlineColumnSegmentBytes} is the SEGMENT-SLOT layout's inline carrier (parallel to
   * {@code columnSegmentIds}): a bare segment slot stores small payloads in its own slot value, not
   * in the descriptor (which is zone-map-only), so those bytes are captured at directory-build time
   * and supplied straight to the column fill. A {@code null} carrier means every segment slot is
   * referenced; a {@code null} element means that one segment is referenced and its bytes come from
   * the page at {@code columnSegmentOffsets[i]}.
   */
  public record RowGroupDirectory(long rowGroupId, byte[] descriptor, int[] columnSegmentIds,
      long[] columnSegmentOffsets, byte @Nullable [] @Nullable [] inlineColumnSegmentBytes) {

    public RowGroupDirectory {
      checkRowGroupId(rowGroupId);
      Objects.requireNonNull(descriptor, "descriptor");
      Objects.requireNonNull(columnSegmentIds, "columnSegmentIds");
      Objects.requireNonNull(columnSegmentOffsets, "columnSegmentOffsets");
      RowGroupDescriptor.validate(descriptor);
      final int descriptorEntries = RowGroupDescriptor.columnSegmentCount(descriptor);
      if (columnSegmentIds.length != descriptorEntries || columnSegmentOffsets.length != descriptorEntries
          || (inlineColumnSegmentBytes != null && inlineColumnSegmentBytes.length != descriptorEntries)) {
        throw new IllegalArgumentException("projection row-group directory arrays must be index-aligned");
      }
      for (int i = 0; i < descriptorEntries; i++) {
        final int id = columnSegmentIds[i];
        if (id != RowGroupDescriptor.entryColumnSegmentId(descriptor, i)) {
          throw new IllegalArgumentException(
              "projection row-group directory id " + id + " does not match descriptor entry " + i);
        }
        final byte[] inline = inlineColumnSegmentBytes == null
            ? null
            : inlineColumnSegmentBytes[i];
        final long offset = columnSegmentOffsets[i];
        final boolean hasInline = inline != null;
        final boolean hasOffset = offset >= 0 && offset != Constants.NULL_ID_LONG;
        if (hasInline == hasOffset) {
          throw new IllegalArgumentException("projection row-group directory segment " + id
              + " must have exactly one physical source (inline slot or durable offset)");
        }
        if (hasInline) {
          ProjectionIndexColumnSegmentCodec.verifyColumnSegment(descriptor, inline, id,
              ProjectionIndexColumnSegmentCodec.expectedSegmentKind(id), i);
        }
      }
    }

    /**
     * The captured inline bytes at descriptor ENTRY INDEX {@code entryIndex}, or {@code null} if the
     * segment slot is referenced (or this directory carries no inline segment slots at all).
     *
     * <p>
     * Indexed by entry, not searched by id: all three parallel arrays here are filled in
     * descriptor-entry order, so one {@link RowGroupDescriptor#entryIndexOf} binary search resolves the
     * inline bytes, the storage class and the offset together. Searching by id per array made a column
     * fill O(rowGroups × segments), which the 21844-column cap turned from negligible into the dominant
     * cost of the pruned read.
     * </p>
     */
    public byte @Nullable [] inlineBytesAt(final int entryIndex) {
      return inlineColumnSegmentBytes == null
          ? null
          : inlineColumnSegmentBytes[entryIndex];
    }
  }

  /**
   * ONE range scan builds each row group's {@link RowGroupDirectory} — its zone-map descriptor, each
   * REFERENCED segment's durable page offset (captured, not fetched), and each bare INLINE segment's
   * bytes (captured from the slot value, since a segment-slot descriptor is zone-map-only and carries
   * no inline region). Zero segment PAGE reads (only referenced descriptors, if any, read their
   * page). This is the construction input of a column-pruned segment-slot handle: a later column fill
   * batches ONLY the queried column's offsets.
   *
   * <p>
   * Returns {@code null} when any referenced segment (or descriptor) page is unresolved (uncommitted,
   * this-transaction) — offset-lazy fetching cannot serve those, so the caller falls back to the
   * eager whole-leaf read (which resolves them in-walk).
   */
  public static @Nullable List<RowGroupDirectory> readAllRowGroupDirectoriesFromColumnSegmentSlots(
      final StorageEngineReader reader, final int indexNumber, final int rowGroupCount) {
    return readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, indexNumber, rowGroupCount,
        persistedPhysicalOrder(reader, indexNumber, rowGroupCount));
  }

  public static @Nullable List<RowGroupDirectory> readAllRowGroupDirectoriesFromColumnSegmentSlots(
      final StorageEngineReader reader, final int indexNumber, final int rowGroupCount, final int[] physicalOrder) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return rowGroupCount == 0
          ? List.of()
          : null;
    }
    final RowGroupDirectory[] out = new RowGroupDirectory[rowGroupCount];
    final DirectoryWalk walk = new DirectoryWalk(out, rowGroupCount, indexNumber, physicalOrder);
    if (!collectRowGroupDirectorySlots(reader, indexNumber, rootRef, walk)) {
      return null; // an unresolved page — offset-lazy fetching cannot serve it
    }
    walk.finish();
    return Arrays.asList(out);
  }

  /**
   * {@link #readAllRowGroupDirectoriesFromColumnSegmentSlots(StorageEngineReader, int, int)} with the
   * per-leaf page decode fanned out across {@code workerReaders}' leases.
   *
   * <p>
   * The serial form is one HOT range scan that demand-faults every leaf page in turn and pays that
   * page's decompress+expand before it can look at the next reference — strictly serial CPU over
   * strictly serial I/O. Measured on a 977-row-group ClickBench projection: 160 ms, the single
   * largest item of a cold serve. Nothing about the walk needs order (see {@link DirectoryWalk} — row
   * groups are completed in place and emitted together), so the leaf pages can be decoded
   * independently and replayed afterwards.
   *
   * <p>
   * <b>Committed read-only contexts only.</b> A writer's reader resolves through a transaction intent
   * log whose read path mutates shared state (reference rebinding), so it keeps the serial cursor
   * walk — as does every caller that passes no {@code workerReaders}. This is the same line
   * {@code decodeRowGroups(StorageEngineReader, IndexDef)} draws for the eager hydrate.
   *
   * <p>
   * <b>Fail-soft.</b> Anything the parallel machinery itself gets wrong — an unexpected page type, a
   * worker that could not be run — falls back to the full serial walk and returns its result.
   * Verified-content corruption ({@link IllegalStateException} from the decode and verify helpers)
   * propagates instead, exactly as it does from the serial walk, so the catalog still marks the index
   * unusable rather than silently re-reading it.
   *
   * @param workerReaders opens one short-lived read-only reader per worker, or {@code null} to force
   *        the serial walk
   */
  public static @Nullable List<RowGroupDirectory> readAllRowGroupDirectoriesFromColumnSegmentSlots(
      final StorageEngineReader reader, final int indexNumber, final int rowGroupCount,
      final @Nullable ParallelWalkReaders workerReaders) {
    return readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, indexNumber, rowGroupCount,
        persistedPhysicalOrder(reader, indexNumber, rowGroupCount), workerReaders);
  }

  public static @Nullable List<RowGroupDirectory> readAllRowGroupDirectoriesFromColumnSegmentSlots(
      final StorageEngineReader reader, final int indexNumber, final int rowGroupCount, final int[] physicalOrder,
      final @Nullable ParallelWalkReaders workerReaders) {
    if (workerReaders != null && PARALLEL_DIRECTORY_WALK && !reader.hasTrxIntentLog()) {
      List<RowGroupDirectory> parallel;
      try {
        parallel = parallelRowGroupDirectories(reader, indexNumber, rowGroupCount, physicalOrder, workerReaders);
      } catch (final IllegalStateException corrupt) {
        throw corrupt; // verified-content corruption — the serial walk would fail identically
      } catch (final RuntimeException infrastructure) {
        LOGGER.debug("Parallel projection directory walk failed for indexNumber=" + indexNumber
            + " — falling back to the serial walk (" + infrastructure + ")");
        parallel = PARALLEL_WALK_DECLINED;
      }
      if (parallel != PARALLEL_WALK_DECLINED) {
        return parallel;
      }
    }
    return readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, indexNumber, rowGroupCount, physicalOrder);
  }

  /**
   * Opens one short-lived read-only reader for a parallel directory-walk worker and closes it again
   * when the worker returns. Implemented by the caller, which owns the session and the revision — a
   * worker must never share the coordinating reader (page resolution keeps per-reader positional
   * state).
   */
  @FunctionalInterface
  public interface ParallelWalkReaders {
    /** Runs {@code worker} against a fresh reader, closing that reader before returning. */
    void runWithReader(Consumer<StorageEngineReader> worker);
  }

  /**
   * Master switch for the parallel directory walk, {@code -Dsirix.projection.parallelWalk=false} to
   * disable. Resolved once: it is a JVM-lifetime switch, and reading it per walk would put a lookup
   * in the JDK's synchronized system-properties table on a path that also runs under a read lock.
   * Tests exercise the two routes by entry point rather than by property.
   */
  private static final boolean PARALLEL_DIRECTORY_WALK =
      !"false".equalsIgnoreCase(System.getProperty("sirix.projection.parallelWalk", "true"));

  /**
   * Worker ceiling. The walk is one-per-(resource, revision) and bounded by page I/O, not by cores;
   * past a handful of concurrent readers the device is saturated and the extra threads only add
   * transaction-open cost and lock traffic on the shared buffer manager.
   */
  private static final int MAX_DIRECTORY_WALK_WORKERS = 8;

  /**
   * Two partitions already halve a cold walk whose cost is per-leaf page expansion (measured: a
   * 977-row-group store is FIVE fat leaves under one root — ~33 ms of decode each, 165 ms serial), so
   * the only shape worth declining is the one that cannot be split at all.
   */
  private static final int MIN_PARTITIONS_FOR_PARALLEL_WALK = 2;

  /** Partitions to aim for before handing the frontier to the workers — see {@link #walkFrontier}. */
  private static final int PARTITIONS_PER_WORKER = 8;

  /** Page references per batched {@link StorageEngineReader#prefetchPageSpans} hint. */
  private static final int WALK_PREFETCH_BATCH = 128;

  /** Mirrors {@code HOTTrieReader.MAX_TREE_HEIGHT}: a descent past it means a corrupt trie. */
  private static final int MAX_WALK_DEPTH = 64;

  /** Names the parallel walk in {@link HOTTrieReader#recoverTorn} exhaustion diagnostics. */
  private static final String PARALLEL_WALK_OP = "projection parallel directory walk";

  /** Decline/engage diagnostics for the parallel walk, off unless {@code -Dsirix.projDiag=true}. */
  private static final boolean WALK_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Distinguishes "the parallel walk did not run" from its two real answers (a directory list, or
   * {@code null} for an unresolved page). A private instance, so no legitimate empty result can be
   * mistaken for it.
   */
  private static final List<RowGroupDirectory> PARALLEL_WALK_DECLINED =
      Collections.unmodifiableList(new ArrayList<>(0));

  /**
   * The parallel walk proper: enumerate a frontier of subtree references without decoding leaves,
   * decode each partition of it on its own reader, then replay the captures into one
   * {@link DirectoryWalk}.
   *
   * @return the directories, {@code null} for an unresolved page, or {@link #PARALLEL_WALK_DECLINED}
   *         when this store is not worth (or not shaped for) the parallel route
   */
  private static @Nullable List<RowGroupDirectory> parallelRowGroupDirectories(final StorageEngineReader reader,
      final int indexNumber, final int rowGroupCount, final int[] physicalOrder,
      final ParallelWalkReaders workerReaders) {
    if (rowGroupCount <= 0) {
      return PARALLEL_WALK_DECLINED;
    }
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return PARALLEL_WALK_DECLINED;
    }
    final int cores = Math.min(Runtime.getRuntime().availableProcessors(), MAX_DIRECTORY_WALK_WORKERS);
    if (cores < 2) {
      return PARALLEL_WALK_DECLINED;
    }
    final PageReference[] frontier;
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      frontier = walkFrontier(reader, trieReader, rootRef, cores * PARTITIONS_PER_WORKER);
    }
    if (frontier == null || frontier.length < MIN_PARTITIONS_FOR_PARALLEL_WALK) {
      if (WALK_DIAG) {
        System.err.println("[parwalk] DECLINED: frontier " + (frontier == null
            ? "null (unresolved page)"
            : frontier.length + " < " + MIN_PARTITIONS_FOR_PARALLEL_WALK));
      }
      return PARALLEL_WALK_DECLINED;
    }
    if (WALK_DIAG) {
      System.err.println("[parwalk] ENGAGED: frontier=" + frontier.length + " workers~" + cores);
    }
    final int chunk = (frontier.length + Math.min(cores, frontier.length) - 1) / Math.min(cores, frontier.length);
    // Re-derive the worker count from the chunk size: a ceiling division can leave the last
    // partitions empty (9 references over 8 workers is 5 chunks of 2), and an empty partition
    // still costs a thread and a read transaction.
    final int workers = (frontier.length + chunk - 1) / chunk;
    final DirectoryWalkWorker[] tasks = new DirectoryWalkWorker[workers];
    final Thread[] threads = new Thread[workers];
    final Throwable[] failures = new Throwable[workers];
    for (int w = 0; w < workers; w++) {
      final int from = w * chunk;
      final DirectoryWalkWorker task =
          new DirectoryWalkWorker(frontier, from, Math.min(from + chunk, frontier.length), indexNumber);
      final int slot = w;
      tasks[w] = task;
      final Thread thread = new Thread(() -> {
        try {
          workerReaders.runWithReader(task);
        } catch (final Throwable failure) {
          failures[slot] = failure;
        }
      }, "sirix-projection-dirwalk-" + w);
      thread.setDaemon(true);
      threads[w] = thread;
      thread.start();
    }
    // Every worker is joined before anything it produced is read, so its buffer, its unresolved
    // flag and its failure slot are all published by the join's happens-before edge — no volatile
    // and no synchronization on the per-worker state.
    boolean interrupted = false;
    for (final Thread thread : threads) {
      try {
        thread.join();
      } catch (final InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      return PARALLEL_WALK_DECLINED;
    }
    IllegalStateException corrupt = null;
    Throwable infrastructure = null;
    for (final Throwable failure : failures) {
      if (failure == null) {
        continue;
      }
      if (failure instanceof Error error) {
        throw error;
      }
      if (failure instanceof IllegalStateException stateFailure) {
        corrupt = stateFailure;
      } else if (infrastructure == null) {
        infrastructure = failure;
      }
    }
    if (corrupt != null) {
      throw corrupt; // slot content that failed a verify — must reach the catalog, not be re-read
    }
    if (infrastructure != null) {
      LOGGER.debug("Parallel projection directory walk worker failed for indexNumber=" + indexNumber
          + " — falling back to the serial walk (" + infrastructure + ")");
      return PARALLEL_WALK_DECLINED;
    }
    for (final DirectoryWalkWorker task : tasks) {
      if (task.unresolved) {
        return null; // an unresolved page — offset-lazy fetching cannot serve it
      }
    }
    final RowGroupDirectory[] out = new RowGroupDirectory[rowGroupCount];
    final DirectoryWalk walk = new DirectoryWalk(out, rowGroupCount, indexNumber, physicalOrder);
    // Partition order, not completion order: the walk is order-agnostic, but a deterministic
    // replay keeps a corrupt store failing with the same message on every run.
    for (final DirectoryWalkWorker task : tasks) {
      task.buffer.replayInto(walk);
    }
    walk.finish();
    return Arrays.asList(out);
  }

  /**
   * The set of subtree references the workers partition: descend the trie one level at a time from
   * the root, widening the frontier until it holds at least {@code target} references.
   *
   * <p>
   * The point is to reach the leaf level WITHOUT decoding a leaf, since decoding one is the very cost
   * being parallelized. A reference's page kind is only knowable by resolving it, so the expansion of
   * a level stops the moment a resolved page turns out to be a leaf: at most ONE leaf is decoded
   * serially, and that one is left swizzled for its worker. The frontier is then whatever the current
   * level holds — leaves in the ordinary case, subtree roots when the trie is deeper than
   * {@code target} needed, and a mix of the two on a ragged level. Workers descend whatever they are
   * handed, so this stays correct for all three; only the balance of the partitions varies.
   *
   * <p>
   * Each level is offset-sorted before it is prefetched and handed on, so the batched span hints —
   * and the workers' contiguous slices of the result — read the file in mostly ascending order
   * instead of in trie order.
   *
   * @return the frontier, or {@code null} when a page is unresolved or of an unexpected kind
   */
  private static PageReference @Nullable [] walkFrontier(final StorageEngineReader reader,
      final HOTTrieReader trieReader, final PageReference rootRef, final int target) {
    PageReference[] frontier = {rootRef};
    int frontierCount = 1;
    for (int level = 0; level < MAX_WALK_DEPTH && frontierCount < target; level++) {
      PageReference[] next = new PageReference[Math.max(32, frontierCount << 2)];
      int nextCount = 0;
      boolean leafLevelReached = false;
      for (int i = 0; i < frontierCount; i++) {
        final Page page = trieReader.resolvePage(frontier[i]);
        if (page == null) {
          return null;
        }
        if (page instanceof HOTLeafPage) {
          leafLevelReached = true;
          break;
        }
        if (!(page instanceof HOTIndirectPage indirect)) {
          return null;
        }
        final int children = indirect.getNumChildren();
        for (int c = 0; c < children; c++) {
          // A null child reference is skipped rather than rejected, mirroring the cursor's
          // sibling walk: its right-hand siblings are still live subtrees.
          final PageReference child = indirect.getChildReference(c);
          if (child == null) {
            continue;
          }
          if (nextCount == next.length) {
            next = Arrays.copyOf(next, nextCount << 1);
          }
          next[nextCount++] = child;
        }
      }
      if (leafLevelReached || nextCount == 0) {
        break;
      }
      frontier = next;
      frontierCount = nextCount;
      Arrays.sort(frontier, 0, frontierCount, Comparator.comparingLong(PageReference::getKey));
      prefetchWalkFrontier(reader, frontier, frontierCount);
    }
    return frontierCount == frontier.length
        ? frontier
        : Arrays.copyOf(frontier, frontierCount);
  }

  /**
   * Test hook: how many partitions {@link #parallelRowGroupDirectories} would fan this store out
   * over. A store whose frontier is narrower than {@link #MIN_PARTITIONS_FOR_PARALLEL_WALK} takes the
   * serial walk, so a differential test needs this to know it is exercising the route it names.
   */
  static int parallelWalkPartitionsForTest(final StorageEngineReader reader, final int indexNumber) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return 0;
    }
    final int cores = Math.min(Runtime.getRuntime().availableProcessors(), MAX_DIRECTORY_WALK_WORKERS);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final PageReference[] frontier = walkFrontier(reader, trieReader, rootRef, cores * PARTITIONS_PER_WORKER);
      return frontier == null
          ? 0
          : frontier.length;
    }
  }

  /**
   * Batched read-ahead hints for a whole frontier level; a no-op on backends without the primitive.
   */
  private static void prefetchWalkFrontier(final StorageEngineReader reader, final PageReference[] refs,
      final int count) {
    if (reader.recordPagePrefetchBatch() <= 0) {
      return;
    }
    final PageReference[] batch = new PageReference[WALK_PREFETCH_BATCH];
    int fill = 0;
    for (int i = 0; i < count; i++) {
      final PageReference ref = refs[i];
      if (ref.getPage() != null || ref.getKey() < 0) {
        continue; // already resident, or log-resident — nothing for the device to read ahead
      }
      batch[fill++] = ref;
      if (fill == batch.length) {
        reader.prefetchPageSpans(batch, fill);
        fill = 0;
      }
    }
    if (fill > 0) {
      reader.prefetchPageSpans(batch, fill);
    }
  }

  /**
   * One partition of the frontier, decoded on its own reader. Holds no shared state: the captures go
   * into its own buffer and are replayed by the coordinator after the join.
   */
  private static final class DirectoryWalkWorker implements Consumer<StorageEngineReader> {
    private final PageReference[] frontier;
    private final int from;
    private final int to;
    private final int indexNumber;

    /** Captured slots, in visit order. Read by the coordinator only after {@link Thread#join()}. */
    final DirectorySlotBuffer buffer = new DirectorySlotBuffer();

    /** Set when a referenced segment or descriptor page could not be resolved. */
    boolean unresolved;

    DirectoryWalkWorker(final PageReference[] frontier, final int from, final int to, final int indexNumber) {
      this.frontier = frontier;
      this.from = from;
      this.to = to;
      this.indexNumber = indexNumber;
    }

    @Override
    public void accept(final StorageEngineReader reader) {
      final SlotCapture capture = new SlotCapture();
      try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
        for (int i = from; i < to; i++) {
          if (!walkDirectorySubtree(reader, trieReader, indexNumber, frontier[i], 0, capture, buffer)) {
            unresolved = true;
            return;
          }
        }
      }
    }
  }

  /**
   * Decode every leaf under {@code ref} into {@code out}. Handles a frontier entry that turns out to
   * be an inner node — the frontier is built without proving each entry's page kind.
   *
   * @return {@code false} as soon as a page is unresolved, matching
   *         {@link #collectRowGroupDirectorySlots}'s early return
   */
  private static boolean walkDirectorySubtree(final StorageEngineReader reader, final HOTTrieReader trieReader,
      final int indexNumber, final PageReference ref, final int depth, final SlotCapture capture,
      final DirectorySlotBuffer out) {
    if (depth >= MAX_WALK_DEPTH) {
      throw new IllegalStateException(
          "segment-slot trie exceeds the maximum height of " + MAX_WALK_DEPTH + " (indexNumber=" + indexNumber + ")");
    }
    final Page page = trieReader.resolvePage(ref);
    if (page == null) {
      return false;
    }
    if (page instanceof HOTLeafPage leaf) {
      return captureLeafDirectorySlots(reader, trieReader, indexNumber, leaf, capture, out);
    }
    if (!(page instanceof HOTIndirectPage indirect)) {
      return false;
    }
    final int children = indirect.getNumChildren();
    for (int c = 0; c < children; c++) {
      final PageReference child = indirect.getChildReference(c);
      if (child == null) {
        continue;
      }
      if (!walkDirectorySubtree(reader, trieReader, indexNumber, child, depth + 1, capture, out)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Every live slot of ONE leaf, captured into {@code out} under the same optimistic-stamp discipline
   * the cursor walk uses: each entry's reads are a batch, the stamp is validated before the capture
   * is kept, and a torn batch is re-read on a freshly reloaded copy of the leaf.
   *
   * <p>
   * The validation is NOT redundant on a committed snapshot. Page CONTENT per reference is immutable,
   * but the off-heap slot backing this page object is not owned by this reader: the ClockSweeper can
   * reclaim it mid-read, which is precisely what the stamp detects. {@code entryCount} survives a
   * reload for the same reason the cursor's entry index does — content per reference is immutable, so
   * the fresh copy has the identical entries.
   */
  private static boolean captureLeafDirectorySlots(final StorageEngineReader reader, final HOTTrieReader trieReader,
      final int indexNumber, final HOTLeafPage resolved, final SlotCapture capture, final DirectorySlotBuffer out) {
    HOTLeafPage leaf = resolved;
    int tornRounds = 0;
    int entryCount;
    while (true) {
      try {
        entryCount = leaf.getEntryCount();
      } catch (final RuntimeException e) {
        if (trieReader.validateCurrentLeaf()) {
          throw e; // stable bytes — genuine corruption, not a torn read
        }
        leaf = reloadTornLeaf(trieReader, ++tornRounds);
        continue;
      }
      if (!trieReader.validateCurrentLeaf()) {
        leaf = reloadTornLeaf(trieReader, ++tornRounds);
        continue;
      }
      break;
    }
    tornRounds = 0;
    for (int entryIndex = 0; entryIndex < entryCount;) {
      try {
        readDirectorySlot(reader, indexNumber, leaf, entryIndex, capture);
      } catch (final RuntimeException e) {
        if (trieReader.validateCurrentLeaf()) {
          throw e;
        }
        leaf = reloadTornLeaf(trieReader, ++tornRounds);
        continue;
      }
      if (!trieReader.validateCurrentLeaf()) {
        leaf = reloadTornLeaf(trieReader, ++tornRounds);
        continue;
      }
      tornRounds = 0;
      if (!capture.skip) {
        if (capture.unresolved) {
          return false;
        }
        out.add(capture);
      }
      entryIndex++;
    }
    return true;
  }

  /** Reload the current leaf after a failed stamp validation and re-adopt the fresh page object. */
  private static HOTLeafPage reloadTornLeaf(final HOTTrieReader trieReader, final int round) {
    trieReader.recoverTorn(round, PARALLEL_WALK_OP);
    final HOTLeafPage refreshed = trieReader.currentLeafPage();
    if (refreshed == null) {
      throw new IllegalStateException(PARALLEL_WALK_OP + ": reloaded page is no longer a leaf");
    }
    return refreshed;
  }

  /**
   * One walking thread's captures, as parallel primitive arrays — the coordinator replays them into
   * the {@link DirectoryWalk} after the join, so nothing here is allocated per slot beyond the
   * payload arrays that are handed on anyway.
   */
  private static final class DirectorySlotBuffer {
    private static final int INITIAL_CAPACITY = 4096;

    private long[] rowGroupIds = new long[INITIAL_CAPACITY];
    private int[] slotKinds = new int[INITIAL_CAPACITY];
    private long[] offsets = new long[INITIAL_CAPACITY];
    private byte[][] payloads = new byte[INITIAL_CAPACITY][];
    private int count;

    void add(final SlotCapture capture) {
      if (count == rowGroupIds.length) {
        final int grown = count << 1;
        rowGroupIds = Arrays.copyOf(rowGroupIds, grown);
        slotKinds = Arrays.copyOf(slotKinds, grown);
        offsets = Arrays.copyOf(offsets, grown);
        payloads = Arrays.copyOf(payloads, grown);
      }
      rowGroupIds[count] = capture.rowGroupId;
      slotKinds[count] = capture.slotKind;
      offsets[count] = capture.segmentOffset;
      payloads[count] = capture.slotKind == 0
          ? capture.descriptor
          : capture.inlinePayload;
      count++;
    }

    /** The same call sequence {@link #emitDirectorySlot} makes, deferred. */
    void replayInto(final DirectoryWalk walk) {
      for (int i = 0; i < count; i++) {
        final int slotKind = slotKinds[i];
        final byte[] payload = payloads[i];
        if (slotKind == 0) {
          walk.beginRowGroup(rowGroupIds[i], payload);
        } else if (payload != null) {
          walk.putInline(rowGroupIds[i], slotKind - 1, payload);
        } else {
          walk.putOffset(rowGroupIds[i], slotKind - 1, offsets[i]);
        }
      }
    }
  }


  /**
   * Builder for the directory walk — the projection tier's one-per-(resource, revision) cold cost, so
   * it is written to allocate only what it hands back.
   *
   * <p>
   * <b>Indexed by id, NOT by arrival order.</b> A slot key is
   * {@code (rowGroupId << 16) | columnSegmentId}, so every row group's state can be addressed
   * directly at {@code rowGroupId - 1} — the same index the output array uses. That needs no map from
   * slot key to captured value and no sorted map to order the row groups. The map-based form this
   * replaced paid a red-black node per row group plus two hash maps sized for every segment slot in
   * the index (≈ {@code rowGroups × segmentsPerGroup} entries, grown by repeated rehashing), all of
   * it discarded once the directories were built.
   *
   * <p>
   * <b>Visit order is NOT assumed.</b> The range cursor walks the trie's sibling chain, and topology
   * order can diverge from key order after splits — measured: a 196-leaf index whose scan yielded
   * {@code 1..159, 192..196, 160..191}. An earlier streaming form of this class emitted a row group
   * when the NEXT descriptor arrived and rejected any id that was not {@code emitted + 1}, so it
   * rejected such a store outright; because the failure is caught and turned into "this index cannot
   * serve", a projection index simply stopped being used above ~160 leaves with nothing logged at
   * default level. Row groups are therefore completed in place and emitted together in
   * {@link #finish()}. Segment slots that arrive before their own descriptor — possible when a row
   * group straddles two leaves visited out of order — are buffered and replayed there.
   *
   * <p>
   * Corruption is still caught, just at the point an order-agnostic pass can see it: a duplicate
   * descriptor, an id past {@code rowGroupCount}, a segment naming a row group that has no descriptor
   * at all, a missing declared segment, and a descriptor count that disagrees with the metadata each
   * fail loudly. Distinct non-duplicate ids in {@code [1, N]} that number {@code N} are exactly
   * {@code {1..N}}, so contiguity needs no separate check.
   */
  private static final class DirectoryWalk {
    private final RowGroupDirectory[] out;
    private final int rowGroupCount;
    private final int indexNumber;
    private final int[] physicalOrder;
    private final int[] logicalByPhysical;

    /** Per-row-group state, indexed by {@code rowGroupId - 1}; null until its descriptor arrives. */
    private final byte[][] descriptors;
    private final int[][] segmentIds;
    private final long[][] offsets;
    private final byte[][][] inlineBytes;
    /** Declared segments already filled, per row group. */
    private final int[] filled;
    /** Rolling hint into {@link #segmentIds}{@code [slot]}: slots arrive in ascending segment id. */
    private final int[] entryHint;
    private int descriptorsSeen;

    /** Segment slots seen before their descriptor; parallel arrays, grown by doubling. */
    private long[] pendingRowGroupIds = EMPTY_LONGS;
    private int[] pendingSegmentIds = EMPTY_INTS;
    private long[] pendingOffsets = EMPTY_LONGS;
    private byte[][] pendingInline = EMPTY_PAYLOADS;
    private int pendingCount;

    private static final long[] EMPTY_LONGS = new long[0];
    private static final int[] EMPTY_INTS = new int[0];
    private static final byte[][] EMPTY_PAYLOADS = new byte[0][];

    DirectoryWalk(final RowGroupDirectory[] out, final int rowGroupCount, final int indexNumber,
        final int[] physicalOrder) {
      this.out = out;
      this.rowGroupCount = rowGroupCount;
      this.indexNumber = indexNumber;
      this.physicalOrder = physicalOrder.clone();
      this.logicalByPhysical = logicalSlotsByPhysical(this.physicalOrder, rowGroupCount);
      this.descriptors = new byte[rowGroupCount][];
      this.segmentIds = new int[rowGroupCount][];
      this.offsets = new long[rowGroupCount][];
      this.inlineBytes = new byte[rowGroupCount][][];
      this.filled = new int[rowGroupCount];
      this.entryHint = new int[rowGroupCount];
    }

    /** A descriptor slot: open the row group it names. */
    void beginRowGroup(final long rowGroupId, final byte[] descriptor) {
      final int slot = slotOf(rowGroupId);
      if (descriptors[slot] != null) {
        throw new IllegalStateException(
            "segment-slot leaf " + rowGroupId + " has two descriptor slots (indexNumber=" + indexNumber + ")");
      }
      RowGroupDescriptor.validate(descriptor);
      final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
      final int[] ids = new int[columnSegmentCount];
      final long[] offs = new long[columnSegmentCount];
      for (int i = 0; i < columnSegmentCount; i++) {
        ids[i] = RowGroupDescriptor.entryColumnSegmentId(descriptor, i);
        offs[i] = Constants.NULL_ID_LONG;
      }
      descriptors[slot] = descriptor;
      segmentIds[slot] = ids;
      offsets[slot] = offs;
      descriptorsSeen++;
    }

    /** A referenced segment slot: record its durable page offset. */
    void putOffset(final long rowGroupId, final int columnSegmentId, final long offset) {
      final int slot = slotOf(rowGroupId);
      if (segmentIds[slot] == null) {
        addPending(rowGroupId, columnSegmentId, offset, null);
        return;
      }
      applyOffset(slot, columnSegmentId, offset);
    }

    /** An inline segment slot: record its bytes (they never touch a page). */
    void putInline(final long rowGroupId, final int columnSegmentId, final byte[] payload) {
      final int slot = slotOf(rowGroupId);
      if (segmentIds[slot] == null) {
        addPending(rowGroupId, columnSegmentId, Constants.NULL_ID_LONG, payload);
        return;
      }
      applyInline(slot, columnSegmentId, payload);
    }

    private void applyOffset(final int slot, final int columnSegmentId, final long offset) {
      if (offset < 0 || offset == Constants.NULL_ID_LONG) {
        throw new IllegalStateException("segment-slot leaf " + physicalOrder[slot] + " segment " + columnSegmentId
            + " has no durable page offset (indexNumber=" + indexNumber + ")");
      }
      final int i = entryIndexOf(slot, columnSegmentId);
      if (i < 0) {
        throw undeclaredSegment(slot, columnSegmentId);
      }
      final byte[][] inline = inlineBytes[slot];
      if (offsets[slot][i] != Constants.NULL_ID_LONG || inline != null && inline[i] != null) {
        throw duplicateSegment(slot, columnSegmentId);
      }
      filled[slot]++;
      offsets[slot][i] = offset;
    }

    private void applyInline(final int slot, final int columnSegmentId, final byte[] payload) {
      final int i = entryIndexOf(slot, columnSegmentId);
      if (i < 0) {
        throw undeclaredSegment(slot, columnSegmentId);
      }
      byte[][] inline = inlineBytes[slot];
      if (inline == null) {
        inline = new byte[segmentIds[slot].length][];
        inlineBytes[slot] = inline;
      }
      if (offsets[slot][i] != Constants.NULL_ID_LONG || inline[i] != null) {
        throw duplicateSegment(slot, columnSegmentId);
      }
      filled[slot]++;
      inline[i] = payload;
    }

    /** Buffer a segment slot whose descriptor has not been visited yet. */
    private void addPending(final long rowGroupId, final int columnSegmentId, final long offset,
        final byte @Nullable [] payload) {
      if (pendingCount == pendingRowGroupIds.length) {
        final int grown = pendingCount == 0
            ? 16
            : pendingCount << 1;
        pendingRowGroupIds = Arrays.copyOf(pendingRowGroupIds, grown);
        pendingSegmentIds = Arrays.copyOf(pendingSegmentIds, grown);
        pendingOffsets = Arrays.copyOf(pendingOffsets, grown);
        pendingInline = Arrays.copyOf(pendingInline, grown);
      }
      pendingRowGroupIds[pendingCount] = rowGroupId;
      pendingSegmentIds[pendingCount] = columnSegmentId;
      pendingOffsets[pendingCount] = offset;
      pendingInline[pendingCount] = payload;
      pendingCount++;
    }

    /** Validate a row group id and turn it into an index into the per-row-group arrays. */
    private int slotOf(final long rowGroupId) {
      if (rowGroupId < 1 || rowGroupId >= logicalByPhysical.length || logicalByPhysical[(int) rowGroupId] < 0) {
        throw new IllegalStateException("segment-slot store names leaf " + rowGroupId + " but metadata declares "
            + rowGroupCount + " live leaves in a different physical order (indexNumber=" + indexNumber + ")");
      }
      return logicalByPhysical[(int) rowGroupId];
    }

    /**
     * Entry index of {@code columnSegmentId} in row group {@code slot}, or {@code -1} when the
     * descriptor does not declare it. Callers turn {@code -1} into corruption; an undeclared live slot
     * cannot be ignored because it would have no lifecycle owner to tombstone. Slots arrive in
     * ascending segment id within a row group, so the per-row-group rolling hint makes this O(1)
     * amortized; the wrap-around scan keeps it correct for a descriptor whose entries are in some other
     * order.
     *
     */
    private int entryIndexOf(final int slot, final int columnSegmentId) {
      final int[] ids = segmentIds[slot];
      final int n = ids.length;
      final int hint = entryHint[slot];
      for (int probe = 0; probe < n; probe++) {
        final int i = hint + probe < n
            ? hint + probe
            : hint + probe - n;
        if (ids[i] == columnSegmentId) {
          entryHint[slot] = i + 1 < n
              ? i + 1
              : 0;
          return i;
        }
      }
      return -1;
    }

    private IllegalStateException undeclaredSegment(final int slot, final int columnSegmentId) {
      return new IllegalStateException("segment-slot leaf " + physicalOrder[slot] + " contains undeclared segment "
          + columnSegmentId + " (indexNumber=" + indexNumber + ")");
    }

    private IllegalStateException duplicateSegment(final int slot, final int columnSegmentId) {
      return new IllegalStateException("segment-slot leaf " + physicalOrder[slot] + " contains duplicate segment "
          + columnSegmentId + " (indexNumber=" + indexNumber + ")");
    }

    /** Replay buffered segments, check every row group is complete, and emit them all. */
    void finish() {
      for (int p = 0; p < pendingCount; p++) {
        final int slot = slotOf(pendingRowGroupIds[p]);
        if (segmentIds[slot] == null) {
          throw new IllegalStateException("segment-slot segment " + pendingSegmentIds[p] + " belongs to leaf "
              + pendingRowGroupIds[p] + " but no descriptor for it was read (indexNumber=" + indexNumber + ")");
        }
        final byte[] payload = pendingInline[p];
        if (payload != null) {
          applyInline(slot, pendingSegmentIds[p], payload);
        } else {
          applyOffset(slot, pendingSegmentIds[p], pendingOffsets[p]);
        }
      }
      if (descriptorsSeen != rowGroupCount) {
        throw new IllegalStateException("segment-slot store has " + descriptorsSeen
            + " live descriptors but metadata declares " + rowGroupCount + " (indexNumber=" + indexNumber + ")");
      }
      for (int slot = 0; slot < rowGroupCount; slot++) {
        final int[] ids = segmentIds[slot];
        if (filled[slot] != ids.length) {
          final byte[][] inline = inlineBytes[slot];
          for (int i = 0; i < ids.length; i++) {
            if (offsets[slot][i] == Constants.NULL_ID_LONG && (inline == null || inline[i] == null)) {
              throw new IllegalStateException("segment-slot leaf " + physicalOrder[slot] + " segment " + ids[i]
                  + " missing (indexNumber=" + indexNumber + ")");
            }
          }
        }
        out[slot] =
            new RowGroupDirectory(physicalOrder[slot], descriptors[slot], ids, offsets[slot], inlineBytes[slot]);
      }
    }
  }

  /**
   * The range-scan half of {@link #readAllRowGroupDirectoriesFromColumnSegmentSlots}: streams every
   * live slot into {@code walk} in key order — a descriptor's bytes are read here (the build is
   * driven by its entries), a segment slot contributes either its inline bytes or its durable page
   * offset, CAPTURED, never fetched.
   *
   * @return {@code false} as soon as any referenced page is unresolved (uncommitted,
   *         this-transaction), which the caller turns into a fall-back to the eager whole-leaf read
   */
  private static boolean collectRowGroupDirectorySlots(final StorageEngineReader reader, final int indexNumber,
      final PageReference rootRef, final DirectoryWalk walk) {
    final byte[] firstRowGroupSlot = slotKeyBytes(1L << 16);
    final byte[] lastRowGroupSlot = slotKeyBytes(((long) MAX_ROW_GROUPS << 16) | 0xFFFFL);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
        HOTRangeCursor cursor = trieReader.range(rootRef, firstRowGroupSlot, lastRowGroupSlot)) {
      // Optimistic-stamp discipline: all reads and capture copies for a slot happen first, the
      // stamp is validated, and only then do the results reach the DirectoryWalk callbacks (whose
      // effects cannot be retracted) or the unresolved-page early return. A torn batch
      // re-evaluates the SAME slot on a refreshed leaf copy.
      int tornRounds = 0;
      final SlotCapture capture = new SlotCapture();
      while (cursor.hasNext()) {
        // Re-read per round: a torn recovery re-adopts the SAME position on a NEW page object.
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIndex = cursor.currentEntryIndex();
        try {
          readDirectorySlot(reader, indexNumber, leaf, entryIndex, capture);
        } catch (RuntimeException e) {
          if (cursor.validateLeaf()) {
            throw e; // stable bytes — genuine corruption, not a torn read
          }
          cursor.recoverTorn(++tornRounds);
          continue;
        }
        if (!cursor.validateLeaf()) {
          cursor.recoverTorn(++tornRounds);
          continue;
        }
        tornRounds = 0;
        if (!capture.skip) {
          if (capture.unresolved) {
            return false;
          }
          emitDirectorySlot(capture, walk);
        }
        cursor.advance();
      }
    }
    return true;
  }

  /**
   * One slot's captured content. Reused across the entries of a walk — a walking thread allocates
   * exactly one of these, and only the payload arrays it hands on are per-slot.
   */
  private static final class SlotCapture {
    long rowGroupId;
    int slotKind;
    boolean skip;
    boolean unresolved;
    byte @Nullable [] descriptor;
    byte @Nullable [] inlinePayload;
    long segmentOffset;

    void reset() {
      rowGroupId = 0;
      slotKind = 0;
      skip = false;
      unresolved = false;
      descriptor = null;
      inlinePayload = null;
      segmentOffset = Constants.NULL_ID_LONG;
    }
  }

  /**
   * Read ONE entry of {@code leaf} into {@code capture}: which row group it belongs to, and either
   * its descriptor bytes, its bare inline payload, or its referenced segment's durable page offset
   * (CAPTURED, never fetched).
   *
   * <p>
   * Deliberately does no stamp validation and no torn-read recovery: the leaf is unpinned, so the
   * caller owns the validate-then-commit boundary — the serial walk validates through its cursor, the
   * parallel walk through its own trie reader, and neither may let a capture reach a consumer whose
   * effects cannot be retracted before the batch has been proven stable. Keeping the decode itself in
   * one place is what stops the two routes from drifting apart.
   */
  private static void readDirectorySlot(final StorageEngineReader reader, final int indexNumber, final HOTLeafPage leaf,
      final int entryIndex, final SlotCapture capture) {
    capture.reset();
    final long slotKey = leaf.decodeKey8BE(entryIndex) ^ 0x8000_0000_0000_0000L;
    if (slotKey < 0) {
      capture.skip = true;
      return;
    }
    final long rowGroupId = slotKey >>> 16;
    capture.rowGroupId = rowGroupId;
    // The value's LOCATION, resolved once, not a slice of it: nine of every ten slots here
    // need only their size plus a discriminator byte, and materializing a MemorySegment per
    // slot made the walk's largest allocation a set of objects it immediately threw away.
    // Every read below reuses this one handle. A malformed slot reports length -1, which the
    // tombstone branch already covers.
    final long valueRef = leaf.valueRef(entryIndex);
    final int valueSize = Math.max(HOTLeafPage.refLength(valueRef), 0);
    if (valueSize > leaf.slotCapacity()) {
      // Never let a length no slot can hold size an allocation: torn (validation fails —
      // retried) or genuine corruption (it holds — the throw escapes).
      throw new IllegalStateException("segment-slot value of " + valueSize + " bytes exceeds the slot capacity");
    }
    // Skip tombstones, the slot-0 metadata and the fence chunks — see the key families
    // documented on readAllRowGroupsFromColumnSegmentSlots.
    if (valueSize == 0 || rowGroupId == 0 || slotKey >= ProjectionIndexFences.CHUNK_SLOT_BASE) {
      capture.skip = true;
      return;
    }
    final int slotKind = (int) (slotKey & 0xFFFF);
    capture.slotKind = slotKind;
    if (slotKind == 0) {
      final byte[] descriptor = resolveDirectoryDescriptorSlot(reader, leaf, valueRef, valueSize, slotKey);
      capture.descriptor = descriptor;
      capture.unresolved = descriptor == null;
      return;
    }
    final byte kind = leaf.refByteAt(valueRef, 0);
    if (kind == SEG_KIND_INLINE) {
      final byte[] inlinePayload = new byte[valueSize - 1];
      leaf.copyRefInto(valueRef, 1, inlinePayload, 0, valueSize - 1);
      capture.inlinePayload = inlinePayload;
      return;
    }
    if (kind != SEG_KIND_REF) {
      throw new IllegalStateException("segment-slot segment slot " + slotKey + " has an unknown" + " discriminator "
          + kind + " (indexNumber=" + indexNumber + ")");
    }
    final long segmentOffset = resolvedSegmentPageOffset(leaf, slotKey);
    capture.segmentOffset = segmentOffset;
    capture.unresolved = segmentOffset == Constants.NULL_ID_LONG;
  }

  /** Hand one captured, stamp-validated slot to the builder. */
  private static void emitDirectorySlot(final SlotCapture capture, final DirectoryWalk walk) {
    if (capture.slotKind == 0) {
      walk.beginRowGroup(capture.rowGroupId, capture.descriptor);
    } else if (capture.inlinePayload != null) {
      walk.putInline(capture.rowGroupId, capture.slotKind - 1, capture.inlinePayload);
    } else {
      walk.putOffset(capture.rowGroupId, capture.slotKind - 1, capture.segmentOffset);
    }
  }



  /**
   * A descriptor slot's bytes during the directory walk. Unlike a segment, the descriptor IS read
   * here when referenced — the whole build is driven by its entries, so deferring it would buy
   * nothing.
   *
   * @return {@code null} when its page is unresolved
   */
  private static byte @Nullable [] resolveDirectoryDescriptorSlot(final StorageEngineReader reader,
      final HOTLeafPage leaf, final long valueRef, final int valueSize, final long slotKey) {
    // Descriptors are the walk's per-row-group cost, so the inline case — the common one, since a
    // descriptor is well under BLOB_INLINE_MAX — copies EXACTLY ONCE, straight from the slot into
    // the array that is handed back. Reading the marker straight off the leaf avoids both the
    // slice object and materializing the marker+payload bytes only to copy the payload back out.
    if (valueSize >= BLOB_MARKER_BYTES && leaf.refIntLEAt(valueRef, 0) == BLOB_MAGIC
        && (leaf.refIntLEAt(valueRef, 5) & BLOB_INLINE_FLAG) != 0) {
      return verifyInlineBlobFromSlot(leaf, valueRef, valueSize, slotKey);
    }
    final byte[] value = new byte[valueSize];
    leaf.copyRefInto(valueRef, 0, value, 0, valueSize);
    if (isInlineBlob(value)) {
      return verifyInlineBlob(value, slotKey);
    }
    final long offset = resolvedSegmentPageOffset(leaf, slotKey);
    if (offset == Constants.NULL_ID_LONG) {
      return null;
    }
    return verifyBlob(value, readSegmentBytesAtOffset(reader, offset), slotKey);
  }

  /**
   * {@link #verifyInlineBlob} against the slot's bytes in place: same marker validation and same
   * content-hash check, but the payload is copied once — into the array the caller keeps — instead of
   * once into a marker+payload buffer and again out of it.
   */
  private static byte[] verifyInlineBlobFromSlot(final HOTLeafPage leaf, final long valueRef, final int valueSize,
      final long slotKey) {
    if (leaf.refByteAt(valueRef, 4) != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int len = leaf.refIntLEAt(valueRef, 5) & ~BLOB_INLINE_FLAG;
    if (len < 0 || valueSize != BLOB_MARKER_BYTES + len) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " has inconsistent length (" + valueSize
          + " bytes, expected " + (BLOB_MARKER_BYTES + len) + ")");
    }
    final byte[] payload = new byte[len];
    leaf.copyRefInto(valueRef, BLOB_MARKER_BYTES, payload, 0, len);
    if (ProjectionIndexColumnSegmentCodec.contentHash(payload) != leaf.refLongLEAt(valueRef, 9)) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " failed hash verification");
    }
    return payload;
  }

  // captureDirectoryColumnSegmentSlot was folded into collectRowGroupDirectorySlots: under
  // optimistic stamps its DirectoryWalk side effects must not fire until the slot's read batch
  // has validated, so the reads and the callbacks had to be split around the validation point.

  /** Durable offset of the page backing {@code slotKey}, or {@link Constants#NULL_ID_LONG}. */
  private static long resolvedSegmentPageOffset(final HOTLeafPage leaf, final long slotKey) {
    final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    return ref == null
        ? Constants.NULL_ID_LONG
        : ref.getKey();
  }

  /**
   * Fetch one segment page's bytes by durable offset through a throwaway reference — the segment-lazy
   * handle's fetch primitive. Returns {@code null} for a null offset.
   */
  public static byte @Nullable [] readSegmentBytesAtOffset(final StorageEngineReader reader, final long offset) {
    if (offset == Constants.NULL_ID_LONG) {
      return null;
    }
    final PageReference ref = new PageReference();
    ref.setKey(offset);
    final OverflowPage page = reader.readSideOverflowPage(ref);
    return page == null
        ? null
        : page.getDataBytes();
  }

  /**
   * Batched {@link #readSegmentBytesAtOffset}: one call per COLUMN FILL instead of one per segment,
   * so the backend can coalesce runs of near-adjacent offsets into single ranged reads (P5b stage
   * 4b). Result is input-aligned; a null/{@code NULL_ID_LONG} offset or an unresolved reference
   * yields {@code null} at that index.
   */
  public static byte @Nullable [] @Nullable [] readSegmentBytesBatch(final StorageEngineReader reader,
      final long[] offsets) {
    final OverflowPage[] pages = reader.readSideOverflowPageBatch(offsets);
    final byte[][] out = new byte[offsets.length][];
    for (int i = 0; i < offsets.length; i++) {
      out[i] = pages[i] == null
          ? null
          : pages[i].getDataBytes();
    }
    if (DIAG) {
      int wanted = 0;
      long bytes = 0;
      for (int i = 0; i < offsets.length; i++) {
        if (offsets[i] != Constants.NULL_ID_LONG && offsets[i] >= 0) {
          wanted++;
        }
        if (out[i] != null) {
          bytes += out[i].length;
        }
      }
      System.err.println("[io] segBatch offsets=" + offsets.length + " wanted=" + wanted + " bytes=" + bytes + "  "
          + FileChannelReader.runDiagSummary());
    }
    return out;
  }

  // ==================== blob slots (slot-0 metadata payload) ====================

  /**
   * Store an opaque payload (the PIXM shape metadata, the per-leaf fence chunks) at {@code slotKey}.
   * Mirroring the descriptor's hybrid split, the payload is either INLINE (bytes in the slot value's
   * trailing region, for payloads ≤ {@link #BLOB_INLINE_MAX}) or REFERENCED (one
   * {@link OverflowPage}, for larger ones); the leading PIXB marker carries byteLen + an XXH3-64 hash
   * for integrity either way (segment pages have no checksum of their own). Whole-blob
   * last-writer-wins, with an unchanged blob carried forward as a true no-op.
   */
  public void putBlob(final long slotKey, final byte[] payload) {
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID);
    final long hash = ProjectionIndexColumnSegmentCodec.contentHash(payload);
    final boolean inline = payload.length <= BLOB_INLINE_MAX;
    final byte[] prior = readSlotValueForWrite(slotKey);
    final byte[] verifiedPrior;
    final boolean priorWasReferencedBlob;
    if (prior == null || prior.length == 0) {
      verifiedPrior = null;
      priorWasReferencedBlob = false;
    } else {
      verifiedPrior = verifyPriorBlobForMutation(slotKey, prior);
      priorWasReferencedBlob = !isInlineBlob(prior);
    }
    // Carry-forward is legal only after the resident payload itself passed length/hash verification.
    // The storage-class bit must also match, so a referenced⇄inline migration is never mistaken for
    // a no-op (its stale page would otherwise linger, or its inline bytes never get written).
    if (verifiedPrior != null) {
      final int priorLenField = ProjectionIndexRowGroupCodec.getIntLE(prior, 5);
      if ((priorLenField & ~BLOB_INLINE_FLAG) == payload.length && ((priorLenField & BLOB_INLINE_FLAG) != 0) == inline
          && ProjectionIndexRowGroupCodec.getLongLE(prior, 9) == hash && Arrays.equals(verifiedPrior, payload)) {
        return;
      }
    }
    if (priorWasReferencedBlob) {
      rejectPendingSidePageMutation(slotKey, BLOB_SEGMENT_ID, "replace");
    }
    if (inline) {
      final byte[] value = new byte[BLOB_MARKER_BYTES + payload.length];
      RowGroupDescriptor.putIntLE(value, 0, BLOB_MAGIC);
      value[4] = BLOB_VERSION;
      RowGroupDescriptor.putIntLE(value, 5, payload.length | BLOB_INLINE_FLAG);
      RowGroupDescriptor.putLongLE(value, 9, hash);
      System.arraycopy(payload, 0, value, BLOB_MARKER_BYTES, payload.length);
      boolean ownerMarkerWritten = false;
      try {
        writeSlotValue(slotKey, value);
        ownerMarkerWritten = true;
        // Referenced → inline migration: drop the now-orphaned page (no-op when there was none).
        if (priorWasReferencedBlob) {
          removeSegmentPage(slotKey, BLOB_SEGMENT_ID);
        }
      } catch (final RuntimeException | Error failure) {
        if (ownerMarkerWritten) {
          poisonMutation(failure);
        }
        throw failure;
      }
    } else {
      final byte[] marker = new byte[BLOB_MARKER_BYTES];
      RowGroupDescriptor.putIntLE(marker, 0, BLOB_MAGIC);
      marker[4] = BLOB_VERSION;
      RowGroupDescriptor.putIntLE(marker, 5, payload.length);
      RowGroupDescriptor.putLongLE(marker, 9, hash);
      boolean ownerMarkerWritten = false;
      try {
        writeSlotValue(slotKey, marker);
        ownerMarkerWritten = true;
        putSegmentPage(slotKey, BLOB_SEGMENT_ID, payload);
      } catch (final RuntimeException | Error failure) {
        if (ownerMarkerWritten) {
          poisonMutation(failure);
        }
        throw failure;
      }
    }
  }

  /** {@code true} iff {@code value} is a blob slot value whose payload is stored inline. */
  private static boolean isInlineBlob(final byte[] value) {
    return value.length >= BLOB_MARKER_BYTES && ProjectionIndexRowGroupCodec.getIntLE(value, 0) == BLOB_MAGIC
        && (ProjectionIndexRowGroupCodec.getIntLE(value, 5) & BLOB_INLINE_FLAG) != 0;
  }

  /** Verify the complete prior blob before a writer may preserve or replace its ownership state. */
  private byte[] verifyPriorBlobForMutation(final long slotKey, final byte[] marker) {
    try {
      final byte[] sidePayload = getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID);
      if (isInlineBlob(marker)) {
        if (sidePayload != null) {
          throw new IllegalStateException("Inline blob at slot " + slotKey + " has an unexpected side-page reference");
        }
        return verifyInlineBlob(marker, slotKey);
      }
      return verifyBlob(marker, sidePayload, slotKey);
    } catch (final RuntimeException | Error failure) {
      poisonMutation(failure);
      throw failure;
    }
  }

  /** Writer-side blob read; {@code null} when absent/tombstoned. Verifies length + hash. */
  public byte @Nullable [] getBlob(final long slotKey) {
    final byte[] value = readSlotValueForWrite(slotKey);
    if (value == null || value.length == 0) {
      return null;
    }
    final byte[] payload = isInlineBlob(value)
        ? verifyInlineBlob(value, slotKey)
        : verifyBlob(value, getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID), slotKey);
    return payload;
  }

  /** Writer-side read of one raw HOT slot (no PIXB framing), for the sparse record locator. */
  byte @Nullable [] getRawSlot(final long slotKey) {
    final byte[] value = readSlotValueForWrite(slotKey);
    return value == null || value.length == 0
        ? null
        : value;
  }

  /** Writer-side raw HOT slot put; callers own their compact value format and validation. */
  void putRawSlot(final long slotKey, final byte[] value) {
    if (slotKey >= 0) {
      throw new IllegalArgumentException("raw sparse-locator slot must be negative: " + slotKey);
    }
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("raw sparse-locator value must be non-empty");
    }
    writeSlotValue(slotKey, value);
  }

  /** Tombstone one raw sparse-locator slot without creating an entry for an absent key. */
  void tombstoneRawSlot(final long slotKey) {
    if (slotKey >= 0) {
      throw new IllegalArgumentException("raw sparse-locator slot must be negative: " + slotKey);
    }
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior != null && prior.length > 0) {
      writeSlotValue(slotKey, TOMBSTONE);
    }
  }

  byte @Nullable [] getStructuralOrderSlot(final long slotKey) {
    validateStructuralOrderSlot(slotKey);
    return getRawSlot(slotKey);
  }

  void putStructuralOrderSlot(final long slotKey, final byte[] value) {
    validateStructuralOrderSlot(slotKey);
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("structural-order slot value must be non-empty");
    }
    writeSlotValue(slotKey, value);
  }

  void tombstoneStructuralOrderSlot(final long slotKey) {
    validateStructuralOrderSlot(slotKey);
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior != null && prior.length > 0) {
      writeSlotValue(slotKey, TOMBSTONE);
    }
  }

  private static void validateStructuralOrderSlot(final long slotKey) {
    if (!ProjectionStructuralOrderDirectory.ownsSlot(slotKey)) {
      throw new IllegalArgumentException("slot is outside the structural-order namespace: " + slotKey);
    }
  }

  /** Reader-side committed read of one raw HOT slot (no blob/segment interpretation). */
  static byte @Nullable [] readRawSlot(final StorageEngineReader reader, final int indexNumber, final long slotKey) {
    if (slotKey >= 0) {
      throw new IllegalArgumentException("raw sparse-locator slot must be negative: " + slotKey);
    }
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      for (int attempt = 0; attempt <= HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
        final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, slotKey, keyBuf);
        if (leaf == null) {
          return null;
        }
        byte @Nullable [] value;
        try {
          final int index = leaf.findEntry(keyBuf);
          value = index < 0
              ? null
              : leaf.copyStoredValue(index);
        } catch (final RuntimeException failure) {
          if (trieReader.validateCurrentLeaf()) {
            throw failure;
          }
          continue;
        }
        if (!trieReader.validateCurrentLeaf()) {
          continue;
        }
        return value == null || value.length == 0
            ? null
            : value;
      }
      throw HOTTrieReader.stampRetriesExhausted("readRawSlot(slot " + slotKey + ")");
    }
  }

  /** Strict prior-blob read for a mutation that must never overwrite unknown ownership state. */
  private byte @Nullable [] getPriorBlobForMutation(final long slotKey) {
    try {
      return getBlob(slotKey);
    } catch (final RuntimeException | Error failure) {
      poisonMutation(failure);
      throw failure;
    }
  }

  private IllegalStateException poisonMalformedPriorDescriptor(final long rowGroupId, final String detail) {
    final IllegalStateException failure = new IllegalStateException(
        "Projection row group " + rowGroupId + " has an unreadable prior descriptor: " + detail);
    poisonMutation(failure);
    return failure;
  }

  /** Preserve the authoritative corruption/failure if transaction poisoning itself also fails. */
  private void poisonMutation(final Throwable failure) {
    try {
      storageEngineWriter.markTransactionRollbackOnly(failure);
    } catch (final RuntimeException | Error poisonFailure) {
      addSuppressedSafely(failure, poisonFailure);
    }
  }

  /** Reader-side blob read for committed revisions. */
  public static byte @Nullable [] readBlob(final StorageEngineReader reader, final int indexNumber,
      final long slotKey) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      final long refKey = HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID);
      for (int attempt = 0; attempt <= HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
        final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, slotKey, keyBuf);
        if (leaf == null) {
          return null;
        }
        byte @Nullable [] value;
        PageReference ref = null;
        try {
          final int idx = leaf.findEntry(keyBuf);
          value = idx < 0
              ? null
              : leaf.copyStoredValue(idx);
          if (value != null && value.length != 0 && !isInlineBlob(value)) {
            ref = leaf.getPageReference(refKey);
          }
        } catch (final RuntimeException failure) {
          if (trieReader.validateCurrentLeaf()) {
            throw failure;
          }
          continue;
        }
        if (!trieReader.validateCurrentLeaf()) {
          continue;
        }
        if (value == null || value.length == 0) {
          return null;
        }
        if (isInlineBlob(value)) {
          return verifyInlineBlob(value, slotKey);
        }
        if (ref == null) {
          return verifyBlob(value, null, slotKey);
        }
        final OverflowPage page = reader.readSideOverflowPage(ref);
        return verifyBlob(value, page == null
            ? null
            : page.getDataBytes(), slotKey);
      }
      throw HOTTrieReader.stampRetriesExhausted("readBlob(slot " + slotKey + ")");
    }
  }

  /** Verify + extract an inline blob's payload from its own slot value (no page). */
  private static byte[] verifyInlineBlob(final byte[] value, final long slotKey) {
    if (value.length < BLOB_MARKER_BYTES || ProjectionIndexRowGroupCodec.getIntLE(value, 0) != BLOB_MAGIC
        || value[4] != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int len = ProjectionIndexRowGroupCodec.getIntLE(value, 5) & ~BLOB_INLINE_FLAG;
    if (value.length != BLOB_MARKER_BYTES + len) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " has inconsistent length (" + value.length
          + " bytes, expected " + (BLOB_MARKER_BYTES + len) + ")");
    }
    final byte[] payload = Arrays.copyOfRange(value, BLOB_MARKER_BYTES, BLOB_MARKER_BYTES + len);
    if (ProjectionIndexColumnSegmentCodec.contentHash(payload) != ProjectionIndexRowGroupCodec.getLongLE(value, 9)) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " failed hash verification");
    }
    return payload;
  }

  private static byte[] verifyBlob(final byte[] marker, final byte @Nullable [] payload, final long slotKey) {
    if (marker.length != BLOB_MARKER_BYTES || ProjectionIndexRowGroupCodec.getIntLE(marker, 0) != BLOB_MAGIC
        || marker[4] != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int expectedLen = ProjectionIndexRowGroupCodec.getIntLE(marker, 5) & ~BLOB_INLINE_FLAG;
    final long expectedHash = ProjectionIndexRowGroupCodec.getLongLE(marker, 9);
    if (payload == null || payload.length != expectedLen
        || ProjectionIndexColumnSegmentCodec.contentHash(payload) != expectedHash) {
      throw new IllegalStateException("Blob at slot " + slotKey + " failed length/hash verification ("
          + (payload == null
              ? "missing segment"
              : payload.length + " bytes")
          + ", expected " + expectedLen + ")");
    }
    return payload;
  }

  /**
   * Diagnostic: the durable offset key of the segment page referenced for
   * {@code (ownerSlotKey, columnSegmentId)} at the reader's revision, or
   * {@link Constants#NULL_ID_LONG} when absent/unresolved. Equal keys across revisions prove the page
   * was shared by reference (the carry-forward no-op), not rewritten — the observable for containment
   * tests and the P8 update-bytes measurements.
   */
  public static long segmentPageOffset(final StorageEngineReader reader, final int indexNumber, final long ownerSlotKey,
      final int columnSegmentId) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return Constants.NULL_ID_LONG;
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      for (int attempt = 0; attempt <= HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
        final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, ownerSlotKey, keyBuf);
        if (leaf == null) {
          return Constants.NULL_ID_LONG;
        }
        long offset;
        try {
          final PageReference ref = leaf.getPageReference(refKey);
          offset = ref == null
              ? Constants.NULL_ID_LONG
              : ref.getKey();
        } catch (final RuntimeException failure) {
          if (trieReader.validateCurrentLeaf()) {
            throw failure;
          }
          continue;
        }
        if (!trieReader.validateCurrentLeaf()) {
          continue;
        }
        return offset;
      }
      throw HOTTrieReader.stampRetriesExhausted(
          "segmentPageOffset(slot " + ownerSlotKey + ", segment " + columnSegmentId + ")");
    }
  }

  // ==================== segment-slot navigation internals ====================

  /**
   * Shared navigation preamble of the reader-side segment-slot helpers: serialize the slot key into
   * {@code keyBuf} and navigate to the HOT leaf covering it. {@code null} when the trie has no such
   * leaf. The caller owns the {@code trieReader} lifetime (segment resolution reads through the
   * returned leaf's side map while the reader is open).
   */
  private static @Nullable HOTLeafPage navigateToSlotLeaf(final HOTTrieReader trieReader, final PageReference rootRef,
      final long slotKey, final byte[] keyBuf) {
    PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    return trieReader.navigateToLeaf(rootRef, keyBuf);
  }

  /** Writer-side raw slot read: {@code null} when the leaf/slot is absent. */
  private byte @Nullable [] readSlotValueForWrite(final long slotKey) {
    final HOTBulkSlotLoader loader = bulkSlotLoader;
    if (loader != null) {
      // Read-through: while accumulating on a virgin tree, a key is either in the loader or it
      // was never written, so an accumulated payload is authoritative (zero-length = tombstoned,
      // exactly what this method returns for one) and a miss falls through to the empty tree.
      final byte[] accumulated = loader.lastPayload(slotKey);
      if (accumulated != null) {
        return accumulated;
      }
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    final HOTLeafPage leaf = acquireLeafForRead(keyBuf);
    if (leaf == null) {
      return null;
    }
    Throwable guardedFailure = null;
    try {
      final int idx = leaf.findEntry(keyBuf);
      if (idx < 0) {
        return null;
      }
      // getValue() deliberately uses null for both an unreadable slot and a physically present
      // zero-length value. Projection needs to distinguish those states: zero bytes are its tombstone,
      // and write-side presence checks must not reinsert/redelete it. The packed length is authoritative.
      return leaf.copyStoredValue(idx);
    } catch (final RuntimeException | Error failure) {
      guardedFailure = failure;
      throw failure;
    } finally {
      releaseLeafReadGuard(leaf, guardedFailure);
    }
  }

  /**
   * Write a slot value through the standard loud put/update/split machinery. Package-private so
   * corruption and unsupported-format tests can fabricate raw slot values without a production API.
   */
  void writeSlotValue(final long slotKey, final byte[] value) {
    Objects.requireNonNull(value, "value");
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    final HOTBulkSlotLoader loader = bulkSlotLoader;
    if (loader != null) {
      if (loader.tryAdd(slotKey, value)) {
        return;
      }
      // Capacity/contract trip: splice the accumulated prefix (the tree is still virgin — every
      // write since begin was accumulated), then fall through to the per-entry path.
      finalizeBulkSlotAccumulation();
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    // One production mutation driver for every HOT index. AbstractHOTIndexWriter selects
    // projection's opaque last-write-wins semantics at the leaf and through every incremental split;
    // inserts, replacements and zero-length tombstones therefore share the same bounded structural
    // machinery as PATH/CAS/NAME without ever decoding projection bytes as NodeReferences.
    doIndex(keyBuf, keyLen, value, value.length);
  }

  /**
   * Attach an encoded segment as its own CoW-versioned {@link OverflowPage}, referenced from the side
   * map of the HOT leaf that owns slot {@code ownerSlotKey}.
   *
   * <p>
   * Segment-directory storage primitive (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3, introduced
   * with the P1 page-layer machinery): the side-map key is
   * {@code (ownerSlotKey << 16) | columnSegmentId}, matching the owner-slot routing in
   * {@code HOTLeafPage#moveOverflowPageRefsAfterSplit} — the reference lives on whichever page holds
   * the owning slot, across arbitrary split cascades. The page is written (and its durable offset key
   * assigned) inside the commit descent; until then it exists only in-memory on the reference, so a
   * rollback simply never writes it.
   *
   * <p>
   * Re-attaching the same {@code (ownerSlotKey, columnSegmentId)} replaces a resolved reference —
   * whole-segment last-writer-wins. A reference still owned by the append-only bulk batch cannot be
   * replaced: its old payload would still be written but no revision could reach it, permanently
   * stranding space inside an otherwise successful commit. An unchanged segment is shared across
   * revisions by NOT re-attaching it (the carried-forward reference keeps its resolved key).
   */
  public void putSegmentPage(final long ownerSlotKey, final int columnSegmentId, final byte[] bytes) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    if (bulkSlotLoader != null) {
      if (bulkSlotLoader.containsKey(ownerSlotKey) && pendingSideBytes + bytes.length <= BULK_SIDE_PENDING_MAX_BYTES) {
        // The owning slot exists only in the accumulator, so a physical attach is impossible —
        // and splicing here would forfeit the rest of the build's accumulation. Defer instead:
        // retain the payload (the same ownership contract as the immediate OverflowPage attach)
        // and run it through this very method right after the splice, against real leaves.
        try {
          final byte[] replaced = pendingSideAttaches.put(refKey, bytes);
          if (replaced != null) {
            pendingSideBytes -= replaced.length;
          }
          pendingSideBytes += bytes.length;
          return;
        } catch (final RuntimeException | Error failure) {
          poisonMutation(failure);
          throw failure;
        }
      }
      // Owner slot not accumulated (a pre-existing caller-order bug surfaces identically on the
      // per-entry path below) or the side budget is exhausted: materialize the prefix and attach
      // against real pages from here on.
      finalizeBulkSlotAccumulation();
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
    // The owner slot MUST already exist on the leaf: split routing
    // (HOTLeafPage#moveOverflowPageRefsAfterSplit) and read navigation both key off owner-slot
    // residency, so a ref attached without its owning slot would be permanently orphaned on
    // whichever leaf covers the key at attach time — durably committed but unreachable after
    // the next split. Callers write the owning slot (descriptor/chunk) before its segments.
    if (navResult.leaf().findEntry(keyBuf) < 0) {
      throw new IllegalStateException("putSegmentPage: owner slot " + ownerSlotKey + " does not exist (indexNumber="
          + indexNumber + ") — write the owning slot before"
          + " attaching its segments, or the reference cannot follow it across splits.");
    }
    final PageReference previousReference = navResult.leaf().getPageReference(refKey);
    if (previousReference != null && previousReference.hasPendingPageWrite()) {
      throw pendingSidePageMutation("replace", ownerSlotKey, columnSegmentId);
    }
    final PageReference ref = new PageReference();
    ref.setPage(new OverflowPage(bytes));
    try {
      navResult.leaf().setPageReference(refKey, ref);

      // Projection row groups are immutable once emitted. Keeping every referenced segment byte[]
      // reachable through transaction-long pinned HOT leaves made the live set grow with the corpus
      // (~1.9 GiB after 10M ClickBench rows). A bulk build therefore stages only NEW side-map keys in
      // the writer's bounded, single-owner append pipeline. Replacements deliberately stay resident:
      // prewriting a value that is superseded before the root is published would leave a permanent
      // hole inside an otherwise successful revision. Unsupported backends return false and likewise
      // fall back to the ordinary recursive final commit.
      if (stageFreshSidePages && previousReference == null) {
        storageEngineWriter.stageUncommittedOverflowPage(ref);
      }
    } catch (final RuntimeException | Error failure) {
      poisonMutation(failure);
      throw failure;
    }
  }

  /**
   * Remove the segment reference for {@code (ownerSlotKey, columnSegmentId)} after a real delete
   * (shrunk or tombstoned leaf). No-op when absent.
   */
  public void removeSegmentPage(final long ownerSlotKey, final int columnSegmentId) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    if (bulkSlotLoader != null) {
      final byte[] pending = pendingSideAttaches.remove(refKey);
      if (pending != null) {
        // A deferred attach removed before the splice was simply never attached.
        pendingSideBytes -= pending.length;
        return;
      }
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    // Probe read-only first: an unconditional prepareLeafOfTree would CoW the leaf (and its
    // indirect spine) into the TIL — emitting a fragment for an UNCHANGED leaf at commit, and
    // on an empty trie it would even create a spurious root leaf. Only pay the CoW when the
    // reference actually exists.
    final HOTLeafPage probeLeaf = acquireLeafForRead(keyBuf);
    if (probeLeaf == null) {
      return;
    }
    Throwable guardedFailure = null;
    try {
      final PageReference existingReference = probeLeaf.getPageReference(refKey);
      if (existingReference == null) {
        return;
      }
      if (existingReference.hasPendingPageWrite()) {
        throw pendingSidePageMutation("remove", ownerSlotKey, columnSegmentId);
      }
    } catch (final RuntimeException | Error failure) {
      guardedFailure = failure;
      throw failure;
    } finally {
      releaseLeafReadGuard(probeLeaf, guardedFailure);
    }
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, 8);
    navResult.leaf().removePageReference(refKey);
  }

  private IllegalStateException pendingSidePageMutation(final String operation, final long ownerSlotKey,
      final int columnSegmentId) {
    return new IllegalStateException("Cannot " + operation + " pending projection side page (ownerSlotKey="
        + ownerSlotKey + ", columnSegmentId=" + columnSegmentId + ", indexNumber=" + indexNumber
        + "): bulk-staged side keys are append-only until foreground publication; abort the transaction instead");
  }

  /**
   * Fail before an owning slot is rewritten when its side reference still belongs to the append
   * batch.
   */
  private void rejectPendingSidePageMutation(final long ownerSlotKey, final int columnSegmentId,
      final String operation) {
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final HOTLeafPage leaf = acquireLeafForRead(keyBuf);
    if (leaf == null) {
      return;
    }
    Throwable guardedFailure = null;
    try {
      final PageReference reference = leaf.getPageReference(refKey);
      if (reference != null && reference.hasPendingPageWrite()) {
        throw pendingSidePageMutation(operation, ownerSlotKey, columnSegmentId);
      }
    } catch (final RuntimeException | Error failure) {
      guardedFailure = failure;
      throw failure;
    } finally {
      releaseLeafReadGuard(leaf, guardedFailure);
    }
  }

  /**
   * Writer-side segment read: resolve the side-map reference on the leaf owning {@code ownerSlotKey}
   * and materialise the segment bytes (in-memory page for uncommitted segments of this transaction,
   * disk read for committed ones). {@code null} when the leaf, the reference, or the page is absent.
   */
  public byte @Nullable [] getSegmentPageBytes(final long ownerSlotKey, final int columnSegmentId) {
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    if (bulkSlotLoader != null) {
      final byte[] pending = pendingSideAttaches.get(refKey);
      if (pending != null) {
        // Deferred-attach read-through — the same no-mutate contract as the shared backing store.
        return pending;
      }
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final HOTLeafPage leaf = acquireLeafForRead(keyBuf);
    if (leaf == null) {
      return null;
    }
    Throwable guardedFailure = null;
    try {
      final PageReference ref = leaf.getPageReference(refKey);
      if (ref == null) {
        return null;
      }
      final OverflowPage page = storageEngineWriter.readSideOverflowPage(ref);
      // Zero-copy contract: the returned array is the shared page instance's backing store
      // (swizzled onto the reference for every reader of this revision) — callers MUST NOT
      // mutate it.
      return page == null
          ? null
          : page.getDataBytes();
    } catch (final RuntimeException | Error failure) {
      guardedFailure = failure;
      throw failure;
    } finally {
      releaseLeafReadGuard(leaf, guardedFailure);
    }
  }

  /**
   * Reader-side segment read for committed revisions: navigate the queried revision's trie to the
   * leaf owning {@code ownerSlotKey}, resolve the side-map reference, and load the segment page by
   * its offset key. {@code null} when the sub-tree, leaf, or reference is absent.
   */
  public static byte @Nullable [] readSegmentPageBytes(final StorageEngineReader reader, final int indexNumber,
      final long ownerSlotKey, final int columnSegmentId) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      for (int attempt = 0; attempt <= HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
        final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, ownerSlotKey, keyBuf);
        if (leaf == null) {
          return null;
        }
        PageReference ref;
        try {
          ref = leaf.getPageReference(refKey);
        } catch (final RuntimeException failure) {
          if (trieReader.validateCurrentLeaf()) {
            throw failure;
          }
          continue;
        }
        if (!trieReader.validateCurrentLeaf()) {
          continue;
        }
        if (ref == null) {
          return null;
        }
        final OverflowPage page = reader.readSideOverflowPage(ref);
        // Zero-copy contract: shared page backing store — callers MUST NOT mutate.
        return page == null
            ? null
            : page.getDataBytes();
      }
      throw HOTTrieReader.stampRetriesExhausted(
          "readSegmentPageBytes(slot " + ownerSlotKey + ", segment " + columnSegmentId + ")");
    }
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(final byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }

  @Override
  protected int serializeKey(final Long key, final byte[] buffer, final int offset) {
    // Key passed here is the already-encoded composite key — unused
    // AbstractHOTIndexWriter hook. Required by the abstract parent for
    // the generic {@code index(K, ...)} path we don't expose.
    return keySerializer.serialize(key, buffer, offset);
  }

  /**
   * Root reference of the projection sub-tree for {@code indexNumber} under the given reader's
   * current revision, or {@code null} if no index is installed.
   */
  public static @Nullable PageReference rootReference(final StorageEngineReader reader, final int indexNumber) {
    final RevisionRootPage rrp = reader.getActualRevisionRootPage();
    final ProjectionIndexPage projPage = reader.getProjectionIndexPage(rrp);
    if (projPage == null)
      return null;
    final PageReference ref = projPage.getIndexReference(indexNumber);
    if (ref == null)
      return null;
    if (ref.getKey() == Constants.NULL_ID_LONG && ref.getLogKey() == Constants.NULL_ID_INT && ref.getPage() == null) {
      return null;
    }
    return ref;
  }

}
