/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Constants;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * HOT-backed persistent storage for projection-index leaf payloads in the
 * <b>segment-slot layout</b> (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md
 * §2.3, §3, §4).
 *
 * <h2>Storage contract</h2>
 *
 * <ul>
 *   <li><b>Slot keys</b> — every segment gets its OWN HOT slot under a composite key
 *       {@code (rowGroupId << 16) | slotKind}, serialized by {@code PathKeySerializer}
 *       (sign-flipped 8-byte BE, so unsigned byte comparison preserves signed-long order).
 *       {@code slotKind 0} is the row group's zone-map descriptor, {@code slotKind
 *       columnSegmentId + 1} that segment's bytes — so one row group's slots are
 *       key-adjacent and a range scan reads them in one descent. Live descriptor slots are
 *       contiguous from 1 (invariant 5.1-11); {@link #readAllRowGroupsFromColumnSegmentSlots}
 *       enforces this and fails loudly on gaps, because positional consumers (the catalog
 *       matches leaves to metadata fences by position) would silently mislabel every
 *       following leaf.</li>
 *   <li><b>Descriptor slot value = RowGroupDescriptor (PIXD)</b> — a zone-map-only directory
 *       of the leaf's semantic segments (KEYS, per-column BODY/DICT), each entry carrying
 *       columnSegmentId, byteLen and an XXH3-64 content hash. It holds no segment bytes: the
 *       hashes are what a later assembly verifies its slots against.</li>
 *   <li><b>Segment slot value</b> — BARE (no marker, no second hash: the descriptor entry
 *       already carries byteLen + hash). Payloads no larger than {@link #BLOB_INLINE_MAX} sit inline
 *       behind a discriminator byte; larger ones become a lone reference byte plus one
 *       CoW-versioned {@link OverflowPage}, hung off the owning HOT leaf's side map under
 *       {@code (slotKey << 16) | 0} — references follow their owning slot across arbitrary
 *       split cascades.</li>
 *   <li><b>Blob slots (PIXB)</b> — opaque payloads (the PIXM metadata bytes, the fence chunks)
 *       stored via {@link #putBlob}: the value is a small marker with byteLen + hash, the
 *       payload is inline or one page, and reads are length/hash-verified
 *       ({@link #verifyBlob}).</li>
 *   <li><b>Assembly</b> — {@link #getRowGroupFromColumnSegmentSlots} /
 *       {@link #readRowGroupFromColumnSegmentSlots} /
 *       {@link #readAllRowGroupsFromColumnSegmentSlots} reassemble the raw leaf form from the
 *       descriptor's segment slots; {@code ProjectionIndexColumnSegmentCodec} verifies each
 *       segment's hash so torn or mixed-layout stores fail loudly instead of misparsing.</li>
 *   <li><b>Tombstone vs live-empty</b> — a zero-length slot value is a
 *       tombstone (absent leaf, skipped by enumeration); a live EMPTY leaf is
 *       a descriptor whose segments encode zero rows and still round-trips.
 *       An unchanged segment is carried forward by reference (equal byteLen +
 *       hash → no write at all), which is the SLIDING_SNAPSHOT containment
 *       no-op asserted on durable offsets by {@link #segmentPageOffset}.</li>
 * </ul>
 *
 * <h2>Historical failure families (regression-guarded)</h2>
 *
 * Two pre-redesign bug families remain guarded by tests: <b>grow-overwrite</b>
 * (larger re-puts silently dropped values that no longer fit — all writes now
 * funnel through the loud update-or-split path) and <b>stale-swizzle
 * use-after-close</b> (CoW'd references resolving a closed {@link HOTLeafPage}
 * — {@link PageReference#getPage()} treats a closed leaf as a cache miss).
 * See {@code ProjectionPersistForceRebuildTest} (sirix-query).
 */
public final class ProjectionIndexHOTStorage extends AbstractHOTIndexWriter<Long> {

  private static final LogWrapper LOGGER =
      new LogWrapper(LoggerFactory.getLogger(ProjectionIndexHOTStorage.class));

  /** Zero-length slot value marking a tombstoned slot (HOT has no per-entry delete). */
  private static final byte[] TOMBSTONE = new byte[0];

  /** 8-byte scratch for encoding slot keys. */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[8]);

  private final PathKeySerializer keySerializer = PathKeySerializer.INSTANCE;

  public ProjectionIndexHOTStorage(final StorageEngineWriter storageEngineWriter, final int indexNumber) {
    super(storageEngineWriter, IndexType.PROJECTION, indexNumber);
    initializeProjectionIndex();
  }

  private void initializeProjectionIndex() {
    final ProjectionIndexPage projPage = prepareWritableProjectionIndexPage();
    final PageReference existingRef = projPage.getOrCreateReference(indexNumber);
    final boolean exists = existingRef != null
        && (existingRef.getKey() != Constants.NULL_ID_LONG
            || existingRef.getLogKey() != Constants.NULL_ID_INT
            || existingRef.getPage() != null);
    if (!exists) {
      projPage.createProjectionIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
    }
    rootReference = projPage.getOrCreateReference(indexNumber);
  }

  /** The writer's private CoW copy of the projection container page (task #57 discipline). */
  private ProjectionIndexPage prepareWritableProjectionIndexPage() {
    final RevisionRootPage revisionRootPage = storageEngineWriter.getActualRevisionRootPage();
    final PageReference projPageRef = revisionRootPage.getProjectionIndexPageReference();

    final PageContainer projContainer = storageEngineWriter.getLog().get(projPageRef);
    if (projContainer != null && projContainer.getModified() instanceof ProjectionIndexPage modifiedProj) {
      return modifiedProj;
    }
    // Top-down CoW (task #57): the writer must mutate a private deep-copy. Without this the
    // cached prior-revision instance shares the reference array (and the rootRef slot) with
    // the historical revisions, so write-side mutations bleed into historical reads.
    final ProjectionIndexPage cached = storageEngineWriter.getProjectionIndexPage(revisionRootPage);
    final ProjectionIndexPage projPage = new ProjectionIndexPage(cached);
    storageEngineWriter.appendLogRecord(projPageRef, PageContainer.getInstance(cached, projPage));
    return projPage;
  }

  /**
   * Discard this definition's ENTIRE sub-tree and start a fresh empty one — the v1→v2
   * migration primitive: a rebuild over a pre-descriptor (chunked) store must not inherit its
   * composite chunk slots, which would poison descriptor enumeration with mixed-layout errors
   * forever. Earlier revisions keep their own sub-tree (CoW); the current transaction
   * continues on the fresh root.
   */
  public void resetTree() {
    final ProjectionIndexPage projPage = prepareWritableProjectionIndexPage();
    projPage.resetProjectionIndexTree(storageEngineWriter, indexNumber, storageEngineWriter.getLog());
    rootReference = projPage.getOrCreateReference(indexNumber);
  }

  /**
   * Count live descriptor slots by upward probe (slots are contiguous from 1 — invariant
   * 5.1-11). The recovery source for prior leaf counts when metadata is a stale tombstone or
   * unreadable: rebuilds must tombstone orphans above the new count even when the tombstoned
   * metadata no longer carries the old count.
   *
   * <p>Probes the segment-slot key space: a row group's descriptor lives at
   * {@code rowGroupId << 16} (slotKind 0). This is the count that decides orphan tombstoning.</p>
   */
  public int probeLiveRowGroupCount() {
    return probeLiveRowGroupCountFrom(0);
  }

  /**
   * Whether the sub-tree holds a row group at a RAW slot id — the shape the retired descriptor
   * layout wrote and the one a segment-slot store never writes. The witness a rebuild uses to tell
   * "no metadata, empty sub-tree" from "no metadata, but a pre-retirement store underneath".
   */
  public boolean hasRawKeyedRowGroup() {
    return readSlotValueForWrite(1L) != null;
  }

  /**
   * {@link #probeLiveRowGroupCount()} that trusts the first {@code knownLiveCount} row groups
   * and only probes UPWARD from there, returning the true contiguous live count.
   *
   * <p>Recovers the count after a rebuild that follows a PARTIALLY-APPLIED incremental patch: the
   * patch may have written fresh row groups past the declared count and then failed before it could
   * update slot 0, leaving live row groups the stale metadata does not know about. Rebuilding
   * against the declared count alone would leave those above the new count untombstoned, and a
   * segment-slot store rejects such an orphan on every later full read (permanently unusable).
   * Costs one extra slot read in the common case where nothing is above the declared count.</p>
   *
   * @param knownLiveCount row groups already known live (the metadata's declared count)
   */
  public int probeLiveRowGroupCountFrom(final int knownLiveCount) {
    if (knownLiveCount < 0) {
      throw new IllegalArgumentException("knownLiveCount must be >= 0, got " + knownLiveCount);
    }
    int count = knownLiveCount;
    for (long slot = knownLiveCount + 1L; slot <= MAX_PROBED_LEAVES; slot++) {
      final byte[] value =
          readSlotValueForWrite(rowGroupDescriptorSlotKey(slot));
      if (value == null || value.length == 0) {
        return count;
      }
      count++;
    }
    throw new IllegalStateException("More than " + MAX_PROBED_LEAVES
        + " contiguous projection leaves — implausible store, refusing to probe further");
  }

  /** Safety bound for {@link #probeLiveRowGroupCount} (16M leaves ≈ 16G rows — far beyond scale). */
  private static final int MAX_PROBED_LEAVES = 1 << 24;

  /**
   * Shared slow path for slot writes: place {@code sized} under
   * {@code keyBuf} when the in-place fast paths failed.
   *
   * <p>For an existing entry ({@code idx >= 0}) the grown value is first
   * retried as a copying update (the page may just be fragmented —
   * {@link HOTLeafPage#updateValue} compacts internally). If the page
   * genuinely has no room, the leaf is SPLIT via the standard trie-writer
   * machinery, which for {@link IndexType#PROJECTION} leaves replaces the
   * existing slot value in the receiving half (CoW-versioned like any other
   * split). A brand-new key on a full page takes the split immediately.
   *
   * <p>ONE split is not always enough. The split partitions on the leaf's most significant
   * discriminative bit, and that bit can belong to a single outlier key — the fence chunks sit at
   * {@code 2^42}, far above every row-group slot, so a leaf holding slot 0 through the fences
   * splits into "everything" and "the fences" and frees nothing. The split still STANDS
   * ({@link HOTLeafPage#SPLIT_WITHOUT_INSERT}); the caller re-navigates and splits again, and the
   * next split partitions on the remaining keys' own MSDB. Each round strictly shrinks the leaf
   * the key routes to, so the cascade terminates — {@link #MAX_SPLIT_CASCADE} bounds it anyway,
   * and exhausting it is loud.
   *
   * <p>Both failure modes are loud: silently dropping a slot write would
   * leave the previous revision's bytes in the slot and corrupt the logical
   * leaf on read. A split that cannot partition at all leaves the page in its pre-split state
   * (atomic rollback), so the thrown exception is a clean abort.
   *
   * @return {@code true} iff {@code sized} is now stored — either updated in place or placed by
   *         the split. {@code false} means the tree changed but the value is NOT stored: the
   *         caller must re-navigate (the leaf is a different page now) and try again.
   */
  private boolean updateOrSplitInsert(final HOTLeafPage currentLeaf,
      final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final int idx, final byte[] sized) {
    if (idx >= 0 && currentLeaf.updateValue(idx, sized)) {
      return true;
    }
    final int outcome = trieWriter.handleLeafSplitAndInsert(
        storageEngineWriter, storageEngineWriter.getLog(), currentLeaf, navResult.leafRef(),
        rootReference, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
        navResult.pathDepth(), keyBuf, keyLen, sized, sized.length);
    prepareIndexPage();
    if (outcome == HOTLeafPage.SPLIT_ABORTED) {
      final long rawKey = PathKeySerializer.INSTANCE.deserialize(keyBuf, 0, keyLen);
      throw new SirixIOException("Projection HOT slot " + (idx >= 0 ? "update" : "insert")
          + " failed after split for key=" + rawKey + " (" + sized.length
          + " bytes, indexNumber=" + indexNumber + ")");
    }
    return outcome == HOTLeafPage.SPLIT_WITH_INSERT;
  }

  /**
   * Bound on the split cascade one slot write may drive. A split at least halves the leaf the key
   * routes to in the balanced case and peels off at least one entry in the degenerate one, so
   * {@link HOTLeafPage#MAX_ENTRIES} rounds is an upper bound no healthy store approaches; this is
   * a runaway backstop, not a tuning knob.
   */
  private static final int MAX_SPLIT_CASCADE = 64;

  // ==================== descriptor-based layout (segment directory) ====================
  //
  // Slot key = PathKeySerializer(rowGroupId); slot value = RowGroupDescriptor (PIXD) or a
  // zero-length tombstone; segment bytes live in OverflowPages referenced from the
  // HOT leaf's side map under (rowGroupId << 16 | columnSegmentId).

  /** Blob marker magic for slot values that reference one opaque segment ("PIXB" LE). */
  private static final int BLOB_MAGIC = 0x42584950;
  private static final byte BLOB_VERSION = 1;
  private static final int BLOB_MARKER_BYTES = 4 + 1 + 4 + 8;
  private static final int BLOB_SEGMENT_ID = 0;

  /**
   * High bit of a blob marker's length field marking the payload as INLINE (bytes in the slot
   * value's trailing region, right after the marker) rather than REFERENCED (bytes in a side-map
   * {@link OverflowPage}) — the blob-slot analogue of {@link RowGroupDescriptor#SEG_INLINE_FLAG}.
   * A blob is capped at {@link RowGroupDescriptor#MAX_SEGMENT_BYTES} (16 MB ≪ 2^31) so the true
   * length never touches the sign bit.
   */
  private static final int BLOB_INLINE_FLAG = 0x8000_0000;

  /**
   * Write-side threshold: a payload of at most this many bytes is stored inline in the slot value
   * (no page, no random read to resolve it), larger ones spill to an {@link OverflowPage} as
   * before. The reader keys off the stored {@link #BLOB_INLINE_FLAG} alone, so this bound can
   * change without breaking already-written blobs. Sized to inline the small PIXM shape metadata
   * (slot 0, a few hundred bytes). A <em>full</em> 8 KiB fence chunk stays referenced — inlining
   * that would bloat the HOT leaf pages that hold the fence slots; a small partial tail chunk
   * (≤ 32 leaves = ≤ 512 B) does inline, which is harmless and even saves it a page.
   */
  private static final int BLOB_INLINE_MAX = 512;

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

  /** The lone slot value of a REFERENCED bare segment (bytes live in the page). Immutable + shared —
   * {@code writeSlotValue} copies it into the slot heap, so one instance serves every referenced write. */
  private static final byte[] SEG_REF_VALUE = {SEG_KIND_REF};

  // ==================================================================================
  // Segment ⇔ slot layout — one HOT slot per segment. The only storage layout there is.
  //
  // Composite slot key = (rowGroupId << 16) | slotKind:
  //   slotKind 0        → the zone-map DESCRIPTOR (rowCount, fences, kinds, per-seg entry array)
  //   slotKind columnSegmentId+1  → segment `columnSegmentId`'s bytes
  // The DESCRIPTOR slot is a hashed blob (putBlob container) — nothing else backs its integrity.
  // SEGMENT slots are BARE (putColumnSegmentSlot): a 1-byte inline/referenced discriminator plus either the
  // raw bytes (inline) or a side-map OverflowPage (referenced), and NO magic/version/hash — the
  // segment's byteLen + XXH3 hash live in the descriptor entry and are re-checked by verifyColumnSegment, so
  // a second on-disk hash would be pure redundancy. A range scan groups slots by the high (rowGroupId)
  // bits; countRows and zone-map pruning read slotKind 0 alone; an aggregate over column c reads only
  // that column's segment slots and skips the rest. See scratchpad SEGMENT_SLOT_DESIGN.md.
  // ==================================================================================

  /** slotKind 0 — the zone-map descriptor slot for {@code rowGroupId}. */
  static long rowGroupDescriptorSlotKey(final long rowGroupId) {
    return rowGroupId << 16;
  }

  /** slotKind {@code columnSegmentId+1} — the slot holding segment {@code columnSegmentId}'s bytes for {@code rowGroupId}. */
  static long columnSegmentSlotKey(final long rowGroupId, final int columnSegmentId) {
    // columnSegmentId+1 must fit the 16-bit slotKind; columnSegmentId==0xFFFF would alias the NEXT leaf's descriptor slot.
    if (columnSegmentId < 0 || columnSegmentId >= HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID) {
      throw new IllegalArgumentException(
          "columnSegmentId out of range for segment-slot key (columnSegmentId+1 must fit the 16-bit slotKind): " + columnSegmentId);
    }
    return (rowGroupId << 16) | (columnSegmentId + 1);
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
  public void putRowGroupAsColumnSegmentSlots(final long rowGroupId,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    if (rowGroupId < 1) {
      throw new IllegalArgumentException(
          "rowGroupId must be >= 1 (slot 0 is the metadata blob): " + rowGroupId);
    }
    if (encoded == null) {
      throw new IllegalArgumentException("encoded leaf must not be null — use tombstoneRowGroupAsColumnSegmentSlots");
    }
    // Zone-map-only descriptor: strip any inline region so every segment lives ONLY in its slot,
    // never doubly in the descriptor (F1) — assembleRaw then resolves EVERY segment through its slot.
    final byte[] descriptor = RowGroupDescriptor.toZoneMapOnly(encoded.descriptor());
    final int[] columnSegmentIds = encoded.columnSegmentIds();
    final byte[][] segments = encoded.segments();

    // Diff prior vs new columnSegmentId set so a shrunk leaf (dropped DICT, fewer columns) tombstones the
    // segment slots that vanished — read the prior descriptor BEFORE overwriting it.
    final byte[] prior = getBlobIfReadable(rowGroupDescriptorSlotKey(rowGroupId));
    final boolean priorIsDescriptor = prior != null && RowGroupDescriptor.isDescriptor(prior);

    // Descriptor FIRST so the row group's leading slot is never headless.
    putBlob(rowGroupDescriptorSlotKey(rowGroupId), descriptor);
    writeChangedColumnSegmentSlots(rowGroupId, descriptor, columnSegmentIds, segments,
        priorIsDescriptor ? prior : null);
    if (priorIsDescriptor) {
      tombstoneVanishedColumnSegmentSlots(rowGroupId, descriptor, prior);
    }
  }

  /**
   * Write each segment whose bytes actually changed. An unchanged segment (same byteLen +
   * contentHash as the prior descriptor's entry) needs no write at all — its bare slot value and
   * page survive the leaf's CoW copy, which is the §6.3 sharing this layout exists to preserve.
   *
   * <p>Entry {@code i} of {@code descriptor} describes {@code segments[i]}, and both id lists
   * ascend, so one monotonic cursor over the prior entries makes the per-segment carry-forward test
   * O(1) amortized rather than a binary search — and the compare needs no second hash pass over the
   * bytes.
   *
   * @param prior the prior descriptor, or {@code null} when there was none (every segment is new)
   */
  private void writeChangedColumnSegmentSlots(final long rowGroupId, final byte[] descriptor,
      final int[] columnSegmentIds, final byte[][] segments, final byte @Nullable [] prior) {
    final int priorSegCount = prior == null ? 0 : RowGroupDescriptor.columnSegmentCount(prior);
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
            && RowGroupDescriptor.entryContentHash(prior, priorCursor)
                == RowGroupDescriptor.entryContentHash(descriptor, i)) {
          continue;
        }
      }
      putColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, columnSegmentId), segments[i]);
    }
  }

  /**
   * Tombstone the segment slots whose id vanished from the new descriptor — a shrunk row group
   * (dropped DICT, fewer columns) would otherwise leave them readable but unreferenced. Both id
   * lists ascend, so a second monotonic cursor over the NEW descriptor replaces a per-prior lookup.
   */
  private void tombstoneVanishedColumnSegmentSlots(final long rowGroupId, final byte[] descriptor,
      final byte[] prior) {
    final int priorSegCount = RowGroupDescriptor.columnSegmentCount(prior);
    final int newSegCount = RowGroupDescriptor.columnSegmentCount(descriptor);
    int newCursor = 0;
    for (int i = 0; i < priorSegCount; i++) {
      final int priorSegId = RowGroupDescriptor.entryColumnSegmentId(prior, i);
      while (newCursor < newSegCount
          && RowGroupDescriptor.entryColumnSegmentId(descriptor, newCursor) < priorSegId) {
        newCursor++;
      }
      final boolean present = newCursor < newSegCount
          && RowGroupDescriptor.entryColumnSegmentId(descriptor, newCursor) == priorSegId;
      if (!present) {
        tombstoneBlobSlot(columnSegmentSlotKey(rowGroupId, priorSegId));
      }
    }
  }

  /**
   * Write one segment's bytes into its own BARE slot — no blob marker/hash. Small payloads
   * (≤ {@link #BLOB_INLINE_MAX}) go inline (leading {@link #SEG_KIND_INLINE} byte + raw bytes, no
   * page); larger ones go referenced (a lone {@link #SEG_KIND_REF} byte in the slot value + one
   * {@link OverflowPage}). The slot value is written BEFORE the page ({@link #putSegmentPage}'s
   * owner-slot-residency precondition); a referenced→inline shrink drops the now-orphan page, an
   * inline→referenced growth's prior (page-less) slot just gets overwritten.
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
      writeSlotValue(slotKey, value);
      removeSegmentPage(slotKey, BLOB_SEGMENT_ID); // no-op unless the prior segment was referenced
    } else {
      writeSlotValue(slotKey, SEG_REF_VALUE);
      putSegmentPage(slotKey, BLOB_SEGMENT_ID, bytes);
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
    throw new IllegalStateException("segment slot " + slotKey + " has an unknown discriminator "
        + kind + " — not a bare segment slot");
  }

  /** Writer-side (same-transaction) bare segment read; {@code null} when absent/tombstoned. */
  private byte @Nullable [] getColumnSegmentSlot(final long slotKey) {
    final byte[] value = readSlotValueForWrite(slotKey);
    if (value == null || value.length == 0) {
      return null;
    }
    final byte[] inline = inlineColumnSegmentPayload(value, slotKey);
    return inline != null ? inline : getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID);
  }

  /** Reader-side (committed) bare segment read; {@code null} when absent/tombstoned. */
  static byte @Nullable [] readColumnSegmentSlot(final StorageEngineReader reader,
      final int indexNumber, final long slotKey) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, slotKey, keyBuf);
      if (leaf == null) {
        return null;
      }
      final int idx = leaf.findEntry(keyBuf);
      if (idx < 0) {
        return null;
      }
      final byte[] value = leaf.getValue(idx);
      if (value == null || value.length == 0) {
        return null;
      }
      final byte[] inline = inlineColumnSegmentPayload(value, slotKey);
      if (inline != null) {
        return inline;
      }
      final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
      if (ref == null) {
        return null;
      }
      final OverflowPage page = reader.readSideOverflowPage(ref);
      return page == null ? null : page.getDataBytes();
    }
  }

  /**
   * Tombstone a whole row group in the segment-slot layout: drop every segment slot named by the
   * descriptor, then the descriptor slot itself.
   */
  public void tombstoneRowGroupAsColumnSegmentSlots(final long rowGroupId) {
    if (rowGroupId < 1) {
      throw new IllegalArgumentException(
          "rowGroupId must be >= 1 (slot 0 is the metadata blob): " + rowGroupId);
    }
    final byte[] descriptor = getBlobIfReadable(rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor != null && RowGroupDescriptor.isDescriptor(descriptor)) {
      final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
      for (int i = 0; i < columnSegmentCount; i++) {
        tombstoneBlobSlot(columnSegmentSlotKey(rowGroupId, RowGroupDescriptor.entryColumnSegmentId(descriptor, i)));
      }
    }
    tombstoneBlobSlot(rowGroupDescriptorSlotKey(rowGroupId));
  }

  /** Remove a blob slot: drop its (possible) referenced page, then zero the slot value. */
  private void tombstoneBlobSlot(final long slotKey) {
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior == null) {
      return;
    }
    removeSegmentPage(slotKey, BLOB_SEGMENT_ID); // no-op when the blob was inline (no page)
    if (prior.length > 0) {
      writeSlotValue(slotKey, TOMBSTONE);
    }
  }

  /**
   * Reader-side (committed) assembly of a leaf from its segment slots — byte-identical to the raw
   * scan form. {@code null} when the descriptor slot is absent or tombstoned.
   */
  public static byte @Nullable [] readRowGroupFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final long rowGroupId) {
    final byte[] descriptor = readBlob(reader, indexNumber, rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null || !RowGroupDescriptor.isDescriptor(descriptor)) {
      return null;
    }
    return ProjectionIndexColumnSegmentCodec.assembleRaw(descriptor,
        columnSegmentId -> readColumnSegmentSlot(reader, indexNumber, columnSegmentSlotKey(rowGroupId, columnSegmentId)));
  }

  /** Writer-side (same-transaction) assembly from segment slots; {@code null} if absent. */
  public byte @Nullable [] getRowGroupFromColumnSegmentSlots(final long rowGroupId) {
    final byte[] descriptor = getBlobIfReadable(rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null || !RowGroupDescriptor.isDescriptor(descriptor)) {
      return null;
    }
    return ProjectionIndexColumnSegmentCodec.assembleRaw(descriptor,
        columnSegmentId -> getColumnSegmentSlot(columnSegmentSlotKey(rowGroupId, columnSegmentId)));
  }

  /**
   * Descriptor-only row count for the segment-slot layout: reads slotKind 0 alone, touching no
   * segment slots. {@code -1} when the descriptor is absent — the count/pruning path never pays for
   * segment I/O.
   */
  public static long readRowCountFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final long rowGroupId) {
    final byte[] descriptor = readBlob(reader, indexNumber, rowGroupDescriptorSlotKey(rowGroupId));
    if (descriptor == null || !RowGroupDescriptor.isDescriptor(descriptor)) {
      return -1L;
    }
    // isDescriptor only guarantees the 4-byte magic; rowCount reads a 4-byte field at offset 5.
    // A truncated-but-magic descriptor must raise the contracted IllegalStateException (caught and
    // negative-cached by the count tier), never an AIOOBE that slips past that guard.
    if (descriptor.length < RowGroupDescriptor.MIN_BYTES) {
      throw new IllegalStateException("segment-slot descriptor for leaf " + rowGroupId
          + " truncated: " + descriptor.length + " < " + RowGroupDescriptor.MIN_BYTES
          + " bytes (indexNumber=" + indexNumber + ")");
    }
    return RowGroupDescriptor.rowCount(descriptor);
  }

  /**
   * One live leaf's assembly state: its zone-map descriptor plus a columnSegmentId→payload table sized exactly
   * from the descriptor's entry count. Positions are filled in the post-walk resolution pass.
   */
  private static final class ColumnSegmentSlotRowGroupAccum {
    private byte[] descriptor;
    private int[] columnSegmentIds;
    private byte[][] payloads;
  }

  /**
   * A slot captured during the walk. Its payload source is exactly one of: {@code inlineValue}
   * (a DESCRIPTOR's whole blob value, verified on resolution; or a SEGMENT's already-stripped raw
   * payload), {@code resolved} (an uncommitted/swizzled ref read in-walk — the descriptor verified,
   * a segment raw), or {@code marker}+{@code offset} (a committed reference batch-resolved after the
   * walk; {@code marker} is non-null only for descriptors, which still hash-verify).
   */
  private record RawBlobSlot(long rowGroupId, int slotKind, byte[] inlineValue, byte[] resolved,
      byte[] marker, long offset, long slotKey) {
  }

  /**
   * A referenced SEGMENT awaiting one batched page read. No marker: a segment's integrity is the
   * descriptor entry's byteLen + contentHash, re-checked by {@code verifyColumnSegment} at assembly.
   */
  private record PendingSegRef(byte[][] target, int idx, long offset, long slotKey) {
  }

  /**
   * Reader-side enumeration of ALL leaves in the segment-slot layout, ascending rowGroupId
   * {@code 1..rowGroupCount}, each assembled byte-identically to {@link #readRowGroupFromColumnSegmentSlots}.
   *
   * <p>HFT read path (P3, replacing the per-slot point-read version): ONE trie range scan captures
   * every blob slot (vs {@code O(rowGroupCount×segments)} independent {@link #readBlob} root-to-leaf
   * descents before); referenced blobs are then resolved by TWO coalesced {@link #readSegmentBytesBatch}
   * calls (descriptors, then segments), and assembly fans out across the common pool for large stores.
   *
   * <p>The walk makes NO assumption about slot VISIT order — like {@link #readAllRowGroups} (whose
   * topology order "can diverge from key order after splits") it collects everything first and
   * resolves positions afterward, so a leaf's segment slot seen before its descriptor is fine.
   * Loud-on-gap and loud-on-orphan both fall out of validating the descriptor key set is exactly
   * {@code {1..rowGroupCount}} — checked before the committed segment BATCH — and a segment for a
   * non-existent leaf, or with no matching descriptor entry, throws unambiguously. (An uncommitted
   * blob is read in-walk, so its page read precedes validation, but still throws just as loudly.)
   *
   * <p>Serves both committed and uncommitted (writer, this-transaction) reads, like the descriptor
   * path's {@link #readAllRowGroups}: a referenced blob whose durable offset is not yet resolved (a
   * swizzled, unflushed page) is read in-walk through its live reference, while committed references
   * take the coalesced batch — so a same-transaction build-then-query still serves from the store.
   */
  /**
   * ONE trie range scan over a projection sub-tree, returning the row groups' DESCRIPTOR slots
   * positionally ({@code [i]} describes rowGroupId {@code i + 1}) with the key set already
   * validated as exactly {@code {1..rowGroupCount}}.
   *
   * <p>This is the shared front half of every full read. Doing it as a range scan rather than
   * {@code rowGroupCount} independent {@link #readBlob} descents is the whole point: a descent is
   * a root-to-leaf walk plus a fresh {@link HOTTrieReader}, so the point-read shape costs
   * {@code O(rowGroupCount × height)} page touches where the scan costs one pass.
   *
   * <p>Slots are captured unresolved — a referenced blob keeps only its 17-byte marker and durable
   * offset — so the caller can resolve them in ONE coalesced batch instead of a random page read
   * each. Nothing here reads a segment page.
   *
   * <p>The walk makes NO assumption about visit order (topology order can diverge from key order
   * after splits): everything is collected first and positions resolved afterwards.
   *
   * @param segmentSlotsOut collects the row groups' SEGMENT slots (slotKind >= 1) for a caller that
   *        goes on to assemble; {@code null} skips capturing them entirely, which is what makes a
   *        descriptor-tier count cheap
   */
  private static RawBlobSlot[] orderedDescriptorSlots(final StorageEngineReader reader,
      final int indexNumber, final int rowGroupCount,
      final @Nullable ArrayList<RawBlobSlot> segmentSlotsOut) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      if (rowGroupCount != 0) {
        throw new IllegalStateException("segment-slot sub-tree missing but metadata declares "
            + rowGroupCount + " leaves (indexNumber=" + indexNumber + ")");
      }
      return NO_DESCRIPTOR_SLOTS;
    }
    final byte[] minKey = new byte[8];
    final byte[] maxKey = new byte[8];
    Arrays.fill(maxKey, (byte) 0xFF);
    final Long2ObjectRBTreeMap<RawBlobSlot> descriptors = new Long2ObjectRBTreeMap<>();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIdx = cursor.currentEntryIndex();
        final long slotKey = leaf.decodeKey8BE(entryIdx) ^ 0x8000_0000_0000_0000L;
        final long rowGroupId = slotKey >>> 16;
        final MemorySegment valueSlice = cursor.currentValueSlice();
        final int valueSize = valueSlice == null ? 0 : (int) valueSlice.byteSize();
        // The leaf slots must be told apart from the two other blob families by KEY: the metadata
        // (PIXM) blob at slot 0, and the fence chunk blobs at slotKey >= CHUNK_SLOT_BASE (2^42, far
        // above any rowGroupId<<16). Both are skipped; tombstones are zero-length. The DESCRIPTOR slot
        // (slotKind 0) is a hashed blob; SEGMENT slots (slotKind >= 1) are BARE (discriminator byte).
        if (valueSize == 0 || rowGroupId == 0 || slotKey >= ProjectionIndexFences.CHUNK_SLOT_BASE) {
          cursor.advance();
          continue;
        }
        final int slotKind = (int) (slotKey & 0xFFFF);
        if (slotKind == 0) {
          final RawBlobSlot desc =
              captureDescriptorSlot(reader, leaf, valueSlice, valueSize, rowGroupId, slotKey, indexNumber);
          if (descriptors.put(rowGroupId, desc) != null) {
            throw new IllegalStateException("segment-slot leaf " + rowGroupId
                + " has two descriptor slots (indexNumber=" + indexNumber + ")");
          }
        } else if (segmentSlotsOut != null) {
          segmentSlotsOut.add(captureColumnSegmentSlot(reader, leaf, valueSlice, valueSize, rowGroupId,
              slotKind, slotKey, indexNumber));
        }
        cursor.advance();
      }
    }
    // Validate the descriptor key set is exactly {1..rowGroupCount} BEFORE any page I/O: a size
    // mismatch is a gap/truncation/leaked-orphan, a non-1..N key is a contiguity break.
    if (descriptors.size() != rowGroupCount) {
      throw new IllegalStateException("segment-slot store has " + descriptors.size()
          + " live descriptors but metadata declares " + rowGroupCount + " (indexNumber=" + indexNumber
          + ") — truncated, stale, or leaked orphan");
    }
    // Drain the (key-ordered) descriptor map into a positional array while validating contiguity —
    // one pass, no per-leaf tree probes downstream. descArr[i] is the descriptor for rowGroupId i+1.
    final RawBlobSlot[] descArr = new RawBlobSlot[rowGroupCount];
    long expected = 1;
    for (final Long2ObjectMap.Entry<RawBlobSlot> e : descriptors.long2ObjectEntrySet()) {
      final long slot = e.getLongKey();
      if (slot != expected) {
        throw new IllegalStateException("segment-slot leaves are not contiguous: expected leaf "
            + expected + ", found " + slot + " (indexNumber=" + indexNumber + ")");
      }
      descArr[(int) (expected - 1)] = e.getValue();
      expected++;
    }
    return descArr;
  }

  private static final RawBlobSlot[] NO_DESCRIPTOR_SLOTS = new RawBlobSlot[0];

  public static List<byte[]> readAllRowGroupsFromColumnSegmentSlots(final StorageEngineReader reader,
      final int indexNumber, final int rowGroupCount) {
    final ArrayList<RawBlobSlot> segmentSlots = new ArrayList<>();
    // Phases 1-2 — one walk, then validate the descriptor key set is exactly {1..rowGroupCount}.
    final RawBlobSlot[] descArr =
        orderedDescriptorSlots(reader, indexNumber, rowGroupCount, segmentSlots);
    // Phase 3 — resolve descriptors (referenced ones in one batch), then size each leaf's accum. The
    // ordered array is indexed by rowGroupId-1 (keys were just validated contiguous 1..rowGroupCount).
    final ColumnSegmentSlotRowGroupAccum[] ordered = new ColumnSegmentSlotRowGroupAccum[rowGroupCount];
    resolveDescriptors(reader, descArr, ordered);
    // Phase 4 — resolve segment positions (order-agnostic) and fill; referenced ones in one batch.
    final ArrayList<PendingSegRef> pendingSeg = new ArrayList<>();
    for (final RawBlobSlot s : segmentSlots) {
      // A segment naming a leaf past rowGroupCount is a leaked orphan (rowGroupId is an unsigned >>>16 of a
      // non-zero, sub-CHUNK_SLOT_BASE key, so it is always >= 1). Caught before this segment's I/O.
      if (s.rowGroupId() > rowGroupCount) {
        throw new IllegalStateException("segment-slot segment slot " + s.slotKey() + " names leaf "
            + s.rowGroupId() + " beyond rowGroupCount " + rowGroupCount + " (leaked orphan, indexNumber="
            + indexNumber + ")");
      }
      final ColumnSegmentSlotRowGroupAccum accum = ordered[(int) (s.rowGroupId() - 1)];
      final int columnSegmentId = s.slotKind() - 1;
      final int pos = indexOf(accum.columnSegmentIds, columnSegmentId);
      if (pos < 0) {
        throw new IllegalStateException("segment-slot leaf " + s.rowGroupId() + " segment " + columnSegmentId
            + " has no descriptor entry (headless or corrupt store, indexNumber=" + indexNumber
            + ")");
      }
      // Bare segment payloads carry their integrity in the descriptor entry (byteLen + contentHash),
      // re-checked by assembleRaw's verifyColumnSegment — no marker, no re-hash. A segment's inlineValue is
      // already the raw payload (the walk stripped the 1-byte discriminator); referenced ones batch.
      if (s.resolved() != null) {
        accum.payloads[pos] = s.resolved();
      } else if (s.inlineValue() != null) {
        accum.payloads[pos] = s.inlineValue();
      } else {
        pendingSeg.add(new PendingSegRef(accum.payloads, pos, s.offset(), s.slotKey()));
      }
    }
    resolvePending(reader, pendingSeg);
    // Phase 5 — assemble each (independent) leaf; fan out for large stores.
    final byte[][] assembled = new byte[ordered.length][];
    assembleColumnSegmentSlotRowGroups(ordered, assembled, ordered.length >= PARALLEL_ASSEMBLE_MIN);
    final ArrayList<byte[]> out = new ArrayList<>(assembled.length);
    Collections.addAll(out, assembled);
    return out;
  }

  /**
   * Capture a DESCRIPTOR slot (slotKind 0, a hashed blob) during the walk: inline blobs keep the whole
   * small value (verified on resolution); referenced blobs keep only the 17-byte marker + durable
   * offset, or — for an unresolved (uncommitted, swizzled) page — read + verify it in-walk while the
   * leaf/ref is valid.
   */
  private static RawBlobSlot captureDescriptorSlot(final StorageEngineReader reader,
      final HOTLeafPage leaf, final MemorySegment valueSlice, final int valueSize,
      final long rowGroupId, final long slotKey, final int indexNumber) {
    if (valueSize < BLOB_MARKER_BYTES
        || valueSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 0) != BLOB_MAGIC) {
      throw new IllegalStateException("segment-slot descriptor slot " + slotKey + " is not a blob"
          + " marker (" + valueSize + " bytes) — mixed storage layouts in one sub-tree (indexNumber="
          + indexNumber + ")");
    }
    final boolean inline =
        (valueSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 5) & BLOB_INLINE_FLAG) != 0;
    if (inline) {
      final byte[] value = new byte[valueSize];
      MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, value, 0, valueSize);
      return new RawBlobSlot(rowGroupId, 0, value, null, null, Constants.NULL_ID_LONG, slotKey);
    }
    final byte[] marker = new byte[BLOB_MARKER_BYTES];
    MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, marker, 0, BLOB_MARKER_BYTES);
    final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    final long offset = ref == null ? Constants.NULL_ID_LONG : ref.getKey();
    if (offset != Constants.NULL_ID_LONG) {
      return new RawBlobSlot(rowGroupId, 0, null, null, marker, offset, slotKey);
    }
    // Unresolved (uncommitted, this-transaction) or absent — resolve in-walk. The descriptor slot is
    // the ONLY content-integrity check on itself (RowGroupDescriptor.validate is structural), so verify.
    final OverflowPage page = ref == null ? null : reader.readSideOverflowPage(ref);
    final byte[] resolved = verifyBlob(marker, page == null ? null : page.getDataBytes(), slotKey);
    return new RawBlobSlot(rowGroupId, 0, null, resolved, null, Constants.NULL_ID_LONG, slotKey);
  }

  /**
   * Capture a BARE SEGMENT slot (slotKind >= 1) during the walk: the leading discriminator byte marks
   * it inline (the rest of the value IS the raw payload) or referenced (bytes in a page — kept as a
   * durable offset for the coalesced batch, or read in-walk when unresolved). No marker, no hash;
   * assembleRaw's verifyColumnSegment is the sole integrity check against the descriptor entry.
   */
  private static RawBlobSlot captureColumnSegmentSlot(final StorageEngineReader reader,
      final HOTLeafPage leaf, final MemorySegment valueSlice, final int valueSize,
      final long rowGroupId, final int slotKind, final long slotKey, final int indexNumber) {
    final byte kind = valueSlice.get(ValueLayout.JAVA_BYTE, 0);
    if (kind == SEG_KIND_INLINE) {
      final byte[] payload = new byte[valueSize - 1];
      MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 1, payload, 0, valueSize - 1);
      return new RawBlobSlot(rowGroupId, slotKind, payload, null, null, Constants.NULL_ID_LONG, slotKey);
    }
    if (kind != SEG_KIND_REF) {
      throw new IllegalStateException("segment-slot segment slot " + slotKey + " has an unknown"
          + " discriminator " + kind + " (" + valueSize + " bytes) — mixed storage layouts in one"
          + " sub-tree (indexNumber=" + indexNumber + ")");
    }
    final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    final long offset = ref == null ? Constants.NULL_ID_LONG : ref.getKey();
    if (offset != Constants.NULL_ID_LONG) {
      return new RawBlobSlot(rowGroupId, slotKind, null, null, null, offset, slotKey);
    }
    // Unresolved (uncommitted, this-transaction) or absent — resolve in-walk while the ref is valid.
    final OverflowPage page = ref == null ? null : reader.readSideOverflowPage(ref);
    final byte[] rawBytes = page == null ? null : page.getDataBytes();
    if (rawBytes == null) {
      throw new IllegalStateException("segment-slot uncommitted segment at slot " + slotKey
          + " has no page bytes (indexNumber=" + indexNumber + ")");
    }
    return new RawBlobSlot(rowGroupId, slotKind, null, rawBytes, null, Constants.NULL_ID_LONG, slotKey);
  }

  /**
   * Resolve every descriptor (referenced ones through ONE coalesced batch), verify + validate it,
   * and size its leaf's accum into {@code ordered[i]}. {@code descArr[i]} is rowGroupId {@code i+1}'s
   * descriptor slot; the referenced-collection and build passes iterate it in the same order so the
   * batch result stays aligned. The descriptor slot keeps its full content-hash verify — it is the
   * only integrity check on the descriptor itself (its {@code validate} is structural).
   */
  private static void resolveDescriptors(final StorageEngineReader reader,
      final RawBlobSlot[] descArr, final ColumnSegmentSlotRowGroupAccum[] ordered) {
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
   * bytes flow straight to the accum, and {@code assembleRaw}'s {@code verifyColumnSegment} checks byteLen +
   * contentHash against the descriptor entry (a genuinely-missing page yields {@code null} → a loud
   * "Missing segment bytes" there). {@code indexNumber} is unused — kept out of the hot loop.
   */
  private static void resolvePending(final StorageEngineReader reader,
      final ArrayList<PendingSegRef> pending) {
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
   * <p>Binary search, not a scan: the table is filled in descriptor-entry order and
   * {@link RowGroupDescriptor#serialize} enforces strictly ascending ids. This is the resolver
   * {@code assembleRaw} calls once per segment, so a linear scan made a row-group assembly
   * O(segments²) — negligible while the column cap was 84, but the dominant cost of a full read at
   * the widened cap.</p>
   */
  private static int indexOf(final int[] columnSegmentIds, final int columnSegmentId) {
    final int pos = Arrays.binarySearch(columnSegmentIds, columnSegmentId);
    return pos >= 0 ? pos : -1;
  }

  /** Assemble each accumulated leaf (independent per leaf); fans out for large stores. */
  private static void assembleColumnSegmentSlotRowGroups(final ColumnSegmentSlotRowGroupAccum[] ordered, final byte[][] out,
      final boolean parallel) {
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
      return pos < 0 ? null : accum.payloads[pos];
    });
  }

  /**
   * Descriptor-tier row count for the segment-slot layout: sums {@code rowCount} across the
   * descriptor slots (slotKind 0) of leaves {@code 1..rowGroupCount}, reading NO segment slots.
   *
   * <p>This serves counting queries WITHOUT hydrating, so it has to be cheap in page touches, not
   * just in bytes: it takes the same single {@link #orderedDescriptorSlots} range scan the full
   * read does, and resolves whatever descriptors spilled past the inline threshold in ONE
   * coalesced batch. Per-row-group {@link #readRowCountFromColumnSegmentSlots} probes would be
   * {@code rowGroupCount} root-to-leaf descents plus a random page read each — at 100k row groups
   * that is a planning-path cost the count tier exists to avoid.
   *
   * <p>Loud on a gap, a duplicate, or a leaked orphan (the scan sees EVERY live descriptor, so an
   * orphan anywhere above the count is caught, not just the next slot), so the count can never
   * disagree with a full hydrate.
   */
  public static long sumRowsFromColumnSegmentSlots(final StorageEngineReader reader, final int indexNumber,
      final int rowGroupCount) {
    final RawBlobSlot[] descArr = orderedDescriptorSlots(reader, indexNumber, rowGroupCount, null);
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
  private static byte[][] resolveDescriptorPayloads(final StorageEngineReader reader,
      final RawBlobSlot[] descArr) {
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
      payloads[i] = ds.resolved() != null ? ds.resolved()
          : ds.inlineValue() != null ? verifyInlineBlob(ds.inlineValue(), ds.slotKey())
          : verifyBlob(ds.marker(), refPages[ri++], ds.slotKey());
    }
    return payloads;
  }

  /**
   * Tombstone a slot: remove all its segment refs (descriptor leaves AND blob slots — leaving
   * a blob's side-map ref behind would leak its MB-scale segment page into every future
   * fragment), then write the zero-length slot value. A truly absent slot is a free no-op —
   * inserting a tombstone entry would CoW the leaf and emit a fragment for nothing.
   */
  public void tombstoneRowGroup(final long rowGroupId) {
    final byte[] prior = readSlotValueForWrite(rowGroupId);
    if (prior == null) {
      return;
    }
    if (RowGroupDescriptor.isDescriptor(prior)) {
      final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(prior);
      for (int i = 0; i < columnSegmentCount; i++) {
        removeSegmentPage(rowGroupId, RowGroupDescriptor.entryColumnSegmentId(prior, i));
      }
    } else if (prior.length >= BLOB_MARKER_BYTES
        && ProjectionIndexRowGroupCodec.getIntLE(prior, 0) == BLOB_MAGIC) {
      // Referenced blob → drop its page; inline blob → carries no page (removeSegmentPage no-ops).
      removeSegmentPage(rowGroupId, BLOB_SEGMENT_ID);
    }
    if (prior.length > 0) {
      writeSlotValue(rowGroupId, TOMBSTONE);
    }
  }

  /** Minimum deferred leaves before phase-2 assembly fans out to the common pool. */
  private static final int PARALLEL_ASSEMBLE_MIN = 64;

  /**
   * One live leaf's directory — descriptor plus resolved segment page offsets, WITHOUT any
   * segment fetch or assembly (P5b stage 2): the construction input of the segment-lazy
   * handle. {@code columnSegmentIds}/{@code columnSegmentOffsets} are parallel, ascending-id.
   *
   * <p>{@code inlineColumnSegmentBytes} is the SEGMENT-SLOT layout's inline carrier (parallel to
   * {@code columnSegmentIds}): a bare segment slot stores small payloads in its own slot value, not in
   * the descriptor (which is zone-map-only), so those bytes are captured at directory-build time
   * and supplied straight to the column fill. {@code null} for the descriptor layout (whose inline
   * segments ride the descriptor's own inline region); a {@code null} element = a referenced
   * segment (its bytes come from the page at {@code columnSegmentOffsets[i]}).
   */
  public record RowGroupDirectory(long rowGroupId, byte[] descriptor, int[] columnSegmentIds,
      long[] columnSegmentOffsets, byte @Nullable [] @Nullable [] inlineColumnSegmentBytes) {

    /** Descriptor-layout directory: no per-slot inline carrier (inline rides the descriptor). */
    public RowGroupDirectory(final long rowGroupId, final byte[] descriptor, final int[] columnSegmentIds,
        final long[] columnSegmentOffsets) {
      this(rowGroupId, descriptor, columnSegmentIds, columnSegmentOffsets, null);
    }

    /**
     * The captured inline bytes at descriptor ENTRY INDEX {@code entryIndex}, or {@code null} if the
     * segment is referenced (or this directory carries no inline segments at all).
     *
     * <p>Indexed by entry, not searched by id: all three parallel arrays here are filled in
     * descriptor-entry order, so one {@link RowGroupDescriptor#entryIndexOf} binary search resolves
     * the inline bytes, the storage class and the offset together. Searching by id per array made a
     * column fill O(rowGroups × segments), which the 21844-column cap turned from negligible into
     * the dominant cost of the pruned read.</p>
     */
    public byte @Nullable [] inlineBytesAt(final int entryIndex) {
      return inlineColumnSegmentBytes == null ? null : inlineColumnSegmentBytes[entryIndex];
    }
  }

  /**
   * ONE range scan builds each row group's {@link RowGroupDirectory} — its zone-map descriptor, each REFERENCED segment's durable page offset
   * (captured, not fetched), and each bare INLINE segment's bytes (captured from the slot value,
   * since a segment-slot descriptor is zone-map-only and carries no inline region). Zero segment PAGE
   * reads (only referenced descriptors, if any, read their page). This is the construction input of a
   * column-pruned segment-slot handle: a later column fill batches ONLY the queried column's offsets.
   *
   * <p>Returns {@code null} when any referenced segment (or descriptor) page is unresolved
   * (uncommitted, this-transaction) — offset-lazy fetching cannot serve those, so the caller falls
   * back to the eager whole-leaf read (which resolves them in-walk).
   */
  public static @Nullable List<RowGroupDirectory> readAllRowGroupDirectoriesFromColumnSegmentSlots(
      final StorageEngineReader reader, final int indexNumber, final int rowGroupCount) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return rowGroupCount == 0 ? List.of() : null;
    }
    final Long2ObjectRBTreeMap<byte[]> descriptors = new Long2ObjectRBTreeMap<>();
    final Long2ObjectOpenHashMap<byte[]> columnSegmentInline = new Long2ObjectOpenHashMap<>();
    final Long2LongOpenHashMap columnSegmentOffset = new Long2LongOpenHashMap();
    columnSegmentOffset.defaultReturnValue(Constants.NULL_ID_LONG);
    if (!collectRowGroupDirectorySlots(reader, indexNumber, rootRef, descriptors, columnSegmentInline,
        columnSegmentOffset)) {
      return null; // an unresolved page — offset-lazy fetching cannot serve it
    }
    if (descriptors.size() != rowGroupCount) {
      throw new IllegalStateException("segment-slot store has " + descriptors.size()
          + " live descriptors but metadata declares " + rowGroupCount + " (indexNumber=" + indexNumber + ")");
    }
    return buildRowGroupDirectories(descriptors, columnSegmentInline, columnSegmentOffset,
        rowGroupCount, indexNumber);
  }

  /**
   * The range-scan half of {@link #readAllRowGroupDirectoriesFromColumnSegmentSlots}: fills
   * {@code descriptors} (keyed by rowGroupId) and, per segment slot, either its inline bytes or its
   * durable page offset — CAPTURED, never fetched, apart from a referenced descriptor's own page.
   *
   * @return {@code false} as soon as any referenced page is unresolved (uncommitted,
   *         this-transaction), which the caller turns into a fall-back to the eager whole-leaf read
   */
  private static boolean collectRowGroupDirectorySlots(final StorageEngineReader reader,
      final int indexNumber, final PageReference rootRef,
      final Long2ObjectRBTreeMap<byte[]> descriptors,
      final Long2ObjectOpenHashMap<byte[]> columnSegmentInline,
      final Long2LongOpenHashMap columnSegmentOffset) {
    final byte[] minKey = new byte[8];
    final byte[] maxKey = new byte[8];
    Arrays.fill(maxKey, (byte) 0xFF);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final long slotKey = leaf.decodeKey8BE(cursor.currentEntryIndex()) ^ 0x8000_0000_0000_0000L;
        final long rowGroupId = slotKey >>> 16;
        final MemorySegment valueSlice = cursor.currentValueSlice();
        final int valueSize = valueSlice == null ? 0 : (int) valueSlice.byteSize();
        // Skip tombstones, the slot-0 metadata and the fence chunks — see the key families
        // documented on readAllRowGroupsFromColumnSegmentSlots.
        if (valueSize == 0 || rowGroupId == 0 || slotKey >= ProjectionIndexFences.CHUNK_SLOT_BASE) {
          cursor.advance();
          continue;
        }
        if ((int) (slotKey & 0xFFFF) == 0) {
          final byte[] descriptor =
              resolveDirectoryDescriptorSlot(reader, leaf, valueSlice, valueSize, slotKey);
          if (descriptor == null) {
            return false;
          }
          if (descriptors.put(rowGroupId, descriptor) != null) {
            throw new IllegalStateException("segment-slot leaf " + rowGroupId
                + " has two descriptor slots (indexNumber=" + indexNumber + ")");
          }
        } else if (!captureDirectoryColumnSegmentSlot(leaf, valueSlice, valueSize, slotKey,
            indexNumber, columnSegmentInline, columnSegmentOffset)) {
          return false;
        }
        cursor.advance();
      }
    }
    return true;
  }

  /**
   * A descriptor slot's bytes during the directory walk. Unlike a segment, the descriptor IS read
   * here when referenced — the whole build is driven by its entries, so deferring it would buy
   * nothing.
   *
   * @return {@code null} when its page is unresolved
   */
  private static byte @Nullable [] resolveDirectoryDescriptorSlot(final StorageEngineReader reader,
      final HOTLeafPage leaf, final MemorySegment valueSlice, final int valueSize, final long slotKey) {
    final byte[] value = new byte[valueSize];
    MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, value, 0, valueSize);
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
   * One segment slot during the directory walk: an inline slot contributes its bytes, a referenced
   * one only its durable offset (no page read — that is the point of the offset-lazy handle).
   *
   * @return {@code false} when a referenced slot's page is unresolved
   */
  private static boolean captureDirectoryColumnSegmentSlot(final HOTLeafPage leaf,
      final MemorySegment valueSlice, final int valueSize, final long slotKey, final int indexNumber,
      final Long2ObjectOpenHashMap<byte[]> columnSegmentInline,
      final Long2LongOpenHashMap columnSegmentOffset) {
    final byte kind = valueSlice.get(ValueLayout.JAVA_BYTE, 0);
    if (kind == SEG_KIND_INLINE) {
      final byte[] payload = new byte[valueSize - 1];
      MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 1, payload, 0, valueSize - 1);
      columnSegmentInline.put(slotKey, payload);
      return true;
    }
    if (kind != SEG_KIND_REF) {
      throw new IllegalStateException("segment-slot segment slot " + slotKey + " has an unknown"
          + " discriminator " + kind + " (indexNumber=" + indexNumber + ")");
    }
    final long offset = resolvedSegmentPageOffset(leaf, slotKey);
    if (offset == Constants.NULL_ID_LONG) {
      return false;
    }
    columnSegmentOffset.put(slotKey, offset);
    return true;
  }

  /** Durable offset of the page backing {@code slotKey}, or {@link Constants#NULL_ID_LONG}. */
  private static long resolvedSegmentPageOffset(final HOTLeafPage leaf, final long slotKey) {
    final PageReference ref =
        leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
    return ref == null ? Constants.NULL_ID_LONG : ref.getKey();
  }

  /**
   * Turn the walk's captures into one {@link RowGroupDirectory} per row group, validating that the
   * descriptor keys are contiguous {@code 1..rowGroupCount} as they are drained in key order.
   */
  private static List<RowGroupDirectory> buildRowGroupDirectories(
      final Long2ObjectRBTreeMap<byte[]> descriptors,
      final Long2ObjectOpenHashMap<byte[]> columnSegmentInline,
      final Long2LongOpenHashMap columnSegmentOffset, final int rowGroupCount, final int indexNumber) {
    final ArrayList<RowGroupDirectory> out = new ArrayList<>(rowGroupCount);
    long expected = 1;
    for (final Long2ObjectMap.Entry<byte[]> e : descriptors.long2ObjectEntrySet()) {
      final long rowGroupId = e.getLongKey();
      if (rowGroupId != expected) {
        throw new IllegalStateException("segment-slot leaves are not contiguous: expected leaf "
            + expected + ", found " + rowGroupId + " (indexNumber=" + indexNumber + ")");
      }
      expected++;
      final byte[] descriptor = e.getValue();
      final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
      final int[] columnSegmentIds = new int[columnSegmentCount];
      final long[] segOffsets = new long[columnSegmentCount];
      byte[][] inline = null;
      for (int i = 0; i < columnSegmentCount; i++) {
        final int columnSegmentId = RowGroupDescriptor.entryColumnSegmentId(descriptor, i);
        columnSegmentIds[i] = columnSegmentId;
        final long segSlotKey = columnSegmentSlotKey(rowGroupId, columnSegmentId);
        final byte[] inlineBytes = columnSegmentInline.get(segSlotKey);
        if (inlineBytes != null) {
          if (inline == null) {
            inline = new byte[columnSegmentCount][];
          }
          inline[i] = inlineBytes;
          segOffsets[i] = Constants.NULL_ID_LONG;
        } else {
          final long off = columnSegmentOffset.get(segSlotKey);
          if (off == Constants.NULL_ID_LONG) {
            throw new IllegalStateException("segment-slot leaf " + rowGroupId + " segment "
                + columnSegmentId + " missing (indexNumber=" + indexNumber + ")");
          }
          segOffsets[i] = off;
        }
      }
      out.add(new RowGroupDirectory(rowGroupId, descriptor, columnSegmentIds, segOffsets, inline));
    }
    return out;
  }

  /**
   * Fetch one segment page's bytes by durable offset through a throwaway reference — the
   * segment-lazy handle's fetch primitive. Returns {@code null} for a null offset.
   */
  public static byte @Nullable [] readSegmentBytesAtOffset(final StorageEngineReader reader,
      final long offset) {
    if (offset == Constants.NULL_ID_LONG) {
      return null;
    }
    final PageReference ref = new PageReference();
    ref.setKey(offset);
    final OverflowPage page = reader.readSideOverflowPage(ref);
    return page == null ? null : page.getDataBytes();
  }

  /**
   * Batched {@link #readSegmentBytesAtOffset}: one call per COLUMN FILL instead of one per
   * segment, so the backend can coalesce runs of near-adjacent offsets into single ranged
   * reads (P5b stage 4b). Result is input-aligned; a null/{@code NULL_ID_LONG} offset or an
   * unresolved reference yields {@code null} at that index.
   */
  public static byte @Nullable [] @Nullable [] readSegmentBytesBatch(
      final StorageEngineReader reader, final long[] offsets) {
    final OverflowPage[] pages = reader.readSideOverflowPageBatch(offsets);
    final byte[][] out = new byte[offsets.length][];
    for (int i = 0; i < offsets.length; i++) {
      out[i] = pages[i] == null ? null : pages[i].getDataBytes();
    }
    return out;
  }

  // ==================== blob slots (slot-0 metadata payload) ====================

  /**
   * Store an opaque payload (the PIXM shape metadata, the per-leaf fence chunks) at
   * {@code slotKey}. Mirroring the descriptor's hybrid split, the payload is either INLINE (bytes
   * in the slot value's trailing region, for payloads ≤ {@link #BLOB_INLINE_MAX}) or REFERENCED
   * (one {@link OverflowPage}, for larger ones); the leading PIXB marker carries byteLen + an
   * XXH3-64 hash for integrity either way (segment pages have no checksum of their own).
   * Whole-blob last-writer-wins, with an unchanged blob carried forward as a true no-op.
   */
  public void putBlob(final long slotKey, final byte[] payload) {
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID);
    final long hash = ProjectionIndexColumnSegmentCodec.contentHash(payload);
    final boolean inline = payload.length <= BLOB_INLINE_MAX;
    final byte[] prior = readSlotValueForWrite(slotKey);
    // Carry-forward: an unchanged blob is a true no-op — the marker already carries byteLen + hash.
    // The storage-class bit must also match, so a referenced⇄inline migration is never mistaken
    // for a no-op (its stale page would otherwise linger, or its inline bytes never get written).
    if (prior != null && prior.length >= BLOB_MARKER_BYTES
        && ProjectionIndexRowGroupCodec.getIntLE(prior, 0) == BLOB_MAGIC && prior[4] == BLOB_VERSION) {
      final int priorLenField = ProjectionIndexRowGroupCodec.getIntLE(prior, 5);
      if ((priorLenField & ~BLOB_INLINE_FLAG) == payload.length
          && ((priorLenField & BLOB_INLINE_FLAG) != 0) == inline
          && ProjectionIndexRowGroupCodec.getLongLE(prior, 9) == hash) {
        return;
      }
    }
    // Referenced ⇔ 17-byte marker, blob magic, AND the inline flag clear — a 0-length inline
    // payload is also exactly 17 bytes, so the flag (not the length alone) is the discriminator.
    final boolean priorWasReferencedBlob = prior != null && prior.length == BLOB_MARKER_BYTES
        && ProjectionIndexRowGroupCodec.getIntLE(prior, 0) == BLOB_MAGIC
        && (ProjectionIndexRowGroupCodec.getIntLE(prior, 5) & BLOB_INLINE_FLAG) == 0;
    if (inline) {
      final byte[] value = new byte[BLOB_MARKER_BYTES + payload.length];
      RowGroupDescriptor.putIntLE(value, 0, BLOB_MAGIC);
      value[4] = BLOB_VERSION;
      RowGroupDescriptor.putIntLE(value, 5, payload.length | BLOB_INLINE_FLAG);
      RowGroupDescriptor.putLongLE(value, 9, hash);
      System.arraycopy(payload, 0, value, BLOB_MARKER_BYTES, payload.length);
      writeSlotValue(slotKey, value);
      // Referenced → inline migration: drop the now-orphaned page (no-op when there was none).
      if (priorWasReferencedBlob) {
        removeSegmentPage(slotKey, BLOB_SEGMENT_ID);
      }
    } else {
      final byte[] marker = new byte[BLOB_MARKER_BYTES];
      RowGroupDescriptor.putIntLE(marker, 0, BLOB_MAGIC);
      marker[4] = BLOB_VERSION;
      RowGroupDescriptor.putIntLE(marker, 5, payload.length);
      RowGroupDescriptor.putLongLE(marker, 9, hash);
      writeSlotValue(slotKey, marker);
      putSegmentPage(slotKey, BLOB_SEGMENT_ID, payload);
    }
  }

  /** {@code true} iff {@code value} is a blob slot value whose payload is stored inline. */
  private static boolean isInlineBlob(final byte[] value) {
    return value.length >= BLOB_MARKER_BYTES
        && ProjectionIndexRowGroupCodec.getIntLE(value, 0) == BLOB_MAGIC
        && (ProjectionIndexRowGroupCodec.getIntLE(value, 5) & BLOB_INLINE_FLAG) != 0;
  }

  /** Writer-side blob read; {@code null} when absent/tombstoned. Verifies length + hash. */
  public byte @Nullable [] getBlob(final long slotKey) {
    final byte[] value = readSlotValueForWrite(slotKey);
    if (value == null || value.length == 0) {
      return null;
    }
    if (isInlineBlob(value)) {
      return verifyInlineBlob(value, slotKey);
    }
    return verifyBlob(value, getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID), slotKey);
  }

  /**
   * {@link #getBlob} that reports an UNREADABLE blob as {@code null} instead of throwing — for the
   * write paths that read a PRIOR value only to diff against it.
   *
   * <p>The descriptor-directory write paths read their prior descriptor with the raw, never-throwing
   * {@link #readSlotValueForWrite}, so a damaged prior simply fails the {@code isDescriptor} test and
   * the write proceeds (or maintenance returns {@code false} and a full rebuild repairs the store).
   * The segment-slot twins keep their descriptor in a verified blob, so using {@link #getBlob} there
   * made the same condition fatal instead of self-healing: the throw escapes into the change
   * listener's corruption valve, which tombstones; {@code rebuildFully} then returns early on a
   * stale marker, and a fresh create re-enters this very read and throws again — a permanently dead
   * index where the descriptor layout would have rebuilt cleanly. Treating it as "no usable prior"
   * restores that parity: the caller overwrites, and the worst case is an orphaned segment page
   * rather than an unusable index.</p>
   */
  private byte @Nullable [] getBlobIfReadable(final long slotKey) {
    try {
      return getBlob(slotKey);
    } catch (final IllegalStateException unreadable) {
      LOGGER.warn("Projection blob at slot " + slotKey + " is unreadable (" + unreadable.getMessage()
          + ") — treating it as absent so the write path can overwrite it");
      return null;
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
      final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, slotKey, keyBuf);
      if (leaf == null) {
        return null;
      }
      final int idx = leaf.findEntry(keyBuf);
      if (idx < 0) {
        return null;
      }
      final byte[] value = leaf.getValue(idx);
      if (value == null || value.length == 0) {
        return null;
      }
      if (isInlineBlob(value)) {
        return verifyInlineBlob(value, slotKey);
      }
      final PageReference ref = leaf.getPageReference(HOTLeafPage.overflowPageRefKey(slotKey, BLOB_SEGMENT_ID));
      if (ref == null) {
        return verifyBlob(value, null, slotKey);
      }
      final OverflowPage page = reader.readSideOverflowPage(ref);
      return verifyBlob(value, page == null ? null : page.getDataBytes(), slotKey);
    }
  }

  /** Verify + extract an inline blob's payload from its own slot value (no page). */
  private static byte[] verifyInlineBlob(final byte[] value, final long slotKey) {
    if (value.length < BLOB_MARKER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(value, 0) != BLOB_MAGIC || value[4] != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int len = ProjectionIndexRowGroupCodec.getIntLE(value, 5) & ~BLOB_INLINE_FLAG;
    if (value.length != BLOB_MARKER_BYTES + len) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " has inconsistent length ("
          + value.length + " bytes, expected " + (BLOB_MARKER_BYTES + len) + ")");
    }
    final byte[] payload = Arrays.copyOfRange(value, BLOB_MARKER_BYTES, BLOB_MARKER_BYTES + len);
    if (ProjectionIndexColumnSegmentCodec.contentHash(payload)
        != ProjectionIndexRowGroupCodec.getLongLE(value, 9)) {
      throw new IllegalStateException("Inline blob at slot " + slotKey + " failed hash verification");
    }
    return payload;
  }

  private static byte[] verifyBlob(final byte[] marker, final byte @Nullable [] payload, final long slotKey) {
    if (marker.length != BLOB_MARKER_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(marker, 0) != BLOB_MAGIC || marker[4] != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int expectedLen = ProjectionIndexRowGroupCodec.getIntLE(marker, 5) & ~BLOB_INLINE_FLAG;
    final long expectedHash = ProjectionIndexRowGroupCodec.getLongLE(marker, 9);
    if (payload == null || payload.length != expectedLen
        || ProjectionIndexColumnSegmentCodec.contentHash(payload) != expectedHash) {
      throw new IllegalStateException("Blob at slot " + slotKey + " failed length/hash verification ("
          + (payload == null ? "missing segment" : payload.length + " bytes") + ", expected "
          + expectedLen + ")");
    }
    return payload;
  }

  /**
   * Diagnostic: the durable offset key of the segment page referenced for
   * {@code (ownerSlotKey, columnSegmentId)} at the reader's revision, or {@link Constants#NULL_ID_LONG}
   * when absent/unresolved. Equal keys across revisions prove the page was shared by reference
   * (the carry-forward no-op), not rewritten — the observable for containment tests and the P8
   * update-bytes measurements.
   */
  public static long segmentPageOffset(final StorageEngineReader reader, final int indexNumber,
      final long ownerSlotKey, final int columnSegmentId) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return Constants.NULL_ID_LONG;
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, ownerSlotKey, keyBuf);
      if (leaf == null) {
        return Constants.NULL_ID_LONG;
      }
      final PageReference ref = leaf.getPageReference(refKey);
      return ref == null ? Constants.NULL_ID_LONG : ref.getKey();
    }
  }

  // ==================== descriptor-layout internals ====================

  /**
   * Shared navigation preamble of the reader-side descriptor-layout statics: serialize the
   * slot key into {@code keyBuf} and navigate to the HOT leaf covering it. {@code null} when
   * the trie has no such leaf. The caller owns the {@code trieReader} lifetime (segment
   * resolution reads through the returned leaf's side map while the reader is open).
   */
  private static @Nullable HOTLeafPage navigateToSlotLeaf(final HOTTrieReader trieReader,
      final PageReference rootRef, final long slotKey, final byte[] keyBuf) {
    PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    return trieReader.navigateToLeaf(rootRef, keyBuf);
  }

  /** Writer-side raw slot read: {@code null} when the leaf/slot is absent. */
  private byte @Nullable [] readSlotValueForWrite(final long slotKey) {
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    final HOTLeafPage leaf = getLeafForRead(keyBuf);
    if (leaf == null) {
      return null;
    }
    final int idx = leaf.findEntry(keyBuf);
    return idx < 0 ? null : leaf.getValue(idx);
  }

  /**
   * Write a slot value through the standard loud put/update/split machinery.
   * Package-private so migration tests can fabricate legacy-layout slot values (raw composite
   * keys) without a production API.
   */
  void writeSlotValue(final long slotKey, final byte[] value) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = PathKeySerializer.INSTANCE.serialize(slotKey, keyBuf, 0);
    // Re-navigate after every split that did not place the value: the leaf the key belongs to is
    // a different page now (see updateOrSplitInsert for why one split can free nothing).
    for (int attempt = 0; attempt < MAX_SPLIT_CASCADE; attempt++) {
      final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
      final HOTLeafPage leaf = navResult.leaf();
      if (leaf.put(keyBuf, value)) {
        return;
      }
      final int idx = leaf.findEntry(keyBuf);
      if (updateOrSplitInsert(leaf, navResult, keyBuf, keyLen, idx, value)) {
        return;
      }
    }
    throw new SirixIOException("Projection HOT slot write for key=" + slotKey + " (" + value.length
        + " bytes, indexNumber=" + indexNumber + ") did not settle within " + MAX_SPLIT_CASCADE
        + " leaf splits");
  }

  /**
   * Attach an encoded segment as its own CoW-versioned {@link OverflowPage},
   * referenced from the side map of the HOT leaf that owns slot {@code ownerSlotKey}.
   *
   * <p>Segment-directory storage primitive (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3,
   * introduced with the P1 page-layer machinery): the side-map key is
   * {@code (ownerSlotKey << 16) | columnSegmentId}, matching the owner-slot routing in
   * {@code HOTLeafPage#moveOverflowPageRefsAfterSplit} — the reference lives on whichever page
   * holds the owning slot, across arbitrary split cascades. The page is written (and its
   * durable offset key assigned) inside the commit descent; until then it exists only
   * in-memory on the reference, so a rollback simply never writes it.
   *
   * <p>Re-attaching the same {@code (ownerSlotKey, columnSegmentId)} replaces the reference —
   * whole-segment last-writer-wins. An unchanged segment is shared across revisions by NOT
   * re-attaching it (the carried-forward reference keeps its resolved key).
   */
  public void putSegmentPage(final long ownerSlotKey, final int columnSegmentId, final byte[] bytes) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
    // The owner slot MUST already exist on the leaf: split routing
    // (HOTLeafPage#moveOverflowPageRefsAfterSplit) and read navigation both key off owner-slot
    // residency, so a ref attached without its owning slot would be permanently orphaned on
    // whichever leaf covers the key at attach time — durably committed but unreachable after
    // the next split. Callers write the owning slot (descriptor/chunk) before its segments.
    if (navResult.leaf().findEntry(keyBuf) < 0) {
      throw new IllegalStateException("putSegmentPage: owner slot " + ownerSlotKey
          + " does not exist (indexNumber=" + indexNumber + ") — write the owning slot before"
          + " attaching its segments, or the reference cannot follow it across splits.");
    }
    final PageReference ref = new PageReference();
    ref.setPage(new OverflowPage(bytes));
    navResult.leaf().setPageReference(refKey, ref);
  }

  /**
   * Remove the segment reference for {@code (ownerSlotKey, columnSegmentId)} — a real delete
   * (shrunk or tombstoned leaf), replacing the old zero-length-chunk tombstone convention.
   * No-op when absent.
   */
  public void removeSegmentPage(final long ownerSlotKey, final int columnSegmentId) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    // Probe read-only first: an unconditional prepareLeafOfTree would CoW the leaf (and its
    // indirect spine) into the TIL — emitting a fragment for an UNCHANGED leaf at commit, and
    // on an empty trie it would even create a spurious root leaf. Only pay the CoW when the
    // reference actually exists.
    final HOTLeafPage probeLeaf = getLeafForRead(keyBuf);
    if (probeLeaf == null || probeLeaf.getPageReference(refKey) == null) {
      return;
    }
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, 8);
    navResult.leaf().removePageReference(refKey);
  }

  /**
   * Writer-side segment read: resolve the side-map reference on the leaf owning
   * {@code ownerSlotKey} and materialise the segment bytes (in-memory page for uncommitted
   * segments of this transaction, disk read for committed ones). {@code null} when the leaf,
   * the reference, or the page is absent.
   */
  public byte @Nullable [] getSegmentPageBytes(final long ownerSlotKey, final int columnSegmentId) {
    final long refKey = HOTLeafPage.overflowPageRefKey(ownerSlotKey, columnSegmentId);
    final byte[] keyBuf = KEY_BUFFER.get();
    PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final HOTLeafPage leaf = getLeafForRead(keyBuf);
    if (leaf == null) {
      return null;
    }
    final PageReference ref = leaf.getPageReference(refKey);
    if (ref == null) {
      return null;
    }
    final OverflowPage page = storageEngineWriter.readSideOverflowPage(ref);
    // Zero-copy contract: the returned array is the shared page instance's backing store
    // (swizzled onto the reference for every reader of this revision) — callers MUST NOT
    // mutate it.
    return page == null ? null : page.getDataBytes();
  }

  /**
   * Reader-side segment read for committed revisions: navigate the queried revision's trie to
   * the leaf owning {@code ownerSlotKey}, resolve the side-map reference, and load the
   * segment page by its offset key. {@code null} when the sub-tree, leaf, or reference is
   * absent.
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
      final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, ownerSlotKey, keyBuf);
      if (leaf == null) {
        return null;
      }
      final PageReference ref = leaf.getPageReference(refKey);
      if (ref == null) {
        return null;
      }
      final OverflowPage page = reader.readSideOverflowPage(ref);
      // Zero-copy contract: shared page backing store — callers MUST NOT mutate.
      return page == null ? null : page.getDataBytes();
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
   * Root reference of the projection sub-tree for {@code indexNumber} under
   * the given reader's current revision, or {@code null} if no index is
   * installed.
   */
  public static @Nullable PageReference rootReference(final StorageEngineReader reader, final int indexNumber) {
    final RevisionRootPage rrp = reader.getActualRevisionRootPage();
    final ProjectionIndexPage projPage = reader.getProjectionIndexPage(rrp);
    if (projPage == null) return null;
    final PageReference ref = projPage.getOrCreateReference(indexNumber);
    if (ref == null) return null;
    if (ref.getKey() == Constants.NULL_ID_LONG && ref.getLogKey() == Constants.NULL_ID_INT && ref.getPage() == null) {
      return null;
    }
    return ref;
  }

}
