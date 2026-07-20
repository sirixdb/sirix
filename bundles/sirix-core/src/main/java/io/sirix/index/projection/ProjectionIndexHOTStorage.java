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
import io.sirix.page.ProjectionSegmentPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.jspecify.annotations.Nullable;

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
 * <b>segment-directory layout</b> (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md
 * §2.3, §3, §4).
 *
 * <h2>Storage contract</h2>
 *
 * <ul>
 *   <li><b>Slot keys</b> — one HOT slot per logical leaf:
 *       {@code hotKey = PathKeySerializer.serialize(leafIndex)} (sign-flipped
 *       8-byte BE, so unsigned byte comparison preserves signed-long order).
 *       Live descriptor slots are contiguous from 1 (invariant 5.1-11) —
 *       {@link #readAllLeaves(StorageEngineReader, int)} enforces this and
 *       fails loudly on gaps, because positional consumers (the catalog
 *       matches leaves to metadata fences by position) would silently
 *       mislabel every following leaf.</li>
 *   <li><b>Slot value = LeafDescriptor (PIXD)</b> — a tiny directory of the
 *       leaf's semantic segments (KEYS, per-column BODY/DICT), each entry
 *       carrying segmentId, byteLen and an XXH3-64 content hash. The segment
 *       bytes themselves live in their own CoW-versioned
 *       {@link ProjectionSegmentPage}s, referenced from the owning HOT leaf's
 *       side map under {@code (leafIndex << 8) | segmentId} — references
 *       follow their owning slot across arbitrary split cascades.</li>
 *   <li><b>Blob slots (PIXB)</b> — opaque payloads (the PIXM metadata bytes)
 *       stored via {@link #putBlob}: the value is a small marker with
 *       byteLen + hash, the payload is one segment page, and reads are
 *       length/hash-verified ({@link #verifyBlob}).</li>
 *   <li><b>Assembly</b> — {@link #getLeaf}/{@link #readLeaf}/{@link #readAllLeaves}
 *       reassemble the raw leaf form from the descriptor's segments;
 *       {@code ProjectionIndexSegmentCodec} verifies each segment's hash so
 *       torn or mixed-layout stores fail loudly instead of misparsing.</li>
 *   <li><b>Tombstone vs live-empty</b> — a zero-length slot value is a
 *       tombstone (absent leaf, skipped by enumeration); a live EMPTY leaf is
 *       a descriptor whose segments encode zero rows and still round-trips.
 *       An unchanged segment is carried forward by reference (equal byteLen +
 *       hash → no page write), which is the SLIDING_SNAPSHOT containment
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
   */
  public int probeLiveLeafCount() {
    int count = 0;
    for (long slot = 1; slot <= MAX_PROBED_LEAVES; slot++) {
      final byte[] value = readSlotValueForWrite(slot);
      if (value == null || value.length == 0) {
        return count;
      }
      count++;
    }
    throw new IllegalStateException("More than " + MAX_PROBED_LEAVES
        + " contiguous projection leaves — implausible store, refusing to probe further");
  }

  /** Safety bound for {@link #probeLiveLeafCount} (16M leaves ≈ 16G rows — far beyond scale). */
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
   * <p>Both failure modes are loud: silently dropping a slot write would
   * leave the previous revision's bytes in the slot and corrupt the logical
   * leaf on read. The split path leaves the page in its pre-split state when
   * it fails (atomic rollback), so the thrown exception is a clean abort.
   *
   * @return {@code true} iff the value landed on {@code currentLeaf} without
   *         a split ({@code false} = split happened; caller must re-navigate
   *         for the next key)
   */
  private boolean updateOrSplitInsert(final HOTLeafPage currentLeaf,
      final LeafNavigationResult navResult, final byte[] keyBuf, final int keyLen,
      final int idx, final byte[] sized) {
    if (idx >= 0 && currentLeaf.updateValue(idx, sized)) {
      return true;
    }
    final boolean inserted = trieWriter.handleLeafSplitAndInsert(
        storageEngineWriter, storageEngineWriter.getLog(), currentLeaf, navResult.leafRef(),
        rootReference, navResult.pathNodes(), navResult.pathRefs(), navResult.pathChildIndices(),
        navResult.pathDepth(), keyBuf, keyLen, sized, sized.length);
    prepareIndexPage();
    if (!inserted) {
      final long rawKey = PathKeySerializer.INSTANCE.deserialize(keyBuf, 0, keyLen);
      throw new SirixIOException("Projection HOT slot " + (idx >= 0 ? "update" : "insert")
          + " failed after split for key=" + rawKey + " (" + sized.length
          + " bytes, indexNumber=" + indexNumber + ")");
    }
    return false;
  }

  // ==================== descriptor-based layout (segment directory) ====================
  //
  // Slot key = PathKeySerializer(leafIndex); slot value = LeafDescriptor (PIXD) or a
  // zero-length tombstone; segment bytes live in ProjectionSegmentPages referenced from the
  // HOT leaf's side map under (leafIndex << 8 | segmentId).

  /** Blob marker magic for slot values that reference one opaque segment ("PIXB" LE). */
  private static final int BLOB_MAGIC = 0x42584950;
  private static final byte BLOB_VERSION = 1;
  private static final int BLOB_MARKER_BYTES = 4 + 1 + 4 + 8;
  private static final int BLOB_SEGMENT_ID = 0;

  /**
   * Write one logical projection leaf in the descriptor layout: encode into semantic segments,
   * carry forward every segment whose (byteLen, contentHash) matches the prior revision's
   * descriptor entry (CoW share by reference — no page write, the §3 no-op), write changed and
   * new segments as {@link ProjectionSegmentPage}s, drop side-map refs for segments that no
   * longer exist (real deletes), and store the descriptor as the slot value.
   */
  public void putLeaf(final long leafIndex, final byte[] rawLeafPayload) {
    if (rawLeafPayload == null) {
      throw new IllegalArgumentException("rawLeafPayload must not be null — use tombstoneLeaf");
    }
    putEncodedLeaf(leafIndex, ProjectionIndexSegmentCodec.encode(rawLeafPayload));
  }

  /**
   * {@link #putLeaf(long, byte[])} for a pre-encoded leaf — callers that need the encoded
   * sizes (bench stats, maintenance instrumentation) encode once and hand the result over
   * instead of paying a second codec pass.
   */
  public void putEncodedLeaf(final long leafIndex, final ProjectionIndexSegmentCodec.EncodedLeaf encoded) {
    if (encoded == null) {
      throw new IllegalArgumentException("encoded leaf must not be null — use tombstoneLeaf");
    }
    // Validate the side-map key precondition BEFORE any write: failing after the descriptor
    // slot is written would leave a descriptor whose segments were never attached, and a
    // same-trx retry would carry-forward against that poisoned descriptor (hashes match) and
    // skip attaching everything.
    HOTLeafPage.segmentRefKey(leafIndex, 0);
    final byte[] prior = readSlotValueForWrite(leafIndex);
    final boolean priorIsDescriptor = prior != null && LeafDescriptor.isDescriptor(prior);
    // Write the descriptor slot FIRST so putSegmentPage's owner-slot-residency check holds
    // (ordering within the transaction is crash-irrelevant — everything rides one CoW commit).
    writeSlotValue(leafIndex, encoded.descriptor());

    final byte[] segIds = encoded.segmentIds();
    final byte[][] segments = encoded.segments();
    for (int i = 0; i < segIds.length; i++) {
      final int segId = segIds[i] & 0xFF;
      if (priorIsDescriptor) {
        final int priorEntry = LeafDescriptor.entryIndexOf(prior, segId);
        // Compare against the hash encode() already computed into the NEW descriptor —
        // entries are emitted in the same ascending-id order as segmentIds(), so entry i of
        // the new descriptor describes segments[i]; no second hashing pass over the bytes.
        if (priorEntry >= 0
            && LeafDescriptor.entryByteLen(prior, priorEntry)
                == LeafDescriptor.entryByteLen(encoded.descriptor(), i)
            && LeafDescriptor.entryContentHash(prior, priorEntry)
                == LeafDescriptor.entryContentHash(encoded.descriptor(), i)) {
          continue; // unchanged — the carried-forward reference keeps its resolved key
        }
      }
      putSegmentPage(leafIndex, segId, segments[i]);
    }
    // Real deletes: refs of segments present before but absent now (shrunk leaf, dropped dict).
    if (priorIsDescriptor) {
      dropVanishedSegments(leafIndex, prior, encoded.descriptor());
    }
  }

  /**
   * Tombstone a slot: remove all its segment refs (descriptor leaves AND blob slots — leaving
   * a blob's side-map ref behind would leak its MB-scale segment page into every future
   * fragment), then write the zero-length slot value. A truly absent slot is a free no-op —
   * inserting a tombstone entry would CoW the leaf and emit a fragment for nothing.
   */
  public void tombstoneLeaf(final long leafIndex) {
    final byte[] prior = readSlotValueForWrite(leafIndex);
    if (prior == null) {
      return;
    }
    if (LeafDescriptor.isDescriptor(prior)) {
      final int segCount = LeafDescriptor.segCount(prior);
      for (int i = 0; i < segCount; i++) {
        removeSegmentPage(leafIndex, LeafDescriptor.entrySegmentId(prior, i));
      }
    } else if (prior.length == BLOB_MARKER_BYTES
        && ProjectionIndexLeafCodec.getIntLE(prior, 0) == BLOB_MAGIC) {
      removeSegmentPage(leafIndex, BLOB_SEGMENT_ID);
    }
    if (prior.length > 0) {
      writeSlotValue(leafIndex, TOMBSTONE);
    }
  }

  /**
   * Writer-side leaf read in the descriptor layout: {@code null} for absent or tombstoned
   * slots; otherwise the byte-identical raw scan form assembled from the leaf's segments.
   */
  public byte @Nullable [] getLeaf(final long leafIndex) {
    final byte[] descriptor = readSlotValueForWrite(leafIndex);
    if (!isLiveDescriptor(descriptor, leafIndex, indexNumber)) {
      return null;
    }
    return ProjectionIndexSegmentCodec.assembleRaw(descriptor,
        segmentId -> getSegmentPageBytes(leafIndex, segmentId));
  }

  /** Reader-side counterpart of {@link #getLeaf} for committed revisions. */
  public static byte @Nullable [] readLeaf(final StorageEngineReader reader, final int indexNumber,
      final long leafIndex) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = KEY_BUFFER.get();
      final HOTLeafPage leaf = navigateToSlotLeaf(trieReader, rootRef, leafIndex, keyBuf);
      if (leaf == null) {
        return null;
      }
      final int idx = leaf.findEntry(keyBuf);
      if (idx < 0) {
        return null;
      }
      final byte[] descriptor = leaf.getValue(idx);
      if (!isLiveDescriptor(descriptor, leafIndex, indexNumber)) {
        return null;
      }
      return assembleFromLeafPage(reader, leaf, leafIndex, descriptor);
    }
  }

  /**
   * Walk every descriptor slot of the sub-tree in ascending {@code leafIndex} order and
   * assemble each leaf's raw form. Skips tombstones and the slot-0 blob (metadata). The
   * cursor's topology order can diverge from key order after splits, so results are collected
   * into an ordered map first.
   */
  public static List<byte[]> readAllLeaves(final StorageEngineReader reader, final int indexNumber) {
    return readAllLeaves(reader, indexNumber, true);
  }

  /**
   * {@link #readAllLeaves(StorageEngineReader, int)} with an explicit parallelism switch:
   * committed-revision hydrates assemble leaves across the common pool (phase 2 resolves
   * segment pages by their durable offsets through throwaway references — no page instances
   * or cursors are shared between threads); uncommitted (writer) reads and small stores take
   * the serial in-walk path.
   *
   * <p>Enforces the slot-contiguity invariant (5.1-11): live descriptor slots must be exactly
   * {@code 1..N} — a gap means a mid-store leaf was tombstoned or lost, and positional
   * consumers (the catalog matches leaves to metadata fences by position) would silently
   * mislabel every following leaf, so it throws instead.
   */
  public static List<byte[]> readAllLeaves(final StorageEngineReader reader, final int indexNumber,
      final boolean parallel) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return Collections.emptyList();
    }
    final byte[] minKey = new byte[8];
    final byte[] maxKey = new byte[8];
    Arrays.fill(maxKey, (byte) 0xFF);
    final Long2ObjectRBTreeMap<PendingLeaf> ordered = new Long2ObjectRBTreeMap<>();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIdx = cursor.currentEntryIndex();
        final long leafIndex = leaf.decodeKey8BE(entryIdx) ^ 0x8000_0000_0000_0000L;
        final MemorySegment valueSlice = cursor.currentValueSlice();
        final int valueSize = valueSlice == null ? 0 : (int) valueSlice.byteSize();
        if (valueSize > 0) {
          // Peek the magic from the slice before copying — blob markers are skipped without a
          // heap copy, and anything that is neither descriptor, blob, nor tombstone fails as
          // loudly here as the point reads do (silent skipping would mask exactly the
          // mixed-layout corruption readLeaf is designed to catch).
          final int magic = valueSize >= 4 ? valueSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 0) : 0;
          if (magic == LeafDescriptor.MAGIC) {
            final byte[] descriptor = new byte[valueSize];
            MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, descriptor, 0, valueSize);
            ordered.put(leafIndex, collectPendingLeaf(reader, leaf, leafIndex, descriptor, parallel));
          } else if (magic != BLOB_MAGIC) {
            throw new IllegalStateException("Slot " + leafIndex + " holds neither a leaf descriptor, a"
                + " blob marker, nor a tombstone (" + valueSize + " bytes) — mixed storage layouts in"
                + " one sub-tree (indexNumber=" + indexNumber + ")");
          }
        }
        cursor.advance();
      }
    }
    // Contiguity (5.1-11): live slots must be exactly 1..N.
    long expected = 1;
    for (final long slot : ordered.keySet()) {
      if (slot != expected) {
        throw new IllegalStateException("Projection leaf slots are not contiguous: expected slot "
            + expected + ", found " + slot + " (indexNumber=" + indexNumber
            + ") — positional hydration would mislabel every following leaf");
      }
      expected++;
    }
    final PendingLeaf[] pending = ordered.values().toArray(new PendingLeaf[0]);
    final byte[][] assembled = new byte[pending.length][];
    int unassembled = 0;
    for (int i = 0; i < pending.length; i++) {
      if (pending[i].assembled() != null) {
        assembled[i] = pending[i].assembled();
      } else {
        unassembled++;
      }
    }
    if (unassembled > 0) {
      assemblePending(reader, pending, assembled, parallel && unassembled >= PARALLEL_ASSEMBLE_MIN);
    }
    final ArrayList<byte[]> out = new ArrayList<>(assembled.length);
    Collections.addAll(out, assembled);
    return out;
  }

  /** Minimum deferred leaves before phase-2 assembly fans out to the common pool. */
  private static final int PARALLEL_ASSEMBLE_MIN = 64;

  /**
   * One live descriptor slot awaiting assembly. For the parallel path only the segments'
   * durable offset keys are carried out of the cursor walk (no page instances); leaves whose
   * refs are unresolved (uncommitted, this-transaction) or whose walk requested serial mode
   * are assembled inline and carry the result instead.
   */
  private record PendingLeaf(long leafIndex, byte[] descriptor, int[] segmentIds,
      long[] segmentOffsets, byte @Nullable [] assembled) {
  }

  /**
   * Descriptor-tier row count (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.5, P5b): sum
   * of {@code rowCount} over the live descriptors at slots {@code 1..expectedLeafCount} —
   * one trie range walk, ZERO segment-page reads. Enforces the same contiguity invariant
   * (5.1-11) and the same truncated-store check as {@link #readAllLeaves}: a slot gap or a
   * live-descriptor count differing from the metadata's {@code expectedLeafCount} throws
   * (callers fail soft), so descriptor-tier answers can never disagree with what a full
   * hydrate would have counted.
   *
   * @return the total row count across all live leaves (0 for an empty store)
   * @throws IllegalStateException on contiguity/count violations or a non-descriptor,
   *         non-blob, non-tombstone slot value
   */
  public static long sumLiveDescriptorRows(final StorageEngineReader reader, final int indexNumber,
      final int expectedLeafCount) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      if (expectedLeafCount != 0) {
        throw new IllegalStateException("Projection sub-tree missing but metadata declares "
            + expectedLeafCount + " leaves (indexNumber=" + indexNumber + ")");
      }
      return 0L;
    }
    final byte[] minKey = new byte[8];
    final byte[] maxKey = new byte[8];
    Arrays.fill(maxKey, (byte) 0xFF);
    long totalRows = 0;
    long expectedSlot = 1;
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
         HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int entryIdx = cursor.currentEntryIndex();
        final long slot = leaf.decodeKey8BE(entryIdx) ^ 0x8000_0000_0000_0000L;
        final MemorySegment valueSlice = cursor.currentValueSlice();
        final int valueSize = valueSlice == null ? 0 : (int) valueSlice.byteSize();
        if (valueSize > 0) {
          final int magic = valueSize >= 4 ? valueSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 0) : 0;
          if (magic == LeafDescriptor.MAGIC) {
            if (slot != expectedSlot) {
              throw new IllegalStateException("Projection leaf slots are not contiguous: expected "
                  + expectedSlot + ", found " + slot + " (indexNumber=" + indexNumber + ")");
            }
            expectedSlot++;
            final byte version = valueSlice.get(ValueLayout.JAVA_BYTE, 4);
            if (version != LeafDescriptor.VERSION || valueSize < LeafDescriptor.MIN_BYTES) {
              throw new IllegalStateException("Corrupt descriptor at slot " + slot + " (version "
                  + version + ", " + valueSize + " bytes, indexNumber=" + indexNumber + ")");
            }
            // rowCount sits at a fixed offset — read it straight off the slice, no copy.
            totalRows += valueSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 5);
          } else if (magic != BLOB_MAGIC) {
            throw new IllegalStateException("Slot " + slot + " holds neither a leaf descriptor, a"
                + " blob marker, nor a tombstone (" + valueSize + " bytes) — mixed storage layouts"
                + " in one sub-tree (indexNumber=" + indexNumber + ")");
          }
        }
        cursor.advance();
      }
    }
    final long liveLeaves = expectedSlot - 1;
    if (liveLeaves != expectedLeafCount) {
      throw new IllegalStateException("Descriptor count " + liveLeaves + " != metadata leafCount "
          + expectedLeafCount + " (indexNumber=" + indexNumber + ") — truncated or stale store");
    }
    return totalRows;
  }

  private static PendingLeaf collectPendingLeaf(final StorageEngineReader reader, final HOTLeafPage leaf,
      final long leafIndex, final byte[] descriptor, final boolean parallel) {
    LeafDescriptor.validate(descriptor);
    final int segCount = LeafDescriptor.segCount(descriptor);
    final int[] segIds = new int[segCount];
    final long[] segOffsets = new long[segCount];
    boolean allResolved = true;
    for (int i = 0; i < segCount; i++) {
      segIds[i] = LeafDescriptor.entrySegmentId(descriptor, i);
      final PageReference ref = leaf.getPageReference(HOTLeafPage.segmentRefKey(leafIndex, segIds[i]));
      segOffsets[i] = ref == null ? Constants.NULL_ID_LONG : ref.getKey();
      allResolved &= segOffsets[i] != Constants.NULL_ID_LONG;
    }
    if (!parallel || !allResolved) {
      return new PendingLeaf(leafIndex, descriptor, segIds, segOffsets,
          assembleFromLeafPage(reader, leaf, leafIndex, descriptor));
    }
    return new PendingLeaf(leafIndex, descriptor, segIds, segOffsets, null);
  }

  private static void assemblePending(final StorageEngineReader reader, final PendingLeaf[] pending,
      final byte[][] out, final boolean parallel) {
    if (!parallel) {
      for (int i = 0; i < pending.length; i++) {
        if (out[i] == null) {
          out[i] = assembleFromOffsets(reader, pending[i]);
        }
      }
      return;
    }
    final int n = pending.length;
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
                if (out[i] == null) {
                  out[i] = assembleFromOffsets(reader, pending[i]);
                }
              }
            }
          };
        }
        invokeAll(subs);
      }
    });
  }

  /** Offset-based assembly: resolve each segment by durable key through a throwaway reference. */
  private static byte[] assembleFromOffsets(final StorageEngineReader reader, final PendingLeaf pl) {
    return ProjectionIndexSegmentCodec.assembleRaw(pl.descriptor(), segmentId -> {
      final int[] ids = pl.segmentIds();
      for (int i = 0; i < ids.length; i++) {
        if (ids[i] == segmentId) {
          final long offset = pl.segmentOffsets()[i];
          if (offset == Constants.NULL_ID_LONG) {
            return null;
          }
          final PageReference ref = new PageReference();
          ref.setKey(offset);
          final ProjectionSegmentPage page = reader.readProjectionSegmentPage(ref);
          return page == null ? null : page.getDataBytes();
        }
      }
      return null;
    });
  }

  /** Assemble one leaf using the side map of the HOT page that holds its slot. */
  private static byte[] assembleFromLeafPage(final StorageEngineReader reader, final HOTLeafPage leaf,
      final long leafIndex, final byte[] descriptor) {
    return ProjectionIndexSegmentCodec.assembleRaw(descriptor, segmentId -> {
      final PageReference ref = leaf.getPageReference(HOTLeafPage.segmentRefKey(leafIndex, segmentId));
      if (ref == null) {
        return null;
      }
      final ProjectionSegmentPage page = reader.readProjectionSegmentPage(ref);
      return page == null ? null : page.getDataBytes();
    });
  }

  // ==================== blob slots (slot-0 metadata payload) ====================

  /**
   * Store an opaque payload (the PIXM metadata bytes, which can reach MBs once per-leaf fences
   * scale) at {@code slotKey}: the payload becomes ONE segment page; the slot value is a tiny
   * PIXB marker carrying byteLen + XXH3-64 for integrity (segment pages have no checksum of
   * their own). Whole-blob last-writer-wins.
   */
  public void putBlob(final long slotKey, final byte[] payload) {
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    HOTLeafPage.segmentRefKey(slotKey, BLOB_SEGMENT_ID);
    final long hash = ProjectionIndexSegmentCodec.contentHash(payload);
    // Carry-forward: an unchanged blob (the steady-state metadata case where fences did not
    // move) is a true no-op — the prior marker already carries byteLen + hash.
    final byte[] prior = readSlotValueForWrite(slotKey);
    if (prior != null && prior.length == BLOB_MARKER_BYTES
        && ProjectionIndexLeafCodec.getIntLE(prior, 0) == BLOB_MAGIC
        && ProjectionIndexLeafCodec.getIntLE(prior, 5) == payload.length
        && ProjectionIndexLeafCodec.getLongLE(prior, 9) == hash) {
      return;
    }
    final byte[] marker = new byte[BLOB_MARKER_BYTES];
    LeafDescriptor.putIntLE(marker, 0, BLOB_MAGIC);
    marker[4] = BLOB_VERSION;
    LeafDescriptor.putIntLE(marker, 5, payload.length);
    LeafDescriptor.putLongLE(marker, 9, hash);
    writeSlotValue(slotKey, marker);
    putSegmentPage(slotKey, BLOB_SEGMENT_ID, payload);
  }

  /** Writer-side blob read; {@code null} when absent/tombstoned. Verifies length + hash. */
  public byte @Nullable [] getBlob(final long slotKey) {
    final byte[] marker = readSlotValueForWrite(slotKey);
    if (marker == null || marker.length == 0) {
      return null;
    }
    return verifyBlob(marker, getSegmentPageBytes(slotKey, BLOB_SEGMENT_ID), slotKey);
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
      final byte[] marker = leaf.getValue(idx);
      if (marker == null || marker.length == 0) {
        return null;
      }
      final PageReference ref = leaf.getPageReference(HOTLeafPage.segmentRefKey(slotKey, BLOB_SEGMENT_ID));
      if (ref == null) {
        return verifyBlob(marker, null, slotKey);
      }
      final ProjectionSegmentPage page = reader.readProjectionSegmentPage(ref);
      return verifyBlob(marker, page == null ? null : page.getDataBytes(), slotKey);
    }
  }

  private static byte[] verifyBlob(final byte[] marker, final byte @Nullable [] payload, final long slotKey) {
    if (marker.length != BLOB_MARKER_BYTES
        || ProjectionIndexLeafCodec.getIntLE(marker, 0) != BLOB_MAGIC || marker[4] != BLOB_VERSION) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a blob marker");
    }
    final int expectedLen = ProjectionIndexLeafCodec.getIntLE(marker, 5);
    final long expectedHash = ProjectionIndexLeafCodec.getLongLE(marker, 9);
    if (payload == null || payload.length != expectedLen
        || ProjectionIndexSegmentCodec.contentHash(payload) != expectedHash) {
      throw new IllegalStateException("Blob at slot " + slotKey + " failed length/hash verification ("
          + (payload == null ? "missing segment" : payload.length + " bytes") + ", expected "
          + expectedLen + ")");
    }
    return payload;
  }

  /**
   * Diagnostic: the durable offset key of the segment page referenced for
   * {@code (ownerSlotKey, segmentId)} at the reader's revision, or {@link Constants#NULL_ID_LONG}
   * when absent/unresolved. Equal keys across revisions prove the page was shared by reference
   * (the carry-forward no-op), not rewritten — the observable for containment tests and the P8
   * update-bytes measurements.
   */
  public static long segmentPageOffset(final StorageEngineReader reader, final int indexNumber,
      final long ownerSlotKey, final int segmentId) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return Constants.NULL_ID_LONG;
    }
    final long refKey = HOTLeafPage.segmentRefKey(ownerSlotKey, segmentId);
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

  /**
   * Shared slot-value classification for leaf reads: {@code null}/zero-length → absent
   * (tombstone), descriptor → {@code true}, anything else → loud mixed-layout error. One
   * authority so writer- and reader-side reads of the same corrupt slot fail identically.
   */
  private static boolean isLiveDescriptor(final byte @Nullable [] value, final long slotKey,
      final int indexNumber) {
    if (value == null || value.length == 0) {
      return false;
    }
    if (!LeafDescriptor.isDescriptor(value)) {
      throw new IllegalStateException("Slot " + slotKey + " does not hold a leaf descriptor — mixed"
          + " storage layouts in one sub-tree (indexNumber=" + indexNumber + ")");
    }
    return true;
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
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
    final HOTLeafPage leaf = navResult.leaf();
    if (leaf.put(keyBuf, value)) {
      return;
    }
    final int idx = leaf.findEntry(keyBuf);
    updateOrSplitInsert(leaf, navResult, keyBuf, keyLen, idx, value);
  }

  /** Remove side-map refs for segment ids present in {@code prior} but absent in {@code next}. */
  private void dropVanishedSegments(final long leafIndex, final byte[] prior, final byte[] next) {
    final int priorCount = LeafDescriptor.segCount(prior);
    for (int i = 0; i < priorCount; i++) {
      final int segId = LeafDescriptor.entrySegmentId(prior, i);
      if (LeafDescriptor.entryIndexOf(next, segId) < 0) {
        removeSegmentPage(leafIndex, segId);
      }
    }
  }

  /**
   * Attach an encoded segment as its own CoW-versioned {@link ProjectionSegmentPage},
   * referenced from the side map of the HOT leaf that owns slot {@code ownerSlotKey}.
   *
   * <p>Segment-directory storage primitive (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.3,
   * introduced with the P1 page-layer machinery): the side-map key is
   * {@code (ownerSlotKey << 8) | segmentId}, matching the owner-slot routing in
   * {@code HOTLeafPage#moveSegmentRefsAfterSplit} — the reference lives on whichever page
   * holds the owning slot, across arbitrary split cascades. The page is written (and its
   * durable offset key assigned) inside the commit descent; until then it exists only
   * in-memory on the reference, so a rollback simply never writes it.
   *
   * <p>Re-attaching the same {@code (ownerSlotKey, segmentId)} replaces the reference —
   * whole-segment last-writer-wins. An unchanged segment is shared across revisions by NOT
   * re-attaching it (the carried-forward reference keeps its resolved key).
   */
  public void putSegmentPage(final long ownerSlotKey, final int segmentId, final byte[] bytes) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    final long refKey = HOTLeafPage.segmentRefKey(ownerSlotKey, segmentId);
    final byte[] keyBuf = KEY_BUFFER.get();
    final int keyLen = PathKeySerializer.INSTANCE.serialize(ownerSlotKey, keyBuf, 0);
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keyBuf, keyLen);
    // The owner slot MUST already exist on the leaf: split routing
    // (HOTLeafPage#moveSegmentRefsAfterSplit) and read navigation both key off owner-slot
    // residency, so a ref attached without its owning slot would be permanently orphaned on
    // whichever leaf covers the key at attach time — durably committed but unreachable after
    // the next split. Callers write the owning slot (descriptor/chunk) before its segments.
    if (navResult.leaf().findEntry(keyBuf) < 0) {
      throw new IllegalStateException("putSegmentPage: owner slot " + ownerSlotKey
          + " does not exist (indexNumber=" + indexNumber + ") — write the owning slot before"
          + " attaching its segments, or the reference cannot follow it across splits.");
    }
    final PageReference ref = new PageReference();
    ref.setPage(new ProjectionSegmentPage(bytes));
    navResult.leaf().setPageReference(refKey, ref);
  }

  /**
   * Remove the segment reference for {@code (ownerSlotKey, segmentId)} — a real delete
   * (shrunk or tombstoned leaf), replacing the old zero-length-chunk tombstone convention.
   * No-op when absent.
   */
  public void removeSegmentPage(final long ownerSlotKey, final int segmentId) {
    if (rootReference == null) {
      throw new SirixIOException("Projection HOT index not initialised for indexNumber=" + indexNumber);
    }
    final long refKey = HOTLeafPage.segmentRefKey(ownerSlotKey, segmentId);
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
  public byte @Nullable [] getSegmentPageBytes(final long ownerSlotKey, final int segmentId) {
    final long refKey = HOTLeafPage.segmentRefKey(ownerSlotKey, segmentId);
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
    final ProjectionSegmentPage page = storageEngineWriter.readProjectionSegmentPage(ref);
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
      final long ownerSlotKey, final int segmentId) {
    final PageReference rootRef = rootReference(reader, indexNumber);
    if (rootRef == null) {
      return null;
    }
    final long refKey = HOTLeafPage.segmentRefKey(ownerSlotKey, segmentId);
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
      final ProjectionSegmentPage page = reader.readProjectionSegmentPage(ref);
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
