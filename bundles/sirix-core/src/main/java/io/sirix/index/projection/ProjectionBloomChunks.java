/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * Bounded streaming store for the projection's contiguous string-fingerprint acceleration.
 *
 * <p>
 * Fingerprints are stored only as fixed {@link #CHUNK_LEAVES}-row-group blobs. A streaming bulk
 * load writes each full chunk immediately and retains only one reusable reference array per string
 * column.
 * </p>
 *
 * <p>
 * A column's small, versioned manifest occupies slot {@code 16 + column} and is published only after
 * every chunk is durable in the transaction. The manifest-plus-chunks representation is the sole
 * persisted block format. A missing or malformed manifest disables block pruning for that column;
 * a missing or malformed chunk keeps every leaf in that span, preserving the Bloom filter's
 * no-false-negative contract.
 * </p>
 */
public final class ProjectionBloomChunks {

  /** Row groups per persisted fingerprint chunk. */
  static final int CHUNK_LEAVES = 256;

  /**
   * First reserved chunk slot. Row-group composite slots end at {@code 2^40 + 65535}, fence chunks
   * start at {@code 2^42}, and this namespace occupies less than {@code 2^44}. It therefore cannot
   * collide with either family and stays well inside the side-map owner-key limit
   * ({@code |ownerSlotKey| < 2^47}).
   */
  static final long CHUNK_SLOT_BASE = 1L << 43;

  /** One 16-bit chunk id covers the shared 2^24 row-group limit at 256 leaves per chunk. */
  private static final int MAX_CHUNKS = ProjectionIndexHOTStorage.MAX_ROW_GROUPS / CHUNK_LEAVES;

  /** Manifest magic, {@code "PBMF"} in little-endian byte order. */
  private static final int MANIFEST_MAGIC = 0x464D4250;
  private static final byte MANIFEST_VERSION = 0;
  private static final int MANIFEST_BYTES = Integer.BYTES + 1 + 4 * Integer.BYTES;

  /** Referenced chunk payloads held at once by one pruning call. */
  static final int FETCH_WINDOW_CHUNKS = 4;

  /** Fixed owner-thread scratch; payload references are cleared before every window is released. */
  private static final ThreadLocal<FetchScratch> FETCH_SCRATCH = ThreadLocal.withInitial(FetchScratch::new);

  private ProjectionBloomChunks() {}

  /** Number of fixed chunks needed for {@code rowGroupCount}. */
  /**
   * Drops every Bloom byte a column owns, for a column that is ceasing to be a string kind.
   *
   * <p>
   * Needed because {@link #rewriteTouchedChunks} SKIPS any column whose kind is not a string kind —
   * so once a column has been flipped to {@code COLUMN_KIND_STRING_GLOBAL} the ordinary maintenance
   * path will never look at its chunks again, and they become bytes that are stored, paid for, and
   * unreachable. **[M]** at 1M that is 1.82 MB across the four ClickBench fat columns, silently.
   * A storage lever that leaks bytes is not a storage lever, so the flip drops them explicitly.
   * </p>
   *
   * @param physicalRowGroupCount how many row groups the index holds, which bounds the chunk ids
   * @return the number of blobs tombstoned
   */
  static int dropColumn(final ProjectionIndexHOTStorage storage, final int column,
      final int physicalRowGroupCount) {
    int dropped = 0;
    final long manifestSlot = ProjectionIndexHOTStorage.bloomBlockSlotKey(column);
    if (storage.getBlob(manifestSlot) != null) {
      storage.tombstoneBlob(manifestSlot);
      dropped++;
    }
    final int chunks = chunkCount(physicalRowGroupCount);
    for (int chunkId = 0; chunkId < chunks; chunkId++) {
      final long chunkSlot = chunkSlotKey(column, chunkId);
      if (storage.getBlob(chunkSlot) != null) {
        storage.tombstoneBlob(chunkSlot);
        dropped++;
      }
    }
    return dropped;
  }

  static int chunkCount(final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    return (rowGroupCount + CHUNK_LEAVES - 1) / CHUNK_LEAVES;
  }

  /**
   * Collision-free HOT blob key for one column/chunk pair.
   *
   * <p>
   * The column occupies the high 14 useful bits of the low namespace and the chunk id the low 16.
   * Addition is intentional and safe because {@link #CHUNK_SLOT_BASE}'s low 43 bits are zero.
   * </p>
   */
  static long chunkSlotKey(final int column, final int chunkId) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    if (chunkId < 0 || chunkId >= MAX_CHUNKS) {
      throw new IllegalArgumentException("chunkId out of range [0, " + MAX_CHUNKS + "): " + chunkId);
    }
    final long key = CHUNK_SLOT_BASE + ((long) column << 16) + chunkId;
    // Keep the owner-key proof executable rather than relying on the constants' documentary math.
    HOTLeafPage.overflowPageRefKey(key, 0);
    return key;
  }

  private static void checkRowGroupCount(final int rowGroupCount) {
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException(
          "rowGroupCount out of range [0, " + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + "]: " + rowGroupCount);
    }
  }

  /** Fixed manifest payload; the enclosing PIXB blob supplies length and XXH3 verification. */
  private static byte[] manifest(final int rowGroupCount) {
    return manifest(rowGroupCount, rowGroupCount);
  }

  /**
   * Manifest over a physical high-water mark. Incremental maintenance may unlink a split leaf without
   * renumbering its suffix, so the Bloom chunks remain physical-slot indexed while metadata and query
   * masks remain live/logical-count indexed.
   */
  private static byte[] manifest(final int rowGroupCount, final int physicalRowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    checkRowGroupCount(physicalRowGroupCount);
    if (physicalRowGroupCount < rowGroupCount) {
      throw new IllegalArgumentException(
          "physical row-group count " + physicalRowGroupCount + " is smaller than live count " + rowGroupCount);
    }
    final byte[] bytes = new byte[MANIFEST_BYTES];
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, 0, MANIFEST_MAGIC);
    bytes[Integer.BYTES] = MANIFEST_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1, rowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1 + Integer.BYTES, physicalRowGroupCount);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1 + 2 * Integer.BYTES, CHUNK_LEAVES);
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, Integer.BYTES + 1 + 3 * Integer.BYTES,
        chunkCount(physicalRowGroupCount));
    return bytes;
  }

  /** Parsed manifest, or {@code null}; a negative expected count accepts any valid live count. */
  private static @Nullable Manifest parseManifest(final byte @Nullable [] bytes, final int expectedRowGroupCount) {
    if (bytes == null || bytes.length != MANIFEST_BYTES
        || ProjectionIndexRowGroupCodec.getIntLE(bytes, 0) != MANIFEST_MAGIC
        || bytes[Integer.BYTES] != MANIFEST_VERSION) {
      return null;
    }
    final int rowGroupCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1);
    final int physicalRowGroupCount = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1 + Integer.BYTES);
    final int chunkLeaves = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1 + 2 * Integer.BYTES);
    final int chunks = ProjectionIndexRowGroupCodec.getIntLE(bytes, Integer.BYTES + 1 + 3 * Integer.BYTES);
    if (rowGroupCount < 0 || rowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS
        || (expectedRowGroupCount >= 0 && rowGroupCount != expectedRowGroupCount)
        || physicalRowGroupCount < rowGroupCount || physicalRowGroupCount > ProjectionIndexHOTStorage.MAX_ROW_GROUPS
        || chunkLeaves != CHUNK_LEAVES || chunks != chunkCount(physicalRowGroupCount)) {
      return null;
    }
    return new Manifest(rowGroupCount, physicalRowGroupCount, chunks);
  }

  /** Whether {@code bytes} is the exact manifest for {@code expectedRowGroupCount}. */
  static boolean isManifest(final byte @Nullable [] bytes, final int expectedRowGroupCount) {
    return parseManifest(bytes, expectedRowGroupCount) != null;
  }

  private record Manifest(int rowGroupCount, int physicalRowGroupCount, int chunkCount) {
  }

  private static boolean isStringKind(final byte kind) {
    return kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
  }

  /**
   * Immutable per-column pruning evidence. A manifest-backed instance retains only primitive durable
   * locators (plus at most one small inline tail); payloads are fetched and released in a fixed
   * {@link #FETCH_WINDOW_CHUNKS}-chunk window by {@link #prune}.
   */
  public static final class ColumnEvidence {
    private final ProjectionIndexHOTStorage.BlobLocators chunks;
    private final int rowGroupCount;
    private final int physicalRowGroupCount;
    private final int @Nullable [] logicalByPhysical;

    private ColumnEvidence(final ProjectionIndexHOTStorage.BlobLocators chunks, final int rowGroupCount,
        final int physicalRowGroupCount) {
      this.chunks = chunks;
      this.rowGroupCount = rowGroupCount;
      this.physicalRowGroupCount = physicalRowGroupCount;
      this.logicalByPhysical = null;
    }

    private ColumnEvidence(final ColumnEvidence source, final int[] logicalByPhysical) {
      this.chunks = source.chunks;
      this.rowGroupCount = source.rowGroupCount;
      this.physicalRowGroupCount = source.physicalRowGroupCount;
      this.logicalByPhysical = logicalByPhysical;
    }

    private static ColumnEvidence chunked(final ProjectionIndexHOTStorage.BlobLocators chunks, final int rowGroupCount,
        final int physicalRowGroupCount) {
      return new ColumnEvidence(chunks, rowGroupCount, physicalRowGroupCount);
    }

    /** Resident bytes charged to the decoded-handle cache. */
    private long retainedBytes() {
      return 48L + chunks.retainedBytes();
    }

    /**
     * Clear bits proved absent by this evidence.
     *
     * @return newly cleared bits
     */
    int prune(final long hash, final long[] keep, final int leafCount,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (leafCount != rowGroupCount) {
        throw new IllegalArgumentException("leafCount " + leafCount + " != evidence rowGroupCount " + rowGroupCount);
      }
      if (keep == null || keep.length < ((leafCount + 63) >>> 6) || fetcher == null) {
        throw new IllegalArgumentException("keep/fetcher must cover the evidence leaf count");
      }
      return pruneChunks(new long[] {hash}, new long[][] {keep}, fetcher, 0, chunks.size());
    }

    /**
     * {@link #prune} for MANY literals in ONE walk over the evidence: {@code keeps[j]} is narrowed by
     * {@code hashes[j]}, every chunk fetched and validated once and every leaf's fingerprint located
     * once for all literals. A disjunction of equalities or a planner pricing candidate values pays
     * the chunk walk once instead of once per literal (measured: one walk ≈ one {@link #prune}).
     *
     * @param chunkFrom first chunk (inclusive), {@code chunkTo} exclusive — callers that split the
     *        walk over threads hand each a disjoint chunk range; chunks own disjoint 256-leaf ranges,
     *        so two ranges never touch the same keep word
     * @return newly cleared bits summed over every mask
     */
    int pruneMany(final long[] hashes, final long[][] keeps, final int leafCount,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher, final int chunkFrom, final int chunkTo) {
      if (leafCount != rowGroupCount) {
        throw new IllegalArgumentException("leafCount " + leafCount + " != evidence rowGroupCount " + rowGroupCount);
      }
      if (hashes == null || keeps == null || hashes.length != keeps.length || fetcher == null) {
        throw new IllegalArgumentException("hashes/keeps must pair up and a fetcher is required");
      }
      final int words = (leafCount + 63) >>> 6;
      for (final long[] keep : keeps) {
        if (keep == null || keep.length < words) {
          throw new IllegalArgumentException("every keep mask must cover the evidence leaf count");
        }
      }
      if (chunkFrom < 0 || chunkTo > chunks.size() || chunkFrom > chunkTo) {
        throw new IllegalArgumentException("chunk range [" + chunkFrom + ", " + chunkTo + ") outside 0.." + chunks.size());
      }
      if (hashes.length == 0) {
        return 0;
      }
      return pruneChunks(hashes, keeps, fetcher, chunkFrom, chunkTo);
    }

    /** How many 256-leaf chunks this evidence spans (the unit {@link #pruneMany} splits over). */
    int chunkCount() {
      return chunks.size();
    }

    private int pruneChunks(final long[] hashes, final long[][] keeps,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher, final int chunkFrom, final int chunkTo) {
      final ProjectionIndexHOTStorage.BlobLocators localChunks = chunks;
      final FetchScratch scratch = acquireScratch();
      int dropped = 0;
      try {
        for (int windowBase = chunkFrom; windowBase < chunkTo; windowBase += FETCH_WINDOW_CHUNKS) {
          scratch.clearPayloadsAndOffsets();
          final int inWindow = Math.min(FETCH_WINDOW_CHUNKS, chunkTo - windowBase);
          boolean needsFetch = false;
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            final int expectedLeaves = expectedChunkLeaves(chunkId, physicalRowGroupCount);
            if (localChunks.inlinePayload(chunkId) == null && localChunks.offset(chunkId) != Constants.NULL_ID_LONG
                && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(localChunks.length(chunkId),
                    expectedLeaves)) {
              scratch.offsets[j] = localChunks.offset(chunkId);
              needsFetch = true;
            }
          }
          boolean fetchSucceeded = !needsFetch;
          if (needsFetch) {
            try {
              fetcher.fetchRange(scratch.offsets, 0, FETCH_WINDOW_CHUNKS, scratch.payloads);
              fetchSucceeded = true;
            } catch (final RuntimeException unreadable) {
              // Optional evidence. Referenced chunks in this window stay kept; an inline tail in
              // the same window is still independently useful below.
            }
          }
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            final int expectedLeaves = expectedChunkLeaves(chunkId, physicalRowGroupCount);
            final byte[] inline = localChunks.inlinePayload(chunkId);
            final byte[] block;
            if (inline != null) {
              block = ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(inline, expectedLeaves)
                  ? inline
                  : null;
            } else {
              final byte[] fetched = fetchSucceeded
                  ? scratch.payloads[j]
                  : null;
              block = referencedBlockIsValid(fetched, localChunks.length(chunkId), localChunks.hash(chunkId),
                  expectedLeaves)
                      ? fetched
                      : null;
            }
            if (block != null) {
              dropped += pruneBlock(block, chunkId * CHUNK_LEAVES, expectedLeaves, hashes, keeps, logicalByPhysical);
            }
          }
          // The payload window is not live across the next fetch. This explicit clear matters for
          // owner-thread scratch, which otherwise promotes the last pages into a long-lived thread.
          Arrays.fill(scratch.payloads, null);
        }
        return dropped;
      } finally {
        releaseScratch(scratch);
      }
    }
  }

  private static int expectedChunkLeaves(final int chunkId, final int rowGroupCount) {
    return Math.min(CHUNK_LEAVES, rowGroupCount - chunkId * CHUNK_LEAVES);
  }

  private static boolean referencedBlockIsValid(final byte @Nullable [] block, final int expectedLength,
      final long expectedHash, final int expectedLeaves) {
    return block != null && block.length == expectedLength
        && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(expectedLength, expectedLeaves)
        && ProjectionIndexColumnSegmentCodec.contentHash(block) == expectedHash
        && ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(block, expectedLeaves);
  }

  private static int pruneBlock(final byte[] block, final int firstLeaf, final int leafCount, final long[] hashes,
      final long[][] keeps, final int @Nullable [] logicalByPhysical) {
    int dropped = 0;
    final int literals = hashes.length;
    for (int localLeaf = 0; localLeaf < leafCount; localLeaf++) {
      final int physicalLeaf = firstLeaf + localLeaf;
      final int leaf = logicalByPhysical == null
          ? physicalLeaf
          : physicalLeaf + 1 < logicalByPhysical.length
              ? logicalByPhysical[physicalLeaf + 1]
              : -1;
      if (leaf < 0) {
        // Recycled physical slot: it has no logical keep bit and contributes no negative evidence.
        continue;
      }
      final int word = leaf >>> 6;
      final long mask = 1L << (leaf & 63);
      if (literals == 1) {
        // The single-literal path keeps the allocation-free probe it always had.
        final long[] keep = keeps[0];
        if ((keep[word] & mask) != 0
            && !ProjectionIndexColumnSegmentCodec.bloomBlockMayContainHashValidated(block, localLeaf, leafCount,
                hashes[0])) {
          keep[word] &= ~mask;
          dropped++;
        }
        continue;
      }
      // Locate the leaf's fingerprint words once, then probe every literal against them.
      long packed = 0L;
      boolean located = false;
      for (int j = 0; j < literals; j++) {
        final long[] keep = keeps[j];
        if ((keep[word] & mask) == 0) {
          continue;
        }
        if (!located) {
          packed = ProjectionIndexColumnSegmentCodec.bloomBlockLeafWords(block, localLeaf, leafCount);
          located = true;
          if (packed == ProjectionIndexColumnSegmentCodec.NO_FINGERPRINT) {
            break;
          }
        }
        if (!ProjectionIndexColumnSegmentCodec.bloomWordsMayContainHash(block, packed, hashes[j])) {
          keep[word] &= ~mask;
          dropped++;
        }
      }
    }
    return dropped;
  }

  private static FetchScratch acquireScratch() {
    final FetchScratch scratch = FETCH_SCRATCH.get();
    if (scratch.inUse) {
      // Re-entrant pruning is not a production shape, but correctness must not depend on it. The
      // bounded fallback retains the same four-payload ceiling.
      final FetchScratch nested = new FetchScratch();
      nested.inUse = true;
      nested.clearPayloadsAndOffsets();
      return nested;
    }
    scratch.inUse = true;
    scratch.clearPayloadsAndOffsets();
    return scratch;
  }

  private static void releaseScratch(final FetchScratch scratch) {
    scratch.clearPayloadsAndOffsets();
    scratch.inUse = false;
  }

  /** Package-private regression probe: owner-thread scratch must never retain fetched pages. */
  static boolean fetchScratchIsClearForTesting() {
    final FetchScratch scratch = FETCH_SCRATCH.get();
    if (scratch.inUse) {
      return false;
    }
    for (final byte[] payload : scratch.payloads) {
      if (payload != null) {
        return false;
      }
    }
    return true;
  }

  private static final class FetchScratch {
    private final long[] offsets = new long[FETCH_WINDOW_CHUNKS];
    private final byte[][] payloads = new byte[FETCH_WINDOW_CHUNKS][];
    private boolean inUse;

    private void clearPayloadsAndOffsets() {
      Arrays.fill(offsets, Constants.NULL_ID_LONG);
      Arrays.fill(payloads, null);
    }
  }

  /**
   * Read every string column's chunk manifest from a committed projection. Corruption is deliberately
   * local: an unreadable manifest disables the column acceleration; an unreadable chunk leaves only
   * its 256-row-group span unpruned.
   */
  static ColumnEvidence @Nullable [] read(final StorageEngineReader reader, final int indexNumber,
      final byte[] columnKinds, final int rowGroupCount) {
    checkRowGroupCount(rowGroupCount);
    if (reader == null || columnKinds == null || columnKinds.length > RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("reader and a bounded columnKinds array are required");
    }
    final ColumnEvidence[] evidence = new ColumnEvidence[columnKinds.length];
    final ProjectionIndexHOTStorage.BlobLocators roots;
    try {
      roots = ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber,
          ProjectionIndexHOTStorage.bloomBlockSlotKey(0), columnKinds.length);
    } catch (final IllegalStateException unreadable) {
      return null;
    }
    boolean any = false;
    for (int c = 0; c < columnKinds.length; c++) {
      if (!isStringKind(columnKinds[c])) {
        continue;
      }
      final ColumnEvidence column = readColumn(reader, indexNumber, roots, c, rowGroupCount);
      if (column != null) {
        evidence[c] = column;
        any = true;
      }
    }
    return any
        ? evidence
        : null;
  }

  static ColumnEvidence @Nullable [] reorder(final ColumnEvidence @Nullable [] evidence, final int[] physicalOrder) {
    if (evidence == null) {
      return null;
    }
    final int rowGroupCount = physicalOrder.length;
    int physicalRowGroupCount = rowGroupCount;
    for (final ColumnEvidence column : evidence) {
      if (column != null) {
        physicalRowGroupCount = column.physicalRowGroupCount;
        break;
      }
    }
    final int[] logicalByPhysical = new int[physicalRowGroupCount + 1];
    Arrays.fill(logicalByPhysical, -1);
    boolean identity = physicalRowGroupCount == rowGroupCount;
    for (int logical = 0; logical < rowGroupCount; logical++) {
      final int physical = physicalOrder[logical];
      if (physical < 1 || physical > physicalRowGroupCount || logicalByPhysical[physical] >= 0) {
        throw new IllegalStateException("physical Bloom order is not a permutation at leaf " + physical);
      }
      logicalByPhysical[physical] = logical;
      identity &= physical == logical + 1;
    }
    if (identity) {
      return evidence;
    }
    final ColumnEvidence[] reordered = evidence.clone();
    for (int column = 0; column < reordered.length; column++) {
      if (reordered[column] != null) {
        reordered[column] = new ColumnEvidence(reordered[column], logicalByPhysical);
      }
    }
    return reordered;
  }

  private static @Nullable ColumnEvidence readColumn(final StorageEngineReader reader, final int indexNumber,
      final ProjectionIndexHOTStorage.BlobLocators roots, final int column, final int rowGroupCount) {
    final byte[] root = roots.inlinePayload(column);
    final Manifest manifest = parseManifest(root, rowGroupCount);
    if (manifest == null) {
      return null;
    }
    try {
      return ColumnEvidence.chunked(ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber,
          chunkSlotKey(column, 0), manifest.chunkCount()), rowGroupCount, manifest.physicalRowGroupCount());
    } catch (final IllegalStateException unreadable) {
      return null;
    }
  }

  /** Resident primitive/inline evidence bytes charged to the catalog's decoded-handle weight. */
  static long retainedBytes(final ColumnEvidence @Nullable [] evidence) {
    if (evidence == null) {
      return 0L;
    }
    long bytes = 32L + (long) evidence.length * Long.BYTES;
    for (final ColumnEvidence column : evidence) {
      if (column != null) {
        bytes += column.retainedBytes();
      }
    }
    return bytes;
  }

  static RewriteStats rewriteTouchedChunks(final ProjectionIndexHOTStorage storage, final byte[] columnKinds,
      final int rowGroupCount, final LongSet changedLeafSlots) {
    if (columnKinds == null || changedLeafSlots == null) {
      throw new NullPointerException("columnKinds and changedLeafSlots are required");
    }
    final long[] allColumns = new long[(columnKinds.length + Long.SIZE - 1) / Long.SIZE];
    Arrays.fill(allColumns, -1L);
    if (columnKinds.length % Long.SIZE != 0) {
      allColumns[allColumns.length - 1] = (1L << (columnKinds.length % Long.SIZE)) - 1L;
    }
    final Long2ObjectOpenHashMap<long[]> changedColumnsByLeaf = new Long2ObjectOpenHashMap<>();
    for (final LongIterator iterator = changedLeafSlots.iterator(); iterator.hasNext();) {
      changedColumnsByLeaf.put(iterator.nextLong(), allColumns);
    }
    return rewriteTouchedChunks(storage, columnKinds, rowGroupCount, changedColumnsByLeaf, true);
  }

  static RewriteStats rewriteTouchedChunks(final ProjectionIndexHOTStorage storage, final byte[] columnKinds,
      final int rowGroupCount, final Long2ObjectMap<long[]> changedColumnsByLeaf, final boolean rowGroupCountChanged) {
    return rewriteTouchedChunks(storage, columnKinds, rowGroupCount, rowGroupCount, changedColumnsByLeaf,
        rowGroupCountChanged);
  }

  static RewriteStats rewriteTouchedChunks(final ProjectionIndexHOTStorage storage, final byte[] columnKinds,
      final int rowGroupCount, final int physicalRowGroupCount, final Long2ObjectMap<long[]> changedColumnsByLeaf,
      final boolean rowGroupCountChanged) {
    checkRowGroupCount(rowGroupCount);
    checkRowGroupCount(physicalRowGroupCount);
    if (physicalRowGroupCount < rowGroupCount) {
      throw new IllegalArgumentException(
          "physical row-group count " + physicalRowGroupCount + " is smaller than live count " + rowGroupCount);
    }
    if (storage == null || columnKinds == null || changedColumnsByLeaf == null) {
      throw new NullPointerException("storage, columnKinds, and changedColumnsByLeaf are required");
    }
    if (changedColumnsByLeaf.isEmpty()) {
      return new RewriteStats(0, 0, 0L, 0L);
    }
    boolean hasBloomColumn = false;
    for (int column = 0; column < columnKinds.length; column++) {
      if (isStringKind(columnKinds[column])
          && (rowGroupCountChanged || anyLeafSelectsColumn(changedColumnsByLeaf, column))) {
        hasBloomColumn = true;
        break;
      }
    }
    if (!hasBloomColumn) {
      for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
        final long slot = iterator.nextLong();
        if (slot < 1 || slot > physicalRowGroupCount) {
          throw new IllegalArgumentException("changed leaf slot out of range: " + slot);
        }
      }
      return new RewriteStats(0, 0, 0L, 0L);
    }
    final IntOpenHashSet chunkIds = new IntOpenHashSet();
    for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
      final long slot = iterator.nextLong();
      if (slot < 1 || slot > physicalRowGroupCount) {
        throw new IllegalArgumentException("changed leaf slot out of range: " + slot);
      }
      chunkIds.add((int) ((slot - 1L) / CHUNK_LEAVES));
    }
    int rowGroupsRead = 0;
    int chunksWritten = 0;
    long bytesRead = 0L;
    long bytesWritten = 0L;
    for (final int chunkId : chunkIds) {
      final int firstLeaf = chunkId * CHUNK_LEAVES + 1;
      final int leafCount = Math.min(CHUNK_LEAVES, physicalRowGroupCount - firstLeaf + 1);
      for (int c = 0; c < columnKinds.length; c++) {
        if (!isStringKind(columnKinds[c])
            || (!rowGroupCountChanged && !chunkSelectsColumn(changedColumnsByLeaf, firstLeaf, leafCount, c))) {
          continue;
        }
        final long chunkSlot = chunkSlotKey(c, chunkId);
        final byte[] prior = storage.getBlob(chunkSlot);
        if (prior != null)
          bytesRead += prior.length;
        final int priorLeafCount = ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(prior);
        final byte[][] priorSlices = priorLeafCount >= 0 && priorLeafCount <= leafCount
            ? ProjectionIndexColumnSegmentCodec.copyBloomBlockSlices(prior, priorLeafCount)
            : null;
        final byte[][] slices = priorSlices == null
            ? new byte[leafCount][]
            : Arrays.copyOf(priorSlices, leafCount);
        for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
          final long slot = iterator.nextLong();
          final int localLeaf = Math.toIntExact(slot - firstLeaf);
          if (localLeaf < 0 || localLeaf >= leafCount
              || (!rowGroupCountChanged && !columnSelected(changedColumnsByLeaf.get(slot), c))) {
            continue;
          }
          final byte[] segment =
              storage.getVerifiedColumnSegment(slot, ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(c),
                  ProjectionIndexColumnSegmentCodec.SEG_KIND_STRING_BLOOM);
          slices[localLeaf] = segment;
          rowGroupsRead++;
          if (segment != null)
            bytesRead += segment.length;
        }
        final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(slices, leafCount);
        if (Arrays.equals(prior, block)) {
          continue;
        }
        if (block == null) {
          storage.tombstoneBlob(chunkSlot);
        } else {
          storage.putBlob(chunkSlot, block);
          bytesWritten += block.length;
        }
        chunksWritten++;
      }
    }
    for (int c = 0; c < columnKinds.length; c++) {
      if (isStringKind(columnKinds[c]) && (rowGroupCountChanged || anyLeafSelectsColumn(changedColumnsByLeaf, c))) {
        final long manifestSlot = ProjectionIndexHOTStorage.bloomBlockSlotKey(c);
        final byte[] nextManifest = manifest(rowGroupCount, physicalRowGroupCount);
        final byte[] priorManifest = storage.getBlob(manifestSlot);
        if (priorManifest != null)
          bytesRead += priorManifest.length;
        final Manifest parsedPrior = parseManifest(priorManifest, -1);
        final int nextChunkCount = chunkCount(physicalRowGroupCount);
        if (parsedPrior != null && parsedPrior.chunkCount() > nextChunkCount) {
          for (int chunkId = nextChunkCount; chunkId < parsedPrior.chunkCount(); chunkId++) {
            storage.tombstoneBlob(chunkSlotKey(c, chunkId));
            chunksWritten++;
          }
        }
        if (!Arrays.equals(priorManifest, nextManifest)) {
          storage.putBlob(manifestSlot, nextManifest);
          bytesWritten += nextManifest.length;
        }
      }
    }
    return new RewriteStats(rowGroupsRead, chunksWritten, bytesRead, bytesWritten);
  }

  private static boolean chunkSelectsColumn(final Long2ObjectMap<long[]> changedColumnsByLeaf, final int firstLeaf,
      final int leafCount, final int column) {
    final long lastLeaf = (long) firstLeaf + leafCount - 1L;
    for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
      final long slot = iterator.nextLong();
      if (slot >= firstLeaf && slot <= lastLeaf && columnSelected(changedColumnsByLeaf.get(slot), column)) {
        return true;
      }
    }
    return false;
  }

  private static boolean anyLeafSelectsColumn(final Long2ObjectMap<long[]> changedColumnsByLeaf, final int column) {
    for (final long[] words : changedColumnsByLeaf.values()) {
      if (columnSelected(words, column)) {
        return true;
      }
    }
    return false;
  }

  private static boolean columnSelected(final long[] words, final int column) {
    if (words == null) {
      throw new IllegalStateException("changed leaf has no column mask");
    }
    final int word = column >>> 6;
    return word < words.length && (words[word] & (1L << (column & 63))) != 0L;
  }

  record RewriteStats(int rowGroupsRead, int chunksWritten, long bytesRead, long bytesWritten) {
  }

  /**
   * Owner-confined streaming writer. Accepting a row group allocates nothing: the encoded Bloom
   * segment references are placed into reusable 256-entry arrays. Only a persisted chunk payload and
   * the final fixed manifests allocate.
   */
  public static final class Writer {
    private ColumnBuffer @Nullable [] columns;
    private int @Nullable [] stringColumns;
    private int acceptedRowGroups;
    private int pendingLeaves;
    private int nextChunkId;
    private byte @Nullable [] publicationKinds;
    private boolean chunksFinished;

    /** Add one encoded row group, in exact ascending id order. */
    public void append(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final int rowGroupId,
        final ProjectionIndexHOTStorage storage) {
      if (chunksFinished) {
        throw new IllegalStateException("Bloom chunk writer is already finished");
      }
      if (rowGroupId != acceptedRowGroups + 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
        throw new IllegalArgumentException("rowGroupId must be the next id " + (acceptedRowGroups + 1)
            + " within MAX_ROW_GROUPS=" + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + ", got " + rowGroupId);
      }
      if (encoded == null || storage == null) {
        throw new NullPointerException("encoded and storage must not be null");
      }
      ensureColumns(encoded.descriptor());
      final ColumnBuffer[] buffers = columns;
      final int slot = pendingLeaves;
      final int[] ids = encoded.columnSegmentIds();
      final byte[][] segments = encoded.segments();
      if (ids.length != segments.length) {
        throw new IllegalArgumentException("encoded segment ids/bytes must be index-aligned");
      }
      for (int i = 0; i < ids.length; i++) {
        final int id = ids[i];
        if (id <= 0 || id >= ProjectionIndexColumnSegmentCodec.DICT_HASH_SEGMENT_BASE
            || id % ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN != 0) {
          continue;
        }
        final int column = id / ProjectionIndexColumnSegmentCodec.SEGMENTS_PER_COLUMN - 1;
        if (column < 0 || column >= buffers.length) {
          throw new IllegalStateException(
              "Bloom segment id " + id + " names out-of-shape column " + column + " of " + buffers.length);
        }
        final ColumnBuffer buffer = buffers[column];
        if (buffer == null) {
          throw new IllegalStateException("Bloom segment id " + id + " names non-string column " + column);
        }
        if (buffer.segments[slot] != null) {
          throw new IllegalStateException(
              "Encoded row group " + rowGroupId + " carries two Bloom segments for column " + column);
        }
        buffer.segments[slot] = segments[i];
      }
      acceptedRowGroups++;
      pendingLeaves++;
      if (pendingLeaves == CHUNK_LEAVES) {
        flush(storage, CHUNK_LEAVES);
      }
    }

    private void ensureColumns(final byte[] descriptor) {
      final int columnCount = RowGroupDescriptor.columnCount(descriptor);
      final ColumnBuffer[] existing = columns;
      if (existing != null) {
        if (existing.length != columnCount) {
          throw new IllegalStateException(
              "Projection column count changed during streaming build: " + existing.length + " -> " + columnCount);
        }
        return;
      }
      final ColumnBuffer[] created = new ColumnBuffer[columnCount];
      int stringColumnCount = 0;
      for (int c = 0; c < columnCount; c++) {
        final byte kind = RowGroupDescriptor.kind(descriptor, c);
        if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
            || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          created[c] = new ColumnBuffer();
          stringColumnCount++;
        }
      }
      final int[] compactColumns = new int[stringColumnCount];
      int compactIndex = 0;
      for (int c = 0; c < created.length; c++) {
        if (created[c] != null) {
          compactColumns[compactIndex++] = c;
        }
      }
      columns = created;
      stringColumns = compactColumns;
    }

    private void flush(final ProjectionIndexHOTStorage storage, final int leafCount) {
      final ColumnBuffer[] buffers = columns;
      final int[] compactColumns = stringColumns;
      if (buffers != null && compactColumns != null) {
        for (final int c : compactColumns) {
          final ColumnBuffer buffer = buffers[c];
          final long slotKey = chunkSlotKey(c, nextChunkId);
          final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(buffer.segments, leafCount);
          if (block != null) {
            storage.putBlob(slotKey, block);
          }
          Arrays.fill(buffer.segments, 0, leafCount, null);
        }
      }
      nextChunkId++;
      pendingLeaves = 0;
    }

    /**
     * Persist a partial tail and capture the final publication shape for this virgin build.
     */
    public void finishChunks(final ProjectionIndexHOTStorage storage, final int rowGroupCount,
        final byte[] columnKinds) {
      if (chunksFinished) {
        throw new IllegalStateException("Bloom chunks already finished");
      }
      if (storage == null || columnKinds == null || columnKinds.length > RowGroupDescriptor.MAX_COLUMNS) {
        throw new IllegalArgumentException("storage and a bounded columnKinds array are required");
      }
      if (rowGroupCount != acceptedRowGroups) {
        throw new IllegalArgumentException("rowGroupCount " + rowGroupCount + " != accepted " + acceptedRowGroups);
      }
      final ColumnBuffer[] buffers = columns;
      if (buffers != null && buffers.length != columnKinds.length) {
        throw new IllegalArgumentException(
            "columnKinds length " + columnKinds.length + " != streamed descriptor width " + buffers.length);
      }
      publicationKinds = columnKinds.clone();
      if (pendingLeaves != 0) {
        flush(storage, pendingLeaves);
      }
      if (nextChunkId != chunkCount(rowGroupCount)) {
        throw new IllegalStateException("Persisted " + nextChunkId + " Bloom chunks for " + rowGroupCount
            + " row groups; expected " + chunkCount(rowGroupCount));
      }
      chunksFinished = true;
    }

    /**
     * Publish one manifest per string column after {@link #finishChunks}; this is the visibility point
     * for all chunks in the virgin build.
     */
    public void publishManifests(final ProjectionIndexHOTStorage storage, final int rowGroupCount) {
      if (!chunksFinished || rowGroupCount != acceptedRowGroups) {
        throw new IllegalStateException("finishChunks must complete for the same rowGroupCount before publication");
      }
      final byte[] kinds = publicationKinds;
      if (kinds == null) {
        throw new IllegalStateException("finishChunks did not capture publication shape");
      }
      // PBMF is the visibility point and therefore strictly last.
      for (int c = 0; c < kinds.length; c++) {
        if (isStringKind(kinds[c])) {
          storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(c), manifest(rowGroupCount));
        }
      }
    }

    /** Drop references to the bounded pending segment set after finish or abort. */
    public void release() {
      final ColumnBuffer[] buffers = columns;
      if (buffers != null) {
        for (final ColumnBuffer buffer : buffers) {
          if (buffer != null) {
            Arrays.fill(buffer.segments, null);
          }
        }
      }
      columns = null;
      stringColumns = null;
      publicationKinds = null;
      pendingLeaves = 0;
    }

    /** Pending leaf window size; package-visible bounded-retention test/diagnostic telemetry. */
    int pendingLeavesForTesting() {
      return pendingLeaves;
    }

    /** Number of encoded segment references currently retained by the pending leaf window. */
    int retainedSegmentReferencesForTesting() {
      int retained = 0;
      final ColumnBuffer[] buffers = columns;
      final int[] compactColumns = stringColumns;
      if (buffers == null || compactColumns == null) {
        return 0;
      }
      for (final int column : compactColumns) {
        final byte[][] segments = buffers[column].segments;
        for (int leaf = 0; leaf < pendingLeaves; leaf++) {
          if (segments[leaf] != null) {
            retained++;
          }
        }
      }
      return retained;
    }
  }

  private static final class ColumnBuffer {
    private final byte[][] segments = new byte[CHUNK_LEAVES][];
  }
}
