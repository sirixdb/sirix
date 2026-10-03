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
 * A column's fingerprints live in two shapes. Every <em>sealed</em> chunk of {@link #CHUNK_LEAVES}
 * row groups is one contiguous block blob ({@link #chunkSlotKey}); the <em>open</em> chunk, the one
 * still filling at the physical high-water mark, keeps one small blob per row group
 * ({@link #tailSlotKey}) holding that row group's raw {@code STRING_BLOOM} segment. A commit that
 * changes one open row group's fingerprint therefore writes that row group's blob only; the chunk
 * is folded into its block once, in the commit that completes it, and the folded row groups' tail
 * blobs are tombstoned in the same commit. A streaming bulk load writes each full chunk
 * immediately, its partial tail as tail blobs, and retains only one reusable reference array per
 * string column.
 * </p>
 *
 * <p>
 * A column's small, versioned manifest occupies slot {@code 16 + column} and is published only
 * after every chunk and tail blob is durable in the transaction. Readers derive the sealed/open
 * split from the manifest's physical row-group count, so a revision reads exactly the shape its
 * commit wrote. A missing or malformed manifest disables block pruning for that column; a missing
 * or malformed block keeps every leaf in its span and a missing or malformed tail blob keeps its
 * one leaf, preserving the Bloom filter's no-false-negative contract.
 * </p>
 */
public final class ProjectionBloomChunks {

  /** Row groups per persisted fingerprint chunk. */
  static final int CHUNK_LEAVES = 256;

  /**
   * First reserved tail slot. Set-summary slots occupy {@code 2^44 + column} and flag-summary chunks
   * start at {@code 2^45}; tail keys are {@code 2^44 + 2^43 + (column << 24) + rowGroupId}, which
   * stays below {@code 2^45} for every admissible column and row group and inside the side-map
   * owner-key limit ({@code |ownerSlotKey| < 2^47}).
   */
  static final long TAIL_SLOT_BASE = (1L << 44) + (1L << 43);

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
  /** Version 1: sealed chunks are blocks, the open chunk is per-row-group tail blobs. */
  private static final byte MANIFEST_VERSION = 1;
  private static final int MANIFEST_BYTES = Integer.BYTES + 1 + 4 * Integer.BYTES;

  /**
   * Referenced BLOCK payloads held at once by one pruning call ({@code
   * -Dsirix.projection.bloomFetchWindowChunks}, clamped to 1–64, default 16). Every window is one
   * ranged fetch on a fresh read transaction, so a wider window trades a few hundred KiB of
   * owner-thread scratch for proportionally fewer transaction opens per column. The open chunk's
   * tails are NOT paginated by this: they are one window of their own ({@link #CHUNK_LEAVES}
   * single-leaf payloads, at most ~515 KiB — less than this window of blocks), because a block-sized
   * window would open up to 16 read transactions for one chunk.
   */
  static final int FETCH_WINDOW_CHUNKS =
      Math.max(1, Math.min(64, Integer.getInteger("sirix.projection.bloomFetchWindowChunks", 16)));

  /** Fixed owner-thread scratch; payload references are cleared before every window is released. */
  private static final ThreadLocal<FetchScratch> FETCH_SCRATCH = ThreadLocal.withInitial(FetchScratch::new);

  private ProjectionBloomChunks() {}

  /**
   * Drops every Bloom byte a column owns, for a column that is ceasing to be a string kind.
   *
   * <p>
   * Needed because {@link #rewriteTouchedChunks} SKIPS any column whose kind is not a string kind —
   * so once a column has been flipped to {@code COLUMN_KIND_STRING_GLOBAL} the ordinary maintenance
   * path will never look at its chunks again, and they become bytes that are stored, paid for, and
   * unreachable. **[M]** at 1M that is 1.82 MB across the four ClickBench fat columns, silently. A
   * storage lever that leaks bytes is not a storage lever, so the flip drops them explicitly.
   * </p>
   *
   * @param physicalRowGroupCount how many row groups the index holds, which bounds the chunk ids
   * @return the number of blobs tombstoned
   */
  static int dropColumn(final ProjectionIndexHOTStorage storage, final int column, final int physicalRowGroupCount) {
    int dropped = 0;
    final long manifestSlot = ProjectionIndexHOTStorage.bloomBlockSlotKey(column);
    if (storage.getRawSlot(manifestSlot) != null) {
      storage.tombstoneBlob(manifestSlot);
      dropped++;
    }
    final int chunks = chunkCount(physicalRowGroupCount);
    for (int chunkId = 0; chunkId < chunks; chunkId++) {
      final long chunkSlot = chunkSlotKey(column, chunkId);
      if (storage.getRawSlot(chunkSlot) != null) {
        storage.tombstoneBlob(chunkSlot);
        dropped++;
      }
    }
    final int openFirst = sealedChunkCount(physicalRowGroupCount) * CHUNK_LEAVES + 1;
    for (int rowGroupId = openFirst; rowGroupId <= physicalRowGroupCount; rowGroupId++) {
      final long tailSlot = tailSlotKey(column, rowGroupId);
      if (storage.getRawSlot(tailSlot) != null) {
        storage.tombstoneBlob(tailSlot);
        dropped++;
      }
    }
    return dropped;
  }

  /** Chunks whose {@link #CHUNK_LEAVES} row groups all exist: the ones persisted as blocks. */
  static int sealedChunkCount(final int physicalRowGroupCount) {
    checkRowGroupCount(physicalRowGroupCount);
    return physicalRowGroupCount / CHUNK_LEAVES;
  }

  /** Row groups of the open chunk (0 when the high-water mark ends exactly on a chunk boundary). */
  static int openLeafCount(final int physicalRowGroupCount) {
    checkRowGroupCount(physicalRowGroupCount);
    return physicalRowGroupCount % CHUNK_LEAVES;
  }

  /** Collision-free HOT blob key for one open-chunk row group's fingerprint of one column. */
  static long tailSlotKey(final int column, final int rowGroupId) {
    if (column < 0 || column >= RowGroupDescriptor.MAX_COLUMNS) {
      throw new IllegalArgumentException("column out of range [0, " + RowGroupDescriptor.MAX_COLUMNS + "): " + column);
    }
    if (rowGroupId < 1 || rowGroupId > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException(
          "rowGroupId out of range [1, " + ProjectionIndexHOTStorage.MAX_ROW_GROUPS + "]: " + rowGroupId);
    }
    final long key = TAIL_SLOT_BASE + ((long) column << 24) + rowGroupId;
    HOTLeafPage.overflowPageRefKey(key, 0);
    return key;
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
   * locators plus the open chunk's bounded inline fingerprints; referenced block payloads are fetched
   * and released by {@link #prune} in a fixed {@link #FETCH_WINDOW_CHUNKS}-chunk window, and the open
   * chunk's referenced tails in one window of their own.
   */
  public static final class ColumnEvidence {
    /** Locators of the sealed chunks' blocks, index = chunk id. */
    private final ProjectionIndexHOTStorage.BlobLocators chunks;
    /** Locators of the open chunk's tail blobs, index = row group offset inside the open chunk. */
    private final ProjectionIndexHOTStorage.BlobLocators tails;
    private final int rowGroupCount;
    private final int physicalRowGroupCount;
    private final int @Nullable [] logicalByPhysical;

    private ColumnEvidence(final ProjectionIndexHOTStorage.BlobLocators chunks,
        final ProjectionIndexHOTStorage.BlobLocators tails, final int rowGroupCount, final int physicalRowGroupCount) {
      this.chunks = chunks;
      this.tails = tails;
      this.rowGroupCount = rowGroupCount;
      this.physicalRowGroupCount = physicalRowGroupCount;
      this.logicalByPhysical = null;
    }

    private ColumnEvidence(final ColumnEvidence source, final int[] logicalByPhysical) {
      this.chunks = source.chunks;
      this.tails = source.tails;
      this.rowGroupCount = source.rowGroupCount;
      this.physicalRowGroupCount = source.physicalRowGroupCount;
      this.logicalByPhysical = logicalByPhysical;
    }

    private static ColumnEvidence chunked(final ProjectionIndexHOTStorage.BlobLocators chunks,
        final ProjectionIndexHOTStorage.BlobLocators tails, final int rowGroupCount, final int physicalRowGroupCount) {
      if (chunks.size() != sealedChunkCount(physicalRowGroupCount)
          || tails.size() != openLeafCount(physicalRowGroupCount)) {
        throw new IllegalArgumentException("evidence locators do not match the physical row-group count");
      }
      return new ColumnEvidence(chunks, tails, rowGroupCount, physicalRowGroupCount);
    }

    /** Resident bytes charged to the decoded-handle cache. */
    private long retainedBytes() {
      return 56L + chunks.retainedBytes() + tails.retainedBytes();
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
      return pruneChunks(new long[] {hash}, new long[][] {keep}, fetcher, 0, chunkCount());
    }

    /**
     * {@link #prune} for MANY literals in ONE walk over the evidence: {@code keeps[j]} is narrowed by
     * {@code hashes[j]}, every chunk fetched and validated once and every leaf's fingerprint located
     * once for all literals. A disjunction of equalities or a planner pricing candidate values pays the
     * chunk walk once instead of once per literal (measured: one walk ≈ one {@link #prune}).
     *
     * @param chunkFrom first chunk (inclusive), {@code chunkTo} exclusive — callers that split the walk
     *        over threads hand each a disjoint chunk range; chunks own disjoint 256-leaf ranges, so two
     *        ranges never touch the same keep word
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
      if (chunkFrom < 0 || chunkTo > chunkCount() || chunkFrom > chunkTo) {
        throw new IllegalArgumentException(
            "chunk range [" + chunkFrom + ", " + chunkTo + ") outside 0.." + chunkCount());
      }
      if (hashes.length == 0) {
        return 0;
      }
      return pruneChunks(hashes, keeps, fetcher, chunkFrom, chunkTo);
    }

    /**
     * How many chunks this evidence spans (the unit {@link #pruneMany} splits over): every sealed block
     * plus, when the high-water mark is inside a chunk, the open chunk of tail blobs. What each shape
     * COSTS is {@link #weightedRangeBounds}'s model, not this count.
     */
    int chunkCount() {
      return chunks.size() + (tails.size() > 0
          ? 1
          : 0);
    }

    /**
     * Cut {@code [0, chunkCount())} into {@code ranges} contiguous chunk ranges of comparable WORK
     * rather than of comparable index width, as ascending bounds with {@code [0] == 0} and the last
     * entry {@code chunkCount()}. A range may come out empty; every chunk falls in exactly one.
     *
     * <p>
     * A sealed block is one page read. The open chunk is one per REFERENCED tail, because only those
     * have a side page to fetch — an inline tail is carried in the locator and is probed in place, so
     * an all-inline open chunk weighs nothing and the even cut is used. A referenced open chunk cannot
     * be divided without giving up its single ranged fetch, so an even cut over the index can hand the
     * range that happens to hold it {@link #CHUNK_LEAVES} page reads while a sibling range of whole
     * blocks does sixteen, and the only remedy is to give that range fewer blocks. Which shape wins
     * depends on how many of its tails are referenced, so both are priced and the lower peak is taken.
     * </p>
     */
    int[] weightedRangeBounds(final int ranges) {
      if (ranges < 1) {
        throw new IllegalArgumentException("ranges must be positive: " + ranges);
      }
      final int sealed = chunks.size();
      final int count = chunkCount();
      final int openTails = tails.size();
      int openWeight = 0;
      for (int tail = 0; tail < openTails; tail++) {
        if (tailNeedsFetch(tails, tail)) {
          openWeight++;
        }
      }
      final int evenLen = (count + ranges - 1) / ranges;
      // The open chunk is the last index, and under the even cut it shares its range with whatever
      // blocks precede it THERE — which is not the last range whenever that one comes out empty. So
      // price the range that actually holds index count - 1 rather than assuming it is the last.
      final int openRangeBlocks = count - 1 - (count - 1) / evenLen * evenLen;
      final int evenPeak = Math.max(evenLen, openRangeBlocks + openWeight);
      final boolean isolateOpenChunk =
          openWeight > 0 && ranges > 1 && Math.max((sealed + ranges - 2) / (ranges - 1), openWeight) < evenPeak;
      final int blockRanges = isolateOpenChunk
          ? ranges - 1
          : ranges;
      final int units = isolateOpenChunk
          ? sealed
          : count;
      final int len = (units + blockRanges - 1) / blockRanges;
      final int[] bounds = new int[ranges + 1];
      for (int r = 1; r <= blockRanges; r++) {
        bounds[r] = Math.min(r * len, units);
      }
      bounds[ranges] = count;
      return bounds;
    }

    /** Physical chunk boundaries must also be disjoint logical mask-word boundaries. */
    boolean parallelPruningIsSafe() {
      return logicalByPhysical == null;
    }

    private int pruneChunks(final long[] hashes, final long[][] keeps,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher, final int chunkFrom, final int chunkTo) {
      final ProjectionIndexHOTStorage.BlobLocators localChunks = chunks;
      final int sealed = localChunks.size();
      final int blockTo = Math.min(chunkTo, sealed);
      final FetchScratch scratch = acquireScratch();
      int dropped = 0;
      try {
        if (chunkTo > sealed && chunkFrom <= sealed) {
          dropped += pruneOpenChunk(hashes, keeps, fetcher, scratch);
        }
        for (int windowBase = chunkFrom; windowBase < blockTo; windowBase += FETCH_WINDOW_CHUNKS) {
          scratch.clearPayloadsAndOffsets();
          final int inWindow = Math.min(FETCH_WINDOW_CHUNKS, blockTo - windowBase);
          boolean needsFetch = false;
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            if (localChunks.inlinePayload(chunkId) == null && localChunks.offset(chunkId) != Constants.NULL_ID_LONG
                && ProjectionIndexColumnSegmentCodec.bloomBlockLengthCouldBeWellFormed(localChunks.length(chunkId),
                    CHUNK_LEAVES)) {
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
              // Optional evidence. Referenced chunks in this window stay kept; an inline block in
              // the same window is still independently useful below.
            }
          }
          for (int j = 0; j < inWindow; j++) {
            final int chunkId = windowBase + j;
            final byte[] inline = localChunks.inlinePayload(chunkId);
            final byte[] block;
            if (inline != null) {
              block = ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(inline, CHUNK_LEAVES)
                  ? inline
                  : null;
            } else {
              final byte[] fetched = fetchSucceeded
                  ? scratch.payloads[j]
                  : null;
              block =
                  referencedBlockIsValid(fetched, localChunks.length(chunkId), localChunks.hash(chunkId), CHUNK_LEAVES)
                      ? fetched
                      : null;
            }
            if (block != null) {
              dropped += pruneBlock(block, chunkId * CHUNK_LEAVES, CHUNK_LEAVES, hashes, keeps, logicalByPhysical);
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

    /**
     * Prune with the open chunk's tail blobs: one raw fingerprint segment per row group, all of them in
     * ONE ranged fetch. An inline tail is probed in place; a referenced tail is length- and
     * hash-verified first. A missing or malformed tail keeps its one leaf.
     *
     * <p>
     * The chunk holds at most {@link #CHUNK_LEAVES} tails, so one window covers it whatever the block
     * window is. That matters because each {@code fetchRange} is one read transaction: a block-sized
     * window would open one per {@link #FETCH_WINDOW_CHUNKS} tails for the single chunk a sealed block
     * gets in one, and would leave the open chunk's range in the parallel split far heavier than its
     * siblings.
     * </p>
     */
    private int pruneOpenChunk(final long[] hashes, final long[][] keeps,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher, final FetchScratch scratch) {
      final ProjectionIndexHOTStorage.BlobLocators localTails = tails;
      final int openFirstLeaf = chunks.size() * CHUNK_LEAVES;
      final int count = localTails.size();
      scratch.clearTailPayloadsAndOffsets();
      boolean needsFetch = false;
      for (int tail = 0; tail < count; tail++) {
        if (tailNeedsFetch(localTails, tail)) {
          scratch.tailOffsets[tail] = localTails.offset(tail);
          needsFetch = true;
        }
      }
      boolean fetchSucceeded = !needsFetch;
      if (needsFetch) {
        try {
          fetcher.fetchRange(scratch.tailOffsets, 0, CHUNK_LEAVES, scratch.tailPayloads);
          fetchSucceeded = true;
        } catch (final RuntimeException unreadable) {
          // Optional evidence: referenced tails stay kept.
        }
      }
      int dropped = 0;
      for (int tail = 0; tail < count; tail++) {
        final byte[] inline = localTails.inlinePayload(tail);
        final byte[] segment;
        if (inline != null) {
          segment = inline;
        } else {
          final byte[] fetched = fetchSucceeded
              ? scratch.tailPayloads[tail]
              : null;
          segment = fetched != null && fetched.length == localTails.length(tail)
              && ProjectionIndexColumnSegmentCodec.contentHash(fetched) == localTails.hash(tail)
                  ? fetched
                  : null;
        }
        if (segment != null) {
          dropped += pruneTail(segment, openFirstLeaf + tail, hashes, keeps, logicalByPhysical);
        }
      }
      // The payload window is not live past this call. The explicit clear matters for owner-thread
      // scratch, which otherwise promotes the last pages into a long-lived thread.
      Arrays.fill(scratch.tailPayloads, null);
      return dropped;
    }
  }

  private static int pruneTail(final byte[] segment, final int physicalLeaf, final long[] hashes, final long[][] keeps,
      final int @Nullable [] logicalByPhysical) {
    final int leaf = logicalByPhysical == null
        ? physicalLeaf
        : physicalLeaf + 1 < logicalByPhysical.length
            ? logicalByPhysical[physicalLeaf + 1]
            : -1;
    if (leaf < 0) {
      return 0;
    }
    final long packed = ProjectionIndexColumnSegmentCodec.bloomSegmentWords(segment);
    if (packed == ProjectionIndexColumnSegmentCodec.NO_FINGERPRINT) {
      return 0;
    }
    final int word = leaf >>> 6;
    final long mask = 1L << (leaf & 63);
    int dropped = 0;
    for (int j = 0; j < hashes.length; j++) {
      final long[] keep = keeps[j];
      if ((keep[word] & mask) != 0
          && !ProjectionIndexColumnSegmentCodec.bloomWordsMayContainHash(segment, packed, hashes[j])) {
        keep[word] &= ~mask;
        dropped++;
      }
    }
    return dropped;
  }

  /**
   * Whether this open-chunk tail has a payload to fetch. An inline tail is carried in its locator, so
   * it costs no page read and the open-chunk prune probes it in place; a length outside a single
   * leaf's bound can never be a usable fingerprint, so it is not fetched either. The prune and the
   * range split read this one predicate, so what the walk fetches and what the split prices cannot
   * drift apart.
   */
  /**
   * How many of {@code chunkId}'s tail slots a column with published physical count
   * {@code priorPhysical} can own: its whole span when no mark parsed ({@code priorPhysical < 0}),
   * the published open span when this IS the chunk at that mark, and none otherwise.
   */
  private static int tailScanBound(final int priorPhysical, final int chunkId, final int leafCount) {
    if (priorPhysical < 0) {
      return leafCount;
    }
    return chunkId == sealedChunkCount(priorPhysical)
        ? openLeafCount(priorPhysical)
        : 0;
  }

  private static boolean tailNeedsFetch(final ProjectionIndexHOTStorage.BlobLocators tails, final int tail) {
    return tails.inlinePayload(tail) == null && tails.offset(tail) != Constants.NULL_ID_LONG && tails.length(tail) > 0
        && tails.length(tail) <= ProjectionIndexColumnSegmentCodec.maxBloomBlockBytes(1);
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
        if ((keep[word] & mask) != 0 && !ProjectionIndexColumnSegmentCodec.bloomBlockMayContainHashValidated(block,
            localLeaf, leafCount, hashes[0])) {
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
      nested.clearTailPayloadsAndOffsets();
      return nested;
    }
    scratch.inUse = true;
    scratch.clearPayloadsAndOffsets();
    scratch.clearTailPayloadsAndOffsets();
    return scratch;
  }

  private static void releaseScratch(final FetchScratch scratch) {
    scratch.clearPayloadsAndOffsets();
    scratch.clearTailPayloadsAndOffsets();
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
    for (final byte[] payload : scratch.tailPayloads) {
      if (payload != null) {
        return false;
      }
    }
    return true;
  }

  private static final class FetchScratch {
    private final long[] offsets = new long[FETCH_WINDOW_CHUNKS];
    private final byte[][] payloads = new byte[FETCH_WINDOW_CHUNKS][];
    /** The open chunk is one window, so its scratch is sized by the chunk, not by the block window. */
    private final long[] tailOffsets = new long[CHUNK_LEAVES];
    private final byte[][] tailPayloads = new byte[CHUNK_LEAVES][];
    private boolean inUse;

    private void clearPayloadsAndOffsets() {
      Arrays.fill(offsets, Constants.NULL_ID_LONG);
      Arrays.fill(payloads, null);
    }

    private void clearTailPayloadsAndOffsets() {
      Arrays.fill(tailOffsets, Constants.NULL_ID_LONG);
      Arrays.fill(tailPayloads, null);
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
    final int physical = manifest.physicalRowGroupCount();
    final int sealed = sealedChunkCount(physical);
    final int open = openLeafCount(physical);
    try {
      final ProjectionIndexHOTStorage.BlobLocators blocks =
          ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber, chunkSlotKey(column, 0), sealed);
      final ProjectionIndexHOTStorage.BlobLocators tails = open == 0
          ? ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber, chunkSlotKey(column, 0), 0)
          : ProjectionIndexHOTStorage.collectBlobLocators(reader, indexNumber,
              tailSlotKey(column, sealed * CHUNK_LEAVES + 1), open);
      return ColumnEvidence.chunked(blocks, tails, rowGroupCount, physical);
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
      return new RewriteStats(0, 0, 0L, 0L, 0);
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
      return new RewriteStats(0, 0, 0L, 0L, 0);
    }
    final int sealedNew = sealedChunkCount(physicalRowGroupCount);
    // openFirst splits the changed leaves exactly where the NEW manifest splits the column: everything
    // the new high-water mark seals is a block, the remainder is the open chunk's tails. A chunk this
    // commit completes therefore takes the sealed-rewrite path, which rebuilds it from its earlier
    // tails plus this commit's leaves and tombstones those tails — one block write, no writing a tail
    // only to delete it in the same transaction. That path visits a chunk only if a changed leaf lands
    // in it, which every completed chunk has: its last leaf (chunkId + 1) * CHUNK_LEAVES lies above the
    // prior physical count, so it is a slot this commit allocated and wrote, and allocation records the
    // slot for every column.
    //
    // priorPhysical[c] is that column's published physical high-water mark, or -1 when its manifest is
    // missing or unparsable. Tails are only ever written at or above a published mark and a published
    // count P satisfies P < (sealedChunkCount(P) + 1) * CHUNK_LEAVES, so a column's tails live in the
    // ONE chunk sealedChunkCount(P) and only across its openLeafCount(P) row groups — a parsed mark
    // bounds every tail scan exactly. Without one there is no bound to apply, so the scan stays
    // conservative and sweeps the whole chunk.
    final int[] priorPhysical = new int[columnKinds.length];
    Arrays.fill(priorPhysical, -1);
    for (int c = 0; c < columnKinds.length; c++) {
      if (!isStringKind(columnKinds[c])) {
        continue;
      }
      final Manifest prior = parseManifest(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(c)), -1);
      if (prior != null) {
        priorPhysical[c] = prior.physicalRowGroupCount();
      }
    }
    final int openFirst = sealedNew * CHUNK_LEAVES + 1;
    final IntOpenHashSet sealedChunkIds = new IntOpenHashSet();
    boolean touchesOpenChunk = false;
    for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
      final long slot = iterator.nextLong();
      if (slot < 1 || slot > physicalRowGroupCount) {
        throw new IllegalArgumentException("changed leaf slot out of range: " + slot);
      }
      if (slot < openFirst) {
        sealedChunkIds.add((int) ((slot - 1L) / CHUNK_LEAVES));
      } else {
        touchesOpenChunk = true;
      }
    }
    int rowGroupsRead = 0;
    int chunksWritten = 0;
    long bytesRead = 0L;
    long bytesWritten = 0L;
    int tailSlotReads = 0;
    // Sealed chunks: a touched row group rewrites its column's whole block (rare outside appends).
    for (final int chunkId : sealedChunkIds) {
      final int firstLeaf = chunkId * CHUNK_LEAVES + 1;
      final int leafCount = CHUNK_LEAVES;
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
        // No usable block in a chunk that can still own tails: its fingerprints live in tail blobs (a
        // chunk this commit is completing, or a block lost to corruption). Start from them so a rewrite
        // never loses a leaf. Outside the published open span there is no tail to find, so nothing is
        // probed there.
        final int tailScanTo = tailScanBound(priorPhysical[c], chunkId, leafCount);
        final boolean recoverTails = priorSlices == null && tailScanTo > 0;
        if (recoverTails) {
          for (int i = 0; i < tailScanTo; i++) {
            final byte[] tail = storage.getBlob(tailSlotKey(c, firstLeaf + i));
            tailSlotReads++;
            if (tail != null) {
              slices[i] = tail;
              bytesRead += tail.length;
            }
          }
        }
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
        // The tails are retired as soon as this rewrite has absorbed them, BEFORE the unchanged-block
        // shortcut below: a chunk whose every leaf turned out to carry nothing rebuilds to a null block
        // that equals its absent prior, and leaving its tails behind would resurrect them in the fold
        // as a fingerprint for a leaf that no longer has one. This never writes on the unchanged-block
        // path, because recoverTails means the prior block was absent or malformed, and a malformed one
        // can never equal a well-formed rebuild.
        if (recoverTails) {
          for (int i = 0; i < tailScanTo; i++) {
            storage.tombstoneBlob(tailSlotKey(c, firstLeaf + i));
          }
        }
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
    // A receding high-water mark can reopen a sealed chunk. Preserve its remaining fingerprints
    // as tails before applying the changed rows, and remove the block from the new open span.
    final int openLeaves = openLeafCount(physicalRowGroupCount);
    if (openLeaves > 0) {
      for (int c = 0; c < columnKinds.length; c++) {
        // Per column, against that column's OWN published mark: one column whose manifest is missing
        // must not stop every other column from reopening its block.
        if (!isStringKind(columnKinds[c]) || priorPhysical[c] < 0 || sealedChunkCount(priorPhysical[c]) <= sealedNew) {
          continue;
        }
        final long chunkSlot = chunkSlotKey(c, sealedNew);
        final byte[] prior = storage.getBlob(chunkSlot);
        if (prior == null) {
          continue;
        }
        bytesRead += prior.length;
        final byte[][] slices = ProjectionIndexColumnSegmentCodec.copyBloomBlockSlices(prior, CHUNK_LEAVES);
        if (slices != null) {
          for (int i = 0; i < openLeaves; i++) {
            final byte[] segment = slices[i];
            if (segment != null) {
              storage.putBlob(tailSlotKey(c, sealedNew * CHUNK_LEAVES + i + 1), segment);
              bytesWritten += segment.length;
              chunksWritten++;
            }
          }
        }
        storage.tombstoneBlob(chunkSlot);
        chunksWritten++;
      }
    }
    // Open chunk: a touched row group rewrites only its own tail blob, per column, and only when the
    // fingerprint really changed.
    if (touchesOpenChunk) {
      for (int c = 0; c < columnKinds.length; c++) {
        if (!isStringKind(columnKinds[c])) {
          continue;
        }
        for (final LongIterator iterator = changedColumnsByLeaf.keySet().iterator(); iterator.hasNext();) {
          final long slot = iterator.nextLong();
          if (slot < openFirst || (!rowGroupCountChanged && !columnSelected(changedColumnsByLeaf.get(slot), c))) {
            continue;
          }
          final byte[] segment =
              storage.getVerifiedColumnSegment(slot, ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(c),
                  ProjectionIndexColumnSegmentCodec.SEG_KIND_STRING_BLOOM);
          rowGroupsRead++;
          if (segment != null)
            bytesRead += segment.length;
          final long tailSlot = tailSlotKey(c, (int) slot);
          final byte[] prior = storage.getBlob(tailSlot);
          tailSlotReads++;
          if (prior != null)
            bytesRead += prior.length;
          if (Arrays.equals(prior, segment)) {
            continue;
          }
          if (segment == null) {
            storage.tombstoneBlob(tailSlot);
          } else {
            storage.putBlob(tailSlot, segment);
            bytesWritten += segment.length;
          }
          chunksWritten++;
        }
      }
    }
    // Folds and manifests: a chunk completed by this maintenance becomes one block, its tail blobs
    // go, and the manifest publishes the new physical high-water mark.
    for (int c = 0; c < columnKinds.length; c++) {
      if (!isStringKind(columnKinds[c]) || !(rowGroupCountChanged || anyLeafSelectsColumn(changedColumnsByLeaf, c))) {
        continue;
      }
      final long manifestSlot = ProjectionIndexHOTStorage.bloomBlockSlotKey(c);
      final byte[] nextManifest = manifest(rowGroupCount, physicalRowGroupCount);
      final byte[] priorManifest = storage.getBlob(manifestSlot);
      if (priorManifest != null)
        bytesRead += priorManifest.length;
      final Manifest parsedPrior = parseManifest(priorManifest, -1);
      final int foldFrom = parsedPrior == null
          ? 0
          : sealedChunkCount(parsedPrior.physicalRowGroupCount());
      for (int chunkId = foldFrom; chunkId < sealedNew; chunkId++) {
        final long chunkSlot = chunkSlotKey(c, chunkId);
        // Presence only: materialising and verifying the block just to drop it would also turn a
        // locally corrupt one into an exception out of a path whose contract is to fail open.
        if (storage.getRawSlot(chunkSlot) != null) {
          continue; // already a block (rewritten above, or built as a full chunk): never fold over it
        }
        final int firstLeaf = chunkId * CHUNK_LEAVES + 1;
        final int tailScanTo = tailScanBound(parsedPrior == null
            ? -1
            : parsedPrior.physicalRowGroupCount(), chunkId, CHUNK_LEAVES);
        final byte[][] slices = new byte[CHUNK_LEAVES][];
        for (int i = 0; i < tailScanTo; i++) {
          final byte[] tail = storage.getBlob(tailSlotKey(c, firstLeaf + i));
          tailSlotReads++;
          slices[i] = tail;
          if (tail != null)
            bytesRead += tail.length;
        }
        final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(slices, CHUNK_LEAVES);
        if (block != null) {
          storage.putBlob(chunkSlot, block);
          bytesWritten += block.length;
          chunksWritten++;
        }
        for (int i = 0; i < tailScanTo; i++) {
          if (slices[i] != null) {
            storage.tombstoneBlob(tailSlotKey(c, firstLeaf + i));
          }
        }
      }
      if (parsedPrior != null && parsedPrior.physicalRowGroupCount() > physicalRowGroupCount) {
        // The high-water mark receded: drop blocks and tail blobs beyond it.
        final int priorSealed = sealedChunkCount(parsedPrior.physicalRowGroupCount());
        for (int chunkId = sealedNew; chunkId < priorSealed; chunkId++) {
          final long chunkSlot = chunkSlotKey(c, chunkId);
          if (storage.getRawSlot(chunkSlot) != null) {
            storage.tombstoneBlob(chunkSlot);
            chunksWritten++;
          }
        }
        final int firstRemovedTail = Math.max(physicalRowGroupCount + 1, priorSealed * CHUNK_LEAVES + 1);
        for (int rowGroupId = firstRemovedTail; rowGroupId <= parsedPrior.physicalRowGroupCount(); rowGroupId++) {
          final long tailSlot = tailSlotKey(c, rowGroupId);
          tailSlotReads++;
          if (storage.getRawSlot(tailSlot) != null) {
            storage.tombstoneBlob(tailSlot);
            chunksWritten++;
          }
        }
      }
      if (!Arrays.equals(priorManifest, nextManifest)) {
        storage.putBlob(manifestSlot, nextManifest);
        bytesWritten += nextManifest.length;
      }
    }
    return new RewriteStats(rowGroupsRead, chunksWritten, bytesRead, bytesWritten, tailSlotReads);
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

  /**
   * Work one maintenance performed. {@code tailSlotReads} counts tail slots this maintenance READ —
   * the sealed-rewrite recovery scan, each changed open row group's prior tail, the fold's scan, and
   * the receding cleanup's presence probes. It does NOT count the tombstone loops, which descend to
   * the same slots the recovery scan just read and so are bounded by the same range.
   */
  record RewriteStats(int rowGroupsRead, int chunksWritten, long bytesRead, long bytesWritten, int tailSlotReads) {
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
        flush(storage);
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

    private void flush(final ProjectionIndexHOTStorage storage) {
      final ColumnBuffer[] buffers = columns;
      final int[] compactColumns = stringColumns;
      if (buffers != null && compactColumns != null) {
        for (final int c : compactColumns) {
          final ColumnBuffer buffer = buffers[c];
          final long slotKey = chunkSlotKey(c, nextChunkId);
          final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(buffer.segments, CHUNK_LEAVES);
          if (block != null) {
            storage.putBlob(slotKey, block);
          }
          Arrays.fill(buffer.segments, null);
        }
      }
      nextChunkId++;
      pendingLeaves = 0;
    }

    /**
     * Persist the partial tail as per-row-group tail blobs and capture the final publication shape for
     * this virgin build.
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
        // The partial chunk stays open: one tail blob per row group, folded by maintenance later.
        final int[] compactColumns = stringColumns;
        if (buffers != null && compactColumns != null) {
          for (final int c : compactColumns) {
            final ColumnBuffer buffer = buffers[c];
            for (int i = 0; i < pendingLeaves; i++) {
              final byte[] segment = buffer.segments[i];
              if (segment != null) {
                storage.putBlob(tailSlotKey(c, nextChunkId * CHUNK_LEAVES + i + 1), segment);
              }
            }
            Arrays.fill(buffer.segments, 0, pendingLeaves, null);
          }
        }
        pendingLeaves = 0;
      }
      if (nextChunkId != sealedChunkCount(rowGroupCount)) {
        throw new IllegalStateException("Persisted " + nextChunkId + " sealed Bloom chunks for " + rowGroupCount
            + " row groups; expected " + sealedChunkCount(rowGroupCount));
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
