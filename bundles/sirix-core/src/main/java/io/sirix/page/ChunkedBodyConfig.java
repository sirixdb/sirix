/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import java.util.concurrent.atomic.LongAdder;

/**
 * Switches and wire constants for the chunk-framed record-page body.
 *
 * <p>
 * A monolith body compresses compact dir, template pool, column and elision sections and the whole
 * record heap into one codec frame, so reading one slot costs one whole-page decode. The chunked
 * body keeps the same section bytes but frames them apart: all page-global metadata in a single
 * META frame, the heap split into chunks at entry boundaries, each frame independently compressed
 * and checksummed. Nothing here changes what a decoded page looks like — only how few of its bytes
 * a reader has to touch.
 *
 * <p>
 * The writer is off by default while the format lands; both bodies are readable in the meantime,
 * the envelope flag bit saying which one a page carries.
 */
public final class ChunkedBodyConfig {

  /**
   * Page-envelope flag bit marking a record page whose body is chunk-framed. A build that predates
   * the format rejects the page at the envelope, by the fence that
   * {@code PageKind.readVersionAndFlags} has always applied to unknown flag bits.
   */
  public static final byte FLAG_CHUNKED_BODY = 0x01;

  /**
   * Wire codec id for a frame stored verbatim, used whenever the elected codec's output would not be
   * smaller than the input. Ids 0..3 are the codecs the monolith body already writes.
   */
  public static final int CODEC_STORED = 4;

  /**
   * Chunk-table capacity: the count is one byte, and the planner grows chunks rather than exceed it.
   */
  public static final int MAX_CHUNKS = 128;

  /** Fixed bytes of a META frame header: rawLen, encLen, codec, xxh3. */
  public static final int META_FRAME_HEADER_BYTES = 4 + 4 + 1 + 8;

  /** Fixed bytes of one chunk-table row: firstEntry, entryCount, rawLen, encLen, codec, xxh3. */
  public static final int CHUNK_TABLE_ROW_BYTES = 2 + 2 + 4 + 4 + 1 + 8;

  /**
   * Raw heap bytes a chunk is filled to before it is closed. 4 KiB puts roughly 200 records in a
   * chunk on record-shaped corpora — small enough that a point read expands a fraction of the page,
   * large enough that the codecs still see cross-record redundancy.
   */
  private static final int DEFAULT_TARGET_CHUNK_BYTES = 4096;

  /**
   * Smallest target a caller can dial in. Below this the chunk table and per-frame codec headers
   * start to cost more than the expansion they save.
   */
  private static final int MIN_TARGET_CHUNK_BYTES = 64;

  private static volatile boolean enabled = Boolean.getBoolean("sirix.chunkedBody.enable");

  private static volatile int targetChunkBytes =
      clampTarget(Integer.getInteger("sirix.chunkedBody.targetChunkBytes", DEFAULT_TARGET_CHUNK_BYTES));

  private static volatile boolean diag = Boolean.getBoolean("sirix.chunkedBody.diag");

  private static volatile boolean poison = Boolean.getBoolean("sirix.chunkedBody.poison");

  /** Pages whose body was parsed without expanding a single record. */
  private static final LongAdder LAZY_LOADS = new LongAdder();

  /** Chunks expanded on demand, across every lazily parsed page. */
  private static final LongAdder CHUNK_MATERIALIZATIONS = new LongAdder();

  /** Point-lookup loads the policy had to serve eagerly after all. */
  private static final LongAdder EAGER_FALLBACKS = new LongAdder();

  private ChunkedBodyConfig() {
    throw new AssertionError("no instances");
  }

  /** Whether newly serialized record pages get a chunk-framed body. Reading never consults this. */
  public static boolean enabled() {
    return enabled;
  }

  /** Raw heap bytes a chunk is filled to before the planner closes it. */
  public static int targetChunkBytes() {
    return targetChunkBytes;
  }

  /**
   * Test seam: switch the writer between body formats within one JVM, so a test can serialize the
   * same logical page both ways and compare. Production selects the format once, from the system
   * property.
   *
   * @param value whether to write chunked bodies
   * @return the previous setting, for restoring in a finally block
   */
  public static boolean setEnabledForTesting(final boolean value) {
    final boolean previous = enabled;
    enabled = value;
    return previous;
  }

  /**
   * Test seam: dial the chunk target down so a small page still produces several chunks.
   *
   * @param value target raw bytes per chunk, clamped to a sane floor
   * @return the previous setting, for restoring in a finally block
   */
  public static int setTargetChunkBytesForTesting(final int value) {
    final int previous = targetChunkBytes;
    targetChunkBytes = clampTarget(value);
    return previous;
  }

  private static int clampTarget(final int value) {
    return Math.max(MIN_TARGET_CHUNK_BYTES, value);
  }

  /**
   * Whether lazy loads and chunk expansions are counted.
   *
   * <p>
   * A counter nobody reads is how the column read path was disabled twice without a test noticing, so
   * the numbers exist — but a {@link LongAdder} increment per materialized chunk is not something the
   * point-read path should pay for by default.
   */
  public static boolean diagEnabled() {
    return diag;
  }

  /** Record a page whose body was parsed without expanding any record. */
  public static void recordLazyLoad() {
    if (diag) {
      LAZY_LOADS.increment();
    }
  }

  /** Record a chunk expanded on demand. */
  public static void recordChunkMaterialization() {
    if (diag) {
      CHUNK_MATERIALIZATIONS.increment();
    }
  }

  /** Pages parsed lazily since the last {@link #resetDiag()}. Zero unless {@link #diagEnabled()}. */
  public static long lazyLoads() {
    return LAZY_LOADS.sum();
  }

  /** Chunks expanded on demand since the last {@link #resetDiag()}. */
  public static long chunkMaterializations() {
    return CHUNK_MATERIALIZATIONS.sum();
  }

  /**
   * Record a point-lookup load that could not be served lazily.
   *
   * <p>
   * Counted at the policy site rather than inferred from the difference between loads and lazy
   * loads, because the two have different denominators — and a feature that quietly stops firing
   * while its tests still pass is how the column read path was disabled twice.
   */
  public static void recordEagerFallback() {
    if (diag) {
      EAGER_FALLBACKS.increment();
    }
  }

  /** Point-lookup loads served eagerly since the last {@link #resetDiag()}. */
  public static long eagerFallbacks() {
    return EAGER_FALLBACKS.sum();
  }

  /** Zero the diagnostic counters, so a test can attribute what one operation did. */
  public static void resetDiag() {
    LAZY_LOADS.reset();
    CHUNK_MATERIALIZATIONS.reset();
    EAGER_FALLBACKS.reset();
  }

  /**
   * Test seam: count lazy loads and chunk expansions.
   *
   * @param value whether to count
   * @return the previous setting, for restoring in a finally block
   */
  public static boolean setDiagForTesting(final boolean value) {
    final boolean previous = diag;
    diag = value;
    return previous;
  }

  /**
   * Whether a lazily parsed page fills the heap ranges of its unexpanded chunks with a poison byte.
   *
   * <p>
   * This is the enforcement the gate list cannot get from enumeration. A reader that reaches the heap
   * without going through {@link KeyValueLeafPage#ensureChunkFor} sees {@code 0xCC} instead of a
   * record and fails its comparison deterministically, rather than reading whatever the allocator
   * happened to leave behind — which on a recycled page is frequently a plausible-looking record.
   */
  public static boolean poisonEnabled() {
    return poison;
  }

  /**
   * Test seam: fill unexpanded heap ranges with a poison byte.
   *
   * @param value whether to poison
   * @return the previous setting, for restoring in a finally block
   */
  public static boolean setPoisonForTesting(final boolean value) {
    final boolean previous = poison;
    poison = value;
    return previous;
  }

  /** The byte an unexpanded heap range is filled with when {@link #poisonEnabled()}. */
  public static final byte POISON_BYTE = (byte) 0xCC;
}
