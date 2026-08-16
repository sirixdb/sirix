/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

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
}
