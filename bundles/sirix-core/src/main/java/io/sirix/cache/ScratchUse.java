/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

/**
 * What a scratch buffer is borrowed for. The value only labels the per-use counters of
 * {@link ScratchStats}; every use is served by the same allocator, capacities and budget.
 */
public enum ScratchUse {

  /** Page, run, region and revision reads: the bytes a positional read lands in. */
  READ,

  /** Page serialization and its compressed output while a commit builds a page image. */
  SERIALIZATION,

  /** The commit's write-behind buffer between serialization and the data file. */
  COMMIT_FLUSH,

  /** Compression and decompression staging: LZ4, LZ77, FSST, lazily expanded chunk bodies. */
  CODEC,

  /** Page-internal staging: side-slot images, overflow resizing, value-block landing. */
  PAGE,

  /** Writer-side I/O windows: zero fill, frame hashing, beacon and tail-log records. */
  WRITE_IO,

  /** Bulk-import chunks handed from the scanner to the shredding workers. */
  IMPORT,

  /** Group-table, probe and spill chunks of an aggregation. */
  AGGREGATION,

  /** Windowed decode of projection column slices. */
  PROJECTION;

  private static final ScratchUse[] VALUES = values();

  /** Number of uses; the length of every per-use counter array. */
  static final int COUNT = VALUES.length;

  /** The use with the given {@link #ordinal()}. */
  static ScratchUse of(final int ordinal) {
    return VALUES[ordinal];
  }
}
