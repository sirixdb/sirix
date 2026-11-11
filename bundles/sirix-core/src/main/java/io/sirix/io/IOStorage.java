/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.io;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface to generate access to the storage. The storage is flexible as long as {@link Reader}
 * and {@link Writer}-implementations are provided. Utility methods for common interaction with the
 * storage are provided via the {@code Storage}.
 *
 * @author Sebastian Graf, University of Konstanz
 */
public interface IOStorage {

  /**
   * Data file name.
   */
  String FILENAME = "sirix.data";

  /**
   * Revisions file name.
   */
  String REVISIONS_FILENAME = "sirix.revisions";

  /**
   * Length-prefix size of every page record.
   */
  int OTHER_BEACON = Integer.BYTES;

  // ===== V0 file layout (superblock + spaced beacons), see docs/DISK_FORMAT.md =====

  /**
   * Each uber-page beacon slot reserves one filesystem block. The two copies live in DIFFERENT 4 KiB
   * blocks so block-granular torn writes / firmware corruption can no longer kill both.
   */
  int BEACON_SLOT_BYTES = 4096;

  /** Primary uber beacon slot (the superblock occupies [0, 4096)). */
  long PRIMARY_BEACON_OFFSET = 4096;

  /** Secondary uber beacon slot — one full block past the primary. */
  long SECONDARY_BEACON_OFFSET = PRIMARY_BEACON_OFFSET + BEACON_SLOT_BYTES;

  /** First data-page offset in {@code sirix.data}. */
  long DATA_REGION_START = SECONDARY_BEACON_OFFSET + BEACON_SLOT_BYTES;

  /** First revision-record offset in {@code sirix.revisions} (superblock + reserved before it). */
  long REVISIONS_RECORDS_START = 4096;

  /** Size of a little-endian (offset, timestamp, checksum, hash-or-tag) record. */
  int REVISIONS_FILE_RECORD_SIZE = 32;

  /** A tombstone has no data-file address. Only the exact sentinel/tag pair is valid. */
  long PRUNED_REVISION_OFFSET = Long.MIN_VALUE;

  /**
   * Flag bit of a page key that addresses a writer's spill file instead of the data file. A storage
   * epoch writes the pages it flushes before the commit there (see {@code Writer#writeSpilled}); the
   * commit copies the ones still referenced into the data file and gives them ordinary keys, so the
   * data file receives every page exactly once, as if the transaction had never rotated. Spill keys
   * never reach a committed page or a shared cache.
   */
  long SPILL_KEY_BIT = 1L << 62;

  /**
   * Bits of a spill key that hold the spill-file offset; the bits above carry the spill generation.
   */
  int SPILL_OFFSET_BITS = 40;

  long SPILL_OFFSET_MASK = (1L << SPILL_OFFSET_BITS) - 1L;

  /** Whether {@code key} addresses the writer's spill file. */
  static boolean isSpillKey(final long key) {
    return key > 0L && (key & SPILL_KEY_BIT) != 0L;
  }

  /** The spill-file offset a spill key addresses. */
  static long spillOffset(final long key) {
    return key & SPILL_OFFSET_MASK;
  }

  /**
   * A spill key for {@code offset} in spill generation {@code generation}. The generation advances
   * whenever the spill file is discarded, so an offset reused by a later epoch set never repeats a
   * key a cache may still remember.
   */
  static long spillKey(final long generation, final long offset) {
    if (offset < 0L || offset > SPILL_OFFSET_MASK) {
      throw new IllegalArgumentException("Spill offset out of range: " + offset);
    }
    final long maxGeneration = (1L << (62 - SPILL_OFFSET_BITS)) - 1L;
    if (generation < 0L || generation > maxGeneration) {
      throw new IllegalArgumentException("Spill generation out of range: " + generation);
    }
    return SPILL_KEY_BIT | (generation << SPILL_OFFSET_BITS) | offset;
  }

  /** Tombstone type/version, covered by the record checksum. */
  long PRUNED_REVISION_TAG = 0x5052554E45440001L;

  /**
   * Nonzero representation of the exceptionally rare all-zero payload hash. Readers do not undo the
   * remap: such a payload fails verification rather than bypassing it. A payload whose hash already
   * equals the sentinel verifies unchanged.
   */
  long ZERO_HASH_SENTINEL = 0xFFFF_FFFF_FFFF_FFFFL;

  /**
   * Normalize a RevisionRootPage hash for storage: an all-zero hash is remapped to
   * {@link #ZERO_HASH_SENTINEL} so the stored field is never {@code 0} when a hash IS present
   * ({@code 0} is invalid in persisted records). Any non-zero hash is stored unchanged. The remap is
   * one-way (readers never reverse it) — see {@link #ZERO_HASH_SENTINEL} for the fail-closed
   * rationale.
   *
   * @param pageHash the page hash as a long (canonical, as produced from {@code PageHasher})
   * @return the value to store in the record's hash field (never {@code 0} for a present hash)
   */
  static long normalizeRevisionRootPageHash(final long pageHash) {
    return pageHash == 0L
        ? ZERO_HASH_SENTINEL
        : pageHash;
  }

  /**
   * The revisions file is a fixed-slot array: record for {@code revision} lives at this offset. The
   * SINGLE definition of the on-disk layout — every reader and writer must use it; a diverging
   * hand-expanded formula is exactly the slot-shift corruption class the deterministic slots were
   * introduced to prevent.
   *
   * @param revision revision number (0-based)
   * @return byte offset of the revision's record in the revisions file
   */
  static long revisionsFileOffset(final int revision) {
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must not be negative");
    }
    return REVISIONS_RECORDS_START + (long) revision * REVISIONS_FILE_RECORD_SIZE;
  }

  // ===== Commit profile =====

  /**
   * Whether the preallocated-commit profile is on ({@code -Dsirix.commit.preallocated}, default
   * {@code true}). Backends that cannot support it (MEMORY_MAPPED, whose readers only remap on
   * physical growth) pass {@code false} down explicitly instead of consulting this.
   *
   * @return whether preallocated commits are enabled by configuration
   */
  static boolean preallocatedCommitsEnabled() {
    return Boolean.parseBoolean(System.getProperty("sirix.commit.preallocated", "true"));
  }

  /**
   * Whether the lazy-revision-record profile is on ({@code -Dsirix.commit.lazyRevisionRecord},
   * default {@code true}, and only meaningful together with {@link #preallocatedCommitsEnabled()}).
   *
   * <p>
   * The OWNING STORAGE derives both flags ONCE per writer and passes them down: the revisions
   * channel's open mode and the writer's durability protocol must agree, and re-reading the
   * properties later could pair a buffered channel with a writer that stages no tail-log — silent
   * committed-data loss after a crash.
   *
   * @return whether lazy revision records are enabled by configuration
   */
  static boolean lazyRevisionRecordsEnabled() {
    return preallocatedCommitsEnabled()
        && Boolean.parseBoolean(System.getProperty("sirix.commit.lazyRevisionRecord", "true"));
  }

  // ===== Revision-record tail log (lazy revision records) =====

  /**
   * One tail-log entry: {@code [u32 revision][u32 reserved=0][32-byte revision record][u64 xxh3]},
   * the trailing hash covering the 40-byte prefix.
   */
  int REVISION_RECORD_TAIL_LOG_ENTRY_BYTES = 48;

  /** Number of entries in the ring — the salvage window, in commits. */
  int REVISION_RECORD_TAIL_LOG_CAPACITY = 16;

  /** Bytes the ring occupies at the END of each beacon slot's zero pad. */
  int REVISION_RECORD_TAIL_LOG_BYTES = REVISION_RECORD_TAIL_LOG_ENTRY_BYTES * REVISION_RECORD_TAIL_LOG_CAPACITY;

  /** Offset of the ring within a {@link #BEACON_SLOT_BYTES} slot image. */
  int REVISION_RECORD_TAIL_LOG_SLOT_OFFSET = BEACON_SLOT_BYTES - REVISION_RECORD_TAIL_LOG_BYTES;

  /** The ring slot a revision's entry deterministically occupies. */
  static int tailLogRingIndex(final int revision) {
    return Math.floorMod(revision, REVISION_RECORD_TAIL_LOG_CAPACITY);
  }

  /**
   * Byte offset of a revision's tail-log entry inside a beacon slot.
   *
   * @param revision revision number (0-based)
   * @return absolute offset of the entry within the slot image
   */
  static int tailLogEntryOffsetInSlot(final int revision) {
    return tailLogEntryOffsetAtIndex(tailLogRingIndex(revision));
  }

  /**
   * Byte offset of the entry at the given RING INDEX — the index-keyed twin of
   * {@link #tailLogEntryOffsetInSlot(int)} for the merge/scan loops that walk the ring positionally.
   * Both derive the layout HERE, so a layout change cannot leave one call site reading the wrong 48
   * bytes.
   *
   * @param ringIndex ring slot index in {@code [0, REVISION_RECORD_TAIL_LOG_CAPACITY)}
   * @return absolute offset of the entry within the slot image
   */
  static int tailLogEntryOffsetAtIndex(final int ringIndex) {
    return REVISION_RECORD_TAIL_LOG_SLOT_OFFSET + ringIndex * REVISION_RECORD_TAIL_LOG_ENTRY_BYTES;
  }

  /**
   * XXH3-64 over a tail-log entry's 40-byte prefix ({@code revision, reserved, offset, timestamp,
   * recordChecksum, pageHash}), little-endian — the entry's own integrity trailer, covering the
   * revision number as well so an entry can never be mistaken for a different revision's.
   *
   * <p>
   * Zero-allocation: a thread-local 40-byte scratch buffer, no per-call array.
   */
  static long tailLogEntryChecksum(final int revision, final long offset, final long timestampMillis,
      final long recordChecksum, final long pageHash) {
    final ByteBuffer scratch = TailLogScratch.BUFFER.get();
    scratch.putInt(0, revision);
    scratch.putInt(4, 0);
    scratch.putLong(8, offset);
    scratch.putLong(16, timestampMillis);
    scratch.putLong(24, recordChecksum);
    scratch.putLong(32, pageHash);
    return TailLogScratch.XX3.hashBytes(scratch.array(), 0, 40);
  }

  /**
   * THE canonical tail-log entry decoder: an entry is valid only when its reserved word is zero, its
   * own checksum matches, AND the 32-byte record it carries passes the record checksum. Both recovery
   * paths (the reader's salvage and the writer's ring merge) go through this method, so they can
   * never diverge on what counts as a valid entry.
   *
   * @param slot a little-endian buffer holding at least {@code base + ENTRY_BYTES} bytes
   * @param base offset of the entry within {@code slot}
   * @return whether the entry is intact
   */
  static boolean tailLogEntryValidAt(final ByteBuffer slot, final int base) {
    final int revision = slot.getInt(base);
    if (revision < 0 || slot.getInt(base + Integer.BYTES) != 0) {
      return false;
    }
    final long offset = slot.getLong(base + 8);
    final long timestampMillis = slot.getLong(base + 16);
    final long recordChecksum = slot.getLong(base + 24);
    final long pageHash = slot.getLong(base + 32);
    return slot.getLong(base + 40) == tailLogEntryChecksum(revision, offset, timestampMillis, recordChecksum, pageHash)
        && recordChecksum == expectedRevisionRecordChecksum(offset, timestampMillis, pageHash);
  }

  /**
   * XXH3-64 over (offset, timestamp, hash-or-tag), each little-endian. Reuses thread-local scratch:
   * validating a bulk history adds no per-record checksum allocation.
   */
  static long revisionRecordChecksum(final long offset, final long timestampMillis, final long tagOrHash) {
    final ByteBuffer scratch = RevisionRecordScratch.BUFFER.get();
    scratch.putLong(0, offset);
    scratch.putLong(8, timestampMillis);
    scratch.putLong(16, tagOrHash);
    return TailLogScratch.XX3.hashBytes(scratch.array());
  }

  /** The current format always protects all three identity fields. */
  static long expectedRevisionRecordChecksum(final long offset, final long timestampMillis, final long tagOrHash) {
    return revisionRecordChecksum(offset, timestampMillis, tagOrHash);
  }

  /**
   * Decode one complete disk record. Checksum verification precedes tombstone recognition. The
   * revision is supplied by the slot position; corrupt records never become tombstones.
   */
  static RevisionFileData decodeRevisionRecord(final int revision, final long offset, final long timestampMillis,
      final long checksum, final long tagOrHash) {
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must not be negative");
    }
    if (checksum != revisionRecordChecksum(offset, timestampMillis, tagOrHash)) {
      throw new SirixIOException("Corrupt revisions record for revision " + revision + " (checksum mismatch)");
    }
    if (offset == PRUNED_REVISION_OFFSET && tagOrHash == PRUNED_REVISION_TAG) {
      return RevisionFileData.pruned(Instant.ofEpochMilli(timestampMillis));
    }
    if (offset < DATA_REGION_START || offset > Long.MAX_VALUE - OTHER_BEACON || tagOrHash == 0L) {
      throw new SirixIOException("Corrupt revisions record for revision " + revision + " (invalid offset or tag/hash)");
    }
    return new RevisionFileData(offset, Instant.ofEpochMilli(timestampMillis), tagOrHash);
  }

  /**
   * Encode a complete tombstone into caller-owned reusable storage at its current position. Advances
   * the position by 32 bytes and writes little-endian independently of the buffer's order. A
   * MemorySegment caller can reuse its asByteBuffer() view.
   */
  static void writePrunedRevisionRecord(final ByteBuffer target, final long timestampMillis) {
    if (target.remaining() < REVISIONS_FILE_RECORD_SIZE) {
      throw new IllegalArgumentException("A revision record requires 32 remaining bytes");
    }
    final ByteOrder originalOrder = target.order();
    try {
      target.order(ByteOrder.LITTLE_ENDIAN);
      target.putLong(PRUNED_REVISION_OFFSET);
      target.putLong(timestampMillis);
      target.putLong(revisionRecordChecksum(PRUNED_REVISION_OFFSET, timestampMillis, PRUNED_REVISION_TAG));
      target.putLong(PRUNED_REVISION_TAG);
    } finally {
      target.order(originalOrder);
    }
  }

  /**
   * Getting a writer.
   *
   * @return an {@link Writer} instance
   * @throws SirixIOException if the initialization fails
   */
  Writer createWriter();

  /**
   * Getting a reader.
   *
   * @return an {@link Reader} instance
   * @throws SirixIOException if the initialization fails
   */
  Reader createReader();

  /**
   * Closing this storage.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  void close();

  /**
   * Check if storage exists.
   *
   * @return true if storage holds data, false otherwise
   * @throws SirixIOException if storage is not accessible
   */
  boolean exists();

  /**
   * Load the revision file data into an in-memory cache.
   *
   * @param cache the cache to
   */
  default void loadRevisionFileDataIntoMemory(AsyncCache<Integer, RevisionFileData> cache) {
    if (!cache.asMap().isEmpty()) {
      return;
    }

    if (exists()) {
      // try-with-resources: reader.close() used to live INSIDE the Caffeine getAll mapping
      // function, which never runs when revisionNumber == 0 (every resource holding only the
      // bootstrap revision) or when all keys are already cached — leaking two file channels per
      // storage initialization. getAll's mapping runs synchronously here, so closing after the
      // call is safe in all cases.
      try (final Reader reader = createReader()) {
        final PageReference firstRef = reader.readUberPageReference();
        final UberPage uberPage = (UberPage) firstRef.getPage();

        final var revisionNumber = uberPage.getRevisionNumber();
        final var revisionNumbers = new ArrayList<Integer>(revisionNumber);

        for (int i = 1; i <= revisionNumber; i++) {
          revisionNumbers.add(i);
        }

        cache.getAll(revisionNumbers, keys -> {
          final Map<Integer, RevisionFileData> result = new HashMap<>();
          keys.forEach(key -> result.put(key, reader.getRevisionFileData(key)));
          return result;
        }).join();
      }
    }
  }

  /**
   * Get the byte handler pipeline.
   *
   * @return byte handler pipeline
   */
  ByteHandler getByteHandler();

  /**
   * Get the RevisionIndexHolder for fast timestamp-based revision lookups.
   * 
   * <p>
   * The holder provides thread-safe access to the immutable RevisionIndex. Multiple readers can
   * search concurrently while a single writer updates the index during commits.
   *
   * @return the RevisionIndexHolder for this storage
   */
  RevisionIndexHolder getRevisionIndexHolder();

  /**
   * Build and load the RevisionIndex from disk.
   * 
   * <p>
   * Called during storage initialization to populate the RevisionIndex with all existing revisions.
   * The index is then updated incrementally during commits via
   * {@link RevisionIndexHolder#addRevision(long, long)}.
   *
   * @param holder the holder to populate
   */
  default void loadRevisionIndex(RevisionIndexHolder holder) {
    if (!exists()) {
      return;
    }

    try (final Reader reader = createReader()) {
      final PageReference firstRef = reader.readUberPageReference();
      final UberPage uberPage = (UberPage) firstRef.getPage();

      if (uberPage == null) {
        return;
      }

      final int lastRevisionNumber = uberPage.getRevisionNumber();
      if (lastRevisionNumber < 0) {
        return;
      }

      // Build arrays from revision 0 to lastRevisionNumber (inclusive)
      // getRevisionNumber() returns the last revision index (0-indexed)
      final int revisionCount = lastRevisionNumber + 1;

      // The holder is global per resource path and kept current in-JVM: the writer appends per
      // commit (RevisionIndexHolder.addRevision) and the recovery/rollback paths reset or
      // reload it. Re-reading EVERY revision record on EVERY storage open made session opens
      // linear in history — for request-scoped REST use, read throughput collapsed as
      // revisions accumulated. Reload only when the in-memory index disagrees with the
      // on-disk revision count (fewer = fresh process or foreign growth; more = out-of-band
      // truncation — both directions must resync).
      final RevisionIndex currentIndex = holder.get();
      if (currentIndex.size() == revisionCount) {
        currentIndex.requireReadable(0);
        currentIndex.requireReadable(lastRevisionNumber);
        return;
      }

      final long[] timestamps = new long[revisionCount];
      final long[] offsets = new long[revisionCount];

      // Bulk-read the whole revisions file in one request. Reading record-by-record cost one
      // syscall + one direct-buffer allocation per revision, so this rebuild — which runs on
      // a storage open whenever a concurrent commit makes the in-memory index disagree with
      // the on-disk count — made request-scoped opens linear in revision count.
      final RevisionFileData[] data = reader.getRevisionFileData(0, revisionCount);
      for (int i = 0; i < revisionCount; i++) {
        timestamps[i] = data[i].timestamp().toEpochMilli();
        offsets[i] = data[i].offset();
      }

      data[0].requireReadable(0);
      data[lastRevisionNumber].requireReadable(lastRevisionNumber);
      final RevisionIndex index = RevisionIndex.create(timestamps, offsets);
      holder.update(index);
    }
  }
}
