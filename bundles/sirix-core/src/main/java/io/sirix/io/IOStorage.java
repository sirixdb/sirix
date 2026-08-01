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

import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
   * Each uber-page beacon slot reserves one filesystem block. The two copies live in DIFFERENT
   * 4 KiB blocks so block-granular torn writes / firmware corruption can no longer kill both.
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

  /**
   * Size in bytes of one revisions-file record:
   * {@code [u64 revisionRootOffset][u64 epochMillis][u64 recordChecksum][u64 revisionRootPageHash]}
   * (little-endian). The 4th field carries the XXH3-64 of the RevisionRootPage's compressed
   * payload (the same hash the writer puts on every page's PageReference) so the page body can be
   * integrity-checked on the {@code readRevisionRootPage} path, which carries no parent reference.
   * <p>
   * The record checksum self-protects this 4th field: when the hash field is non-zero it covers
   * all 24 leading bytes; when it is zero (a LEGACY beta1-and-earlier record, whose reserved field
   * was {@code putLong(0L)}) it covers only the first 16 bytes. The hash field thus doubles as the
   * format-version discriminator, so older resources open under this build with no false-positive
   * corruption error. See {@link #expectedRevisionRecordChecksum(long, long, long)}.
   * <p>
   * The checksum exists because these records are the ONLY path to any RevisionRootPage and used
   * to be the least-protected bytes in the system.
   */
  int REVISIONS_FILE_RECORD_SIZE = 32;

  /**
   * Sentinel substituted for an all-zero RevisionRootPage hash before it is stored in a record, so
   * that "hash field == {@code 0}" unambiguously means "legacy record, no hash recorded".
   * <p>
   * XXH3-64 producing exactly {@code 0} for a real page payload is a ~1-in-2^64 event. Remapping it
   * to this sentinel means the stored field is NEVER {@code 0} when a hash is genuinely present, so
   * "present" can never be misread as "legacy" — which is the safety-critical direction, because
   * misreading present-as-legacy would SILENTLY SKIP verification (a fail-open miss). The readers
   * do NOT reverse the remap: a real {@code 0}-hash page is verified against the sentinel and so
   * fails the check (a fail-CLOSED false positive on that one astronomically-rare page) rather than
   * going unverified. A page whose real hash happens to equal the sentinel itself is stored and
   * verified as the sentinel unchanged — no false positive there. The value is otherwise arbitrary.
   */
  long ZERO_HASH_SENTINEL = 0xFFFF_FFFF_FFFF_FFFFL;

  /**
   * Normalize a RevisionRootPage hash for storage: an all-zero hash is remapped to
   * {@link #ZERO_HASH_SENTINEL} so the stored field is never {@code 0} when a hash IS present
   * ({@code 0} is reserved to mean "legacy / no hash"). Any non-zero hash is stored unchanged. The
   * remap is one-way (readers never reverse it) — see {@link #ZERO_HASH_SENTINEL} for the
   * fail-closed rationale.
   *
   * @param pageHash the page hash as a long (canonical, as produced from {@code PageHasher})
   * @return the value to store in the record's hash field (never {@code 0} for a present hash)
   */
  static long normalizeRevisionRootPageHash(final long pageHash) {
    return pageHash == 0L ? ZERO_HASH_SENTINEL : pageHash;
  }

  /**
   * The revisions file is a fixed-slot array: record for {@code revision} lives at this offset.
   * The SINGLE definition of the on-disk layout — every reader and writer must use it; a
   * diverging hand-expanded formula is exactly the slot-shift corruption class the deterministic
   * slots were introduced to prevent.
   *
   * @param revision revision number (0-based)
   * @return byte offset of the revision's record in the revisions file
   */
  static long revisionsFileOffset(final int revision) {
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
   * <p>The OWNING STORAGE derives both flags ONCE per writer and passes them down: the revisions
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
  int REVISION_RECORD_TAIL_LOG_BYTES =
      REVISION_RECORD_TAIL_LOG_ENTRY_BYTES * REVISION_RECORD_TAIL_LOG_CAPACITY;

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
   * {@link #tailLogEntryOffsetInSlot(int)} for the merge/scan loops that walk the ring
   * positionally. Both derive the layout HERE, so a layout change cannot leave one call site
   * reading the wrong 48 bytes.
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
   * <p>Zero-allocation: a thread-local 40-byte scratch buffer, no per-call array.
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
   * THE canonical tail-log entry decoder: an entry is valid only when its reserved word is zero,
   * its own checksum matches, AND the 32-byte record it carries passes the record checksum. Both
   * recovery paths (the reader's salvage and the writer's ring merge) go through this method, so
   * they can never diverge on what counts as a valid entry.
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
    return slot.getLong(base + 40)
        == tailLogEntryChecksum(revision, offset, timestampMillis, recordChecksum, pageHash)
        && recordChecksum == expectedRevisionRecordChecksum(offset, timestampMillis, pageHash);
  }

  /**
   * XXH3-64 of a LEGACY revisions record's first 16 bytes (offset + timestamp), little-endian.
   * Used to verify records whose hash field is {@code 0} (beta1 and earlier), and to compute the
   * checksum a legacy record would carry.
   */
  static long revisionRecordChecksum(final long offset, final long timestampMillis) {
    final byte[] first16 = new byte[16];
    final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(first16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    bb.putLong(offset);
    bb.putLong(timestampMillis);
    return net.openhft.hashing.LongHashFunction.xx3().hashBytes(first16);
  }

  /**
   * XXH3-64 of a revisions record's first 24 bytes (offset + timestamp + RevisionRootPage hash),
   * little-endian — the integrity check writers and readers agree on when a page hash is present.
   * The hash field is folded into the checksum so a torn write or bit-rot in the hash itself is
   * detected by the same record check, not silently used to (mis)verify the page body.
   *
   * @param offset           the RevisionRootPage offset in the data file
   * @param timestampMillis  the revision commit timestamp
   * @param storedPageHash   the (already-normalized, non-zero) page-hash field as stored
   * @return the XXH3-64 of the 24-byte prefix
   */
  static long revisionRecordChecksum(final long offset, final long timestampMillis, final long storedPageHash) {
    final byte[] first24 = new byte[24];
    final java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(first24).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    bb.putLong(offset);
    bb.putLong(timestampMillis);
    bb.putLong(storedPageHash);
    return net.openhft.hashing.LongHashFunction.xx3().hashBytes(first24);
  }

  /**
   * The checksum a record is EXPECTED to carry, dispatching on its hash field — the single place
   * the legacy-vs-present rule lives, so every reader (FileChannel, MemoryMapped, recovery scan)
   * applies it identically. {@code storedPageHash == 0} ⇒ legacy 16-byte checksum (offset +
   * timestamp only); otherwise the 24-byte checksum that also covers the hash field.
   *
   * @param offset          the RevisionRootPage offset
   * @param timestampMillis the revision commit timestamp
   * @param storedPageHash  the record's 4th field exactly as read from disk ({@code 0} = legacy)
   * @return the checksum the record's 3rd field must equal to be considered intact
   */
  static long expectedRevisionRecordChecksum(final long offset, final long timestampMillis,
      final long storedPageHash) {
    return storedPageHash == 0L
        ? revisionRecordChecksum(offset, timestampMillis)
        : revisionRecordChecksum(offset, timestampMillis, storedPageHash);
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
      if (holder.get().size() == revisionCount) {
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

      final RevisionIndex index = RevisionIndex.create(timestamps, offsets);
      holder.update(index);
    }
  }
}

/**
 * Thread-local scratch for {@link IOStorage#tailLogEntryChecksum} — the checksum is computed on
 * the commit path and once per ring entry during recovery, so it must not allocate.
 */
final class TailLogScratch {

  static final LongHashFunction XX3 = LongHashFunction.xx3();

  static final ThreadLocal<ByteBuffer> BUFFER =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(40).order(ByteOrder.LITTLE_ENDIAN));

  private TailLogScratch() {
  }
}
