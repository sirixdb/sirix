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
   * Beacon of first references.
   */
  int FIRST_BEACON = Writer.UBER_PAGE_BYTE_ALIGN << 1;

  /**
   * Beacon of the other references.
   */
  int OTHER_BEACON = Integer.BYTES;

  /**
   * Size in bytes of one revisions-file record (revision-root offset + timestamp, a long each).
   */
  int REVISIONS_FILE_RECORD_SIZE = 2 * Long.BYTES;

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
    return FIRST_BEACON + (long) revision * REVISIONS_FILE_RECORD_SIZE;
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
      final long[] timestamps = new long[revisionCount];
      final long[] offsets = new long[revisionCount];

      for (int i = 0; i < revisionCount; i++) {
        final RevisionFileData data = reader.getRevisionFileData(i);
        timestamps[i] = data.timestamp().toEpochMilli();
        offsets[i] = data.offset();
      }

      final RevisionIndex index = RevisionIndex.create(timestamps, offsets);
      holder.update(index);
    }
  }
}
