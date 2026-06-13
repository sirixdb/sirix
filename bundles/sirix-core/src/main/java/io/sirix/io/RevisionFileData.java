package io.sirix.io;

import java.time.Instant;

/**
 * One revisions-file record, in-memory.
 *
 * @param offset    byte offset of the RevisionRootPage in {@code sirix.data}
 * @param timestamp commit timestamp of the revision
 * @param pageHash  XXH3-64 of the RevisionRootPage's compressed on-disk payload, as a {@code long}
 *                  (the same value the writer stored on the page's {@link io.sirix.page.PageReference}).
 *                  {@code 0} means "no hash recorded" — a legacy (beta1 and earlier) record whose
 *                  reserved field was zero, OR a backend (e.g. RAM) that does not persist page
 *                  bytes. A zero {@code pageHash} therefore disables RevisionRootPage body
 *                  verification for that revision (the record checksum still covers offset+timestamp).
 */
public record RevisionFileData(long offset, Instant timestamp, long pageHash) {

  /**
   * Convenience constructor for backends/records without a persisted RevisionRootPage hash (RAM
   * backend, legacy records). Equivalent to {@code RevisionFileData(offset, timestamp, 0L)}.
   */
  public RevisionFileData(long offset, Instant timestamp) {
    this(offset, timestamp, 0L);
  }
}
