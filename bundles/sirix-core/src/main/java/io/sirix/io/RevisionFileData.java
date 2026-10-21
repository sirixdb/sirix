package io.sirix.io;

import io.sirix.exception.SirixRevisionPrunedException;

import java.time.Instant;
import java.util.Objects;

/**
 * Revision metadata. A live record carries its root offset and payload hash; a pruned record
 * carries the tombstone sentinel and tag. RAM records may omit the payload hash.
 */
public record RevisionFileData(long offset, Instant timestamp, long pageHash) {
  public RevisionFileData {
    Objects.requireNonNull(timestamp, "timestamp");
  }

  public RevisionFileData(final long offset, final Instant timestamp) {
    this(offset, timestamp, 0L);
  }

  public static RevisionFileData pruned(final Instant timestamp) {
    return new RevisionFileData(IOStorage.PRUNED_REVISION_OFFSET, timestamp, IOStorage.PRUNED_REVISION_TAG);
  }

  public boolean isPruned() {
    return offset == IOStorage.PRUNED_REVISION_OFFSET && pageHash == IOStorage.PRUNED_REVISION_TAG;
  }

  public void requireReadable(final int revision) {
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must not be negative");
    }
    if (isPruned()) {
      throw new SirixRevisionPrunedException(revision, timestamp);
    }
  }
}
