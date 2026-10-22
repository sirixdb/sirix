package io.sirix.exception;

import java.time.Instant;
import java.util.Objects;

/** A revision's metadata survives retention, but its root is no longer available. */
public final class SirixRevisionPrunedException extends SirixRuntimeException {
  private static final long serialVersionUID = 1L;

  private final int revision;
  private final Instant timestamp;

  public SirixRevisionPrunedException(final int revision, final Instant timestamp) {
    super("Revision " + revision + " was pruned by retention (committed at "
        + Objects.requireNonNull(timestamp, "timestamp") + ")");
    if (revision < 0) {
      throw new IllegalArgumentException("Revision must not be negative");
    }
    this.revision = revision;
    this.timestamp = timestamp;
  }

  public int getRevision() {
    return revision;
  }

  public Instant getTimestamp() {
    return timestamp;
  }
}
