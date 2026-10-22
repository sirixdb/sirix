package io.sirix.index.aggregate;

import io.sirix.api.StorageEngineReader;

import java.util.Objects;

/** Owned immutable derived reader and the source revision it represents. */
public record DerivedIndexSnapshot(StorageEngineReader reader, int sourceRevision) implements AutoCloseable {
  public DerivedIndexSnapshot {
    Objects.requireNonNull(reader);
    if (sourceRevision < 1) {
      throw new IllegalArgumentException("A derived snapshot represents a published source revision");
    }
  }

  @Override
  public void close() {
    reader.close();
  }
}
