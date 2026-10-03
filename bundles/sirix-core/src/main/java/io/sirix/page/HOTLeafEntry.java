/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Detached value of one HOT slot and its optional side-page reference. An empty value preserves a
 * physically present projection tombstone. The value owns its bytes; no native leaf frame escapes.
 */
public record HOTLeafEntry(byte[] value, @Nullable PageReference sideReference) {
  public HOTLeafEntry {
    Objects.requireNonNull(value);
  }

  /** Copy a slot while the caller holds a guard or validates its optimistic read afterwards. */
  public static @Nullable HOTLeafEntry copyOf(final HOTLeafPage leaf, final byte[] key, final long sideReferenceKey) {
    Objects.requireNonNull(leaf);
    Objects.requireNonNull(key);
    if (leaf.getIndexType() != IndexType.PROJECTION) {
      throw new IllegalArgumentException("Projection entry read requires a projection leaf");
    }
    final int entry = leaf.findEntry(key);
    return entry < 0
        ? null
        : new HOTLeafEntry(leaf.copyStoredValue(entry), leaf.getPageReference(sideReferenceKey));
  }
}
