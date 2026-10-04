/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

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
    return copyOf(leaf, key, sideReferenceKey, null);
  }

  /**
   * Copy a slot, omitting its side-map access when detached bytes prove the reference unnecessary. A
   * null predicate retains opaque-slot behavior and copies every present side reference.
   */
  public static @Nullable HOTLeafEntry copyOf(final HOTLeafPage leaf, final byte[] key, final long sideReferenceKey,
      final @Nullable Predicate<byte[]> sideReferenceNeeded) {
    Objects.requireNonNull(leaf);
    Objects.requireNonNull(key);
    if (leaf.getIndexType() != IndexType.PROJECTION) {
      throw new IllegalArgumentException("Projection entry read requires a projection leaf");
    }
    final int entry = leaf.findEntry(key);
    if (entry < 0) {
      return null;
    }
    final byte[] value = leaf.copyStoredValue(entry);
    final PageReference side = sideReferenceNeeded == null || sideReferenceNeeded.test(value)
        ? leaf.getPageReference(sideReferenceKey)
        : null;
    return new HOTLeafEntry(value, side);
  }
}
