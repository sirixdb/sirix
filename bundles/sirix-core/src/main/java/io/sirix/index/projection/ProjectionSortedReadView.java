/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.HOTReadIntent;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Fully validated, heap-owned sorted chunk and optional directory routing. No backing array,
 * transaction, page reference or native guard escapes this immutable facade. Writers receive
 * private encoded bytes; read accessors share only descriptors whose array APIs also copy.
 */
public final class ProjectionSortedReadView {
  private final ProjectionSortedLeaf leaf;
  private final @Nullable ProjectionSortedDirectoryNode directory;

  private ProjectionSortedReadView(final ProjectionSortedLeaf leaf,
      final @Nullable ProjectionSortedDirectoryNode directory) {
    this.leaf = leaf;
    this.directory = directory;
  }

  static ProjectionSortedReadView open(final byte[] verified, final boolean directory, final StorageEngineReader reader,
      final int indexNumber, final HOTReadIntent intent, final @Nullable ReadScope scope) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.openReadOnly(verified);
    return new ProjectionSortedReadView(leaf, directory
        ? ProjectionSortedDirectoryNode.open(leaf, reader, indexNumber, intent, scope)
        : null);
  }

  ProjectionSortedLeaf leaf() {
    return leaf;
  }

  ProjectionSortedDirectoryNode directory() {
    return Objects.requireNonNull(directory, "not a directory view");
  }

  public byte[] copyBytes() {
    return leaf.encodedBytes();
  }

  public int encodedSize() {
    return leaf.encodedSize();
  }

  /** Conservative object headers, references, aligned arrays and descriptor metadata. */
  public long retainedSizeBytes() {
    return 32 + leaf.retainedSizeBytes() + (directory == null
        ? 0
        : directory.retainedMetadataBytes());
  }
}
