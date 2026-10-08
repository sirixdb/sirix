/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.HOTReadIntent;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.cache.HOTMiniPageCache.ReadScope.ImageSnapshot;
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
  private final @Nullable ImageSnapshot routing;

  private ProjectionSortedReadView(final ProjectionSortedLeaf leaf,
      final @Nullable ProjectionSortedDirectoryNode directory, final @Nullable ImageSnapshot routing) {
    this.leaf = leaf;
    this.directory = directory;
    this.routing = routing;
  }

  static ProjectionSortedReadView open(final byte[] verified, final boolean directory, final StorageEngineReader reader,
      final int indexNumber, final HOTReadIntent intent, final @Nullable ReadScope scope) {
    final ProjectionSortedDirectoryNode node = directory
        ? ProjectionSortedDirectoryNode.openReadOnly(verified, reader, indexNumber, intent, scope)
        : null;
    final ProjectionSortedLeaf leaf = node == null
        ? ProjectionSortedLeaf.openReadOnly(verified)
        : node.leaf();
    return new ProjectionSortedReadView(leaf, node, directory && scope != null
        ? scope.snapshotImage()
        : null);
  }

  void restoreRouting(final StorageEngineReader reader, final @Nullable ReadScope scope) {
    if (routing != null && scope != null) {
      scope.restoreImage(reader, routing);
    }
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

  /** Prepare optional physical routing before its complete cache charge is published. */
  public void prepareForCache() {
    if (routing != null) {
      routing.prepareForCache();
    }
  }

  /** Conservative object headers, references, aligned arrays and descriptor metadata. */
  public long retainedSizeBytes() {
    return 40 + leaf.retainedSizeBytes() + (directory == null
        ? 0
        : directory.retainedMetadataBytes())
        + (routing == null
            ? 0
            : routing.retainedSizeBytes());
  }
}
