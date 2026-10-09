/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;

import java.util.AbstractList;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntFunction;

/**
 * Query-owned directory windows. The fixed-size cache retains no transaction or native page view; a
 * loader returns owned, validated bytes and closes its revision-pinned read before publication.
 * Eviction only drops a reference, so concurrent consumers keep their immutable window safely.
 */
final class ProjectionDirectoryWindows extends AbstractList<RowGroupDirectory> implements RandomAccess {
  static final int WINDOW_SIZE = 64;
  static final int CACHE_WINDOWS = 16;

  private record Window(int number, RowGroupDirectory[] directories) {
  }

  private final int rowGroups;
  private final byte[] columnKinds;
  private final IntFunction<RowGroupDirectory[]> loader;
  private final AtomicReferenceArray<Window> windows = new AtomicReferenceArray<>(CACHE_WINDOWS);

  ProjectionDirectoryWindows(final int rowGroups, final byte[] columnKinds,
      final IntFunction<RowGroupDirectory[]> loader) {
    if (rowGroups < 0 || rowGroups > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalArgumentException("rowGroups out of range: " + rowGroups);
    }
    this.rowGroups = rowGroups;
    this.columnKinds = Objects.requireNonNull(columnKinds, "columnKinds").clone();
    this.loader = Objects.requireNonNull(loader, "loader");
  }

  byte[] columnKinds() {
    return columnKinds.clone();
  }

  @Override
  public int size() {
    return rowGroups;
  }

  @Override
  public RowGroupDirectory get(final int index) {
    Objects.checkIndex(index, rowGroups);
    final int number = index / WINDOW_SIZE;
    final int slot = number & (CACHE_WINDOWS - 1);
    Window window = windows.get(slot);
    if (window == null || window.number() != number) {
      final RowGroupDirectory[] loaded = Objects.requireNonNull(loader.apply(number), "directory window");
      final int count = Math.min(WINDOW_SIZE, rowGroups - number * WINDOW_SIZE);
      if (loaded.length != count) {
        throw new IllegalStateException("directory window returned " + loaded.length + " of " + count + " leaves");
      }
      for (int i = 0; i < loaded.length; i++) {
        final byte[] descriptor = Objects.requireNonNull(loaded[i], "directory").descriptor();
        // RowGroupDirectory construction already validated the descriptor. This window adds the
        // persisted-metadata shape check without rescanning its full entry schema on every load.
        if (RowGroupDescriptor.columnCount(descriptor) != columnKinds.length) {
          throw new ProjectionStoreInconsistentException(number * WINDOW_SIZE + i,
              "descriptor column count disagrees with persisted metadata");
        }
        for (int column = 0; column < columnKinds.length; column++) {
          if (RowGroupDescriptor.kind(descriptor, column) != columnKinds[column]) {
            throw new ProjectionStoreInconsistentException(number * WINDOW_SIZE + i,
                "column " + column + " kind disagrees with persisted metadata");
          }
        }
      }
      window = new Window(number, loaded);
      windows.set(slot, window);
    }
    return window.directories()[index % WINDOW_SIZE];
  }
}
