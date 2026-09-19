/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.projection.ProjectionIndexHOTStorage.ParallelWalkReaders;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/** Loads a committed column-major directory without serializing independent metadata reads. */
final class ProjectionDirectoryLoad {
  private ProjectionDirectoryLoad() {}

  record Result(int[] physicalOrder, List<RowGroupDirectory> directories) {
  }

  static Result read(final StorageEngineReader reader, final int indexNumber, final int rowGroupCount,
      final @Nullable ParallelWalkReaders readers, final boolean overlap) {
    Objects.requireNonNull(reader, "reader is required");
    if (!overlap || readers == null || Runtime.getRuntime().availableProcessors() < 2
        || !ProjectionIndexFences.hasBoundedDenseOrder(reader, indexNumber, rowGroupCount)) {
      final int[] order = ProjectionIndexFences.readPhysicalOrder(reader, indexNumber, rowGroupCount);
      return new Result(order,
          ProjectionIndexHOTStorage.readColumnMajorDirectories(reader, indexNumber, rowGroupCount, order, readers));
    }
    final DenseRead task = new DenseRead(readers, indexNumber, rowGroupCount);
    ForkJoinPool.commonPool().execute(task);
    final int[] order;
    try {
      // This reader remains exclusively owned by the caller. The task leases its own revision view.
      order = ProjectionIndexFences.readPhysicalOrder(reader, indexNumber, rowGroupCount);
    } catch (final RuntimeException | Error failure) {
      try {
        task.join();
      } catch (final RuntimeException | Error workerFailure) {
        if (workerFailure != failure) {
          failure.addSuppressed(workerFailure);
        }
      }
      throw failure;
    }
    task.join();
    final List<RowGroupDirectory> physical =
        Objects.requireNonNull(task.directories, "descriptor reader did not produce a directory");
    if (physical.size() != rowGroupCount) {
      throw new IllegalStateException("descriptor count differs from validated dense document order");
    }
    // The validated bounded permutation caps these additional identity/reference arrays at 12 MiB.
    final RowGroupDirectory[] ordered = new RowGroupDirectory[rowGroupCount];
    for (int i = 0; i < rowGroupCount; i++) {
      final int physicalId = order[i];
      final RowGroupDirectory directory = physical.get(physicalId - 1);
      if (directory.rowGroupId() != physicalId) {
        throw new IllegalStateException("descriptor identity differs from validated physical order");
      }
      ordered[i] = directory;
    }
    return new Result(order, Arrays.asList(ordered));
  }

  private static final class DenseRead extends RecursiveAction {
    private final ParallelWalkReaders readers;
    private final int indexNumber;
    private final int rowGroupCount;
    private @Nullable List<RowGroupDirectory> directories;

    private DenseRead(final ParallelWalkReaders readers, final int indexNumber, final int rowGroupCount) {
      this.readers = readers;
      this.indexNumber = indexNumber;
      this.rowGroupCount = rowGroupCount;
    }

    @Override
    protected void compute() {
      final int[] identity = new int[rowGroupCount];
      for (int i = 0; i < rowGroupCount; i++) {
        identity[i] = i + 1;
      }
      readers.runWithReader(lane -> {
        directories =
            ProjectionIndexHOTStorage.readColumnMajorDirectories(lane, indexNumber, rowGroupCount, identity, readers);
      });
    }
  }
}
