package io.sirix.access.trx.page;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

/** Terminal drain order, separate from ordinary writer/database close and all hot paths. */
final class SnapshotExecutorShutdown {
  private SnapshotExecutorShutdown() {}

  static void drain(final ExecutorService append, final ExecutorService serialization) {
    Objects.requireNonNull(append);
    Objects.requireNonNull(serialization);
    // Append tasks may still submit/join serialization work. Do not close its pool first, or
    // on a failed append drain: workers may still own pages and need that pool to finish.
    append.close();
    serialization.close();
  }
}
