/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.page.interfaces.Page;

import java.util.List;
import java.util.Objects;

/**
 * Test-only package bridge: exposes {@link AbstractHOTIndexWriter#spliceBulkBuiltRoot} (package
 * private by design — production callers are the {@link AbstractHOTBulkIndexLoader} family) to
 * benchmarks and probes living outside {@code io.sirix.index.hot}, so the bulk arm of a
 * differential measures the REAL splice path rather than a re-implementation.
 */
public final class BulkSpliceTestBridge {

  private BulkSpliceTestBridge() {
    throw new AssertionError("no instances");
  }

  /**
   * Splice {@code sortedEntries} as {@code writer}'s whole index tree — the production
   * {@code spliceBulkBuiltRoot} path (empty-tree guard, {@link HOTBulkBuilder#build}, fresh-subtree
   * TIL registration).
   *
   * @param writer a writer whose index tree is still empty
   * @param sortedEntries entries sorted strictly ascending by unsigned key, no duplicates
   */
  public static void spliceBulkBuiltRoot(final AbstractHOTIndexWriter<?> writer,
      final List<HOTBulkBuilder.Entry> sortedEntries) {
    Objects.requireNonNull(writer, "writer");
    Objects.requireNonNull(sortedEntries, "sortedEntries");
    writer.spliceBulkBuiltRoot(sortedEntries);
  }

  /**
   * Run {@link HOTMalformedSubtreeDetector} over {@code writer}'s in-transaction tree and return the
   * number of malformed subtrees (0 = clean). The resolver consults the transaction-intent log for
   * pages whose in-memory reference was nulled by {@code log.put}, swizzling them back so the walk
   * sees the whole tree.
   */
  public static int malformedSubtreeCount(final AbstractHOTIndexWriter<?> writer) {
    Objects.requireNonNull(writer, "writer");
    final TransactionIntentLog log = writer.storageEngineWriter.getLog();
    final HOTMalformedSubtreeDetector.PageResolver resolver = pageRef -> {
      final Page page = pageRef.getPage();
      if (page != null) {
        return page;
      }
      final PageContainer container = log.get(pageRef);
      if (container != null && container.getModified() != null) {
        pageRef.setPage(container.getModified());
        return container.getModified();
      }
      return null;
    };
    return HOTMalformedSubtreeDetector.detect(writer.getRootReference(), resolver).size();
  }
}
