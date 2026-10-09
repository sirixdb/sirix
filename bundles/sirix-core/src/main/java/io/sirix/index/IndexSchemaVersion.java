package io.sirix.index;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide fence for query plans that depend on index catalogues or document structure.
 * Core APIs and query-language DDL share this fence. A committed revision can introduce paths,
 * replace a resource or revert to a different catalogue, so revision publication also advances it.
 * This deliberately invalidates more than index DDL; it never retains a stale routing decision.
 */
public final class IndexSchemaVersion {
  private static final AtomicLong VERSION = new AtomicLong();

  private IndexSchemaVersion() {
    throw new AssertionError("no instances");
  }

  public static long current() {
    return VERSION.get();
  }

  public static void changed() {
    VERSION.incrementAndGet();
  }
}
