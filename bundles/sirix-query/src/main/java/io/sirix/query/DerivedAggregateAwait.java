package io.sirix.query;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.aggregate.DerivedIndexMaintainer;

import java.time.Duration;

/** Bounded opportunity for the maintainer to publish a pinned query's exact mapping. */
public final class DerivedAggregateAwait {
  private static final Duration TIMEOUT =
      Duration.ofMillis(Math.max(0, Integer.getInteger("sirix.aggregate.awaitMillis", 20)));

  private DerivedAggregateAwait() {}

  /**
   * A reached frontier only permits retrying the identity-validated exact mapping lookup. Batch
   * interiors may remain unmapped. Unavailability always leaves the exact fallback.
   */
  public static boolean awaitMapping(final StorageEngineReader source) {
    return awaitMapping(source, TIMEOUT);
  }

  static boolean awaitMapping(final StorageEngineReader source, final Duration timeout) {
    if (timeout.isZero() || !(source.getResourceSession() instanceof AbstractResourceSession<?, ?> session)) {
      return false;
    }
    final DerivedIndexMaintainer.Resource resource = session.getDerivedResource();
    if (resource == null || resource.failure() != null || resource.maintainedRevision() >= source.getRevisionNumber()) {
      return false;
    }
    try {
      return resource.awaitRevision(source.getRevisionNumber(), timeout);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    } catch (final IllegalStateException unavailable) {
      return false;
    }
  }
}
