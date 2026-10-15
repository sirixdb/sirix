package io.sirix.index.aggregate;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;


/** Complete derived setup deterministically, outside measured operations. */
public final class DerivedIndexTestSupport {
  private DerivedIndexTestSupport() {}

  public static void await(final ResourceSession<?, ?> session) {
    final DerivedIndexMaintainer.Resource resource = ((AbstractResourceSession<?, ?>) session).getDerivedResource();
    if (resource != null) {
      await(resource, session.getMostRecentRevisionNumber());
    }
  }

  public static void await(final Database<?> database) {
    for (final var path : database.listResources()) {
      // Database handles cache their user sessions; borrowing one must not close a caller's writer.
      await(database.beginResourceSession(path.getFileName().toString()));
    }
  }

  public static void await(final DerivedIndexMaintainer.Resource resource, final int revision) {
    resource.pause();
    try {
      resource.drain();
      if (resource.maintainedRevision() < revision) {
        throw new AssertionError("Requested revision has not been published");
      }
    } finally {
      resource.resume();
    }
  }
}
