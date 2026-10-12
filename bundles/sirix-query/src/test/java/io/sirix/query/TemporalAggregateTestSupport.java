package io.sirix.query;

import io.sirix.access.Databases;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.query.json.BasicJsonDBStore;

import java.io.IOException;
import java.nio.file.Files;

/** Finish asynchronous fixture setup before assertions that require the aggregate route. */
final class TemporalAggregateTestSupport {
  private TemporalAggregateTestSupport() {}

  static void await(final SirixQueryContext context) throws IOException {
    if (context.getJsonItemStore() instanceof BasicJsonDBStore store) {
      try (var paths = Files.list(store.getLocation())) {
        for (final var path : paths.toList()) {
          if (Databases.existsDatabase(path)) {
            DerivedIndexTestSupport.await(store.lookup(path.getFileName().toString()).getDatabase());
          }
        }
      }
    }
  }
}
