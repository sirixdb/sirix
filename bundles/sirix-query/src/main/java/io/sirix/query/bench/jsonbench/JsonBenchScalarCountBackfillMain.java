/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.jsonbench;

import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionScalarCountBackfill;

import java.nio.file.Path;

/** Backfills one scalar count column in a new revision of an existing JSONBench database. */
public final class JsonBenchScalarCountBackfillMain {

  private JsonBenchScalarCountBackfillMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) {
    final boolean repair = args.length == 5 && "--repair-flag-summary-revision".equals(args[3]);
    if (args.length != 3 && !repair) {
      throw new IllegalArgumentException("usage: <database-root> <projection-index-number> <column>"
          + " [--repair-flag-summary-revision <prior-revision>]");
    }
    final Path database = Path.of(args[0]).resolve(JsonBenchSchema.DATABASE);
    final int indexNumber = Integer.parseInt(args[1]);
    final int column = Integer.parseInt(args[2]);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(database);
        JsonResourceSession session = db.beginResourceSession(JsonBenchSchema.RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      if (repair) {
        ProjectionScalarCountBackfill.repairFlagSummaryRevision(wtx, indexNumber, Integer.parseInt(args[4]));
        wtx.commit();
        System.out.println("Retagged unchanged projection flag summary");
        return;
      }
      final ProjectionScalarCountBackfill.Result result =
          ProjectionScalarCountBackfill.run(wtx, indexNumber, column);
      if (!result.published()) {
        throw new IllegalStateException("scalar count backfill unavailable: " + result);
      }
      wtx.commit();
      System.out.println(result);
    }
  }
}
