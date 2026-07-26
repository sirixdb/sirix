package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.pageskip.PageSkipRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression tests for the page-skip registry's negative-nameKey family:
 * nameKeys are {@code String#hashCode()} values and fields like
 * {@code amount}/{@code active} hash NEGATIVE. The scan scheduler's
 * historical {@code anchorNameKey < 0} guards treated every such field as
 * unpublishable (the missing sentinel is exactly {@code -1}), so scans
 * anchored on them never consulted the registry and swept every page on
 * every query.
 *
 * <p>The resource here is built WITHOUT a path summary so the
 * PathSummary-persisted page bitmap (schedule source 1) cannot serve the
 * scan — the registry (sources 2+3) is the only skip index in play.
 */
public final class PageSkipNegativeHashTest {

  private static final String DB = "pageskip-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final int N = 999;

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-pageskip-");
    PageSkipRegistry.clear();
    final Random rng = new Random(13);
    final StringBuilder sb = new StringBuilder(N * 32);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      // "amount" and "active" both hash NEGATIVE.
      sb.append("{\"amount\":").append(rng.nextInt(1000))
        .append(",\"active\":").append(rng.nextBoolean())
        .append('}');
    }
    sb.append(']');
    Databases.createJsonDatabase(new DatabaseConfiguration(dbDir.resolve(DB)));
    try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB))) {
      db.createResource(ResourceConfiguration.newBuilder(RES).buildPathSummary(false).build());
      try (final var session = db.beginResourceSession(RES);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(sb.toString()));
        wtx.commit();
      }
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    PageSkipRegistry.clear();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  void negativeHashAnchorPublishesAndReusesPageSkipBitmap() throws Exception {
    final int amountNameKey = "amount".hashCode();
    org.junit.jupiter.api.Assertions.assertTrue(amountNameKey < 0, "test premise: 'amount' hashes negative");

    final String query = "count(for $u in " + SRC + " where $u.amount gt 500 return $u)";
    final String interpreted = run(query, false);

    try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
         final var session = db.beginResourceSession(RES)) {
      final String resourceKey = session.getResourceConfig().getResource().toString();
      assertNull(PageSkipRegistry.lookup(resourceKey), "registry must start empty");

      // First vectorized run: full sweep, but it must PUBLISH the bitmap for
      // the negative-hash anchor (historically `anchorNameKey < 0` dropped it).
      final String first = run(query, true);
      assertEquals(interpreted, first, "first vectorized run differs from interpreted");
      final PageSkipRegistry.Handle handle = PageSkipRegistry.lookup(resourceKey);
      assertNotNull(handle, "scan must create the per-resource page-skip handle");
      assertNotNull(handle.pagesForOrNull(amountNameKey),
          "the negative-hash anchor's bitmap must be published after a complete scan");

      // Second run (fresh executor — per-executor caches are empty) consults
      // the registry-backed schedule; results stay byte-identical.
      final String second = run(query, true);
      assertEquals(interpreted, second, "registry-backed rerun differs from interpreted");
    }
  }

  @Test
  void booleanNegativeHashAnchorAlsoPublishes() throws Exception {
    final int activeNameKey = "active".hashCode();
    org.junit.jupiter.api.Assertions.assertTrue(activeNameKey < 0, "test premise: 'active' hashes negative");

    final String query = "count(for $u in " + SRC + " where $u.active return $u)";
    final String interpreted = run(query, false);
    final String vectorized = run(query, true);
    assertEquals(interpreted, vectorized);

    try (final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
         final var session = db.beginResourceSession(RES)) {
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final PageSkipRegistry.Handle handle = PageSkipRegistry.lookup(resourceKey);
      assertNotNull(handle);
      assertNotNull(handle.pagesForOrNull(activeNameKey));
    }
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final var session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) exec.close();
      }
    }
  }
}
