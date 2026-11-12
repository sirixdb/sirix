/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A group route decodes the masked lanes of its key and aggregate columns into ONE query-local
 * allocator slab and must hand it back on every exit. This runs the sliced grouped aggregate over a
 * projection whose columns the store does not yet hold resident — the shape that takes the masked
 * fill — proves the answer against the interpreter's, and proves that after the executor and the
 * database are gone the process allocator holds exactly the bytes it held before the query. A route
 * slab that outlived its route, or a decline that skipped the release, shows up as a non-zero
 * delta; the store's own column slabs are returned by retirement and are part of the same balance.
 */
final class GroupRouteLaneSlabReleaseTest {
  private static final String DB = "group-route-lane-slab-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  /** Several leaves, so the route lays out one lane per surviving leaf rather than one in total. */
  private static final int N = 20_000;

  private Path dbDir;
  private String previousGlobalDictMode;

  @BeforeEach
  void setUp() throws Exception {
    previousGlobalDictMode = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    dbDir = Files.createTempDirectory("sirix-group-route-lane-slab-");
    final Random random = new Random(20261106L);
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":")
        .append(i)
        .append(",\"g\":")
        .append(random.nextInt(37))
        .append(",\"v\":")
        .append(random.nextInt(1_000))
        .append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/g', '/[]/v'),
            ('long', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    DerivedCacheBudget.handoffHookForTesting = null;
    if (previousGlobalDictMode == null) {
      System.clearProperty("sirix.projection.globalDict");
    } else {
      System.setProperty("sirix.projection.globalDict", previousGlobalDictMode);
    }
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @DisplayName("a masked grouped aggregate answers like the interpreter and leaves no slab behind")
  void aMaskedGroupedAggregateReleasesItsRouteSlab(final boolean predicateTree) throws Exception {
    // A predicate on a third column makes the route's keep mask selective, so the key and aggregate
    // columns take the MASKED fill rather than a whole-column resident read.
    final String predicate = predicateTree
        ? "($h.id lt 12000 or $h.id gt 18000)"
        : "$h.id lt 12000";
    final String query = "for $h in " + DOC + " where " + predicate + " let $g := $h.g group by $g"
        + " order by $g return {\"g\": $g, \"s\": sum($h.v), \"c\": count($h)}";
    final String generic = run(query, false);
    assertTrue(generic.contains("\"g\":"), "the interpreter answered nothing: " + generic);

    final long slabBytesBefore = Allocators.getInstance().getSlabBytes();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
    final String vectorized = run(query, true);
    assertEquals(generic, vectorized, "the group route diverges from the interpreter");
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "the group route never served");
    assertTrue(SirixVectorizedExecutor.groupAggSlicedServedCount() > slicedBefore,
        "the SLICED arm never ran, so no masked lane was decoded");
    Databases.removeDatabase(dbDir.resolve(DB));
    assertEquals(slabBytesBefore, Allocators.getInstance().getSlabBytes(),
        "the route slab and the store's column slabs are all back with the allocator");
  }

  @ParameterizedTest
  @ValueSource(strings = {"roster", "computed", "bucket"})
  void failedSlabHandoffReturnsEveryRouteOwner(final String point) throws Exception {
    final String query = switch (point) {
      case "bucket" -> "for $h in " + DOC + " let $g := $h.g, $b := $h.id idiv 100 group by $g, $b order by $g, $b"
          + " return {\"g\":$g,\"b\":$b,\"c\":count($h)}";
      case "computed" ->
        "for $h in " + DOC + " where $h.id lt 12000 let $value := $h.v + $h.id, $g := $h.g group by $g order by $g"
            + " return {\"g\":$g,\"s\":sum($value)}";
      default -> "for $h in " + DOC + " where $h.id lt 12000 let $g := $h.g group by $g order by $g"
          + " return {\"g\":$g,\"s\":sum($h.v)}";
    };
    final String expected = run(query, false);
    final OutOfMemoryError failure = new OutOfMemoryError("injected " + point);
    DerivedCacheBudget.handoffHookForTesting = stage -> {
      if (point.equals(stage)) {
        throw failure;
      }
    };
    try {
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> run(query, true)));
    } finally {
      DerivedCacheBudget.handoffHookForTesting = null;
    }
    assertEquals(expected, run(query, true));
    Databases.removeDatabase(dbDir.resolve(DB));
    assertEquals(0, Allocators.getInstance().getSlabBytes());
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
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
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
