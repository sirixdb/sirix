/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.io.StorageType;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A long projection's exact fallback must preserve fractions introduced by numeric updates. */
@Isolated
final class ProjectionNumericUpdateExactnessTest {
  private static final String DICTIONARY_PROPERTY = "sirix.projection.globalDict";
  private static final String QUERY =
      "let $d := jn:doc('carry','resource') return [count($d[]),sum(for $r in $d[] return $r.n)]";

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void fractionalUpdateMatchesInterpreterColdAndWarm(final VersioningType versioning) throws Exception {
    final String previous = System.getProperty(DICTIONARY_PROPERTY);
    try {
      for (final String dictionaries : new String[] {"never", "always"}) {
        System.setProperty(DICTIONARY_PROPERTY, dictionaries);
        final Path location = directory.resolve(dictionaries);
        try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                          .location(location)
                                                          .storageType(StorageType.FILE_CHANNEL)
                                                          .buildPathSummary(true)
                                                          .buildPathStatistics(true)
                                                          .versioningType(versioning)
                                                          .build();
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
            final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
          serialize(chain, context, "jn:store('carry','resource','" + document() + "')");
          serialize(chain, context, """
              let $d := jn:doc('carry','resource')
              let $i := jn:create-projection-index($d, '/[]', ('/[]/n','/[]/s'), ('long','string'))
              return sdb:commit($d)
              """);
        }
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(location.resolve("carry"));
            final JsonResourceSession session = database.beginResourceSession("resource");
            final JsonNodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveToFirstChild()); // array
          assertTrue(writer.moveToFirstChild()); // first record
          assertTrue(writer.moveToFirstChild()); // fused n key and numeric value
          writer.setNumberValue(1.25d);
          writer.commit();
        }
        ProjectionIndexRegistry.clear();
        ProjectionIndexCatalog.clearCache();
        Databases.clearGlobalCaches();
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        final String expected;
        try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
          expected = serialize(new CompileChain(), context, QUERY);
          assertEquals("[900,404551.25]", expected, "independent arithmetic and interpreter must agree");
        }
        ProjectionIndexRegistry.clear();
        ProjectionIndexCatalog.clearCache();
        Databases.clearGlobalCaches();
        try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
            final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
          final Query optimized = new Query(chain, QUERY);
          assertEquals(expected, serialize(optimized, context), "cold optimized sum must retain the fraction");
          assertEquals(expected, serialize(optimized, context), "warm optimized sum must retain the fraction");
        }
      }
    } finally {
      if (previous == null) {
        System.clearProperty(DICTIONARY_PROPERTY);
      } else {
        System.setProperty(DICTIONARY_PROPERTY, previous);
      }
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
    }
  }

  private static String document() {
    final StringBuilder json = new StringBuilder(24_000).append('[');
    for (int row = 0; row < 900; row++) {
      if (row != 0) {
        json.append(',');
      }
      json.append("{\"n\":").append(row).append(",\"s\":\"v").append(row % 3).append("\"}");
    }
    return json.append(']').toString();
  }

  private static String serialize(final CompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    return serialize(new Query(chain, query), context);
  }

  private static String serialize(final Query query, final SirixQueryContext context) throws Exception {
    final StringWriter result = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(result)) {
      query.serialize(context, writer);
    }
    return result.toString().trim();
  }
}
