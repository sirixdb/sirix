package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.io.StorageType;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.bench.bitemporal.BitemporalQueries;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.bench.bitemporal.BitemporalTemporalAggregates;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Executes the indexed SH1 query paths inside the native smoke image, using only shipped metadata.
 */
@Tag("native-image")
@Isolated
final class NativeImageBitemporalQueryTest {
  private static final String INITIAL_CONTRACTS = """
      [{"id":1,"pid":1,"sid":1,"cost":10,"qty":2,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
       {"id":2,"pid":2,"sid":2,"cost":30,"qty":3,"grade":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
       {"id":3,"pid":1,"sid":3,"cost":50,"qty":4,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
      """;
  private static final String CORRECTED_CONTRACTS = """
      [{"id":1,"pid":1,"sid":1,"cost":20,"qty":5,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
       {"id":2,"pid":2,"sid":2,"cost":30,"qty":3,"grade":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
       {"id":4,"pid":2,"sid":4,"cost":60,"qty":6,"grade":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
      """;

  @TempDir
  Path directory;

  @Test
  void indexedTemporalQueriesMatchTheGenericEvaluatorBeforeAndAfterCacheReuse() throws Exception {
    createFixture();
    try (
        final var store =
            BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var routed = SirixCompileChain.createWithJsonStore(store);
        final var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      DerivedIndexTestSupport.await(store.lookup(BitemporalSchema.DATABASE).getDatabase());
      for (int repetition = 0; repetition < 2; repetition++) {
        for (final BitemporalQueries.Query query : BitemporalQueries.all()) {
          final long joinedBefore = SirixVectorizedExecutor.joinGroupServedCount();
          final String actual = execute(routed, context, query.text());
          if (query.index() == 4) {
            assertEquals(joinedBefore + 1, SirixVectorizedExecutor.joinGroupServedCount(),
                "Q4 must execute the masked-column join, including its weak-key weighted cache");
          }
          assertFalse(actual.isEmpty(), "Q" + query.index() + " must exercise a nonempty result");
          assertEquals(execute(generic, context, query.text()), actual,
              "Q" + query.index() + ", repetition " + repetition);
        }
      }
    }
  }

  private void createFixture() throws Exception {
    final Path databasePath = directory.resolve(BitemporalSchema.DATABASE);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final var database = Databases.openJsonDatabase(databasePath)) {
      createBusinessResource(database, BitemporalSchema.CONTRACTS, INITIAL_CONTRACTS);
      createBusinessResource(database, BitemporalSchema.PRODUCTS, """
          [{"id":1,"category":1,"retail":100,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
           {"id":2,"category":2,"retail":200,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
          """);
      createBusinessResource(database, BitemporalSchema.SUPPLIERS, """
          [{"id":1,"region":1,"tier":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
           {"id":2,"region":2,"tier":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
          """);
      try (final var session = database.beginResourceSession(BitemporalSchema.CONTRACTS);
          final var writer = session.beginNodeTrx()) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.remove();
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(CORRECTED_CONTRACTS), JsonNodeTrx.Commit.NO);
        writer.commit("correction and disappearance", Instant.parse("2024-07-01T00:00:00Z"));
      }
      createCalendar(database, BitemporalSchema.EPOCHS, BitemporalSchema.PUBLICATIONS);
      createCalendar(database, BitemporalSchema.DAYS, BitemporalSchema.HORIZON_DAYS);
    }
  }

  private static void createBusinessResource(final Database<JsonResourceSession> database, final String resource,
      final String json) throws Exception {
    database.createResource(ResourceConfiguration.newBuilder(resource)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .customCommitTimestamps(true)
                                                 .validTimePaths("vf", "vt")
                                                 .storeDiffs(false)
                                                 .build());
    try (final var session = database.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      final IndexDef id = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
          IndexDef.DbType.JSON);
      session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(id), writer);
      ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, BitemporalSchema.DATABASE);
      BitemporalProjections.declare(session, writer, resource);
      BitemporalTemporalAggregates.declare(session, writer, resource);
      writer.commit("initial", BitemporalSchema.DAY_ZERO);
    }
  }

  private static void createCalendar(final Database<JsonResourceSession> database, final String resource,
      final int entries) throws Exception {
    database.createResource(ResourceConfiguration.newBuilder(resource)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .customCommitTimestamps(true)
                                                 .storeDiffs(false)
                                                 .build());
    final boolean epochs = resource.equals(BitemporalSchema.EPOCHS);
    final StringBuilder json = new StringBuilder(entries * 64).append('[');
    for (int index = 0; index < entries; index++) {
      if (index > 0) {
        json.append(',');
      }
      json.append(epochs
          ? "{\"epoch\":"
          : "{\"day_no\":")
          .append(index)
          .append(",\"ts\":\"")
          .append(epochs
              ? BitemporalSchema.systemTime(index)
              : BitemporalSchema.day(index))
          .append("\"}");
    }
    json.append(']');
    try (final var session = database.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
      writer.commit("calendar", BitemporalSchema.DAY_ZERO);
    }
  }

  private static String execute(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    final StringWriter result = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(result)) {
      new SirixStringSerializer(writer).serialize(new Query(chain, text).execute(context));
    }
    return result.toString().trim();
  }
}
