package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MembershipProjectionFallbackTest {
  @TempDir
  File directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void separateMembershipAndAggregateProjectionsKeepTheirRoutedPlan(final VersioningType versioning) throws Exception {
    for (final boolean storeChildCount : new boolean[] {true, false}) {
      verifyFallback(versioning, storeChildCount);
    }
  }

  private void verifyFallback(final VersioningType versioning, final boolean storeChildCount) throws Exception {
    final var location = directory.toPath().resolve(Boolean.toString(storeChildCount));
    Files.createDirectories(location);
    final var databasePath = location.resolve("membership");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      build(database, "outer", "[{\"id\":1,\"g\":0,\"n\":10},{\"id\":2,\"g\":0,\"n\":20},{\"g\":1,\"n\":30}]",
          versioning, storeChildCount);
      build(database, "inner", "[{\"id\":2}]", versioning, storeChildCount);
    }
    try (var store = BasicJsonDBStore.newBuilder().location(location).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (final boolean anti : new boolean[] {false, true}) {
        final String query = """
            declare variable $T := xs:dateTime('2024-02-01T00:00:00Z');
            declare variable $V := xs:dateTime('2024-03-01T00:00:00Z');
            let $new := jn:open-bitemporal('membership','inner',$T,$V)
            for $a in jn:open-bitemporal('membership','outer',$T,$V)
            where QUANT(for $b in $new where $a.id eq $b.id return $b.id)
            let $g := $a.g, $n := $a.n
            group by $g
            let $count := count($n), $total := sum($n)
            order by $g
            return {"g":$g,"n":$count,"total":$total}
            """.replace("QUANT", anti
            ? "empty"
            : "exists");
        final String expected = anti
            ? "{\"g\":0,\"n\":1,\"total\":10} {\"g\":1,\"n\":1,\"total\":30}"
            : "{\"g\":0,\"n\":1,\"total\":20}";
        assertEquals(expected, serialize(new Query(generic, query).execute(context)));
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        assertEquals(expected, serialize(new Query(chain, query).execute(context)));
        assertEquals(served + 1, SirixVectorizedExecutor.groupAggServedCount());
      }
    }
  }

  private static void build(final Database<JsonResourceSession> database, final String resource, final String rows,
      final VersioningType versioning, final boolean storeChildCount) throws Exception {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder(resource)
                                                            .storageType(StorageType.FILE_CHANNEL)
                                                            .versioningApproach(versioning)
                                                            .storeChildCount(storeChildCount)
                                                            .validTimePaths("lo", "hi")
                                                            .customCommitTimestamps(true)
                                                            .storeDiffs(false)
                                                            .build()));
    final String json = rows.replace("}", ",\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2024-06-01T00:00:00Z\"}");
    try (final JsonResourceSession session = database.beginResourceSession(resource);
        final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
      ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "membership");
      final IndexDef membership = projection(0, List.of("id"));
      final var controller = session.getWtxIndexController(writer.getRevisionNumber());
      controller.createIndexes(resource.equals("outer")
          ? Set.of(membership, projection(1, List.of("g", "n")))
          : Set.of(membership), writer);
      writer.commit("membership", Instant.parse("2024-01-15T00:00:00Z"));
    }
  }

  private static IndexDef projection(final int id, final List<String> fields) throws Exception {
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
        fields.stream().map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON)).toList(),
        fields.stream().map(field -> Type.LON).toList(), id, IndexDef.DbType.JSON);
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter output = new StringWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(output))) {
      serializer.serialize(sequence);
    }
    return output.toString().trim();
  }
}
