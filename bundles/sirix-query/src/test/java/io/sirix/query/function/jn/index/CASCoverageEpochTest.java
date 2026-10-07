package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASCoverageEpochTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @MethodSource("epochs")
  void successorCoverageSurvivesCommitAndFreshReopen(final VersioningType versioning, final boolean asynchronous) {
    final Path databasePath = directory.resolve("coverage");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    int fractionalRevision;
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .hashKind(HashType.NONE)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder("[{\"item\":{\"id\":1,\"value\":10}}");
        for (int i = 0; i < 2_048; i++) {
          json.append(",{\"item\":{\"id\":0,\"value\":0}}");
        }
        json.append(']');
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse("/[]/item/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit();
        if (!asynchronous) {
          moveToFirstId(writer);
          writer.setNumberValue(1.5d);
          writer.commit();
          fractionalRevision = session.getMostRecentRevisionNumber();
          replaceIdWithString(writer);
          writer.commit();
        } else {
          fractionalRevision = -1;
        }
      }
      if (asynchronous) {
        try (var session = database.beginResourceSession("rows");
            var writer = session.beginNodeTrx(3, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
          moveToFirstId(writer);
          for (int i = 0; i < 8; i++) {
            writer.setNumberValue(1);
          }
          writer.awaitPendingAsyncCommit();
          moveToFirstId(writer);
          writer.setNumberValue(1.5d);
          writer.moveToRightSibling();
          for (int i = 0; i < 8; i++) {
            writer.setNumberValue(10);
          }
          writer.commit();
          fractionalRevision = session.getMostRecentRevisionNumber();
          replaceIdWithString(writer);
          writer.moveToRightSibling();
          for (int i = 0; i < 8; i++) {
            writer.setNumberValue(10);
          }
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      final IndexDef initial = session.getRtxIndexController(1).getIndexes().getIndexDef(0, IndexType.CAS);
      assertTrue(initial.hasNumericValuesOnly());
      assertTrue(initial.hasCompleteNumericCoverage());
      final IndexDef fractional =
          session.getRtxIndexController(fractionalRevision).getIndexes().getIndexDef(0, IndexType.CAS);
      assertTrue(fractional.hasNumericValuesOnly());
      assertFalse(fractional.hasCompleteNumericCoverage());
      final IndexDef latest = session.getRtxIndexController(session.getMostRecentRevisionNumber())
                                     .getIndexes()
                                     .getIndexDef(0, IndexType.CAS);
      assertFalse(latest.hasNumericValuesOnly());
      assertFalse(latest.hasCompleteNumericCoverage());
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final CompileChain generic = new CompileChain();
      for (final int revision : List.of(1, fractionalRevision)) {
        final String text = "jn:doc('coverage','rows'," + revision + ")[].item[?$$.id gt 1].value";
        final List<Double> expected = revision == 1
            ? List.of()
            : List.of(10d);
        assertEquals(expected, values(new Query(generic, text).execute(context)));
        assertEquals(expected, values(new Query(chain, text).execute(context)));
      }
    }
  }

  private static void moveToFirstId(final JsonNodeTrx writer) {
    writer.moveToDocumentRoot();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
  }

  private static void replaceIdWithString(final JsonNodeTrx writer) {
    moveToFirstId(writer);
    writer.remove();
    writer.moveToParent();
    writer.insertObjectRecordAsFirstChild("id", new StringValue("1"));
  }

  private static Stream<Arguments> epochs() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(type -> Stream.of(Arguments.of(type, false), Arguments.of(type, true)));
  }

  private static List<Double> values(final Sequence sequence) {
    final List<Double> values = new ArrayList<>();
    if (sequence != null) {
      try (var iterator = sequence.iterate()) {
        Item item;
        while ((item = iterator.next()) != null) {
          values.add(((Numeric) item).doubleValue());
        }
      }
    }
    return values;
  }
}
