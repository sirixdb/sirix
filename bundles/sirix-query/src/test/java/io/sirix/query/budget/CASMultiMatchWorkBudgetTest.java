package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Isolated
final class CASMultiMatchWorkBudgetTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @MethodSource("sources")
  @SuppressWarnings("unchecked")
  void unavailableOrderDeclinesAfterTwoDistinctRows(final boolean filter, final boolean duplicateArray,
      final boolean deweyIDs) {
    final Path databasePath = directory.resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .useDeweyIDs(deweyIDs)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(24_000);
        if (duplicateArray) {
          json.append("{\"rows\":[{\"id\":0}],\"rows\":");
        }
        json.append('[');
        for (int i = 0; i < 2_048; i++) {
          if (i > 0) {
            json.append(',');
          }
          json.append("{\"id\":1}");
        }
        json.append(']');
        if (duplicateArray) {
          json.append('}');
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse(duplicateArray
                   ? "/rows/[]/id"
                   : "/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final var realCollection = store.lookup("budget");
      final var realDatabase = realCollection.getDatabase();
      final var realSession = realDatabase.beginResourceSession("rows");
      final JsonResourceSession session = mock(JsonResourceSession.class, delegatesTo(realSession));
      final Database<JsonResourceSession> database = mock(Database.class, delegatesTo(realDatabase));
      doReturn(session).when(database).beginResourceSession("rows");
      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(realCollection));
      doReturn(database).when(collection).getDatabase();
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup("budget");
      final List<JsonNodeReadOnlyTrx> cursors = new ArrayList<>();
      doAnswer(call -> {
        final JsonNodeReadOnlyTrx raw = realSession.beginNodeReadOnlyTrx(call.getArgument(0, Integer.class));
        final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(raw));
        doReturn(session).when(cursor).getResourceSession();
        cursors.add(cursor);
        return cursor;
      }).when(session).beginNodeReadOnlyTrx(anyInt());
      try (var context = SirixQueryContext.createWithJsonStore(observedStore);
          var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(observedStore)) {
        final String source = "jn:doc('budget','rows')" + (duplicateArray
            ? ".rows[]"
            : "[]");
        final String text = filter
            ? "sum(" + source + "[?$$.id eq 1].id)"
            : "sum(for $c in " + source + " where $c.id eq 1 return $c.id)";
        final Query query = new Query(chain, text);
        cursors.clear();
        assertEquals(duplicateArray
            ? 0
            : 2_048, ((Numeric) query.evaluate(context)).intValue());
        assertEquals(1, cursors.size());
        final JsonNodeReadOnlyTrx cursor = cursors.getFirst();
        verify(cursor, times(2)).getParentKey();
        verify(cursor, atMost(16)).moveTo(anyLong());
        verify(cursor, atMost(4)).moveToParent();
        verify(cursor, never()).moveToRightSibling();
      }
    }
  }

  private static Stream<Arguments> sources() {
    return Stream.of(Arguments.of(false, false, false), Arguments.of(true, false, false),
        Arguments.of(false, true, false), Arguments.of(true, true, false), Arguments.of(false, true, true),
        Arguments.of(true, true, true));
  }
}
