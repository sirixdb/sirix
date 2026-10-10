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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Counts transaction navigation and timestamp lookups, never elapsed time. Mutation evidence:
 * disabling CAS yields zero indexed transactions instead of 50; bypassing the memo resolves 50
 * instants instead of two. Both keep the numerical answer.
 */
@Isolated
final class CASLookupWorkBudgetTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void pointLookupsDoBoundedNodeWorkAndMemoizeRepeatedPublicationTimes(final boolean decimalIds) {
    create(decimalIds);
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
        final JsonNodeReadOnlyTrx real = realSession.beginNodeReadOnlyTrx(call.getArgument(0, Integer.class));
        final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(real));
        doReturn(session).when(cursor).getResourceSession();
        cursors.add(cursor);
        return cursor;
      }).when(session).beginNodeReadOnlyTrx(anyInt());
      try (var context = SirixQueryContext.createWithJsonStore(observedStore);
          var chain = SirixCompileChain.createWithJsonStore(observedStore);
          var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(observedStore)) {
        final String epochs = "[{\"ts\":\"2020-01-01T00:00:00Z\"},{\"ts\":\"2021-01-01T00:00:00Z\"}][]";
        final String text = "sum(for $repeat in 1 to 25 for $e in " + epochs
            + " for $c in jn:open('budget','rows',xs:dateTime($e.ts))[] where $c.id eq 999 return $c.id)";
        assertEquals(49_950,
            ((Numeric) new Query(generic, text.replace("'rows'", "'plain'")).evaluate(context)).intValue());
        final Query query = new Query(chain, text);
        cursors.clear();
        clearInvocations(session, collection);
        assertEquals(49_950, ((Numeric) query.evaluate(context)).intValue());
        assertEquals(50, cursors.size(), "one point lookup per publication row");
        verify(session, times(2)).getRevisionNumber(any(Instant.class));
        verify(collection, never()).getDocument(eq("rows"), any(Instant.class));
        for (final JsonNodeReadOnlyTrx cursor : cursors) {
          verify(cursor, atLeast(1)).moveTo(anyLong());
          verify(cursor, atMost(16)).moveTo(anyLong());
          verify(cursor, atMost(4)).moveToParent();
          verify(cursor, never()).moveToRightSibling();
        }
        // A later commit changes the floor of a future instant even in a reused context.
        final Instant future = Instant.parse("2030-01-01T00:00:00Z");
        assertEquals(2, context.resolveRevision(session, future));
        try (var writer = realSession.beginNodeTrx()) {
          writer.commit(null, Instant.parse("2022-01-01T00:00:00Z"));
        }
        assertEquals(3, context.resolveRevision(session, future));
      }
    }
  }

  private void create(final boolean decimalIds) {
    final Path databasePath = directory.resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : List.of("rows", "plain")) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .customCommitTimestamps(true)
                                                     .storeDiffs(false)
                                                     .build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          final StringBuilder json = new StringBuilder(12_000).append('[');
          for (int i = 0; i < 1_000; i++) {
            if (i > 0) {
              json.append(',');
            }
            json.append("{\"id\":").append(i);
            if (decimalIds) {
              json.append(".0");
            }
            json.append('}');
          }
          json.append(']');
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          if (resource.equals("rows")) {
            session.getWtxIndexController(writer.getRevisionNumber())
                   .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                       Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
          }
          writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
          writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
        }
      }
    }
  }
}
