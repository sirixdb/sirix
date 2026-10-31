package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.access.Databases;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollectionImpl;
import org.junit.jupiter.api.Test;
import io.sirix.service.json.serialize.JsonSerializer;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonWriterLifecycleTest {
  private static final List<Mutation> MUTATIONS = List.of(
      new Mutation("object", "{\"name\":\"original\"}", "replace json value of $$.name with 'Changed'",
          "{\"name\":\"Changed\"}"),
      new Mutation("array", "[\"original\",\"keep\"]", "replace json value of $$[0] with 'Changed'",
          "[\"Changed\",\"keep\"]"),
      new Mutation("primitive", "[\"original\",\"keep\"]", "delete json sdb:select-item($$,2)", "[\"keep\"]"));

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(CommitStrategy.class)
  void queryWritersFollowCommitStrategyAndStoreCloseLifecycle(final CommitStrategy strategy) throws Exception {
    for (final Mutation mutation : MUTATIONS) {
      try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
          final var context = SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, strategy);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {
        final var collection = store.create(mutation.name(), "data", mutation.original());
        final var document = collection.getDocument("data");
        context.setContextItem(document);
        new Query(chain, mutation.expression()).execute(context);
        final var session = document.getResourceSession();
        if (strategy == CommitStrategy.AUTO) {
          assertEquals(2, session.getMostRecentRevisionNumber());
          assertTrue(session.getNodeTrx().isEmpty());
        } else {
          assertEquals(1, session.getMostRecentRevisionNumber());
          assertTrue(session.getNodeTrx().isPresent());
          assertFalse(session.getNodeTrx().orElseThrow().isClosed());
        }
      }
      assertStored(mutation.name(), 2, mutation.expected());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void explicitCommitAndRollbackRemainUnderCallerControl(final boolean commit) throws Exception {
    for (final Mutation mutation : MUTATIONS) {
      try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
          final var context = SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {
        final var collection = store.create(mutation.name(), "data", mutation.original());
        final var document = collection.getDocument("data");
        context.setContextItem(document);
        new Query(chain, mutation.expression()).execute(context);
        final var session = document.getResourceSession();
        final var writer = session.getNodeTrx().orElseThrow();
        assertEquals(1, session.getMostRecentRevisionNumber());
        if (commit) {
          new Query(chain, "sdb:commit($$)").execute(context);
          assertEquals(2, session.getMostRecentRevisionNumber());
        } else {
          writer.rollback();
          assertEquals(1, session.getMostRecentRevisionNumber());
        }
        writer.close();
        assertTrue(session.getNodeTrx().isEmpty());
      }
      assertStored(mutation.name(), commit
          ? 2
          : 1,
          commit
              ? mutation.expected()
              : mutation.original());
    }
  }

  @Test
  void storelessCollectionsSupportObjectArrayAndPrimitiveUpdates() throws Exception {
    for (final Mutation mutation : MUTATIONS) {
      try (final var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
        store.create(mutation.name(), "data", mutation.original());
      }
      try (final var database = Databases.openJsonDatabase(directory.resolve(mutation.name()));
          final var store = BasicJsonDBStore.newBuilder().location(directory).build();
          final var context = SirixQueryContext.createWithJsonStore(store);
          final var chain = SirixCompileChain.createWithJsonStore(store)) {
        final var collection = new JsonDBCollectionImpl(mutation.name(), database);
        assertNull(collection.getJsonDBStore());
        final var document = collection.getDocument("data");
        context.setContextItem(document);
        new Query(chain, mutation.expression()).execute(context);
        assertEquals(2, document.getResourceSession().getMostRecentRevisionNumber());
        assertTrue(document.getResourceSession().getNodeTrx().isEmpty());
      }
      assertStored(mutation.name(), 2, mutation.expected());
    }
  }

  private void assertStored(final String database, final int revision, final String expected) throws Exception {
    try (final var db = Databases.openJsonDatabase(directory.resolve(database));
        final var session = db.beginResourceSession("data");
        final var output = new StringWriter()) {
      assertEquals(revision, session.getMostRecentRevisionNumber());
      JsonSerializer.newBuilder(session, output).build().call();
      assertEquals(expected, output.toString());
    }
  }

  private record Mutation(String name, String original, String expression, String expected) {
  }
}
