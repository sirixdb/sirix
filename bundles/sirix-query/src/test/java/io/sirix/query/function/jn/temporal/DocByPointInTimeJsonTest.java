package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code jn:open($db, $resource, $pointInTime)} — opening a JSON resource as of a
 * wall-clock instant ({@link io.sirix.query.function.jn.io.DocByPointInTime}).
 */
public final class DocByPointInTimeJsonTest {

  private static final Path SIRIX_DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String run(final SirixQueryContext ctx, final SirixCompileChain chain, final String query)
      throws IOException {
    final var seq = new Query(chain, query).execute(ctx);
    final var buf = IOUtils.createBuffer();
    try (final var serializer = new StringSerializer(buf)) {
      serializer.setFormat(true).serialize(seq);
    }
    return buf.toString().trim();
  }

  /**
   * A point in time before the resource's first revision must yield the empty sequence — the resource
   * did not exist yet, so there is nothing to return (regression: previously the first revision's
   * data was returned anachronistically).
   */
  @Test
  public void test_whenPointInTimeBeforeFirstRevision_thenEmpty() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      // The revisions above are committed "now"; 2000-01-01 predates all of them.
      final var result = run(ctx, chain, "jn:open('json-path1','mydoc.jn', xs:dateTime('2000-01-01T00:00:00Z'))");

      assertEquals("", result);
    }
  }

  /**
   * A point in time after the first revision must still return the document (here a far-future
   * instant resolves to the most recent revision).
   */
  @Test
  public void test_whenPointInTimeAfterFirstRevision_thenDocument() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var result = run(ctx, chain, "jn:open('json-path1','mydoc.jn', xs:dateTime('2100-01-01T00:00:00Z'))");

      assertFalse(result.isEmpty(), "expected the resource document, got the empty sequence");
    }
  }

  /**
   * The "resource did not exist yet" answer must not take the resource session down with it.
   * {@code Database.beginResourceSession} hands every caller the one cached session for that
   * resource, so closing it there closed every transaction anybody else still held on it.
   */
  @Test
  public void test_whenPointInTimeBeforeFirstRevision_thenSharedSessionSurvives() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final JsonDBCollection collection = store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("mydoc.jn");
      final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();

      try {
        assertNull(collection.getDocument("mydoc.jn", Instant.parse("2000-01-01T00:00:00Z")),
            "a point in time before the first revision must yield no document");

        assertFalse(session.isClosed(), "the shared resource session was closed");
        assertFalse(rtx.isClosed(), "an unrelated open transaction on the shared session was closed");
        assertTrue(rtx.moveToDocumentRoot(), "the surviving transaction is no longer usable");
      } finally {
        if (!rtx.isClosed()) {
          rtx.close();
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(value = StorageType.class, names = {"FILE_CHANNEL", "MEMORY_MAPPED"})
  public void test_whenPointInTimeSelectsEmptyRevision_thenLookupTransactionCloses(final StorageType storageType) {
    final Instant firstCommit = Instant.parse("2010-01-01T00:00:00Z");
    final Instant pointInTime = Instant.parse("2000-01-01T00:00:00Z");
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build()) {
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(SIRIX_DB_PATH)));
      final JsonDBCollection collection = store.lookup("json-path1");
      collection.getDatabase()
          .createResource(ResourceConfiguration.newBuilder("mydoc.jn")
              .storageType(storageType)
              .customCommitTimestamps(true)
              .build());
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("mydoc.jn");
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertObjectAsFirstChild();
        wtx.commit(null, firstCommit);
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        try (final JsonNodeReadOnlyTrx bootstrap = session.beginNodeReadOnlyTrx(pointInTime)) {
          assertEquals(0, bootstrap.getRevisionNumber());
          assertEquals(Instant.EPOCH, bootstrap.getRevisionTimestamp());
        }
        final int baseline = session.activeTrxCount();
        for (int i = 0; i < 3; i++) {
          assertNull(collection.getDocument("mydoc.jn", pointInTime));
          assertEquals(baseline, session.activeTrxCount(), "an absent document must release its lookup transaction");
          assertFalse(session.isClosed(), "the shared resource session was closed");
          assertFalse(rtx.isClosed(), "an unrelated open transaction was closed");
          assertTrue(rtx.moveToDocumentRoot());
          assertTrue(rtx.moveToFirstChild());
        }

        final JsonDBItem document = collection.getDocument("mydoc.jn", firstCommit);
        assertNotNull(document);
        try (final JsonNodeReadOnlyTrx documentTrx = document.getTrx()) {
          assertEquals(baseline + 1, session.activeTrxCount());
          assertEquals(1, documentTrx.getRevisionNumber());
          assertTrue(documentTrx.isObject());
        }
        assertEquals(baseline, session.activeTrxCount());
      }
    }
  }
}
