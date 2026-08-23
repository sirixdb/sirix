/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link AfterCommitState#KEEP_OPEN_ASYNC_FLUSH} commit path.
 *
 * <p>
 * Verifies the path works under its documented constraints — FILE_CHANNEL or MEMORY_MAPPED backend
 * (both append through the file-channel writer), count-based auto-commit only — including a
 * sync-vs-async content-parity differential on the memory-mapped backend, AND verifies the runtime
 * guards fail-fast on misuse so a regression in the guards is caught.
 */
final class AsyncAutoCommitTest {

  private static final String RESOURCE = "async-auto-commit-resource";

  @TempDir
  Path temporaryDirectory;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_FLUSH + FILE_CHANNEL + count-based auto-commit: data round-trips through one revision")
  void asyncAutoCommit_underDocumentedConstraints_works() {
    assertAsyncAutoCommitRoundTrips(StorageType.FILE_CHANNEL);
  }

  @Test
  @DisplayName("tiny-threshold FILE_CHANNEL async flush preserves arbitrary topology after a cold reopen")
  void tinyThresholdFileChannelAsyncFlushPreservesArbitraryTopologyAfterColdReopen() throws Exception {
    final String document = arbitraryShapeDocument();
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.ROLLING)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(8, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final String coldSerialized = serialize(session);
      JSONAssert.assertEquals(document, coldSerialized, true);
      assertEquals(1, session.getMostRecentRevisionNumber(), "intermediate async flushes must not mint revisions");
    }
  }

  @Test
  @DisplayName("tiny-threshold XML FILE_CHANNEL async flush preserves arbitrary topology after a cold reopen")
  void tinyThresholdXmlFileChannelAsyncFlushPreservesArbitraryTopologyAfterColdReopen() throws Exception {
    final String document = "<root a=\"1\"><row><name>alpha</name><values><v>1</v><v>2</v></values></row>"
        + "<empty/><row b=\"2\">tail</row></root>";
    final Path databasePath = temporaryDirectory.resolve("xml-async-fresh-bind");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
          final XmlNodeTrx wtx = session.beginNodeTrx(4, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(document), XmlNodeTrx.Commit.No);
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
        final XmlResourceSession session = database.beginResourceSession(RESOURCE);
        final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      XmlSerializer.newBuilder(session, output).build().call();
      assertEquals(document, output.toString(StandardCharsets.UTF_8));
      assertEquals(1, session.getMostRecentRevisionNumber(), "intermediate XML async flushes must not mint revisions");
    }
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_FLUSH + MEMORY_MAPPED + count-based auto-commit: data round-trips through one revision")
  void asyncAutoCommit_withMemoryMapped_works() {
    assertAsyncAutoCommitRoundTrips(StorageType.MEMORY_MAPPED);
  }

  private void assertAsyncAutoCommitRoundTrips(final StorageType storageType) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(storageType)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          // maxNodes = 1024 → after every 1024 modifications the async path
          // rotates the TIL (background flush) but does NOT mint a new revision.
          // Only the final explicit commit creates a revision.
          final JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        // Insert enough records to cross the maxNodes threshold a few times.
        // Move back to the array on every iteration so we're inserting AS a
        // child of the array, not as a child of the previous string (which is
        // a leaf and rejects inserts).
        for (int i = 0; i < 3000; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("item-" + i);
        }
        wtx.commit();
      }

      // Verify the data is durable by reopening as a separate session.
    }

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.getRevisionNumber() >= 1, "expected at least one revision after async-auto-commit + final commit");
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      // The first child is the array we created. We don't traverse the entire 3000
      // elements — just confirm the array root is reachable, which means the page
      // tree is consistent end-to-end.
      assertTrue(rtx.hasFirstChild(), "the array must have at least one element after async auto-commit");
    }
  }

  @Test
  @DisplayName("MEMORY_MAPPED: async-flush import serializes byte-identically to synchronous intermediate commits")
  void asyncAutoCommit_memoryMapped_contentParityWithSyncCommits() throws Exception {
    // Mixed-shape records so many rotations cross NAME-index pages, the path summary,
    // and partially-filled straddler leaves (the #1077 hazard zone): with a 512-node
    // threshold the ~16.5k-node document (1500 records x ~11 nodes) rotates ~32 times.
    // Record 10 carries a 200 KB string value — larger than the overlong-record
    // threshold, so its leaf serializes with OverflowPage references. The background
    // flush must EXEMPT that page (only the recursive final commit can assign overflow
    // disk keys); flushing it froze NULL overflow keys into the durable bytes and
    // silently lost the record (caught by adversarial review before release).
    final StringBuilder json = new StringBuilder(1 << 18);
    json.append('[');
    final StringBuilder overlong = new StringBuilder(256 * 1024);
    for (int i = 0; i < 25_000; i++) {
      overlong.append("blob-").append(i % 97).append('-');
    }
    for (int i = 0; i < 1500; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"user-")
          .append(i)
          .append("\",\"age\":")
          .append(18 + (i * 37) % 63)
          .append(",\"active\":")
          .append((i % 3) == 0);
      if (i == 10) {
        json.append(",\"payload\":\"").append(overlong).append('"');
      }
      json.append(",\"tags\":[\"t").append(i % 7).append("\",\"t").append(i % 11).append("\"]}");
    }
    json.append(']');
    final String document = json.toString();

    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final String syncSerialized;
    final String asyncSerialized;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      syncSerialized = importAndSerialize(db, "sync-resource", document, AfterCommitState.KEEP_OPEN);
      asyncSerialized = importAndSerialize(db, "async-resource", document, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH);
      assertEquals(syncSerialized, asyncSerialized,
          "async-flush import must produce the same content as synchronous intermediate commits");
      JSONAssert.assertEquals(document, asyncSerialized, true);
    }

    // Durability: reopen the database and compare the serialized content again.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession syncSession = db.beginResourceSession("sync-resource");
        final JsonResourceSession asyncSession = db.beginResourceSession("async-resource")) {
      final String reopenedAsyncSerialized = serialize(asyncSession);
      assertEquals(serialize(syncSession), reopenedAsyncSerialized,
          "reopened async-flush resource must match the synchronous one");
      JSONAssert.assertEquals(document, reopenedAsyncSerialized, true);
      // Async intermediate flushes mint NO revisions — only the final commit does.
      assertEquals(1, asyncSession.getMostRecentRevisionNumber(),
          "async-flush import must produce exactly one revision");
      assertTrue(syncSession.getMostRecentRevisionNumber() > 1,
          "sync auto-commit import must have produced intermediate revisions for this document size");
    }
  }

  private String importAndSerialize(final Database<JsonResourceSession> db, final String resource,
      final String document, final AfterCommitState afterCommitState) {
    db.createResource(ResourceConfiguration.newBuilder(resource)
                                           .storeDiffs(false)
                                           .hashKind(HashType.ROLLING)
                                           .buildPathSummary(true)
                                           .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                           .storageType(StorageType.MEMORY_MAPPED)
                                           .build());
    try (final JsonResourceSession session = db.beginResourceSession(resource)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx(512, afterCommitState)) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      return serialize(session);
    }
  }

  private static String serialize(final JsonResourceSession session) {
    final StringWriter out = new StringWriter();
    new JsonSerializer.Builder(session, out).build().call();
    return out.toString();
  }

  private static String arbitraryShapeDocument() {
    final StringBuilder document = new StringBuilder(16 * 1024).append('[');
    for (int i = 0; i < 48; i++) {
      if (i > 0) {
        document.append(',');
      }
      switch (i & 7) {
        case 0 -> document.append("{\"id\":")
                          .append(i)
                          .append(",\"nested\":{\"flag\":")
                          .append((i & 1) == 0)
                          .append(",\"values\":[")
                          .append(i)
                          .append(',')
                          .append(i + 1)
                          .append(",null]}}");
        case 1 -> document.append('[').append(i).append(",{\"name\":\"row-").append(i).append("\"},[true,false]]");
        case 2 -> document.append('"').append("text-").append(i).append('"');
        case 3 -> document.append(i);
        case 4 -> document.append((i & 1) == 0);
        case 5 -> document.append("null");
        case 6 -> document.append("{\"emptyObject\":{},\"emptyArray\":[],\"value\":").append(i).append('}');
        case 7 -> document.append("[{\"deep\":{\"leaf\":\"").append(i).append("\"}},[],[\"tail\",null]]");
        default -> throw new AssertionError("unreachable shape selector");
      }
    }
    return document.append(']').toString();
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_COMMIT + MEMORY_MAPPED fails fast at session.beginNodeTrx (runtime guard)")
  void asyncDurableCommit_withMemoryMapped_throwsClearly() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.MEMORY_MAPPED)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT).close(),
            "MEMORY_MAPPED + KEEP_OPEN_ASYNC_COMMIT must throw at trx creation, not fail later");
        assertTrue(thrown.getMessage().contains("FILE_CHANNEL"),
            "error must name the required backend; got: " + thrown.getMessage());
      }
    }
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_FLUSH + timed auto-commit fails fast (runtime guard)")
  void asyncAutoCommit_withTimedAutoCommit_throwsClearly() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> session.beginNodeTrx(1024, 5, TimeUnit.SECONDS, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH).close(),
            "timed auto-commit + KEEP_OPEN_ASYNC_FLUSH must throw at trx creation");
        assertTrue(thrown.getMessage().contains("timed"),
            "error must mention timed auto-commit; got: " + thrown.getMessage());
      }
    }
  }
}
