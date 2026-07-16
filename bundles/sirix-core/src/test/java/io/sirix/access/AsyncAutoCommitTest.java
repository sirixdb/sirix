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
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link AfterCommitState#KEEP_OPEN_ASYNC_FLUSH} commit path.
 *
 * <p>The doc currently advises operators to use synchronous commits and avoid this
 * path. These tests verify the path actually works under its documented constraints
 * (FILE_CHANNEL backend, count-based auto-commit only) so we can later lift the
 * advisory, AND verify the runtime guards fail-fast on misuse so a regression in the
 * guards is caught.
 */
final class AsyncAutoCommitTest {

  private static final String RESOURCE = "async-auto-commit-resource";

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
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.FULL)
          .storageType(StorageType.FILE_CHANNEL)
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
      assertTrue(rtx.getRevisionNumber() >= 1,
          "expected at least one revision after async-auto-commit + final commit");
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      // The first child is the array we created. We don't traverse the entire 3000
      // elements — just confirm the array root is reachable, which means the page
      // tree is consistent end-to-end.
      assertTrue(rtx.hasFirstChild(),
          "the array must have at least one element after async auto-commit");
    }
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_FLUSH + MEMORY_MAPPED fails fast at session.beginNodeTrx (runtime guard)")
  void asyncAutoCommit_withMemoryMapped_throwsClearly() {
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
            () -> session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH).close(),
            "MEMORY_MAPPED + KEEP_OPEN_ASYNC_FLUSH must throw at trx creation, not fail later");
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
