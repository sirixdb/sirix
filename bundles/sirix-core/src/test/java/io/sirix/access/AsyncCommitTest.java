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
 * Tests for {@link AfterCommitState#KEEP_OPEN_ASYNC_COMMIT}: pipelined durable commits — every
 * threshold crossing creates a REAL revision, hardened in the background while the transaction
 * keeps inserting. See {@code docs/ASYNC_COMMIT_DESIGN.md}.
 */
final class AsyncCommitTest {

  private static final String RESOURCE = "async-commit-resource";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_COMMIT: every epoch creates a durable, queryable revision")
  void asyncCommit_createsQueryableRevisions() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(false)
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
          .storageType(StorageType.FILE_CHANNEL)
          .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           // maxNodes = 1024 → each threshold crossing runs phase 1 inline and hardens revision
           // N in the background while inserts continue into epoch N+1.
           final JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < 5000; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("item-" + i);
        }
        // Final explicit commit drains the pipeline first, then commits the tail epoch.
        wtx.commit();
      }
    }

    // Reopen: multiple revisions must exist (≈ 5000/1024 intermediate + the final commit), and
    // every revision must open cleanly with a consistent page tree.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      final int mostRecent = session.getMostRecentRevisionNumber();
      assertTrue(mostRecent >= 4,
          "expected several pipelined revisions plus the final commit, got " + mostRecent);

      long previousCount = -1;
      for (int rev = 1; rev <= mostRecent; rev++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild(), "array must be reachable in revision " + rev);
          final long count = rtx.getChildCount();
          assertTrue(count >= previousCount,
              "child count must be non-decreasing across revisions; rev " + rev + " has " + count
                  + " after " + previousCount);
          previousCount = count;
        }
      }
      assertEquals(5000, previousCount, "the newest revision must contain every inserted item");
    }
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_COMMIT + MEMORY_MAPPED: fails fast")
  void asyncCommit_withMemoryMapped_throwsClearly() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storageType(StorageType.MEMORY_MAPPED)
          .build());
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT));
        assertTrue(thrown.getMessage().contains("FILE_CHANNEL"));
      }
    }
  }

  @Test
  @DisplayName("KEEP_OPEN_ASYNC_COMMIT + timed auto-commit: fails fast")
  void asyncCommit_withTimedAutoCommit_throwsClearly() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .storageType(StorageType.FILE_CHANNEL)
          .build());
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> session.beginNodeTrx(1024, 30, TimeUnit.SECONDS, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT));
        assertTrue(thrown.getMessage().contains("timed"));
      }
    }
  }
}
