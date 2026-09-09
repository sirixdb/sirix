/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral witness that the async-flush epoch bounds arm for a {@code maxNodeCount == 0} writer —
 * the {@code beginNodeTrx(AfterCommitState)} overload. The bounds bound intent-log MEMORY, not
 * commit cadence; they used to be dead-gated behind {@code maxNodeCount > 0}, so such a writer's
 * log grew unbounded for the whole import (~one live entry per dirtied page — hundreds for the
 * workload below, tens of thousands for a real import).
 */
final class AsyncFlushZeroNodeCountBoundTest {

  private static final String RESOURCE = "async-flush-zero-node-count";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("A KEEP_OPEN_ASYNC_FLUSH writer with maxNodeCount == 0 keeps its intent log bounded")
  void zeroNodeCountWriterKeepsIntentLogBounded() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        wtx.insertArrayAsFirstChild();
        // Async-flush epochs never re-instantiate the transaction (no commit is minted), so the
        // writer instance is stable for the whole loop.
        final StorageEngineWriter writer = wtx.getStorageEngineWriter();
        final String value = "x".repeat(1024);
        int maxLogEntries = 0;
        wtx.insertStringValueAsFirstChild(value);
        for (int i = 1; i < 20_000; i++) {
          // The cursor rests on the value just inserted; append the next one after it.
          wtx.insertStringValueAsRightSibling(value);
          maxLogEntries = Math.max(maxLogEntries, writer.getLog().size());
        }
        // ~60 one-KiB records fit a 64 KiB page frame, so the loop dirties ~330 data pages.
        // Without rotation every one stays a live intent-log entry; with the epoch bounds armed
        // the log flushes at the entry bound (default 16) and never grows past a few dozen.
        assertTrue(maxLogEntries <= 64,
            "intent log grew unbounded for a maxNodeCount == 0 async-flush writer: " + maxLogEntries + " entries");
        wtx.commit();
        assertEquals(1, session.getMostRecentRevisionNumber(), "intermediate async flushes must not mint revisions");
      }
    }
  }
}
