/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash-recovery truncation must happen BEFORE the recovering transaction reads anything.
 *
 * <p>{@code createPageTransaction} used to build the {@code StorageEngineWriter} first and truncate
 * afterwards. Constructing the writer eagerly reads pages — {@code NamePage}'s dictionaries go
 * through {@code getRecordPage} — so the new transaction loaded the file while it still carried the
 * aborted commit's bytes, then held those pages in swizzled {@code PageReference}s and in its own
 * page guard. The cache invalidation that follows the truncation reaches neither, so content the
 * recovery exists to discard stayed live in the transaction that triggered the recovery.</p>
 *
 * <p>That ordering also put the invalidation sweep in the position of finding pages guarded by a
 * transaction being constructed on the same call stack, which is what motivated
 * {@code BufferManagerImpl} draining guards it does not own — a use-after-free in its own right.</p>
 *
 * <p>With the truncation first, nothing is loaded and nothing is guarded when the sweep runs, so no
 * record page for this database survives it.</p>
 */
public final class CrashRecoveryOrderingTest {

  private static final String RESOURCE = "crashRecoveryOrderingResource";
  private static final String DOC = "{\"a\":1,\"b\":\"two\",\"c\":[3,4,5]}";
  private static final int GARBAGE_BYTES = 4096;

  private Path databasePath;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    databasePath = PATHS.PATH1.getFile();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private static Path dataFilePath(final Path databasePath, final String resourceName) {
    return databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                       .resolve(resourceName)
                       .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                       .resolve("sirix.data");
  }

  private static Path commitMarkerPath(final Path databasePath, final String resourceName) {
    return databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                       .resolve(resourceName)
                       .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                       .resolve(".commit");
  }

  @Test
  void recoveryTruncatesBeforeTheRecoveringTransactionReadsAnything() throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(databasePath)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOC));
        wtx.commit();
      }

      // Forge a partial write plus the commit marker: exactly the state a mid-commit crash leaves.
      final Path dataFile = dataFilePath(databasePath, RESOURCE);
      assertTrue(Files.exists(dataFile), "data file must exist after a commit; got " + dataFile);
      final long baselineSize = Files.size(dataFile);

      final byte[] garbage = new byte[GARBAGE_BYTES];
      Arrays.fill(garbage, (byte) 0xCC);
      try (final FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        channel.write(ByteBuffer.wrap(garbage));
      }
      Files.createFile(commitMarkerPath(databasePath, RESOURCE));

      // Drop everything, so the only pages that can appear below are ones the recovering
      // transaction itself loads.
      Databases.getGlobalBufferManager().clearAllCaches();

      final long guardedBefore = BufferManagerImpl.getGuardedPagesSweptCount();

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // The recovery sweep must find nothing of this database loaded, let alone guarded. A page
        // guarded at sweep time was read from the PRE-truncation file by the very transaction the
        // recovery is opening for, and it stays reachable through that transaction's page guard and
        // swizzled PageReferences after the sweep drops it from the cache.
        assertEquals(guardedBefore, BufferManagerImpl.getGuardedPagesSweptCount(),
            "crash recovery must truncate and invalidate BEFORE constructing the writer — a guarded "
                + "page at sweep time means the recovering transaction had already read the file");

        wtx.commit();
      }

      // Physical size is not a reliable garbage-gone proxy under preallocated commits (the
      // default): recovery truncates to the logical end and the recovering commit re-preallocates
      // a fresh chunk, so the post-recovery size depends on chunk arithmetic. Assert CONTENT
      // instead — the region where the forged garbage sat must no longer hold the 0xCC pattern
      // (it is either past EOF, zero preallocation padding, or new revision data).
      final long fileSize = Files.size(dataFile);
      boolean garbageSurvives = false;
      if (fileSize > baselineSize) {
        final int len = (int) Math.min(GARBAGE_BYTES, fileSize - baselineSize);
        final ByteBuffer tail = ByteBuffer.allocate(len);
        try (final FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
          while (tail.hasRemaining() && channel.read(tail, baselineSize + tail.position()) >= 0) {
            // read until full or EOF
          }
        }
        garbageSurvives = tail.position() > 0;
        for (int i = 0; i < tail.position(); i++) {
          if (tail.get(i) != (byte) 0xCC) {
            garbageSurvives = false;
            break;
          }
        }
      }
      assertTrue(!garbageSurvives, "the forged partial write must have been truncated away");
    }
  }
}
