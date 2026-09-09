/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionStreamingGlobalDictionaryTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE = "streaming-global-dictionary";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void generationAndResidentFrontUseDisjointHalvesOfTheCombinedEnvelope() {
    final long combinedBudget = 512L << 20;
    final long componentBudget = ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(combinedBudget);
    final ProjectionIndexBuilder.StreamingGlobalDictionary dictionary =
        new ProjectionIndexBuilder.StreamingGlobalDictionary(0, new GlobalValueDictionaryWriter(0, componentBudget));
    try {
      assertEquals(componentBudget, dictionary.generationBudgetBytesForTest());
      assertEquals(componentBudget, dictionary.residentFrontBudgetBytesForTest());
      assertTrue(
          Math.addExact(dictionary.generationBudgetBytesForTest(),
              dictionary.residentFrontBudgetBytesForTest()) <= combinedBudget,
          "the two simultaneously resident caps must not double-spend the planner allocation");
    } finally {
      dictionary.release();
    }
  }

  @ParameterizedTest(name = "{0} keeps global dictionary generations across async epochs")
  @EnumSource(VersioningType.class)
  void generationsKeepGlobalIdsAcrossAsyncEpochsAndColdReopen(final VersioningType versioningType) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).useDeweyIDs(true).versioningApproach(versioningType).build());
    }
    final ProjectionIndexBuilder.StreamingGlobalDictionary dictionary =
        new ProjectionIndexBuilder.StreamingGlobalDictionary(0, new GlobalValueDictionaryWriter(0, Long.MAX_VALUE,
            GlobalValueDictionaryWriter.AdmissionPolicy.FAIL_CLOSED));
    final long headerKey;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
      assertEquals(versioningType, session.getResourceConfig().versioningType);
      wtx.insertObjectAsFirstChild();
      final StorageEngineWriter storage = wtx.getStorageEngineWriter();

      dictionary.bind(storage);
      assertEquals(1, dictionary.intern("alpha"));
      assertEquals(2, dictionary.intern("beta"));
      headerKey = dictionary.flush();
      storage.asyncFlush();
      storage.awaitPendingAsyncFlush();

      dictionary.bind(storage);
      for (int repeat = 0; repeat < 256; repeat++) {
        assertEquals(1, dictionary.intern("alpha"));
      }
      for (int repeat = 0; repeat < 256; repeat++) {
        assertEquals(3, dictionary.intern("gamma"));
      }
      // The resident probe front keeps every (value, id) of the load in memory across generation
      // flushes, so re-interning a flushed value never walks the persistent radix. Zero probes IS
      // the contract now — the old expectation of one probe per flushed value re-encounter was the
      // ~5-page-decode-per-value regime this front exists to remove. The ids are the real pin.
      assertEquals(0L, dictionary.persistentProbeCount());
      assertEquals(headerKey, dictionary.flush());
      storage.asyncFlush();
      storage.awaitPendingAsyncFlush();

      dictionary.bind(storage);
      for (int repeat = 0; repeat < 256; repeat++) {
        assertEquals(2, dictionary.intern("beta"));
      }
      for (int repeat = 0; repeat < 256; repeat++) {
        assertEquals(3, dictionary.intern("gamma"));
      }
      for (int repeat = 0; repeat < 256; repeat++) {
        assertEquals(4, dictionary.intern("delta"));
      }
      assertEquals(0L, dictionary.persistentProbeCount());
      assertEquals(headerKey, dictionary.flush());
      wtx.commit();
    } finally {
      dictionary.release();
    }

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexChangeListener.MaintenanceGlobalDictionary maintenanceDictionary =
          new ProjectionIndexChangeListener.MaintenanceGlobalDictionary(0, headerKey, wtx.getStorageEngineWriter(),
              Long.MAX_VALUE);
      try {
        for (int repeat = 0; repeat < 256; repeat++) {
          assertEquals(1, maintenanceDictionary.intern("alpha"));
          assertEquals(5, maintenanceDictionary.intern("epsilon"));
        }
        assertEquals(2L, maintenanceDictionary.persistentProbeCount());
      } finally {
        maintenanceDictionary.release();
      }
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final ValueDictionaryHeaderNode header = GlobalValueDictionary.header(headerKey, rtx.getStorageEngineReader());
      assertNotNull(header);
      assertEquals(4, header.getEntryCount());
      assertEquals(2, header.getGeneration());
      assertEquals("alpha", GlobalValueDictionary.value(headerKey, 1, rtx.getStorageEngineReader()));
      assertEquals("beta", GlobalValueDictionary.value(headerKey, 2, rtx.getStorageEngineReader()));
      assertEquals("gamma", GlobalValueDictionary.value(headerKey, 3, rtx.getStorageEngineReader()));
      assertEquals("delta", GlobalValueDictionary.value(headerKey, 4, rtx.getStorageEngineReader()));
    }
  }
}
