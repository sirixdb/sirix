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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

  @ParameterizedTest(name = "{0} keeps global dictionary generations across async epochs")
  @EnumSource(VersioningType.class)
  void generationsKeepGlobalIdsAcrossAsyncEpochsAndColdReopen(final VersioningType versioningType) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .useDeweyIDs(true)
          .versioningApproach(versioningType)
          .build());
    }
    final ProjectionIndexBuilder.StreamingGlobalDictionary dictionary =
        new ProjectionIndexBuilder.StreamingGlobalDictionary(0,
            new GlobalValueDictionaryWriter(0, Long.MAX_VALUE,
                GlobalValueDictionaryWriter.AdmissionPolicy.FAIL_CLOSED));
    final long headerKey;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = database.beginResourceSession(RESOURCE);
         JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE,
             AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
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
      assertEquals(1, dictionary.intern("alpha"));
      assertEquals(3, dictionary.intern("gamma"));
      assertEquals(headerKey, dictionary.flush());
      storage.asyncFlush();
      storage.awaitPendingAsyncFlush();

      dictionary.bind(storage);
      assertEquals(2, dictionary.intern("beta"));
      assertEquals(3, dictionary.intern("gamma"));
      assertEquals(4, dictionary.intern("delta"));
      assertEquals(headerKey, dictionary.flush());
      wtx.commit();
    } finally {
      dictionary.release();
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = database.beginResourceSession(RESOURCE);
         JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final ValueDictionaryHeaderNode header =
          GlobalValueDictionary.header(headerKey, rtx.getStorageEngineReader());
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
