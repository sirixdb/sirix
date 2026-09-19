package io.sirix.index.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.StorageType;
import io.sirix.io.SharedArenas;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Ordinary dictionary writes must release overflow-bearing record pages between storage epochs. */
final class StreamingDictionaryOverflowRetentionTest {
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE = "streaming-overflow-retention";
  private static final int EPOCHS = 32;
  private static final int VALUES_PER_EPOCH = 256;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    TransactionIntentLog.resetKvlPromotionDiagnostics();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void ordinaryDictionaryGenerationsReleaseFramesBeforeCommit(final VersioningType versioningType) {
    assumeTrue(Boolean.parseBoolean(System.getProperty("sirix.commit.preallocated", "true")),
        "bounded carrier staging requires a reclaimable backend");
    assumeTrue(SharedArenas.supportsDeterministicClose(),
        "bounded carrier staging requires deterministically closed reservoirs");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioningType)
                                                   .hashKind(HashType.NONE)
                                                   .useDeweyIDs(false)
                                                   .storeNodeHistory(false)
                                                   .build());
    }
    final ProjectionIndexBuilder.StreamingGlobalDictionary dictionary =
        new ProjectionIndexBuilder.StreamingGlobalDictionary(0, new GlobalValueDictionaryWriter(0, 64L << 20));
    long headerKey = 0L;
    int maxResidentRecordPages = 0;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx transaction =
            session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
      transaction.insertObjectAsFirstChild();
      final StorageEngineWriter writer = transaction.getStorageEngineWriter();
      for (int epoch = 0; epoch < EPOCHS; epoch++) {
        dictionary.bind(writer);
        for (int offset = 1; offset <= VALUES_PER_EPOCH; offset++) {
          final int id = epoch * VALUES_PER_EPOCH + offset;
          assertEquals(id, dictionary.intern(value(id)));
        }
        if (epoch != 0) {
          assertEquals(1, dictionary.intern(value(1)), "old IDs must survive a storage epoch");
        }
        headerKey = dictionary.flush();
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        int residentRecordPages = 0;
        for (final PageContainer container : writer.getLog().getList()) {
          if (container != null && container.getModified() instanceof KeyValueLeafPage) {
            residentRecordPages++;
          }
        }
        maxResidentRecordPages = Math.max(maxResidentRecordPages, residentRecordPages);
        final long activeBytes = Allocators.getInstance() instanceof FrameSlotAllocator allocator
            ? allocator.getActiveMemoryBytes()
            : -1L;
        System.out.printf("epoch=%d values=%d residentRecordPages=%d pinned=%d activeFrameBytes=%d%n", epoch + 1,
            (epoch + 1) * VALUES_PER_EPOCH, residentRecordPages, writer.getLog().pinnedSize(), activeBytes);
      }
      transaction.commit();
    } finally {
      dictionary.release();
    }

    final long pinnedPages = TransactionIntentLog.kvlPagesPinnedByPromotion();
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx()) {
      final ValueDictionaryHeaderNode header =
          GlobalValueDictionary.header(headerKey, transaction.getStorageEngineReader());
      assertNotNull(header);
      assertEquals(EPOCHS * VALUES_PER_EPOCH, header.getEntryCount());
      for (int id = 1; id <= EPOCHS * VALUES_PER_EPOCH; id++) {
        assertEquals(value(id), GlobalValueDictionary.value(headerKey, id, transaction.getStorageEngineReader()),
            "cold dictionary value " + id);
      }
    }
    assertEquals(0L, pinnedPages, "ordinary overflow records must not pin their pages until final commit");
    assertTrue(maxResidentRecordPages <= 16,
        "record-page residency must stay within the current flush windows: " + maxResidentRecordPages);
  }

  private static String value(final int id) {
    return "value-" + id + "-" + "abcdefghijklmno".repeat(8);
  }
}
