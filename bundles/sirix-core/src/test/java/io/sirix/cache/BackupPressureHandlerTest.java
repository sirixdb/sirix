package io.sirix.cache;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.backup.BackupManager;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ResourceLock("FrameSlotAllocator.pressureListener")
final class BackupPressureHandlerTest {
  private enum Operation {
    BACKUP, RESTORE, FAILED_RESTORE
  }

  @TempDir
  Path tempDir;

  @ParameterizedTest
  @EnumSource(Operation.class)
  @DisplayName("TXN-613: backup and restore leave live cache pressure eviction effective")
  void verificationPreservesLivePressureEviction(final Operation operation) throws IOException {
    final Path source = tempDir.resolve("source");
    final Path backup = tempDir.resolve("backup");
    final Path restored = tempDir.resolve("restored");
    Databases.createJsonDatabase(new DatabaseConfiguration(source));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(source)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("records")) {
        try (final JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"g":1,"n":10,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
              """), JsonNodeTrx.Commit.NO);
          final IndexDef projection = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/g", PathParser.Type.JSON), parse("/[]/n", PathParser.Type.JSON),
                  parse("/[]/lo", PathParser.Type.JSON), parse("/[]/hi", PathParser.Type.JSON)),
              List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
          session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
          TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer,
              "/[]", List.of("g"), List.of("sum(n)"));
          writer.commit();
          DerivedIndexTestSupport.await(session);
        }
        if (operation != Operation.BACKUP) {
          assertEquals(1, BackupManager.backupDatabase(source, backup).derivedResourcesCopied());
          if (operation == Operation.FAILED_RESTORE) {
            try (final FileChannel file = FileChannel.open(backup.resolve("resources/records$derived/data/sirix.data"),
                StandardOpenOption.WRITE)) {
              file.truncate(IOStorage.DATA_REGION_START);
            }
          }
        }
        final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
        try (final BufferManagerImpl live = new BufferManagerImpl(1 << 20, 1 << 20, 1 << 20, 16, 16, 16)) {
          switch (operation) {
            case BACKUP -> assertEquals(1, BackupManager.backupDatabase(source, backup).derivedResourcesCopied());
            case RESTORE -> assertEquals(1, BackupManager.restoreDatabase(backup, restored).derivedResourcesCopied());
            case FAILED_RESTORE -> assertThrows(SirixIOException.class,
                () -> BackupManager.restoreDatabase(backup, restored));
          }
          assertPressureEvictsLivePages(live);
        } finally {
          FrameSlotAllocator.setPressureListener(previous);
        }
      }
    }
  }

  @Test
  @DisplayName("TXN-613: open verification caches never replace the live pressure handler")
  void openVerificationCachesPreserveLivePressureEviction() {
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    try (final BufferManagerImpl live = new BufferManagerImpl(1 << 20, 1 << 20, 1 << 20, 16, 16, 16)) {
      try (final BufferManagerImpl verification = BufferManagerImpl.forVerification()) {
        assertNotSame(live.getPageCache(), verification.getPageCache());
        assertNotSame(live.getRecordPageCache(), verification.getRecordPageCache());
        assertNotSame(live.getHOTLeafPageCache(), verification.getHOTLeafPageCache());
        assertPressureEvictsLivePages(live);
      }
      assertPressureEvictsLivePages(live);
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
    }
  }

  private static void assertPressureEvictsLivePages(final BufferManagerImpl live) {
    final PageReference key = new PageReference().setKey(123456L).revisionCacheKey(1);
    final NamePage page = new NamePage();
    live.getPageCache().put(key, page);
    assertSame(page, live.getPageCache().get(key));
    Objects.requireNonNull(FrameSlotAllocator.pressureListener()).onPressure();
    assertNull(live.getPageCache().get(key), "TXN-613: pressure must still evict pages from the live cache");
  }
}
