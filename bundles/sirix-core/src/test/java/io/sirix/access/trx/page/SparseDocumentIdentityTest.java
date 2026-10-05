package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct source-only check: a key jump must grow the trie without aliasing an older record. */
@Isolated
final class SparseDocumentIdentityTest {
  private static final long[] KEYS = {2, (1L << 20) + 17, 1_000_000_000_001L, (1L << 52) + 17};

  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(
                     versioning -> Stream.of(HashType.values())
                                         .flatMap(hash -> Stream.of(false, true)
                                                                .map(dewey -> Arguments.of(versioning, hash, dewey))));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void multiLevelKeyJumpsPreserveExistingRecordsAndHistory(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path path = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .hashKind(hash)
                                                   .useDeweyIDs(dewey)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), JsonNodeTrx.Commit.NO);
        writer.commit();
        for (int index = 1; index < KEYS.length; index++) {
          assertTrue(writer.moveTo(1));
          assertEquals(NodeKind.ARRAY, writer.getKind());
          writer.getStorageEngineReader().getActualRevisionRootPage().setMaxNodeKeyInDocumentIndex(KEYS[index] - 1);
          writer.insertNumberValueAsLastChild(index);
          assertEquals(KEYS[index], writer.getNodeKey());
          assertGraph(writer, index + 1, hash);
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= KEYS.length; revision++) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          assertGraph(reader, revision, hash);
          if (revision < KEYS.length) {
            assertFalse(reader.moveTo(KEYS[revision]), "later sparse identity leaked into history");
          }
        }
      }
    }
  }

  private static void assertGraph(final JsonNodeReadOnlyTrx reader, final int size, final HashType hash) {
    assertEquals(KEYS[size - 1], reader.getMaxNodeKey());
    assertTrue(reader.moveToDocumentRoot());
    assertEquals(NodeKind.JSON_DOCUMENT, reader.getKind());
    assertEquals(1, reader.getFirstChildKey());
    assertEquals(1, reader.getLastChildKey());
    assertEquals(1, reader.getChildCount());
    assertTrue(reader.moveTo(1));
    assertEquals(NodeKind.ARRAY, reader.getKind());
    assertEquals(size, reader.getChildCount());
    assertEquals(KEYS[0], reader.getFirstChildKey());
    assertEquals(KEYS[size - 1], reader.getLastChildKey());
    if (hash != HashType.NONE) {
      assertEquals(size, reader.getDescendantCount());
    }
    for (int index = 0; index < size; index++) {
      assertTrue(reader.moveTo(KEYS[index]));
      assertEquals(NodeKind.NUMBER_VALUE, reader.getKind());
      assertEquals(KEYS[index], reader.getNodeKey());
      assertEquals(index, reader.getNumberValue().intValue());
      assertEquals(1, reader.getParentKey());
      assertEquals(index == 0
          ? -1
          : KEYS[index - 1], reader.getLeftSiblingKey());
      assertEquals(index + 1 == size
          ? -1
          : KEYS[index + 1], reader.getRightSiblingKey());
    }
  }
}
