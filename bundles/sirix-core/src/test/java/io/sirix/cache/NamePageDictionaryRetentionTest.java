package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.AbstractForwardingStorageEngineReader;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.name.Names;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class NamePageDictionaryRetentionTest {
  private static final int REVISIONS = 512;
  private static final int NAMES_CACHE_LIMIT = 500;

  @TempDir
  Path directory;

  @BeforeEach
  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(DatabaseType.class)
  void dictionariesRemainBoundedWhileRevisionMetadataStaysCached(final DatabaseType type) throws Exception {
    final Path databasePath = directory.resolve(type.name());
    final DatabaseConfiguration configuration = new DatabaseConfiguration(databasePath);
    if (type == DatabaseType.JSON) {
      Databases.createJsonDatabase(configuration);
    } else {
      Databases.createXmlDatabase(configuration);
    }
    final NodeKind[] kinds = type == DatabaseType.JSON
        ? new NodeKind[] {NodeKind.OBJECT_NAMED_OBJECT, NodeKind.OBJECT_NAMED_ARRAY, NodeKind.OBJECT_NAMED_BOOLEAN,
            NodeKind.OBJECT_NAMED_NUMBER, NodeKind.OBJECT_NAMED_STRING, NodeKind.OBJECT_NAMED_NULL}
        : new NodeKind[] {NodeKind.ATTRIBUTE, NodeKind.ELEMENT, NodeKind.NAMESPACE, NodeKind.PROCESSING_INSTRUCTION};
    try (final Database<?> database = type == DatabaseType.JSON
        ? Databases.openJsonDatabase(databasePath)
        : Databases.openXmlDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("names")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .buildPathSummary(false)
                                                   .storeDiffs(false)
                                                   .build());
      try (final var session = database.beginResourceSession("names")) {
        final int[] nameKeys = new int[kinds.length];
        try (final var writer = session.beginNodeTrx()) {
          for (int revision = 1; revision <= REVISIONS; revision++) {
            for (int index = 0; index < kinds.length; index++) {
              nameKeys[index] = writer.getStorageEngineWriter().createNameKey(name(kinds[index]), kinds[index]);
            }
            writer.commit();
          }
        }
        assertEquals(REVISIONS, session.getMostRecentRevisionNumber());
        clearCaches();
        final NamesCache namesCache = (NamesCache) Databases.getGlobalBufferManager().getNamesCache();
        final List<NamesCacheKey> dictionaryKeys = new ArrayList<>(REVISIONS * kinds.length);
        final List<PageReference> metadataKeys = new ArrayList<>(REVISIONS);
        final List<NodeStorageEngineReader> closedReaders = new ArrayList<>(REVISIONS);
        try (final StorageEngineReader active = session.createStorageEngineReader(1)) {
          for (int index = 0; index < kinds.length; index++) {
            assertEquals(name(kinds[index]), active.getName(nameKeys[index], kinds[index]));
            assertEquals(name(kinds[index]), active.getName(nameKeys[index], kinds[index]));
          }
          assertTrue(retainedReaderDictionaries((NodeStorageEngineReader) active) > 0);
          for (int revision = 1; revision <= REVISIONS; revision++) {
            try (final StorageEngineReader reader = session.createStorageEngineReader(revision)) {
              final NamePage page = reader.getNamePage(reader.getActualRevisionRootPage());
              final PageReference metadataKey =
                  reader.getActualRevisionRootPage().getNamePageReference().revisionCacheKey(revision);
              metadataKeys.add(metadataKey);
              assertSame(page, reader.getBufferManager().getPageCache().get(metadataKey));
              for (int index = 0; index < kinds.length; index++) {
                assertLookup(page, reader, kinds[index], nameKeys[index], revision, revision % 4);
                assertEquals(name(kinds[index]), reader.getName(nameKeys[index], kinds[index]));
                assertEquals(revision, reader.getNameCount(nameKeys[index], kinds[index]));
                dictionaryKeys.add(new NamesCacheKey(reader.getDatabaseId(), reader.getResourceId(), revision,
                    NamePage.dictionaryOffset(kinds[index])));
              }
              closedReaders.add((NodeStorageEngineReader) reader);
            }
          }
          cleanUp(namesCache);
          final int retained = namesCache.getAll(dictionaryKeys).size();
          assertTrue(retained > 0 && retained <= NAMES_CACHE_LIMIT, "the NamesCache retains at most 500 dictionaries");
          int metadataPages = 0;
          int metadataDictionaries = 0;
          for (final PageReference key : metadataKeys) {
            final NamePage page = (NamePage) active.getBufferManager().getPageCache().get(key);
            if (page != null) {
              metadataPages++;
              metadataDictionaries += retainedPageDictionaries(page);
            }
          }
          assertTrue(metadataPages > NAMES_CACHE_LIMIT, "revision-scoped metadata remains cached beyond 500 revisions");
          assertEquals(0, metadataDictionaries, "cached metadata must not extend dictionary retention");
          for (final NodeStorageEngineReader reader : closedReaders) {
            assertEquals(0, retainedReaderDictionaries(reader), "closed readers must release their dictionary memos");
          }
          namesCache.clear();
          for (int index = 0; index < kinds.length; index++) {
            assertEquals(name(kinds[index]), active.getName(nameKeys[index], kinds[index]));
            assertEquals(1, active.getNameCount(nameKeys[index], kinds[index]));
          }
          try (final StorageEngineReader reader = session.createStorageEngineReader(1)) {
            final NamePage page = reader.getNamePage(reader.getActualRevisionRootPage());
            final CountingReader counted = new CountingReader(reader);
            for (int index = 0; index < kinds.length; index++) {
              assertEquals(1, page.getCount(nameKeys[index], kinds[index], counted));
            }
            assertTrue(counted.nameRecords >= 2L * kinds.length,
                "an evicted dictionary must be reconstructed from persisted name/count records");
            final long coldRecords = counted.nameRecords;
            for (int lookup = 0; lookup < 4; lookup++) {
              for (int index = 0; index < kinds.length; index++) {
                assertLookup(page, counted, kinds[index], nameKeys[index], 1, lookup);
              }
            }
            assertEquals(coldRecords, counted.nameRecords, "warm accessors must reuse NamesCache dictionaries");
            assertEquals(0, retainedPageDictionaries(page));
          }
          active.close();
          assertEquals(0, retainedReaderDictionaries((NodeStorageEngineReader) active));
        }
      }
    }
  }

  private static void assertLookup(final NamePage page, final StorageEngineReader reader, final NodeKind kind,
      final int key, final int count, final int lookup) {
    switch (lookup) {
      case 0 -> assertEquals(name(kind), page.getName(key, kind, reader));
      case 1 -> assertEquals(name(kind), new String(page.getRawName(key, kind, reader), StandardCharsets.UTF_8));
      case 2 -> assertEquals(count, page.getCount(key, kind, reader));
      case 3 -> assertEquals(key, page.keyForName(name(kind), kind, reader));
      default -> throw new IllegalArgumentException("unknown lookup: " + lookup);
    }
  }

  private static String name(final NodeKind kind) {
    return "name-" + kind.name();
  }

  private static int retainedPageDictionaries(final NamePage page) throws IllegalAccessException {
    int retained = 0;
    for (final Field field : NamePage.class.getDeclaredFields()) {
      if (field.getType() == Names.class) {
        field.setAccessible(true);
        if (field.get(page) != null) {
          retained++;
        }
      }
    }
    return retained;
  }

  private static int retainedReaderDictionaries(final NodeStorageEngineReader reader) throws Exception {
    final Field field = NodeStorageEngineReader.class.getDeclaredField("namesByOffset");
    field.setAccessible(true);
    int retained = 0;
    for (final Names names : (Names[]) field.get(reader)) {
      if (names != null) {
        retained++;
      }
    }
    return retained;
  }

  private static void cleanUp(final NamesCache namesCache) throws Exception {
    final Field field = NamesCache.class.getDeclaredField("cache");
    field.setAccessible(true);
    ((Cache<?, ?>) field.get(namesCache)).cleanUp();
  }

  private static final class CountingReader extends AbstractForwardingStorageEngineReader {
    private final StorageEngineReader reader;
    private long nameRecords;

    private CountingReader(final StorageEngineReader reader) {
      this.reader = reader;
    }

    @Override
    protected StorageEngineReader delegate() {
      return reader;
    }

    @Override
    public <V extends DataRecord> V getRecord(final long key, final IndexType type, final int index) {
      if (type == IndexType.NAME) {
        nameRecords++;
      }
      return reader.getRecord(key, type, index);
    }
  }
}
