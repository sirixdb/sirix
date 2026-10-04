package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class XmlNameIndexRemovalTest {
  private static final String RESOURCE = "data";

  @TempDir
  Path directory;

  private static Stream<Arguments> removalCases() {
    return Arrays.stream(VersioningType.values()).flatMap(versioning ->
        Stream.of(false, true).flatMap(committed ->
            Stream.of(NodeKind.ATTRIBUTE, NodeKind.PROCESSING_INSTRUCTION).flatMap(kind ->
                Stream.of(false, true).flatMap(subtree ->
                    Stream.of(false, true).map(renamed -> Arguments.of(versioning, committed, kind, subtree, renamed))))));
  }

  @ParameterizedTest
  @MethodSource("removalCases")
  void namedNodeRemovalPreservesOtherPostings(final VersioningType versioning, final boolean committed,
      final NodeKind kind, final boolean subtree, final boolean renamed) {
    final Path path = directory.resolve("xml-removal");
    final Set<IndexDef> definitions = Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML),
        IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.XML),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.XML));
    final Map<IndexType, Set<Long>> expected = new EnumMap<>(IndexType.class);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
        final long rootKey;
        final long namedKey;
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          trx.insertElementAsFirstChild(new QNm("root"));
          rootKey = trx.getNodeKey();
          trx.insertElementAsFirstChild(new QNm("child"));
          final long childKey = trx.getNodeKey();
          if (kind == NodeKind.ATTRIBUTE) {
            trx.insertAttribute(new QNm("a"), "value");
          } else {
            trx.insertPIAsFirstChild("pi", "value");
          }
          namedKey = trx.getNodeKey();
          assertTrue(trx.moveTo(childKey));
          trx.insertTextAsFirstChild("text");
          final long textKey = trx.getNodeKey();
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          controller.createIndexes(definitions, trx);
          expected.put(IndexType.NAME, new TreeSet<>(Set.of(rootKey, childKey)));
          expected.put(IndexType.PATH, new TreeSet<>(Set.of(rootKey, childKey)));
          expected.put(IndexType.CAS, new TreeSet<>(Set.of(textKey)));
          if (kind == NodeKind.ATTRIBUTE) {
            expected.get(IndexType.PATH).add(namedKey);
            expected.get(IndexType.CAS).add(namedKey);
          }
          if (renamed) {
            assertTrue(trx.moveTo(namedKey));
            trx.setName(new QNm("renamed"));
            for (final Set<Long> keys : expected.values()) {
              keys.add(namedKey);
            }
          }
          assertPostings(controller, trx.getStorageEngineReader(), expected);
          if (committed) {
            trx.commit();
          } else {
            removeAndAssert(session, trx, rootKey, namedKey, subtree, expected);
          }
        }
        if (committed) {
          try (final XmlNodeTrx trx = session.beginNodeTrx()) {
            assertPostings(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
                expected);
            removeAndAssert(session, trx, rootKey, namedKey, subtree, expected);
          }
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static void removeAndAssert(final XmlResourceSession session, final XmlNodeTrx trx, final long rootKey,
      final long namedKey, final boolean subtree, final Map<IndexType, Set<Long>> expected) {
    assertTrue(trx.moveTo(subtree ? rootKey : namedKey));
    trx.remove();
    assertFalse(trx.moveTo(namedKey));
    for (final Set<Long> keys : expected.values()) {
      if (subtree) {
        keys.clear();
      } else {
        keys.remove(namedKey);
      }
    }
    assertPostings(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(), expected);
    trx.commit();
    try (final var reader = session.beginNodeReadOnlyTrx()) {
      assertFalse(reader.moveTo(namedKey));
      assertPostings(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
          expected);
    }
  }

  private static void assertPostings(final IndexController<?, ?> controller, final StorageEngineReader reader,
      final Map<IndexType, Set<Long>> expected) {
    for (final IndexDef definition : controller.getIndexes().getIndexDefs()) {
      final Iterator<NodeReferences> postings = switch (definition.getType()) {
        case NAME -> controller.openNameIndex(reader, definition, new NameFilter(Set.of(), Set.of()));
        case PATH -> controller.openPathIndex(reader, definition, null);
        case CAS -> controller.openCASIndex(reader, definition, (CASFilter) null);
        default -> throw new AssertionError(definition.getType());
      };
      final Set<Long> actual = new TreeSet<>();
      while (postings.hasNext()) {
        final LongIterator keys = postings.next().getNodeKeys().getLongIterator();
        while (keys.hasNext()) {
          actual.add(keys.next());
        }
      }
      assertEquals(expected.get(definition.getType()), actual, definition.getType() + " postings");
    }
  }
}
