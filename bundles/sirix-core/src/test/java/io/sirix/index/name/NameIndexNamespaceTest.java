package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.roaringbitmap.longlong.LongIterator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Expanded names, rather than lexical prefixes, identify a NAME posting. */
final class NameIndexNamespaceTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void namespaceIdentitySurvivesBuildMutationAndColdHistory(final VersioningType versioning) {
    for (final BuildMode mode : BuildMode.values()) {
      for (final String prefix : List.of("", "p")) {
        verify(versioning, mode, prefix);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void jsonLiteralNamesSurviveBuildInsertRenameMoveAndRemoval(final VersioningType versioning) {
    final QNm item = new QNm("item");
    final QNm renamed = new QNm("renamed");
    final List<QNm> probes = List.of(item, renamed, new QNm("p:item"), new QNm("{urn:a}item"));
    final Set<IndexDef> definitions = Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON),
        IndexDefs.createSelectiveNameIdxDef(Set.of(item), 1, IndexDef.DbType.JSON),
        IndexDefs.createFilteredNameIdxDef(Set.of(item), 2, IndexDef.DbType.JSON));
    for (final BuildMode mode : BuildMode.values()) {
      final Path path = directory.resolve("json-" + mode);
      final Map<QNm, Set<Long>> expected = new HashMap<>();
      probes.forEach(name -> expected.put(name, new TreeSet<>()));
      Databases.createJsonDatabase(new DatabaseConfiguration(path));
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        database.createResource(ResourceConfiguration.newBuilder("data")
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(versioning)
                                                     .build());
        try (final JsonResourceSession session = database.beginResourceSession("data");
            final JsonNodeTrx trx = session.beginNodeTrx()) {
          if (mode == BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          trx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("[{\"item\":{},\"p:item\":{},\"{urn:a}item\":{}},{}]"),
              JsonNodeTrx.Commit.NO);
          trx.moveToDocumentRoot();
          trx.moveToFirstChild();
          final long arrayKey = trx.getNodeKey();
          trx.moveToFirstChild();
          final long firstObject = trx.getNodeKey();
          trx.moveToRightSibling();
          final long secondObject = trx.getNodeKey();
          trx.moveTo(firstObject);
          trx.moveToFirstChild();
          final long itemKey = trx.getNodeKey();
          do {
            Objects.requireNonNull(expected.get(trx.getName())).add(trx.getNodeKey());
          } while (trx.moveToRightSibling());
          if (mode == BuildMode.COMMITTED) {
            trx.commit();
          }
          if (mode != BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
          trx.moveTo(itemKey);
          trx.setObjectKeyName("renamed");
          Objects.requireNonNull(expected.get(item)).remove(itemKey);
          Objects.requireNonNull(expected.get(renamed)).add(itemKey);
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
          trx.moveTo(secondObject);
          trx.moveSubtreeToRightSibling(firstObject);
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
          trx.moveTo(firstObject);
          trx.insertObjectRecordAsFirstChild("item", ObjectValue.INSTANCE);
          expected.get(item).add(trx.getNodeKey());
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
          trx.moveTo(itemKey);
          trx.remove();
          expected.get(renamed).clear();
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
          trx.moveTo(arrayKey);
          trx.remove();
          expected.values().forEach(Set::clear);
          assertJsonCheckpoint(session, trx, definitions, probes, expected);
        }
      } finally {
        Databases.removeDatabase(path);
      }
    }
  }

  private static void assertJsonCheckpoint(final JsonResourceSession session, final JsonNodeTrx trx,
      final Set<IndexDef> definitions, final List<QNm> probes, final Map<QNm, Set<Long>> expected) {
    assertPostings(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(), definitions,
        probes, expected);
    trx.commit();
    try (final var reader = session.beginNodeReadOnlyTrx()) {
      assertPostings(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
          definitions, probes, expected);
    }
  }

  private void verify(final VersioningType versioning, final BuildMode mode, final String prefix) {
    final Path path = directory.resolve(mode + "-" + prefix);
    final QNm a = new QNm("urn:a", prefix, "item");
    final QNm alias = new QNm("urn:a", "other", "item");
    final QNm b = new QNm("urn:b", prefix, "item");
    final QNm plain = new QNm("item");
    final QNm root = new QNm("root");
    final List<QNm> probes = List.of(a, alias, b, plain, root);
    final Set<IndexDef> definitions = Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML),
        IndexDefs.createSelectiveNameIdxDef(Set.of(alias), 1, IndexDef.DbType.XML),
        IndexDefs.createFilteredNameIdxDef(Set.of(alias), 2, IndexDef.DbType.XML));
    final List<Map<QNm, Set<Long>>> history = new ArrayList<>();
    final Map<QNm, Set<Long>> expected = new HashMap<>();
    for (final QNm name : probes) {
      expected.put(name, new TreeSet<>());
    }
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try {
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
        database.createResource(ResourceConfiguration.newBuilder("data")
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(versioning)
                                                     .build());
        try (final XmlResourceSession session = database.beginResourceSession("data");
            final XmlNodeTrx trx = session.beginNodeTrx()) {
          if (mode == BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          trx.insertElementAsFirstChild(root);
          final long rootKey = trx.getNodeKey();
          Objects.requireNonNull(expected.get(root)).add(rootKey);
          trx.insertElementAsFirstChild(a);
          final long aKey = trx.getNodeKey();
          Objects.requireNonNull(expected.get(a)).add(aKey);
          trx.insertElementAsRightSibling(b);
          final long bKey = trx.getNodeKey();
          Objects.requireNonNull(expected.get(b)).add(bKey);
          trx.insertElementAsRightSibling(plain);
          Objects.requireNonNull(expected.get(plain)).add(trx.getNodeKey());
          if (mode == BuildMode.COMMITTED) {
            trx.commit();
          }
          if (mode != BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          checkpoint(session, trx, definitions, probes, expected, history);

          assertTrue(trx.moveTo(rootKey));
          trx.insertElementAsFirstChild(alias);
          final long aliasKey = trx.getNodeKey();
          expected.get(a).add(aliasKey);
          checkpoint(session, trx, definitions, probes, expected, history);

          // Change only the URI: lexical prefix/local name remain the same.
          assertTrue(trx.moveTo(aKey));
          trx.setName(b);
          expected.get(a).remove(aKey);
          expected.get(b).add(aKey);
          checkpoint(session, trx, definitions, probes, expected, history);

          assertTrue(trx.moveTo(bKey));
          trx.moveSubtreeToFirstChild(aliasKey);
          checkpoint(session, trx, definitions, probes, expected, history);

          assertTrue(trx.moveTo(bKey));
          trx.remove();
          expected.get(b).remove(bKey);
          expected.get(a).remove(aliasKey);
          checkpoint(session, trx, definitions, probes, expected, history);

          assertTrue(trx.moveTo(aKey));
          trx.remove();
          expected.get(b).remove(aKey);
          checkpoint(session, trx, definitions, probes, expected, history);
        }
      }
      Databases.clearGlobalCaches();
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path);
          final XmlResourceSession session = database.beginResourceSession("data")) {
        final int firstRevision = mode == BuildMode.COMMITTED
            ? 2
            : 1;
        for (int i = 0; i < history.size(); i++) {
          try (final var reader = session.beginNodeReadOnlyTrx(firstRevision + i)) {
            assertPostings(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
                definitions, probes, history.get(i));
          }
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static void checkpoint(final XmlResourceSession session, final XmlNodeTrx trx,
      final Set<IndexDef> definitions, final List<QNm> probes, final Map<QNm, Set<Long>> expected,
      final List<Map<QNm, Set<Long>>> history) {
    assertPostings(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(), definitions,
        probes, expected);
    final Map<QNm, Set<Long>> snapshot = new HashMap<>();
    expected.forEach((name, keys) -> snapshot.put(name, Set.copyOf(keys)));
    history.add(snapshot);
    trx.commit();
    try (final var reader = session.beginNodeReadOnlyTrx()) {
      assertPostings(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
          definitions, probes, expected);
    }
  }

  private static void assertPostings(final IndexController<?, ?> controller, final StorageEngineReader reader,
      final Set<IndexDef> definitions, final List<QNm> probes, final Map<QNm, Set<Long>> expected) {
    final List<NameFilter> filters = new ArrayList<>();
    for (final QNm probe : probes) {
      filters.add(new NameFilter(Set.of(probe), Set.of()));
    }
    filters.add(new NameFilter(Set.of(probes.get(1), probes.get(2)), Set.of()));
    filters.add(new NameFilter(Set.of(), Set.of(probes.get(1))));
    filters.add(new NameFilter(Set.of(probes.get(1), probes.get(2)), Set.of(probes.get(2))));
    filters.add(new NameFilter(Set.of(), Set.of()));
    for (final IndexDef definition : definitions) {
      final IndexDef persisted = controller.getIndexes().getIndexDef(definition.getID(), definition.getType());
      assertTrue(definition.hasSameDefinition(persisted), "qualified definition survives persistence");
      for (final NameFilter filter : filters) {
        final Set<Long> wanted = new TreeSet<>();
        expected.forEach((name, keys) -> {
          if ((definition.getIncluded().isEmpty() || definition.getIncluded().contains(name))
              && !definition.getExcluded().contains(name)
              && (filter.getIncludes().isEmpty() || filter.getIncludes().contains(name))
              && !filter.getExcludes().contains(name)) {
            wanted.addAll(keys);
          }
        });
        assertEquals(wanted, collect(controller.openNameIndex(reader, persisted, filter)),
            "index " + definition.getID() + " includes " + filter.getIncludes() + " excludes " + filter.getExcludes());
      }
    }
  }

  private static Set<Long> collect(final Iterator<NodeReferences> postings) {
    final Set<Long> keys = new TreeSet<>();
    while (postings.hasNext()) {
      final LongIterator iterator = postings.next().getNodeKeys().getLongIterator();
      while (iterator.hasNext()) {
        keys.add(iterator.next());
      }
    }
    return keys;
  }

  private enum BuildMode {
    INCREMENTAL, UNCOMMITTED, COMMITTED
  }
}
