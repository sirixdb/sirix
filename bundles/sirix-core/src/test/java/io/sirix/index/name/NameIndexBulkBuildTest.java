package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** NAME builds must resolve dictionary-backed names and agree with incremental maintenance. */
final class NameIndexBulkBuildTest {
  private static final String RESOURCE = "data";
  private static final QNm ROOT = new QNm("root");
  private static final QNm CHILD = new QNm("child");
  private static final QNm MISSING = new QNm("missing");
  private static final QNm QUALIFIED_ROOT = new QNm("urn:root", "r", "root");
  private static final QNm QUALIFIED_CHILD = new QNm("urn:child", "c", "chîld");
  private static final String JSON = """
      [{"number":1,"string":"a","boolean":true,"nil":null,"object":{},"array":[]},
       {"number":2,"string":"b","boolean":false,"nil":null,"object":{},"array":[]}]
      """;
  private static final List<QNm> JSON_NAMES = List.of(new QNm("number"), new QNm("string"), new QNm("boolean"),
      new QNm("nil"), new QNm("object"), new QNm("array"), MISSING);

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlUncommittedBuildMatchesIncremental(final VersioningType versioning) {
    assertXmlBuilds(versioning, ROOT, CHILD, BuildMode.UNCOMMITTED);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlCommittedBuildMatchesIncremental(final VersioningType versioning) {
    assertXmlBuilds(versioning, ROOT, CHILD, BuildMode.COMMITTED);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlBuildResolvesNamespacePrefixAndUnicodeLocalName(final VersioningType versioning) {
    assertXmlBuilds(versioning, QUALIFIED_ROOT, QUALIFIED_CHILD, BuildMode.UNCOMMITTED);
    assertXmlBuilds(versioning, QUALIFIED_ROOT, QUALIFIED_CHILD, BuildMode.COMMITTED);
  }

  private void assertXmlBuilds(final VersioningType versioning, final QNm root, final QNm child, final BuildMode mode) {
    final List<QNm> names = List.of(root, child, MISSING);
    final Set<IndexDef> definitions = definitions(IndexDef.DbType.XML, child);
    final Map<QNm, Set<Long>> expected =
        Map.of(root, new TreeSet<>(Set.of(1L)), child, new TreeSet<>(Set.of(2L, 3L)), MISSING, new TreeSet<>());
    final Map<Integer, Map<QNm, Set<Long>>> bulk =
        xmlPostings(versioning, mode, root, child, names, definitions, expected);
    assertEquals(xmlPostings(versioning, BuildMode.INCREMENTAL, root, child, names, definitions, expected), bulk,
        "XML name lookups must agree for " + mode);
  }

  private Map<Integer, Map<QNm, Set<Long>>> xmlPostings(final VersioningType versioning, final BuildMode mode,
      final QNm root, final QNm child, final List<QNm> names, final Set<IndexDef> definitions,
      final Map<QNm, Set<Long>> expected) {
    final Path path = directory.resolve("xml-" + mode);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Map<Integer, Map<QNm, Set<Long>>> live;
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          if (mode == BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          trx.insertElementAsFirstChild(root);
          trx.insertElementAsFirstChild(child);
          trx.insertElementAsRightSibling(child);
          if (mode == BuildMode.COMMITTED) {
            trx.commit();
          }
          trx.moveToDocumentRoot();
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          if (mode != BuildMode.INCREMENTAL) {
            controller.createIndexes(definitions, trx);
          }
          live = lookups(controller, trx.getStorageEngineReader(), definitions, names, expected);
          trx.commit();
        }
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertEquals(live, lookups(session.getRtxIndexController(reader.getRevisionNumber()),
              reader.getStorageEngineReader(), definitions, names, expected), "committed XML postings");
        }
        return live;
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void jsonFusedObjectKeysResolveEveryValueKindBeforeAndAfterCommit(final VersioningType versioning) {
    final QNm selected = new QNm("object");
    final Set<IndexDef> definitions = definitions(IndexDef.DbType.JSON, selected);
    final Map<Integer, Map<QNm, Set<Long>>> incremental = jsonPostings(versioning, BuildMode.INCREMENTAL, definitions);
    for (final BuildMode mode : List.of(BuildMode.UNCOMMITTED, BuildMode.COMMITTED)) {
      assertEquals(incremental, jsonPostings(versioning, mode, definitions),
          "JSON name lookups must agree for " + mode);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void longJsonFieldNamesSupportBuildInsertRemoveAndReopen(final VersioningType versioning) {
    final QNm name = new QNm("x".repeat(300));
    final List<QNm> names = List.of(name, MISSING);
    final Set<IndexDef> definitions = definitions(IndexDef.DbType.JSON, name);
    for (final BuildMode mode : BuildMode.values()) {
      final Path path = directory.resolve("json-long-name-" + mode);
      final Map<Integer, Set<Long>> snapshots = new HashMap<>();
      final Map<QNm, Set<Long>> expected = new HashMap<>();
      final Set<Long> keys = new TreeSet<>();
      expected.put(name, keys);
      expected.put(MISSING, Set.of());
      Databases.createJsonDatabase(new DatabaseConfiguration(path));
      try {
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
          database.createResource(configuration(versioning));
          try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
              final JsonNodeTrx trx = session.beginNodeTrx()) {
            if (mode == BuildMode.INCREMENTAL) {
              session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
            }
            trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"" + name.getLocalName() + "\":1}]"),
                JsonNodeTrx.Commit.NO);
            trx.moveToDocumentRoot();
            assertTrue(trx.moveToFirstChild());
            final long arrayKey = trx.getNodeKey();
            assertTrue(trx.moveToFirstChild());
            assertTrue(trx.moveToFirstChild());
            final long originalKey = trx.getNodeKey();
            assertEquals(name, trx.getName());
            keys.add(originalKey);
            if (mode == BuildMode.COMMITTED) {
              trx.commit();
            }
            if (mode != BuildMode.INCREMENTAL) {
              session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
            }
            lookups(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
                definitions, names, expected);
            snapshots.put(trx.getRevisionNumber(), Set.copyOf(keys));
            trx.commit();

            assertTrue(trx.moveTo(arrayKey));
            trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"" + name.getLocalName() + "\":2}"),
                JsonNodeTrx.Commit.NO);
            assertTrue(trx.moveToFirstChild());
            final long insertedKey = trx.getNodeKey();
            assertEquals(name, trx.getName());
            keys.add(insertedKey);
            lookups(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
                definitions, names, expected);
            snapshots.put(trx.getRevisionNumber(), Set.copyOf(keys));
            trx.commit();

            for (final long key : new long[] {originalKey, insertedKey}) {
              assertTrue(trx.moveTo(key));
              trx.remove();
              keys.remove(key);
              lookups(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(),
                  definitions, names, expected);
              snapshots.put(trx.getRevisionNumber(), Set.copyOf(keys));
              trx.commit();
            }
          }
        }
        Databases.clearGlobalCaches();
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
            final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
          for (final Map.Entry<Integer, Set<Long>> snapshot : snapshots.entrySet()) {
            try (final var trx = session.beginNodeReadOnlyTrx(snapshot.getKey())) {
              lookups(session.getRtxIndexController(snapshot.getKey()), trx.getStorageEngineReader(), definitions,
                  names, Map.of(name, snapshot.getValue(), MISSING, Set.of()));
            }
          }
        }
      } finally {
        Databases.removeDatabase(path);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void mixedShortAndLongNamesSupportMultiLeafBuildAndMaintenance(final VersioningType versioning) {
    final String prefix = "x".repeat(300);
    final StringBuilder json = new StringBuilder(160_000).append("[{\"a\":0");
    for (int i = 0; i < 513; i++) {
      json.append(",\"").append(prefix).append((char) ('0' + i / 100))
          .append((char) ('0' + i / 10 % 10)).append((char) ('0' + i % 10)).append("\":0");
    }
    json.append("}]");
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    for (final BuildMode mode : List.of(BuildMode.UNCOMMITTED, BuildMode.COMMITTED)) {
      final Path path = directory.resolve("json-multi-leaf-long-names-" + mode);
      final Map<Integer, Set<Long>> snapshots = new HashMap<>();
      Databases.createJsonDatabase(new DatabaseConfiguration(path));
      try {
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
          database.createResource(configuration(versioning));
          try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
              final JsonNodeTrx trx = session.beginNodeTrx()) {
            trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            final Set<Long> expected = new TreeSet<>();
            trx.moveToDocumentRoot();
            final DescendantAxis axis = new DescendantAxis(trx);
            while (axis.hasNext()) {
              final long key = axis.nextLong();
              if (trx.getName() != null) {
                expected.add(key);
              }
            }
            assertEquals(514, expected.size());
            trx.moveToDocumentRoot();
            assertTrue(trx.moveToFirstChild());
            final long arrayKey = trx.getNodeKey();
            if (mode == BuildMode.COMMITTED) {
              trx.commit();
            }
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(definition), trx);
            assertAllNamePostings(session.getWtxIndexController(trx.getRevisionNumber()),
                trx.getStorageEngineReader(), definition, expected);
            snapshots.put(trx.getRevisionNumber(), Set.copyOf(expected));
            trx.commit();

            for (final String name : new String[] {"a", prefix + "000"}) {
              assertTrue(trx.moveTo(arrayKey));
              trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"" + name + "\":1}"),
                  JsonNodeTrx.Commit.NO);
              assertTrue(trx.moveToFirstChild());
              assertEquals(new QNm(name), trx.getName());
              final long insertedKey = trx.getNodeKey();
              expected.add(insertedKey);
              assertAllNamePostings(session.getWtxIndexController(trx.getRevisionNumber()),
                  trx.getStorageEngineReader(), definition, expected);
              snapshots.put(trx.getRevisionNumber(), Set.copyOf(expected));
              trx.commit();

              assertTrue(trx.moveTo(insertedKey));
              trx.remove();
              expected.remove(insertedKey);
              assertAllNamePostings(session.getWtxIndexController(trx.getRevisionNumber()),
                  trx.getStorageEngineReader(), definition, expected);
              snapshots.put(trx.getRevisionNumber(), Set.copyOf(expected));
              trx.commit();
            }
          }
        }
        Databases.clearGlobalCaches();
        try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
            final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
          for (final Map.Entry<Integer, Set<Long>> snapshot : snapshots.entrySet()) {
            try (final var trx = session.beginNodeReadOnlyTrx(snapshot.getKey())) {
              assertAllNamePostings(session.getRtxIndexController(snapshot.getKey()), trx.getStorageEngineReader(),
                  definition, snapshot.getValue());
            }
          }
        }
      } finally {
        Databases.removeDatabase(path);
      }
    }
  }

  private static void assertAllNamePostings(final IndexController<?, ?> controller, final StorageEngineReader reader,
      final IndexDef definition, final Set<Long> expected) {
    assertEquals(expected, collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(), Set.of()))));
  }

  private Map<Integer, Map<QNm, Set<Long>>> jsonPostings(final VersioningType versioning, final BuildMode mode,
      final Set<IndexDef> definitions) {
    final Path path = directory.resolve("json-" + mode);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Map<Integer, Map<QNm, Set<Long>>> live;
        final Map<QNm, Set<Long>> expected = new HashMap<>();
        for (final QNm name : JSON_NAMES) {
          expected.put(name, new TreeSet<>());
        }
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          if (mode == BuildMode.INCREMENTAL) {
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          }
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(JSON), JsonNodeTrx.Commit.NO);
          trx.moveToDocumentRoot();
          final DescendantAxis axis = new DescendantAxis(trx);
          while (axis.hasNext()) {
            final long key = axis.nextLong();
            final QNm name = trx.getName();
            if (name != null) {
              requireNonNull(expected.get(name)).add(key);
            }
          }
          for (final QNm name : JSON_NAMES) {
            assertEquals(name.equals(MISSING)
                ? 0
                : 2, requireNonNull(expected.get(name)).size(), "fixture field count: " + name);
          }
          if (mode == BuildMode.COMMITTED) {
            trx.commit();
          }
          trx.moveToDocumentRoot();
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          if (mode != BuildMode.INCREMENTAL) {
            controller.createIndexes(definitions, trx);
          }
          live = lookups(controller, trx.getStorageEngineReader(), definitions, JSON_NAMES, expected);
          trx.commit();
        }
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertEquals(live, lookups(session.getRtxIndexController(reader.getRevisionNumber()),
              reader.getStorageEngineReader(), definitions, JSON_NAMES, expected), "committed JSON postings");
        }
        return live;
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlIncrementalRenameAndDeleteResolveNamesAfterCommit(final VersioningType versioning) {
    final Path path = directory.resolve("xml-mutations");
    final QNm renamed = new QNm("urn:renamed", "n", "renamed");
    final List<QNm> names = List.of(ROOT, CHILD, renamed, MISSING);
    final Set<IndexDef> definitions = definitions(IndexDef.DbType.XML, CHILD);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
        final long rootKey;
        final long childKey;
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(definitions, trx);
          trx.insertElementAsFirstChild(ROOT);
          rootKey = trx.getNodeKey();
          trx.insertElementAsFirstChild(CHILD);
          childKey = trx.getNodeKey();
          trx.commit();
        }
        try (final XmlNodeTrx trx = session.beginNodeTrx()) {
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          assertTrue(trx.moveTo(childKey));
          trx.setName(renamed);
          final Map<QNm, Set<Long>> expected = new HashMap<>();
          expected.put(ROOT, new TreeSet<>(Set.of(rootKey)));
          expected.put(CHILD, new TreeSet<>());
          expected.put(renamed, new TreeSet<>(Set.of(childKey)));
          expected.put(MISSING, new TreeSet<>());
          lookups(controller, trx.getStorageEngineReader(), definitions, names, expected);
          trx.commit();
          assertTrue(trx.moveTo(childKey));
          trx.remove();
          expected.put(renamed, new TreeSet<>());
          lookups(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(), definitions,
              names, expected);
          trx.commit();
          try (final var reader = session.beginNodeReadOnlyTrx()) {
            lookups(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
                definitions, names, expected);
          }
        }
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlRenameAfterUncommittedBuildUsesAddressedName(final VersioningType versioning) {
    final Path path = directory.resolve("xml-uncommitted-rename");
    final QNm renamed = new QNm("renamed");
    final List<QNm> names = List.of(ROOT, CHILD, renamed);
    final Set<IndexDef> definitions = definitions(IndexDef.DbType.XML, ROOT);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
          final XmlNodeTrx trx = session.beginNodeTrx()) {
        trx.insertElementAsFirstChild(ROOT);
        final long rootKey = trx.getNodeKey();
        trx.insertElementAsFirstChild(CHILD);
        final long childKey = trx.getNodeKey();
        final var controller = session.getWtxIndexController(trx.getRevisionNumber());
        controller.createIndexes(definitions, trx);
        assertTrue(trx.moveTo(rootKey));
        trx.setName(renamed);
        final Map<QNm, Set<Long>> expected = new HashMap<>();
        expected.put(ROOT, new TreeSet<>());
        expected.put(CHILD, new TreeSet<>(Set.of(childKey)));
        expected.put(renamed, new TreeSet<>(Set.of(rootKey)));
        lookups(controller, trx.getStorageEngineReader(), definitions, names, expected);
        trx.commit();
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          lookups(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
              definitions, names, expected);
        }
        assertTrue(trx.moveTo(rootKey));
        trx.remove();
        expected.put(CHILD, new TreeSet<>());
        expected.put(renamed, new TreeSet<>());
        lookups(session.getWtxIndexController(trx.getRevisionNumber()), trx.getStorageEngineReader(), definitions,
            names, expected);
        trx.commit();
      }
    } finally {
      Databases.removeDatabase(path);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void xmlNamespaceMutationsKeepNamePostings(final VersioningType versioning) {
    for (final NamespaceMutation mutation : NamespaceMutation.values()) {
      final Path path = directory.resolve("xml-namespace-" + mutation);
      final QNm target = new QNm("target");
      final QNm container = new QNm("container");
      final QNm namespace = new QNm("urn:p", "p", "");
      final List<QNm> names = List.of(ROOT, CHILD, target, container);
      final Set<IndexDef> definitions = definitions(IndexDef.DbType.XML, CHILD);
      Databases.createXmlDatabase(new DatabaseConfiguration(path));
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
        database.createResource(configuration(versioning));
        try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
            final XmlNodeTrx trx = session.beginNodeTrx()) {
          trx.insertElementAsFirstChild(ROOT);
          final long rootKey = trx.getNodeKey();
          trx.insertNamespace(namespace);
          final long namespaceKey = trx.getNodeKey();
          assertTrue(trx.moveTo(rootKey));
          trx.insertElementAsFirstChild(CHILD);
          final long childKey = trx.getNodeKey();
          trx.insertNamespace(namespace);
          assertTrue(trx.moveTo(childKey));
          trx.insertElementAsRightSibling(container);
          final long containerKey = trx.getNodeKey();
          trx.insertElementAsFirstChild(target);
          final long targetKey = trx.getNodeKey();
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          controller.createIndexes(definitions, trx);
          final Map<QNm, Set<Long>> expected = new HashMap<>();
          expected.put(ROOT, new TreeSet<>(Set.of(rootKey)));
          expected.put(CHILD, new TreeSet<>(Set.of(childKey)));
          expected.put(target, new TreeSet<>(Set.of(targetKey)));
          expected.put(container, new TreeSet<>(Set.of(containerKey)));
          switch (mutation) {
            case REMOVE_ROOT -> {
              assertTrue(trx.moveTo(rootKey));
              trx.remove();
              for (final Set<Long> keys : expected.values()) {
                keys.clear();
              }
            }
            case REMOVE_NAMESPACE -> {
              assertTrue(trx.moveTo(namespaceKey));
              trx.remove();
              assertTrue(trx.moveTo(rootKey));
              assertEquals(0, trx.getNamespaceCount());
            }
            case RENAME_NAMESPACE -> {
              assertTrue(trx.moveTo(namespaceKey));
              trx.setName(new QNm("urn:q", "q", ""));
              assertEquals("urn:q", trx.getName().getNamespaceURI());
              assertEquals("q", trx.getName().getPrefix());
            }
            case MOVE_FIRST_CHILD, MOVE_LEFT_SIBLING, MOVE_RIGHT_SIBLING -> {
              assertTrue(trx.moveTo(targetKey));
              if (mutation == NamespaceMutation.MOVE_FIRST_CHILD) {
                trx.moveSubtreeToFirstChild(childKey);
              } else if (mutation == NamespaceMutation.MOVE_LEFT_SIBLING) {
                trx.moveSubtreeToLeftSibling(childKey);
              } else {
                trx.moveSubtreeToRightSibling(childKey);
              }
              assertTrue(trx.moveTo(childKey));
              assertEquals(mutation == NamespaceMutation.MOVE_FIRST_CHILD
                  ? targetKey
                  : containerKey, trx.getParentKey());
              assertEquals(1, trx.getNamespaceCount());
            }
          }
          lookups(controller, trx.getStorageEngineReader(), definitions, names, expected);
          final Set<Long> allExpected = new TreeSet<>();
          for (final Set<Long> keys : expected.values()) {
            allExpected.addAll(keys);
          }
          final IndexDef allNames =
              definitions.stream()
                         .filter(definition -> definition.getIncluded().isEmpty() && definition.getExcluded().isEmpty())
                         .findFirst()
                         .orElseThrow();
          assertEquals(allExpected, collect(
              controller.openNameIndex(trx.getStorageEngineReader(), allNames, new NameFilter(Set.of(), Set.of()))));
          trx.commit();
          try (final var reader = session.beginNodeReadOnlyTrx()) {
            lookups(session.getRtxIndexController(reader.getRevisionNumber()), reader.getStorageEngineReader(),
                definitions, names, expected);
          }
        }
      } finally {
        Databases.removeDatabase(path);
      }
    }
  }

  private static ResourceConfiguration configuration(final VersioningType versioning) {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storageType(StorageType.FILE_CHANNEL)
                                .versioningApproach(versioning)
                                .build();
  }

  private static Set<IndexDef> definitions(final IndexDef.DbType type, final QNm selected) {
    return Set.of(IndexDefs.createNameIdxDef(0, type), IndexDefs.createSelectiveNameIdxDef(Set.of(selected), 1, type),
        IndexDefs.createFilteredNameIdxDef(Set.of(selected), 2, type));
  }

  private static Map<Integer, Map<QNm, Set<Long>>> lookups(final IndexController<?, ?> controller,
      final StorageEngineReader reader, final Set<IndexDef> definitions, final List<QNm> names,
      final Map<QNm, Set<Long>> expected) {
    final Map<Integer, Map<QNm, Set<Long>>> result = new HashMap<>();
    for (final IndexDef definition : definitions) {
      final Map<QNm, Set<Long>> postings = new HashMap<>();
      for (final QNm name : names) {
        final Set<Long> keys =
            collect(controller.openNameIndex(reader, definition, new NameFilter(Set.of(name), Set.of())));
        final boolean included = definition.getIncluded().isEmpty() || definition.getIncluded().contains(name);
        final boolean excluded = definition.getExcluded().contains(name);
        assertEquals(included && !excluded
            ? expected.get(name)
            : new TreeSet<Long>(), keys, "exact postings for index " + definition.getID() + ", name " + name);
        postings.put(name, keys);
      }
      result.put(definition.getID(), postings);
    }
    return result;
  }

  private static Set<Long> collect(final Iterator<NodeReferences> hits) {
    final Set<Long> keys = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator postings = hits.next().getNodeKeys().getLongIterator();
      while (postings.hasNext()) {
        keys.add(postings.next());
      }
    }
    return keys;
  }

  private enum BuildMode {
    INCREMENTAL, UNCOMMITTED, COMMITTED
  }

  private enum NamespaceMutation {
    REMOVE_ROOT, REMOVE_NAMESPACE, RENAME_NAMESPACE, MOVE_FIRST_CHILD, MOVE_LEFT_SIBLING, MOVE_RIGHT_SIBLING
  }
}
