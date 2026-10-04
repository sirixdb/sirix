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
    final Map<QNm, TreeSet<Long>> expected =
        Map.of(root, new TreeSet<>(Set.of(1L)), child, new TreeSet<>(Set.of(2L, 3L)), MISSING, new TreeSet<>());
    final Map<Integer, Map<QNm, TreeSet<Long>>> bulk =
        xmlPostings(versioning, mode, root, child, names, definitions, expected);
    assertEquals(xmlPostings(versioning, BuildMode.INCREMENTAL, root, child, names, definitions, expected), bulk,
        "XML name lookups must agree for " + mode);
  }

  private Map<Integer, Map<QNm, TreeSet<Long>>> xmlPostings(final VersioningType versioning, final BuildMode mode,
      final QNm root, final QNm child, final List<QNm> names, final Set<IndexDef> definitions,
      final Map<QNm, TreeSet<Long>> expected) {
    final Path path = directory.resolve("xml-" + mode);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Map<Integer, Map<QNm, TreeSet<Long>>> live;
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
    final Map<Integer, Map<QNm, TreeSet<Long>>> incremental =
        jsonPostings(versioning, BuildMode.INCREMENTAL, definitions);
    for (final BuildMode mode : List.of(BuildMode.UNCOMMITTED, BuildMode.COMMITTED)) {
      assertEquals(incremental, jsonPostings(versioning, mode, definitions),
          "JSON name lookups must agree for " + mode);
    }
  }

  private Map<Integer, Map<QNm, TreeSet<Long>>> jsonPostings(final VersioningType versioning, final BuildMode mode,
      final Set<IndexDef> definitions) {
    final Path path = directory.resolve("json-" + mode);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(configuration(versioning));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final Map<Integer, Map<QNm, TreeSet<Long>>> live;
        final Map<QNm, TreeSet<Long>> expected = new HashMap<>();
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
              expected.get(name).add(key);
            }
          }
          for (final QNm name : JSON_NAMES) {
            assertEquals(name.equals(MISSING)
                ? 0
                : 2, expected.get(name).size(), "fixture field count: " + name);
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
          final Map<QNm, TreeSet<Long>> expected = new HashMap<>();
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

  private static Map<Integer, Map<QNm, TreeSet<Long>>> lookups(final IndexController<?, ?> controller,
      final StorageEngineReader reader, final Set<IndexDef> definitions, final List<QNm> names,
      final Map<QNm, TreeSet<Long>> expected) {
    final Map<Integer, Map<QNm, TreeSet<Long>>> result = new HashMap<>();
    for (final IndexDef definition : definitions) {
      final Map<QNm, TreeSet<Long>> postings = new HashMap<>();
      for (final QNm name : names) {
        final TreeSet<Long> keys =
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

  private static TreeSet<Long> collect(final Iterator<NodeReferences> hits) {
    final TreeSet<Long> keys = new TreeSet<>();
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
}
