package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.name.NameFilter;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.roaringbitmap.longlong.LongIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every XML index builder must cover the same nodes as its incremental listener. */
final class XmlIndexBulkBuildParityTest {
  private static final QNm ROOT = new QNm("root");
  private static final QNm PLAIN = new QNm("flag");
  private static final QNm QUALIFIED = new QNm("urn:a", "a", "flag");
  private static final QNm OTHER = new QNm("urn:b", "b", "flag");
  private static final QNm NAMESPACE = new QNm("urn:a", "a", "");
  private static final QNm TARGET = new QNm("target");
  private static final List<QNm> NAMES =
      List.of(ROOT, PLAIN, QUALIFIED, OTHER, TARGET, new QNm("urn:a", "alias", "flag"), new QNm("missing"));

  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nameBuildMatchesIncremental(final VersioningType versioning) {
    verify(versioning, IndexType.NAME);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void pathBuildMatchesIncremental(final VersioningType versioning) {
    verify(versioning, IndexType.PATH);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void casBuildMatchesIncremental(final VersioningType versioning) {
    verify(versioning, IndexType.CAS);
  }

  private void verify(final VersioningType versioning, final IndexType type) {
    verify(versioning, type.toString(), definitions(type));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void combinedBuildersAndMutationsMatchIncremental(final VersioningType versioning) {
    final Set<IndexDef> definitions = new HashSet<>();
    for (final IndexType type : List.of(IndexType.NAME, IndexType.PATH, IndexType.CAS)) {
      definitions.addAll(definitions(type));
    }
    verify(versioning, "combined", definitions);
  }

  private static Set<IndexDef> definitions(final IndexType type) {
    return switch (type) {
      case NAME -> Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.XML),
          IndexDefs.createSelectiveNameIdxDef(Set.of(PLAIN, QUALIFIED, TARGET), 1, IndexDef.DbType.XML),
          IndexDefs.createFilteredNameIdxDef(Set.of(QUALIFIED), 2, IndexDef.DbType.XML));
      case PATH -> Set.of(IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.XML),
          IndexDefs.createPathIdxDef(Set.of(attributePath(PLAIN), attributePath(QUALIFIED)), 1, IndexDef.DbType.XML));
      case CAS ->
        Set.of(IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.XML), IndexDefs.createCASIdxDef(
            false, Type.STR, Set.of(attributePath(PLAIN), attributePath(QUALIFIED)), 1, IndexDef.DbType.XML));
      default -> throw new AssertionError(type);
    };
  }

  private void verify(final VersioningType versioning, final String fixture, final Set<IndexDef> definitions) {
    final Map<String, Set<Long>> uncommitted = build(versioning, fixture, BuildMode.UNCOMMITTED, definitions);
    final Map<String, Set<Long>> incremental = build(versioning, fixture, BuildMode.INCREMENTAL, definitions);
    assertEquals(incremental, uncommitted, "uncommitted bulk/incremental parity");
    assertEquals(incremental, build(versioning, fixture, BuildMode.COMMITTED, definitions),
        "committed bulk/incremental parity");
  }

  private Map<String, Set<Long>> build(final VersioningType versioning, final String fixture, final BuildMode mode,
      final Set<IndexDef> definitions) {
    final var path = directory.toPath().resolve(fixture + "-" + mode);
    final List<Entry> entries = new ArrayList<>();
    final Map<String, Set<Long>> live;
    final Map<String, Set<Long>> mutated;
    final List<Entry> original;
    final int indexRevision;
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
          populate(trx, entries);
          if (mode == BuildMode.COMMITTED) {
            trx.commit();
          }
          trx.moveToDocumentRoot();
          final var controller = session.getWtxIndexController(trx.getRevisionNumber());
          if (mode != BuildMode.INCREMENTAL) {
            controller.createIndexes(definitions, trx);
          }
          assertEquals(0, trx.getNodeKey(), "build restores cursor");
          live = lookups(trx, controller, definitions, entries);
          original = List.copyOf(entries);
          indexRevision = trx.getRevisionNumber();
          trx.commit();
          mutate(trx, session, definitions, entries);
          mutated = lookups(trx, session.getWtxIndexController(trx.getRevisionNumber()), definitions, entries);
          trx.commit();
        }
      }
      Databases.clearGlobalCaches();
      try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(path);
          final XmlResourceSession session = database.beginResourceSession("data");
          final XmlNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(mutated,
            lookups(reader, session.getRtxIndexController(reader.getRevisionNumber()), definitions, entries),
            "mutated postings survive reopen");
        try (final XmlNodeReadOnlyTrx historical = session.beginNodeReadOnlyTrx(indexRevision)) {
          assertEquals(live, lookups(historical, session.getRtxIndexController(indexRevision), definitions, original),
              "original postings survive mutations and reopen");
        }
      }
      return live;
    } finally {
      Databases.removeDatabase(path);
    }
  }

  private static void populate(final XmlNodeTrx trx, final List<Entry> entries) {
    trx.insertElementAsFirstChild(ROOT);
    final long rootKey = trx.getNodeKey();
    add(trx, ROOT, "", new Path<QNm>().child(ROOT), entries);
    trx.insertNamespace(NAMESPACE);
    add(trx, NAMESPACE, "", new Path<QNm>().child(ROOT).child(NAMESPACE), entries);
    for (final QNm name : List.of(PLAIN, QUALIFIED, OTHER)) {
      trx.moveTo(rootKey);
      trx.insertAttribute(name, "shared");
      add(trx, name, "shared", attributePath(name), entries);
    }
    trx.moveTo(rootKey);
    trx.insertElementAsFirstChild(QUALIFIED);
    final long childKey = trx.getNodeKey();
    add(trx, QUALIFIED, "", new Path<QNm>().child(ROOT).child(QUALIFIED), entries);
    trx.insertTextAsFirstChild("shared");
    add(trx, new QNm(""), "shared", new Path<QNm>().child(ROOT).child(QUALIFIED), entries);
    trx.moveTo(childKey);
    trx.insertPIAsRightSibling("target", "shared");
    add(trx, TARGET, "shared", new Path<QNm>().child(ROOT).child(TARGET), entries);
    trx.moveTo(rootKey);
    trx.insertPIAsFirstChild("target", "shared");
    add(trx, TARGET, "shared", new Path<QNm>().child(ROOT).child(TARGET), entries);
    trx.moveTo(rootKey);
    trx.insertCommentAsFirstChild("shared");
    add(trx, new QNm(""), "shared", new Path<QNm>().child(ROOT), entries);
    trx.moveToDocumentRoot();
    trx.insertCommentAsFirstChild("shared");
    add(trx, new QNm(""), "shared", new Path<>(), entries);
    trx.insertPIAsRightSibling("target", "shared");
    add(trx, TARGET, "shared", new Path<QNm>().child(TARGET), entries);
  }

  private static void add(final XmlNodeTrx trx, final QNm name, final String value, final Path<QNm> path,
      final List<Entry> entries) {
    entries.add(new Entry(trx.getNodeKey(), trx.getKind(), name, value, path));
  }

  private static Path<QNm> attributePath(final QNm name) {
    return new Path<QNm>().child(ROOT).attribute(name);
  }

  private static Map<String, Set<Long>> lookups(final XmlNodeReadOnlyTrx trx, final IndexController<?, ?> controller,
      final Set<IndexDef> definitions, final List<Entry> entries) {
    final Map<String, Set<Long>> results = new HashMap<>();
    final List<NameFilter> nameFilters = new ArrayList<>();
    nameFilters.add(new NameFilter(Set.of(), Set.of()));
    for (final QNm name : NAMES) {
      nameFilters.add(new NameFilter(Set.of(name), Set.of()));
      nameFilters.add(new NameFilter(Set.of(), Set.of(name)));
    }
    nameFilters.add(new NameFilter(Set.of(PLAIN, QUALIFIED, OTHER), Set.of(QUALIFIED)));
    for (final IndexDef definition : definitions) {
      if (definition.getType() == IndexType.NAME) {
        nameLookups(trx, controller, definition, entries, nameFilters, results);
      } else {
        pathLookups(trx, controller, definition, entries, results);
      }
    }
    return results;
  }

  private static void nameLookups(final XmlNodeReadOnlyTrx trx, final IndexController<?, ?> controller,
      final IndexDef definition, final List<Entry> entries, final List<NameFilter> nameFilters,
      final Map<String, Set<Long>> results) {
    for (int i = 0; i < nameFilters.size(); i++) {
      final NameFilter filter = nameFilters.get(i);
      final Set<Long> expected = new TreeSet<>();
      for (final Entry entry : entries) {
        if (isNamed(entry.kind()) && entry.kind() != NodeKind.NAMESPACE
            && matches(entry.name(), definition.getIncluded(), definition.getExcluded())
            && matches(entry.name(), filter.getIncludes(), filter.getExcludes())) {
          expected.add(entry.key());
        }
      }
      check(results, definition.getType() + ":" + definition.getID() + ":name:" + i, expected,
          controller.openNameIndex(trx.getStorageEngineReader(), definition, filter));
    }
  }

  private static void pathLookups(final XmlNodeReadOnlyTrx trx, final IndexController<?, ?> controller,
      final IndexDef definition, final List<Entry> entries, final Map<String, Set<Long>> results) {
    final List<Set<Path<QNm>>> probes =
        List.of(Set.of(), Set.of(attributePath(PLAIN)), Set.of(attributePath(QUALIFIED)), Set.of(attributePath(OTHER)),
            Set.of(attributePath(PLAIN), attributePath(QUALIFIED)), Set.of(attributePath(new QNm("missing"))));
    for (int i = 0; i < probes.size(); i++) {
      final Set<Path<QNm>> paths = probes.get(i);
      final Set<Long> expected = expectedPathPostings(definition, paths, entries);
      final XmlPCRCollector collector = new XmlPCRCollector(trx);
      final String probe = definition.getType() + ":" + definition.getID() + ":path:" + i;
      if (definition.getType() == IndexType.PATH) {
        check(results, probe, expected,
            controller.openPathIndex(trx.getStorageEngineReader(), definition, new PathFilter(paths, collector)));
      } else {
        check(results, probe + ":range", expected, controller.openCASIndex(trx.getStorageEngineReader(), definition,
            new CASFilterRange(paths, new Str("a"), new Str("z"), true, true, collector)));
        for (final String value : List.of("", "shared", "changed", "missing")) {
          final Set<Long> wanted = new TreeSet<>();
          for (final Entry entry : entries) {
            if (expected.contains(entry.key()) && (value.isEmpty() || value.equals(entry.value()))) {
              wanted.add(entry.key());
            }
          }
          check(results, probe + ":" + value, wanted, casPostings(trx, controller, definition, paths, value.isEmpty()
              ? null
              : new Str(value), collector));
        }
      }
    }
  }

  private static Set<Long> expectedPathPostings(final IndexDef definition, final Set<Path<QNm>> paths,
      final List<Entry> entries) {
    final Set<Long> expected = new TreeSet<>();
    for (final Entry entry : entries) {
      final boolean covered = definition.getType() == IndexType.PATH
          ? isNamed(entry.kind())
          : isValue(entry.kind());
      if (covered && (definition.getPaths().isEmpty() || definition.getPaths().contains(entry.path()))
          && (paths.isEmpty() || paths.contains(entry.path()))) {
        expected.add(entry.key());
      }
    }
    return expected;
  }

  @SuppressWarnings("NullAway")
  private static Iterator<NodeReferences> casPostings(final XmlNodeReadOnlyTrx trx,
      final IndexController<?, ?> controller, final IndexDef definition, final Set<Path<QNm>> paths,
      final @Nullable Atomic value, final XmlPCRCollector collector) {
    // A null CAS key is the public full-value scan contract.
    return controller.openCASIndex(trx.getStorageEngineReader(), definition,
        new CASFilter(paths, value, SearchMode.EQUAL, collector));
  }

  private static void mutate(final XmlNodeTrx trx, final XmlResourceSession session, final Set<IndexDef> definitions,
      final List<Entry> entries) {
    for (int i = 0; i < entries.size(); i++) {
      final Entry entry = entries.get(i);
      if (isValue(entry.kind())) {
        assertTrue(trx.moveTo(entry.key()));
        trx.setValue("changed");
        entries.set(i, new Entry(entry.key(), entry.kind(), entry.name(), "changed", entry.path()));
        lookups(trx, session.getWtxIndexController(trx.getRevisionNumber()), definitions, entries);
      }
    }
    for (int i = entries.size() - 1; i > 0; i--) {
      final Entry entry = entries.get(i);
      if (entry.kind() != NodeKind.ELEMENT) {
        assertTrue(trx.moveTo(entry.key()));
        trx.remove();
        entries.remove(i);
        lookups(trx, session.getWtxIndexController(trx.getRevisionNumber()), definitions, entries);
      }
    }
    assertTrue(trx.moveTo(entries.getFirst().key()));
    trx.remove();
    entries.clear();
    lookups(trx, session.getWtxIndexController(trx.getRevisionNumber()), definitions, entries);
  }

  private static boolean matches(final QNm name, final Set<QNm> includes, final Set<QNm> excludes) {
    return (includes.isEmpty() || includes.contains(name)) && !excludes.contains(name);
  }

  private static boolean isNamed(final NodeKind kind) {
    return kind == NodeKind.ELEMENT || kind == NodeKind.ATTRIBUTE || kind == NodeKind.NAMESPACE
        || kind == NodeKind.PROCESSING_INSTRUCTION;
  }

  private static boolean isValue(final NodeKind kind) {
    return kind == NodeKind.ATTRIBUTE || kind == NodeKind.TEXT || kind == NodeKind.COMMENT
        || kind == NodeKind.PROCESSING_INSTRUCTION;
  }

  private static void check(final Map<String, Set<Long>> results, final String probe, final Set<Long> expected,
      final Iterator<NodeReferences> postings) {
    final Set<Long> actual = new TreeSet<>();
    while (postings.hasNext()) {
      final LongIterator keys = postings.next().getNodeKeys().getLongIterator();
      while (keys.hasNext()) {
        assertTrue(actual.add(keys.next()), "duplicate posting for " + probe);
      }
    }
    assertEquals(expected, actual, probe);
    results.put(probe, actual);
  }

  private record Entry(long key, NodeKind kind, QNm name, String value, Path<QNm> path) {
  }

  private enum BuildMode {
    INCREMENTAL, UNCOMMITTED, COMMITTED
  }
}
