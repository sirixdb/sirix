package io.sirix.index;

import com.google.gson.JsonPrimitive;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.longlong.LongIterator;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASCappedDecimalViewTest {
  private static final String TITLE = "/[]/title";
  private static final String ALIAS = "/[]/alias";
  private static final String CURSOR = "cursor sentinel";

  @TempDir
  File directory;

  private record StoredValue(String path, String value) {
  }

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> views() {
    final List<Arguments> cases = new ArrayList<>();
    for (final VersioningType versioning : VersioningType.values()) {
      for (final boolean bulk : new boolean[] {false, true}) {
        for (final String sign : new String[] {"", "-"}) {
          for (final String digits : new String[] {"1".repeat(216), "1" + "0".repeat(215)}) {
            final String prefix = sign + "0." + digits;
            cases.add(Arguments.of(versioning, bulk, Type.DEC,
                new String[] {prefix + "11", prefix + "12", prefix + "13", sign + "0.5", prefix}));
          }
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("views")
  void filtersUseExactValuesInThePostingView(final VersioningType versioning, final boolean bulk, final Type type,
      final String[] values) throws Exception {
    final var databasePath = directory.toPath().resolve("database");
    final var snapshots = new ArrayList<Map<Long, StoredValue>>();
    final var expected = new HashMap<Long, StoredValue>();
    final IndexDef definition = IndexDefs.createCASIdxDef(false, type,
        Set.of(Path.parse(TITLE, PathParser.Type.JSON), Path.parse(ALIAS, PathParser.Type.JSON)), 0,
        IndexDef.DbType.JSON);
    assertTrue(CASKeySerializer.losesInformation(AtomicUtil.toType(new Str(values[0]), type), type));
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("resource")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
          if (!bulk) {
            controller.createIndexes(Set.of(definition), trx);
          }
          final StringBuilder json = new StringBuilder().append('[');
          for (int i = 0; i < values.length; i++) {
            if (i != 0) {
              json.append(',');
            }
            json.append(object(values[i]));
          }
          for (int i = 0; i < 1100; i++) {
            json.append(",\"padding\"");
          }
          json.append(',').append(new JsonPrimitive(CURSOR)).append(']');
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          if (bulk) {
            controller.createIndexes(Set.of(definition), trx);
          }
          collectValues(trx, expected);
          assertWriterViews(trx, controller, definition, expected, values);
          trx.commit();
        }
        snapshots.add(Map.copyOf(expected));
        assertRevision(session, 1, expected, values);
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          final JsonIndexController controller = session.getWtxIndexController(trx.getRevisionNumber());
          final long updated = nodeKey(expected, TITLE, values[1]);
          assertTrue(trx.moveTo(updated));
          trx.setStringValue(values[2]);
          expected.put(updated, new StoredValue(TITLE, values[2]));
          assertWriterViews(trx, controller, definition, expected, values);
          trx.moveToDocumentRoot();
          assertTrue(trx.moveToFirstChild());
          trx.insertSubtreeAsLastChild(JsonShredder.createStringReader(object(values[1])), JsonNodeTrx.Commit.NO);
          final long insertedObject = trx.getNodeKey();
          assertTrue(trx.moveToFirstChild());
          expected.put(trx.getNodeKey(), new StoredValue(TITLE, values[1]));
          assertTrue(trx.moveToRightSibling());
          expected.put(trx.getNodeKey(), new StoredValue(ALIAS, values[1]));
          assertWriterViews(trx, controller, definition, expected, values);
          final long numericUpdated = nodeKey(expected, ALIAS, values[1]);
          assertTrue(trx.moveTo(numericUpdated));
          trx.setNumberValue(new BigDecimal(values[2]));
          expected.put(numericUpdated, new StoredValue(ALIAS, values[2]));
          assertWriterViews(trx, controller, definition, expected, values);
          final long removed = nodeKey(expected, ALIAS, values[0]);
          assertTrue(trx.moveTo(removed));
          trx.remove();
          expected.remove(removed);
          assertWriterViews(trx, controller, definition, expected, values);
          assertTrue(trx.moveTo(insertedObject));
          trx.commit();
        }
        snapshots.add(Map.copyOf(expected));
        for (int revision = 1; revision <= snapshots.size(); revision++) {
          assertRevision(session, revision, snapshots.get(revision - 1), values);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= snapshots.size(); revision++) {
        assertRevision(session, revision, snapshots.get(revision - 1), values);
      }
    }
  }

  private static String object(final String value) {
    final String literal = new JsonPrimitive(value).toString();
    return "{\"title\":" + literal + ",\"alias\":" + value + ",\"ignored\":" + literal + '}';
  }

  private static void collectValues(final JsonNodeReadOnlyTrx trx, final Map<Long, StoredValue> expected) {
    trx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      axis.nextLong();
      final QNm name = trx.getName();
      if ((trx.getKind() == NodeKind.OBJECT_NAMED_STRING || trx.getKind() == NodeKind.OBJECT_NAMED_NUMBER)
          && name != null && (name.getLocalName().equals("title") || name.getLocalName().equals("alias"))) {
        expected.put(trx.getNodeKey(),
            new StoredValue("/[]/" + name.getLocalName(), trx.getKind() == NodeKind.OBJECT_NAMED_NUMBER
                ? trx.getNumberValue().toString()
                : trx.getValue()));
      }
    }
  }

  private static long nodeKey(final Map<Long, StoredValue> expected, final String path, final String value) {
    return expected.entrySet()
                   .stream()
                   .filter(entry -> entry.getValue().path().equals(path) && entry.getValue().value().equals(value))
                   .mapToLong(Map.Entry::getKey)
                   .findFirst()
                   .orElseThrow();
  }

  private static long cursorKey(final JsonNodeReadOnlyTrx trx) {
    trx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(trx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (trx.getKind() == NodeKind.STRING_VALUE && CURSOR.equals(trx.getValue())) {
        return trx.getNodeKey();
      }
    }
    throw new AssertionError("missing sentinel");
  }

  private static void assertWriterViews(final JsonNodeTrx trx, final JsonIndexController controller,
      final IndexDef definition, final Map<Long, StoredValue> expected, final String[] values) throws Exception {
    assertQueries(trx, trx.getStorageEngineWriter(), controller, definition, expected, values);
    assertQueries(trx, trx.getStorageEngineWriter().getStorageEngineReader(), controller, definition, expected, values);
  }

  private static void assertRevision(final JsonResourceSession session, final int revision,
      final Map<Long, StoredValue> expected, final String[] values) throws Exception {
    try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      final IndexDef definition = requireNonNull(controller.getIndexes().getIndexDef(0, IndexType.CAS));
      assertQueries(trx, trx.getStorageEngineReader(), controller, definition, expected, values);
    }
  }

  private static void assertQueries(final JsonNodeReadOnlyTrx trx, final StorageEngineReader reader,
      final JsonIndexController controller, final IndexDef definition, final Map<Long, StoredValue> expected,
      final String[] values) throws Exception {
    final long cursor = cursorKey(trx);
    final StorageEngineReader cursorReader = trx.getStorageEngineReader();
    final NodeStorageEngineReader recordReader =
        (NodeStorageEngineReader) (cursorReader instanceof StorageEngineWriter writer
            ? writer.getStorageEngineReader()
            : cursorReader);
    final KeyValueLeafPage page = recordReader.getCurrentPage();
    final int guards = page == null
        ? 0
        : page.getGuardCount();
    final Type type = definition.getContentType();
    final String shortValue = "0";
    final String[] probes = {shortValue, values[0], values[1], values[2], values[3], values[4]};
    for (final Set<String> paths : List.of(Set.of(TITLE), Set.of(ALIAS), Set.of(TITLE, ALIAS))) {
      for (final String value : probes) {
        final Atomic probe = AtomicUtil.toType(new Str(value), type);
        for (final SearchMode mode : new SearchMode[] {SearchMode.EQUAL, SearchMode.GREATER, SearchMode.LOWER,
            SearchMode.GREATER_OR_EQUAL, SearchMode.LOWER_OR_EQUAL}) {
          final TreeSet<Long> wanted = new TreeSet<>();
          for (final Map.Entry<Long, StoredValue> entry : expected.entrySet()) {
            if (paths.contains(entry.getValue().path())) {
              final int order = AtomicUtil.toType(new Str(entry.getValue().value()), type).compareTo(probe);
              final boolean matches = switch (mode) {
                case EQUAL -> order == 0;
                case LOWER -> order < 0;
                case LOWER_OR_EQUAL -> order <= 0;
                case GREATER -> order > 0;
                case GREATER_OR_EQUAL -> order >= 0;
              };
              if (matches) {
                wanted.add(entry.getKey());
              }
            }
          }
          assertEquals(wanted, postings(controller.openCASIndex(reader, definition,
              controller.createCASFilter(paths, probe, mode, new JsonPCRCollector(trx)))), "comparison " + mode);
          assertCursor(trx, cursor, recordReader, page, guards);
        }
      }
      final String[][] bounds = {{values[0], values[3]}, {values[1], values[1]}, {null, values[1]}, {values[1], null},
          {shortValue, values[1]}, {values[1], shortValue}, {values[4], values[3]}, {values[4], values[4]},
          {null, values[4]}, {values[4], null}, {null, null}};
      for (final String[] bound : bounds) {
        final Atomic min = bound[0] == null
            ? null
            : AtomicUtil.toType(new Str(bound[0]), type);
        final Atomic max = bound[1] == null
            ? null
            : AtomicUtil.toType(new Str(bound[1]), type);
        for (final boolean includeMin : new boolean[] {false, true}) {
          for (final boolean includeMax : new boolean[] {false, true}) {
            final TreeSet<Long> wanted = new TreeSet<>();
            for (final Map.Entry<Long, StoredValue> entry : expected.entrySet()) {
              if (!paths.contains(entry.getValue().path())) {
                continue;
              }
              final Atomic atomic = AtomicUtil.toType(new Str(entry.getValue().value()), type);
              final int lower = min == null
                  ? 1
                  : atomic.compareTo(min);
              final int upper = max == null
                  ? -1
                  : atomic.compareTo(max);
              if ((lower > 0 || lower == 0 && includeMin) && (upper < 0 || upper == 0 && includeMax)) {
                wanted.add(entry.getKey());
              }
            }
            assertEquals(wanted, postings(controller.openCASIndex(reader, definition,
                controller.createCASFilterRange(paths, min, max, includeMin, includeMax, new JsonPCRCollector(trx)))),
                "range inclusivity " + includeMin + ", " + includeMax);
            assertCursor(trx, cursor, recordReader, page, guards);
          }
        }
      }
    }
  }

  private static void assertCursor(final JsonNodeReadOnlyTrx trx, final long cursor,
      final NodeStorageEngineReader reader, final KeyValueLeafPage page, final int guards) {
    assertEquals(cursor, trx.getNodeKey());
    assertEquals(CURSOR, trx.getValue());
    assertSame(page, reader.getCurrentPage());
    if (page != null) {
      assertEquals(guards, page.getGuardCount());
    }
  }

  private static TreeSet<Long> postings(final Iterator<NodeReferences> iterator) throws Exception {
    try (final AutoCloseable closeable = iterator instanceof AutoCloseable resource
        ? resource
        : null) {
      final TreeSet<Long> actual = new TreeSet<>();
      while (iterator.hasNext()) {
        final LongIterator nodes = iterator.next().nodeKeyIterator();
        while (nodes.hasNext()) {
          assertTrue(actual.add(nodes.next()), "duplicate posting");
        }
      }
      return actual;
    }
  }
}
