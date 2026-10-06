package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIdentityEpochOracle;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Query index contents against independent document walks of every generated source revision. */
final class JsonIdentityIndexOracle {
  private JsonIdentityIndexOracle() {}

  private static IndexDef projection() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/Aa", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
  }

  static void declare(final JsonNodeTrx writer) {
    final var controller =
        (JsonIndexController) writer.getResourceSession().getWtxIndexController(writer.getRevisionNumber());
    controller.createIndexes(Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON),
        IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.JSON)), writer);
    controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
  }

  static void clearCaches() {
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
  }

  static void assertIndexes(final JsonNodeReadOnlyTrx original, final JsonNodeReadOnlyTrx copied) {
    final Map<String, LongSet> names = new HashMap<>();
    final Map<String, LongSet> values = new HashMap<>();
    final LongSet paths = new LongOpenHashSet();
    final Map<String, LongSet> pathMemberships = new HashMap<>();
    final Map<String, LongSet> casMemberships = new HashMap<>();
    final LongSet valueKeys = new LongOpenHashSet();
    try (final var summary = original.getResourceSession().openPathSummary(original.getRevisionNumber())) {
      original.moveToDocumentRoot();
      final var axis = new DescendantAxis(original);
      while (axis.hasNext()) {
        final long key = axis.nextLong();
        final NodeKind kind = original.getKind();
        if (kind.playsObjectKeyRole()) {
          names.computeIfAbsent(original.getName().getLocalName(), ignored -> new LongOpenHashSet()).add(key);
        }
        if (kind.playsObjectKeyRole() || kind == NodeKind.ARRAY) {
          paths.add(key);
          addPathMembership(summary, original.getPathNodeKey(), key, pathMemberships);
          if (kind == NodeKind.OBJECT_NAMED_ARRAY) {
            assertTrue(summary.moveTo(original.getPathNodeKey()));
            addPathMembership(summary, summary.getParentKey(), key, pathMemberships);
          }
        }
        final String value = switch (kind) {
          case STRING_VALUE, OBJECT_NAMED_STRING -> original.getValue();
          case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> original.getNumberValue().toString();
          case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> Boolean.toString(original.getBooleanValue());
          default -> null;
        };
        if (value != null) {
          values.computeIfAbsent(value, ignored -> new LongOpenHashSet()).add(key);
          valueKeys.add(key);
          final long pathKey;
          if (kind.playsObjectKeyRole()) {
            pathKey = original.getPathNodeKey();
          } else {
            assertTrue(original.moveToParent());
            pathKey = Math.max(0, original.getPathNodeKey());
            assertTrue(original.moveTo(key));
          }
          if (pathKey > 0) {
            addPathMembership(summary, pathKey, key, casMemberships);
          }
        }
      }
    }
    final var controller = copied.getResourceSession().getRtxIndexController(copied.getRevisionNumber());
    final var storage = copied.getStorageEngineReader();
    final var nameDef =
        controller.getIndexes()
                  .getIndexDef(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID(), IndexType.NAME);
    final LongSet namedKeys = new LongOpenHashSet();
    for (final var entry : names.entrySet()) {
      namedKeys.addAll(entry.getValue());
      assertEquals(entry.getValue(),
          collect(controller.openNameIndex(storage, nameDef, controller.createNameFilter(Set.of(entry.getKey())))),
          "NAME " + entry.getKey());
    }
    assertEquals(namedKeys, collect(controller.openNameIndex(storage, nameDef, controller.createNameFilter(Set.of()))),
        "all NAME memberships");
    assertEquals(paths, collect(controller.openPathIndex(storage,
        controller.getIndexes().getIndexDef(0, IndexType.PATH), controller.createPathFilter(Set.of(), copied))),
        "all PATH memberships");
    for (final var entry : pathMemberships.entrySet()) {
      assertEquals(entry.getValue(),
          collect(controller.openPathIndex(storage, controller.getIndexes().getIndexDef(0, IndexType.PATH),
              controller.createPathFilter(Set.of(entry.getKey()), copied))),
          "PATH " + entry.getKey());
    }
    final var casDef = controller.getIndexes().getIndexDef(0, IndexType.CAS);
    for (final var entry : casMemberships.entrySet()) {
      assertEquals(
          entry.getValue(), collect(controller
                                              .openCASIndex(storage, casDef,
                                                  controller.createCASFilter(Set.of(entry.getKey()), null,
                                                      SearchMode.EQUAL, new JsonPCRCollector(copied)))),
          "CAS path " + entry.getKey());
    }
    for (final var entry : values.entrySet()) {
      assertEquals(
          entry.getValue(), collect(controller
                                              .openCASIndex(storage, casDef,
                                                  controller.createCASFilter(Set.of(), new Str(entry.getKey()),
                                                      SearchMode.EQUAL, new JsonPCRCollector(copied)))),
          "CAS " + entry.getKey());
    }
    assertEquals(valueKeys,
        collect(controller.openCASIndex(storage, casDef,
            controller.createCASFilter(Set.of(), null, SearchMode.EQUAL, new JsonPCRCollector(copied)))),
        "all CAS memberships");
    assertProjection(original, copied);
  }

  private static void addPathMembership(final PathSummaryReader summary, final long pathKey, final long nodeKey,
      final Map<String, LongSet> memberships) {
    assertTrue(summary.moveTo(pathKey), "source path " + pathKey);
    final var path = summary.getPath();
    assertNotNull(path);
    memberships.computeIfAbsent(path.toString(), ignored -> new LongOpenHashSet()).add(nodeKey);
  }

  private static void assertProjection(final JsonNodeReadOnlyTrx original, final JsonNodeReadOnlyTrx copied) {
    final LongArrayList expected = new LongArrayList();
    original.moveToDocumentRoot();
    if (original.moveToFirstChild() && original.getKind() == NodeKind.ARRAY && original.moveToFirstChild()) {
      do {
        expected.add(original.getNodeKey());
      } while (original.moveToRightSibling());
    }
    final var session = copied.getResourceSession();
    final int revision = copied.getRevisionNumber();
    final var handle = ProjectionIndexCatalog.load(session, revision, projection());
    assertNotNull(handle);
    final LongArrayList actual = new LongArrayList();
    byte[] previous = null;
    for (final byte[] payload : handle.rowGroupPayloads(
        ProjectionIndexCatalog.rowGroupMaterializer(session, revision, 0, handle.rowGroupCount()))) {
      final var page = ProjectionIndexRowGroupPage.deserialize(payload);
      boolean unrepresentable = false;
      for (int row = 0; row < page.getRowCount(); row++) {
        final long key = page.recordKeys()[row];
        actual.add(key);
        assertTrue(original.moveTo(key));
        int matches = 0;
        boolean numeric = false;
        long number = 0;
        if (original.getKind() == NodeKind.OBJECT && original.moveToFirstChild()) {
          do {
            if (original.getName().getLocalName().equals("Aa")) {
              matches++;
              numeric = original.getKind() == NodeKind.OBJECT_NAMED_NUMBER;
              if (numeric) {
                number = original.getNumberValue().longValue();
              }
            }
          } while (original.moveToRightSibling());
        }
        assertEquals(matches != 0, (page.presenceColumnBits(0)[row >>> 6] & (1L << row)) != 0,
            "projection presence for record " + key);
        if (matches == 1 && numeric) {
          assertEquals(number, page.numericColumn(0)[row], "projection scalar for record " + key);
        }
        unrepresentable |= matches > 1 || (matches == 1 && !numeric);
        previous = ProjectionIdentityEpochOracle.assertOrder(page, row, previous);
      }
      // Column safety flags are sticky for a persisted leaf. Conservative poisoning is valid;
      // certifying a column that contains an unrepresentable cell is not.
      assertTrue(!unrepresentable || page.columnUnrepresentable(0), "projection type proof");
    }
    assertEquals(expected, actual, "generated projection identities in document order");
  }

  private static LongSet collect(final Iterator<NodeReferences> references) {
    final LongSet keys = new LongOpenHashSet();
    while (references.hasNext()) {
      final var iterator = references.next().getNodeKeys().getLongIterator();
      while (iterator.hasNext()) {
        keys.add(iterator.next());
      }
    }
    return keys;
  }
}
