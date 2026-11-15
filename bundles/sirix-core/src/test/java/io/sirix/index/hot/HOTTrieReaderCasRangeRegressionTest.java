/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.PageReference;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression for issue 1427: a CAS index range whose lower bound is absent from the
 * index silently dropped rows.
 *
 * <p>
 * 2,000 objects carry the titles {@code b0000} to {@code b1999} under a CAS {@code xs:string}
 * index. Every bound that starts with {@code a} sorts below all of them, so {@code title >= bound}
 * must return all 2,000 titles. The bound's low bytes route it into the middle of the title range,
 * where it sorts below the leaf it lands in while earlier leaves hold titles that are in range too;
 * before the fix {@code title >= "a9999"} returned 200 titles starting at {@code b1800}.
 * </p>
 */
final class HOTTrieReaderCasRangeRegressionTest {

  private static final String RESOURCE = "cas-range";
  private static final String PATH = "/[]/title";
  private static final int TITLES = 2_000;

  @TempDir
  File temporaryDirectory;

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void rangesWithAnAbsentLowerBoundReturnEveryTitleInRange() throws Exception {
    final List<String> titles = new ArrayList<>(TITLES);
    final StringBuilder json = new StringBuilder(TITLES * 20).append('[');
    for (int i = 0; i < TITLES; i++) {
      final String title = "b" + String.format("%04d", i);
      titles.add(title);
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"title\":\"").append(title).append("\"}");
    }
    json.append(']');

    final var databasePath = temporaryDirectory.toPath().resolve("cas-range-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      final IndexDef casDef;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        casDef = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(parse(PATH, PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON);
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(casDef), wtx);
        new JsonShredder.Builder(wtx, JsonShredder.createStringReader(json.toString()),
            InsertPosition.AS_FIRST_CHILD).build().call();
        wtx.commit();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final HOTInvariantValidator.Result validation =
            HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.CAS, casDef.getID());
        validation.assertOk();
        assertTrue(validation.observedHeight() >= 1, "the titles must span several leaves");

        final List<String> bounds = new ArrayList<>(List.of("a", "a0000", "a0999", "a1957", "a5555", "a9999", "b",
            "b05", "b0500", "b0500x", "b1999", "b1999z", "c", "`9999", "a9", "a99", "a999"));
        final Random random = new Random(42);
        for (int i = 0; i < 60; i++) {
          bounds.add("a" + String.format("%04d", random.nextInt(10_000)));
        }

        // The reported case first, by name.
        assertEquals(titles, titlesFrom(session, rtx, casDef, "a9999", true, null),
            "title >= \"a9999\" must return all " + TITLES + " titles");

        final IndexController<?, ?> indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        for (final String bound : bounds) {
          final List<String> atOrAbove = new ArrayList<>();
          final List<String> above = new ArrayList<>();
          for (final String title : titles) {
            if (title.compareTo(bound) >= 0) {
              atOrAbove.add(title);
            }
            if (title.compareTo(bound) > 0) {
              above.add(title);
            }
          }
          assertEquals(atOrAbove, titlesFrom(session, rtx, casDef, bound, true, null),
              "range filter [\"" + bound + "\", +inf)");
          assertEquals(above, titlesFrom(session, rtx, casDef, bound, false, null),
              "range filter (\"" + bound + "\", +inf)");
          assertEquals(atOrAbove, titlesFrom(session, rtx, casDef, bound, true, "b1999"),
              "range filter [\"" + bound + "\", \"b1999\"]");
          assertEquals(atOrAbove,
              titlesOf(rtx,
                  indexController.openCASIndex(rtx.getStorageEngineReader(), casDef, indexController.createCASFilter(
                      Set.of(PATH), new Str(bound), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(rtx)))),
              "filter title >= \"" + bound + "\"");
          assertEquals(above,
              titlesOf(rtx,
                  indexController.openCASIndex(rtx.getStorageEngineReader(), casDef, indexController.createCASFilter(
                      Set.of(PATH), new Str(bound), SearchMode.GREATER, new JsonPCRCollector(rtx)))),
              "filter title > \"" + bound + "\"");
        }

        // The fixture must keep reaching the defect: a bound routed into a leaf it sorts below while
        // an earlier leaf holds its true lower bound. A writer change that moves the leaf boundaries
        // would otherwise leave this test green without testing anything.
        final int reached = boundsRoutedBelowALaterLeaf(rtx, casDef, bounds);
        assertTrue(reached >= 10, "only " + reached + " of " + bounds.size()
            + " bounds are routed into a leaf they sort below while an earlier leaf holds their lower bound");
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static List<String> titlesFrom(final JsonResourceSession session, final JsonNodeReadOnlyTrx rtx,
      final IndexDef casDef, final String min, final boolean includeMin, final String max) {
    final IndexController<?, ?> indexController = session.getRtxIndexController(rtx.getRevisionNumber());
    return titlesOf(rtx,
        indexController.openCASIndex(rtx.getStorageEngineReader(), casDef,
            indexController.createCASFilterRange(Set.of(PATH), new Str(min), max == null
                ? null
                : new Str(max), includeMin, true, new JsonPCRCollector(rtx))));
  }

  private static List<String> titlesOf(final JsonNodeReadOnlyTrx rtx, final Iterator<NodeReferences> hits) {
    final List<String> titles = new ArrayList<>();
    while (hits.hasNext()) {
      hits.next().forEachNodeKey(nodeKey -> {
        assertTrue(rtx.moveTo(nodeKey), "index posting " + nodeKey + " must resolve to a node");
        titles.add(rtx.getValue());
      });
    }
    Collections.sort(titles);
    return titles;
  }

  /**
   * How many bounds have the geometry of the defect in the physical trie: the seek key the CAS index
   * derives from the bound is routed to a leaf whose first key is greater than both the seek key and
   * the first stored key that is {@code >=} it.
   */
  private static int boundsRoutedBelowALaterLeaf(final JsonNodeReadOnlyTrx rtx, final IndexDef casDef,
      final List<String> bounds) throws Exception {
    final HOTIndexReader<CASValue> index = HOTIndexReader.create(rtx.getStorageEngineReader(),
        CASKeySerializer.INSTANCE, casDef.getType(), casDef.getID());
    final PageReference root = index.getRootReference();
    assertNotNull(root, "the CAS index must have a root");
    final Iterator<Map.Entry<CASValue, NodeReferences>> stored = index.iterator();
    final CASValue firstStored;
    try {
      firstStored = stored.next().getKey();
    } finally {
      if (stored instanceof AutoCloseable closeable) {
        closeable.close();
      }
    }
    final long pathNodeKey = firstStored.getPathNodeKey();
    final byte[] firstStoredKey = seekKey(firstStored);

    int reached = 0;
    try (HOTTrieReader trie = new HOTTrieReader(rtx.getStorageEngineReader())) {
      for (final String bound : bounds) {
        final byte[] seekKey = seekKey(new CASValue(new Str(bound), Type.STR, pathNodeKey));
        if (Arrays.compareUnsigned(seekKey, firstStoredKey) > 0) {
          continue; // not below every title: its lower bound is not the first stored key
        }
        final byte[] routedFirstKey = firstKeyOfRoutedLeaf(trie, root, seekKey);
        if (Arrays.compareUnsigned(firstStoredKey, routedFirstKey) < 0) {
          reached++;
        }
      }
    }
    return reached;
  }

  /** The physical key a CAS range starts its seek at: the serialized bound and chunk index 0. */
  private static byte[] seekKey(final CASValue value) {
    final byte[] scratch = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(value)];
    final int length = CASKeySerializer.INSTANCE.serialize(value, scratch, 0);
    return Arrays.copyOf(scratch, length + HOTKeySerializer.CHUNK_IDX_BYTES);
  }

  private static byte[] firstKeyOfRoutedLeaf(final HOTTrieReader trie, final PageReference root, final byte[] seekKey) {
    for (int attempt = 0; attempt < HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
      final HOTLeafPage routed = trie.navigateToLeaf(root, seekKey);
      final byte[] firstKey;
      try {
        firstKey = routed.getKey(0);
      } catch (final RuntimeException torn) {
        if (trie.validateCurrentLeaf()) {
          throw torn;
        }
        continue;
      }
      if (trie.validateCurrentLeaf()) {
        return firstKey;
      }
    }
    throw new AssertionError("the routed leaf never stabilised");
  }
}
