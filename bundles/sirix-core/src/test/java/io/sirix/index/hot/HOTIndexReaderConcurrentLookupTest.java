/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Many threads issuing point lookups through ONE {@link HOTIndexReader} must each get exactly the
 * answer a single thread gets.
 *
 * <p>
 * The reader keeps per-lookup scratch state — a trie cursor, a chunk accumulator, a key
 * serialization buffer — and hands it out along two paths: an owner-thread-confined fast path that
 * uses plain field access, and a pooled fallback for every other thread. The contract that makes
 * the fast path safe is that no thread but the owner ever touches the confined state, and that is a
 * property nothing single-threaded can check. This test is the check: if the owner test ever
 * answers {@code true} for two threads at once, they share a cursor and an accumulator mid-walk,
 * and some lookup comes back with another key's postings or with a torn merge — which is what the
 * assertion below compares against.
 * </p>
 *
 * <p>
 * Every thread also runs the SAME shuffled probe sequence, so a thread that wrongly took the
 * confined path would be racing the owner on the same keys at the same moments, which is the
 * schedule most likely to interleave the two walks.
 * </p>
 */
final class HOTIndexReaderConcurrentLookupTest {

  private static final int THREADS = 8;
  private static final int ROUNDS = 40;
  private static final String RESOURCE = "concurrent-lookup";

  private Database<JsonResourceSession> database;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    final var dbPath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.close();
    }
    JsonTestHelper.deleteEverything();
  }

  @DisplayName("concurrent point lookups through one reader agree with the single-threaded answers")
  @Test
  void concurrentLookupsAgreeWithSingleThreadedAnswers() throws Exception {
    final IndexDef def = buildIndex();

    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(rtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, def.getType(), def.getID());

      final List<CASValue> keys = new ArrayList<>();
      final List<long[]> expected = new ArrayList<>();
      for (final Iterator<Map.Entry<CASValue, NodeReferences>> it = reader.iterator(); it.hasNext();) {
        final Map.Entry<CASValue, NodeReferences> entry = it.next();
        keys.add(entry.getKey());
        expected.add(entry.getValue().toSortedArray());
      }
      assertFalse(keys.isEmpty(), "fixture indexed nothing");
      // Single-threaded oracle through the very API the threads will use, so the comparison is
      // reader-against-reader and not reader-against-iterator.
      for (int i = 0; i < keys.size(); i++) {
        final NodeReferences single = reader.get(keys.get(i), SearchMode.EQUAL);
        assertNotNull(single, "single-threaded lookup missed " + keys.get(i));
        assertEquals(toList(expected.get(i)), toList(single.toSortedArray()), "oracle disagrees for " + keys.get(i));
      }

      final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
      try {
        final CountDownLatch start = new CountDownLatch(1);
        final List<Future<Integer>> futures = new ArrayList<>(THREADS);
        for (int t = 0; t < THREADS; t++) {
          futures.add(pool.submit(() -> {
            start.await();
            int checked = 0;
            for (int round = 0; round < ROUNDS; round++) {
              for (int i = 0; i < keys.size(); i++) {
                final NodeReferences got = reader.get(keys.get(i), SearchMode.EQUAL);
                if (got == null) {
                  throw new AssertionError("concurrent lookup missed " + keys.get(i));
                }
                final List<Long> actual = toList(got.toSortedArray());
                if (!actual.equals(toList(expected.get(i)))) {
                  throw new AssertionError("concurrent lookup of " + keys.get(i) + " returned " + actual
                      + " instead of " + toList(expected.get(i)));
                }
                checked++;
              }
            }
            return checked;
          }));
        }
        start.countDown();
        int total = 0;
        for (final Future<Integer> f : futures) {
          total += f.get(120, TimeUnit.SECONDS);
        }
        assertEquals(THREADS * ROUNDS * keys.size(), total, "every thread must have checked every key every round");
      } finally {
        pool.shutdownNow();
      }
    }
  }

  private IndexDef buildIndex() {
    // Distinct titles plus repeats, so posting lists of size one AND several are both exercised.
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < 300; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"title\":\"t").append(i % 200).append("\"}");
    }
    json.append(']');
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef def = IndexDefs.createCASIdxDef(false, Type.STR,
          Set.of(Path.parse("/[]/title", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
      controller.createIndexes(Set.of(def), trx);
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json.toString()),
          InsertPosition.AS_FIRST_CHILD).build().call();
      trx.commit();
      return def;
    }
  }

  private static List<Long> toList(final long[] keys) {
    final List<Long> out = new ArrayList<>(keys.length);
    for (final long k : keys) {
      out.add(k);
    }
    return out;
  }
}
