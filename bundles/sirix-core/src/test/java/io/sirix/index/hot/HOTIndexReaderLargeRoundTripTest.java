/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import com.google.gson.stream.JsonReader;
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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every key a large HOT CAS index holds must come back from a point lookup, over keys that diverge
 * deep and over keys that diverge early.
 *
 * <p>
 * The point-lookup path serializes the probe into an oversized reusable buffer and routes straight
 * from it, handing the descent and the in-leaf search the key's LENGTH rather than an exactly-sized
 * array. Past that length the buffer holds stale bytes of the previous, longer probe. Which of the
 * two consumers the length is load-bearing for was settled by mutation, and the answer is not the
 * obvious one:
 * </p>
 * <ul>
 * <li>The IN-LEAF search. Making {@code findEntry} take the array's length instead misses 15% of
 * the movies corpus here and 82% of the synthetic keys: the probe then compares as a 512-byte key
 * that sorts past every stored key sharing its prefix, the walk starts after the slot it wanted,
 * and the first entry it inspects sorts above the prefix, so it stops with nothing.</li>
 * <li>The DESCENT is not sensitive to trailing bytes at all, and both tests stay green when it is
 * reverted to the array's length. That is HOT's sparse partial keys doing what they are for: a
 * child's key asserts only the bits along its own discriminating path, and every such bit lies
 * within the length of every key in its subtree, so bits past a probe's end can never produce a
 * false subset match. The length is still passed, because the descent's short-key early-out is
 * defined in terms of it — but no test can tell the two apart.</li>
 * </ul>
 *
 * <p>
 * Two fixtures, because they fail differently. The movies corpus is low-entropy — long shared
 * prefixes, deep divergence — and is the shape the original 13% loss was measured on; the synthetic
 * keys diverge within their first few bytes. Probes run SHUFFLED, because that is what puts a
 * random other key's bytes in the buffer past the probe, which is the order a workload has.
 * </p>
 */
final class HOTIndexReaderLargeRoundTripTest {

  private static final int KEY_COUNT = 40_000;
  private static final String RESOURCE = "large-round-trip";

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

  @DisplayName("every stored key of a 40K-key index round-trips through a point lookup")
  @Test
  void everyStoredKeyRoundTrips() {
    assertEveryStoredKeyRoundTrips(buildIndex(), KEY_COUNT * 9 / 10);
  }

  private void assertEveryStoredKeyRoundTrips(final IndexDef def, final int atLeast) {
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(rtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, def.getType(), def.getID());

      // Collected FIRST, then probed in a shuffled order: the buffer's stale bytes past a probe's end
      // are the previous probe's, and a sorted order would make that a lexicographic neighbour
      // agreeing with the probe on most of the bits that matter. Shuffled is the order a workload has.
      final List<Map.Entry<CASValue, NodeReferences>> entries = new ArrayList<>();
      for (final Iterator<Map.Entry<CASValue, NodeReferences>> it = reader.iterator(); it.hasNext();) {
        final Map.Entry<CASValue, NodeReferences> entry = it.next();
        entries.add(Map.entry(entry.getKey(), entry.getValue()));
      }
      Collections.shuffle(entries, new Random(0x5EED));

      final List<CASValue> missed = new ArrayList<>();
      for (final Map.Entry<CASValue, NodeReferences> entry : entries) {
        final NodeReferences got = reader.get(entry.getKey(), SearchMode.EQUAL);
        if (got == null || !got.equals(entry.getValue())) {
          missed.add(entry.getKey());
        }
      }
      // The count is part of the contract: an iterator that quietly yields nothing would pass a
      // loop that asserts only on what it saw.
      final int total = entries.size();
      assertTrue(total >= atLeast, "the index holds only " + total + " keys; expected at least " + atLeast);
      assertEquals(0, missed.size(), () -> missed.size() + " of " + total + " stored keys did not round-trip; first: "
          + missed.subList(0, Math.min(10, missed.size())));
    }
  }

  @DisplayName("every title of the movies corpus round-trips through a point lookup")
  @Test
  void everyMovieTitleRoundTrips() {
    final IndexDef def = buildIndexFrom(JsonShredder.createFileReader(moviesCorpus().toPath()));
    assertEveryStoredKeyRoundTrips(def, 30_000);
  }

  /** The corpus, whether the test runs from the module directory or the repository root. */
  private static File moviesCorpus() {
    final File fromModule = new File("src/test/resources/json/movies.json");
    if (fromModule.exists()) {
      return fromModule;
    }
    final File fromRoot = new File("bundles/sirix-core/src/test/resources/json/movies.json");
    assertTrue(fromRoot.exists(), "movies.json corpus not found from " + new File("").getAbsolutePath());
    return fromRoot;
  }

  private IndexDef buildIndex() {
    return buildIndexFrom(JsonShredder.createStringReader(syntheticJson()));
  }

  private static String syntheticJson() {
    // Varying lengths under one shared prefix, so discriminative bits sit past the end of the short
    // keys for the long ones — the shape that makes zero padding route wrong.
    final Random random = new Random(0x5EED);
    final String alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    final StringBuilder json = new StringBuilder(KEY_COUNT * 40);
    json.append('[');
    for (int i = 0; i < KEY_COUNT; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"title\":\"t");
      final int length = 1 + random.nextInt(24);
      for (int c = 0; c < length; c++) {
        json.append(alphabet.charAt(random.nextInt(alphabet.length())));
      }
      json.append("\"}");
    }
    json.append(']');
    return json.toString();
  }

  private IndexDef buildIndexFrom(final JsonReader reader) {
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx trx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef def = IndexDefs.createCASIdxDef(false, Type.STR,
          Set.of(Path.parse("/[]/title", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
      controller.createIndexes(Set.of(def), trx);
      new JsonShredder.Builder(trx, reader, InsertPosition.AS_FIRST_CHILD).build().call();
      trx.commit();
      return def;
    }
  }
}
