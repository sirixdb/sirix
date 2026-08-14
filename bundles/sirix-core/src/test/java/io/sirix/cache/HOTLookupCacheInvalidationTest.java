/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.brackit.query.jdm.Type;
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
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end cover for the invalidation the HOT point-lookup cache depends on.
 *
 * <p>
 * {@link HOTLookupCacheTest} proves the cache drops the right entries when told to. This proves it
 * is actually TOLD to — that a real point lookup through a real index populates it, and that the
 * sweeps {@code truncateTo}, crash recovery and {@code removeResource} call actually reach it.
 * Without this the wiring is verified only by reading it, which is exactly how the cache shipped
 * with no invalidation at all: the premise "a committed revision is immutable" is true of ordinary
 * commits and false of rollback, which re-issues revision numbers over different content.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTLookupCacheInvalidationTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE_NAME = "hotLookupCacheResource";
  private static final String OTHER_RESOURCE_NAME = "hotLookupCacheSibling";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    // Create the database FIRST. Databases.getGlobalBufferManager() force-initialises the JVM-global
    // buffer manager from a 2 GB fallback budget when nothing has opened a database yet, and
    // initializeGlobalBufferManager then no-ops for the rest of the process — so touching it first
    // would pin every later test class in this JVM to caches a quarter of their configured size,
    // purely because io.sirix.cache.* sorts early. Creating the database applies the real budget.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    // SKIP, do not fail, when the operator disabled the cache. Every test here asserts on occupancy
    // in BOTH directions, and a disabled cache breaks them in both: the "a lookup populated it"
    // assertions fail outright, and — worse — the "the sweep emptied it" assertions then pass
    // VACUOUSLY against a table that can never hold anything, so a completely broken
    // invalidateResource would still be green. sirix.hotLookupCache.maxEntries=0 is a documented
    // setting that HotInMemoryReadBenchmark's instructions actively use, and a disabled cache is not
    // a broken one; an assumption is the only outcome that is honest in that configuration. Matches
    // the guard the sibling HOTIndexMemoizationJsoniqTest already applies per assertion.
    assumeTrue(Databases.getGlobalBufferManager().getHOTLookupCache().isEnabled(), "the HOT lookup cache is disabled ("
        + BufferManagerImpl.HOT_LOOKUP_CACHE_ENTRIES_PROPERTY + "=0), so there is nothing to invalidate");
    Databases.getGlobalBufferManager().getHOTLookupCache().clear();
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * Shred one JSON document into {@code resourceName} and build a CAS index over {@code /[]/title}.
   */
  private static IndexDef buildIndexedResource(final Database<JsonResourceSession> database, final String resourceName,
      final String json) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName).build());
    try (final JsonResourceSession manager = database.beginResourceSession(resourceName);
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final IndexDef casDef = IndexDefs.createCASIdxDef(false, Type.STR,
          Collections.singleton(parse("/[]/title", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      manager.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(casDef), trx);
      trx.commit();
      return casDef;
    }
  }

  private static HOTIndexReader<CASValue> readerFor(final JsonNodeReadOnlyTrx rtx, final IndexDef casDef) {
    return HOTIndexReader.create(rtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, casDef.getType(),
        casDef.getID());
  }

  /**
   * One point lookup through the real reader, which is what populates the cache.
   *
   * <p>
   * The probe key is taken from the index's own iterator rather than constructed: a {@link CASValue}
   * carries the PATH-CLASS RECORD of the indexed node, not the index number, and a hand-built key
   * with the wrong PCR serializes to different bytes and silently finds nothing.
   * </p>
   */
  private static @Nullable NodeReferences lookupFirstKey(final JsonNodeReadOnlyTrx rtx, final IndexDef casDef) {
    final HOTIndexReader<CASValue> reader = readerFor(rtx, casDef);
    final CASValue key = firstKey(reader);
    return reader.get(key, SearchMode.EQUAL);
  }

  /**
   * Point-look up EVERY key of the index, memoizing one entry per key.
   *
   * <p>
   * Lets a test give two resources DIFFERENT cache footprints, which is what makes a scoped-sweep
   * assertion able to tell "swept the target" from "swept the sibling" — with one entry each, both
   * outcomes leave the same occupancy.
   * </p>
   *
   * @return the number of distinct keys looked up
   */
  private static int lookupAllKeys(final JsonNodeReadOnlyTrx rtx, final IndexDef casDef) {
    final HOTIndexReader<CASValue> reader = readerFor(rtx, casDef);
    final List<CASValue> keys = new ArrayList<>();
    for (final Iterator<Map.Entry<CASValue, NodeReferences>> it = cast(reader.iterator()); it.hasNext();) {
      keys.add(it.next().getKey());
    }
    assertFalse(keys.isEmpty(), "the index is empty, so this test would prove nothing");
    for (final CASValue key : keys) {
      assertNotNull(reader.get(key, SearchMode.EQUAL), "an indexed key did not resolve");
    }
    return keys.size();
  }

  private static CASValue firstKey(final HOTIndexReader<CASValue> reader) {
    final Iterator<Map.Entry<CASValue, NodeReferences>> entries = cast(reader.iterator());
    assertTrue(entries.hasNext(), "the index is empty, so this test would prove nothing");
    return entries.next().getKey();
  }

  @SuppressWarnings("unchecked")
  private static Iterator<Map.Entry<CASValue, NodeReferences>> cast(final Iterator<?> iterator) {
    return (Iterator<Map.Entry<CASValue, NodeReferences>>) iterator;
  }

  private static long cachedEntries() {
    return Databases.getGlobalBufferManager().getHOTLookupCache().size();
  }

  @Test
  @DisplayName("a point lookup populates the cache, and the resource sweep empties it")
  void resourceSweepDropsMemoizedLookups() {
    final IndexDef casDef;
    final long databaseId;
    final long resourceId;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      casDef = buildIndexedResource(database, RESOURCE_NAME, "[{\"title\":\"Vertigo\"},{\"title\":\"Rear Window\"}]");
      databaseId = database.getDatabaseConfig().getDatabaseId();

      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        resourceId = rtx.getStorageEngineReader().getResourceId();

        assertEquals(0L, cachedEntries(), "the cache must start cold");
        assertNotNull(lookupFirstKey(rtx, casDef), "the index does not hold the key this test rests on");
        assertTrue(cachedEntries() > 0, "a point lookup did not populate the cache — the wiring is dead");
      }
    }

    // Exactly the call truncateTo, crash recovery and removeResource make.
    Databases.clearCachesForResource(databaseId, resourceId);

    assertEquals(0L, cachedEntries(), "the resource sweep did not reach the lookup cache");
  }

  @Test
  @DisplayName("the DATABASE sweep reaches the lookup cache too, across every resource")
  void databaseSweepDropsMemoizedLookups() {
    // The OTHER half of the wiring. sweepHotCachesAndReport was extracted to serve two call sites
    // that differ only in the key predicate and the invalidation lambda, and every other test here
    // drives the RESOURCE one — so a copy-paste that handed the database site the resource lambda
    // (or narrowed its predicate) would leave the whole class green while removeDatabase and
    // first-commit crash recovery, which are the callers of this path, kept every memoized posting
    // list of the old incarnation. Two resources, because the database sweep's distinguishing
    // property is that it reaches ALL of them.
    final long databaseId;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      final IndexDef firstDef = buildIndexedResource(database, RESOURCE_NAME,
          "[{\"title\":\"Vertigo\"},{\"title\":\"Rear Window\"},{\"title\":\"Notorious\"}]");
      final IndexDef secondDef = buildIndexedResource(database, OTHER_RESOURCE_NAME, "[{\"title\":\"Psycho\"}]");
      databaseId = database.getDatabaseConfig().getDatabaseId();

      assertEquals(0L, cachedEntries(), "the cache must start cold");
      final long afterFirst;
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertNotNull(lookupFirstKey(rtx, firstDef), "the index does not hold the key this test rests on");
        afterFirst = cachedEntries();
        assertTrue(afterFirst > 0, "a point lookup did not populate the cache — the wiring is dead");
      }
      try (final JsonResourceSession manager = database.beginResourceSession(OTHER_RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertNotNull(lookupFirstKey(rtx, secondDef), "the sibling index does not hold the key this test rests on");
        // Strictly MORE than the first resource alone, so the sweep below has something from each
        // to remove: an assertion of "0 afterwards" against a cache holding only one resource's
        // entries would be satisfied by a sweep that reached that resource and no other.
        assertTrue(cachedEntries() > afterFirst, "the sibling resource's lookup was not memoized separately");
      }
    }

    // Exactly the call Databases.removeDatabase and first-commit crash recovery make.
    Databases.clearCachesForDatabase(databaseId);

    assertEquals(0L, cachedEntries(), "the database sweep did not reach the lookup cache");
  }

  @Test
  @DisplayName("the resource sweep leaves a sibling resource's memoized lookups alone")
  void resourceSweepIsScopedToOneResource() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      // DELIBERATELY asymmetric footprints — three keys here, one there. With one entry each, a
      // sweep that inverted the resource comparison and dropped the SIBLING would still take
      // occupancy 2 -> 1 and satisfy both "> 0" and "< populated": the test would be green while
      // the target's stale entries stayed live and the sibling's work was thrown away, which is
      // exactly the failure it is named for. Distinct counts make the two outcomes distinguishable.
      final IndexDef firstDef = buildIndexedResource(database, RESOURCE_NAME,
          "[{\"title\":\"Vertigo\"},{\"title\":\"Rear Window\"},{\"title\":\"Notorious\"}]");
      final IndexDef secondDef = buildIndexedResource(database, OTHER_RESOURCE_NAME, "[{\"title\":\"Psycho\"}]");
      final long databaseId = database.getDatabaseConfig().getDatabaseId();

      final long firstResourceId;
      final int firstKeys;
      final int secondKeys;
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        firstResourceId = rtx.getStorageEngineReader().getResourceId();
        firstKeys = lookupAllKeys(rtx, firstDef);
      }
      try (final JsonResourceSession manager = database.beginResourceSession(OTHER_RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        secondKeys = lookupAllKeys(rtx, secondDef);
      }
      assertTrue(firstKeys > secondKeys,
          "the fixture must give the two resources different footprints, saw " + firstKeys + " and " + secondKeys);
      final long populated = cachedEntries();
      assertEquals(firstKeys + secondKeys, populated, "both resources should have memoized every key they hold");

      Databases.clearCachesForResource(databaseId, firstResourceId);

      // Scope matters as much as the sweep: a sweep that cleared everything would be correct but
      // would throw away every other resource's work on any rollback — and one that swept the wrong
      // resource would be a silent correctness bug. Only the sibling's entries may remain, and
      // exactly as many of them as it memoized.
      assertEquals(secondKeys, cachedEntries(),
          "the sweep should have dropped exactly the target resource's " + firstKeys + " entries");
    }
  }

  @Test
  @DisplayName("a rolled-back revision is not served its pre-truncation answer when re-issued")
  void reIssuedRevisionIsNotServedStaleAnswers() {
    // The bug in its natural habitat. truncateTo re-issues the truncated revision numbers over
    // different content, so an entry keyed by (database, resource, revision, key) can describe a
    // history that no longer exists — which is why truncateTo sweeps the caches at all.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      final IndexDef casDef = buildIndexedResource(database, RESOURCE_NAME, "[{\"title\":\"Vertigo\"}]");
      final long databaseId = database.getDatabaseConfig().getDatabaseId();

      final long resourceId;
      final NodeReferences beforeRollback;
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        resourceId = rtx.getStorageEngineReader().getResourceId();
        beforeRollback = lookupFirstKey(rtx, casDef);
        assertNotNull(beforeRollback, "the index does not hold the key this test rests on");
        assertTrue(cachedEntries() > 0, "the answer was not memoized, so this test proves nothing");
      }

      Databases.clearCachesForResource(databaseId, resourceId);

      // After the sweep the same lookup must go back to the trie rather than to memory. Equality of
      // the recomputed answer is the point: the sweep must not make lookups WRONG either.
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertEquals(0L, cachedEntries(), "entries survived the sweep");
        final NodeReferences recomputed = lookupFirstKey(rtx, casDef);
        assertNotNull(recomputed, "the recomputed lookup lost the key");
        assertEquals(beforeRollback, recomputed, "recomputing after a sweep changed the answer");
      }
    }
  }

  @Test
  @DisplayName("clearAllCaches empties the lookup cache, so clearGlobalCaches really is a cold process")
  void clearAllCachesDropsMemoizedLookups() {
    // Databases.clearGlobalCaches() delegates here and is documented as making the next open read
    // from disk "like a freshly started process would" — the contract the corruption tests rely on.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      final IndexDef casDef = buildIndexedResource(database, RESOURCE_NAME, "[{\"title\":\"Vertigo\"}]");
      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertNotNull(lookupFirstKey(rtx, casDef));
        assertTrue(cachedEntries() > 0);
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();

    assertEquals(0L, cachedEntries(), "clearAllCaches left memoized lookups behind");
  }

  @Test
  @DisplayName("a writer-backed reader neither reads nor populates the cache")
  void writerBackedReaderBypassesTheCache() {
    // The single property the no-invalidation-for-ordinary-commits argument rests on: an uncommitted
    // transaction mutates the index under a revision number that is already a cache key.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      final IndexDef casDef = buildIndexedResource(database, RESOURCE_NAME, "[{\"title\":\"Vertigo\"}]");
      Databases.getGlobalBufferManager().getHOTLookupCache().clear();

      try (final JsonResourceSession manager = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        final HOTIndexReader<CASValue> reader = HOTIndexReader.create(wtx.getStorageEngineReader(),
            CASKeySerializer.INSTANCE, casDef.getType(), casDef.getID());
        final NodeReferences found = reader.get(firstKey(reader), SearchMode.EQUAL);

        assertNotNull(found, "the writer-backed reader lost the key, so the bypass proves nothing");
        assertEquals(0L, cachedEntries(), "a writer-backed reader populated the lookup cache");
      }
    }
  }
}
