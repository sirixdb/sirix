/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness here rests on two things, and both are silent when broken. The KEY must be exact: one
 * that collides across revisions, indexes or resources serves one snapshot's answer to another with
 * nothing to catch it. And INVALIDATION must be scoped: a committed revision's content is normally
 * immutable, but {@code truncateTo} re-issues revision numbers over different content on rollback
 * and crash recovery, and a dropped resource's id is reused — so entries have to be droppable by
 * database and by resource. These pin both, plus the probe-versus-owned split that lets a lookup
 * avoid copying, the admission bound, and the capacity ceiling.
 */
@DisplayName("HOT point-lookup cache")
final class HOTLookupCacheTest {

  private static final long DB = 7L;
  private static final long RESOURCE = 3L;
  private static final int REVISION = 11;
  private static final int INDEX = 2;

  private static byte[] key(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static HOTLookupKey probe(final byte[] bytes) {
    return HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
  }

  @Nested
  @DisplayName("key identity")
  final class KeyIdentityTests {

    @Test
    @DisplayName("a probe and its owned twin are the same key")
    void ownedEqualsProbe() {
      final HOTLookupKey p = probe(key("title"));
      final HOTLookupKey owned = p.owned();
      assertEquals(p, owned);
      assertEquals(owned, p);
      assertEquals(p.hashCode(), owned.hashCode());
    }

    @Test
    @DisplayName("owned() copies, so overwriting the probe buffer does not change it")
    void ownedIsInsulatedFromBufferReuse() {
      // The real caller probes straight out of a reused thread-local serialization buffer, so an
      // owned key that aliased it would silently change identity on the next lookup.
      final byte[] reusable = new byte[16];
      System.arraycopy(key("alpha"), 0, reusable, 0, 5);
      final HOTLookupKey owned =
          HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, reusable, 0, 5).owned();
      final int hashBefore = owned.hashCode();

      Arrays.fill(reusable, (byte) 'z');

      assertEquals(hashBefore, owned.hashCode());
      assertEquals(owned, probe(key("alpha")));
      assertNotEquals(owned, probe(key("zzzzz")));
    }

    @Test
    @DisplayName("a key is read from its range, not from the whole buffer")
    void keyRangeIsHonoured() {
      final byte[] padded = new byte[32];
      System.arraycopy(key("beta"), 0, padded, 7, 4);
      final HOTLookupKey ranged = HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, padded, 7, 4);
      assertEquals(probe(key("beta")), ranged);
    }

    @Test
    @DisplayName("revision, index number, index type and resource all discriminate")
    void everyIdentityComponentDiscriminates() {
      final byte[] bytes = key("same-key-bytes");
      final HOTLookupKey base = probe(bytes);

      assertNotEquals(base,
          HOTLookupKey.probe(DB, RESOURCE, REVISION + 1, IndexType.CAS, INDEX, bytes, 0, bytes.length));
      assertNotEquals(base,
          HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX + 1, bytes, 0, bytes.length));
      assertNotEquals(base, HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.NAME, INDEX, bytes, 0, bytes.length));
      assertNotEquals(base,
          HOTLookupKey.probe(DB, RESOURCE + 1, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length));
      assertNotEquals(base,
          HOTLookupKey.probe(DB + 1, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length));
    }

    @Test
    @DisplayName("a prefix is not the key it prefixes")
    void prefixIsNotTheKey() {
      final byte[] longer = key("titleX");
      assertNotEquals(probe(key("title")),
          HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, longer, 0, longer.length));
    }
  }

  @Nested
  @DisplayName("concurrency")
  final class ConcurrencyTests {

    /**
     * Hammer one cache from several threads over an overlapping key space.
     *
     * <p>
     * Two properties are at stake and neither is provable by reading the code. Publication: an
     * {@code Entry} is handed between threads through an array slot, so a reader must never see one
     * whose fields are not yet visible. Key integrity: sets are chosen by hash and victims rotate on a
     * deliberately racy counter, so a lost update or a doubly-chosen victim must be able to LOSE an
     * entry but never to return one key's answer under another key — that would be silent wrong query
     * results, which is the failure this whole class is one step away from.
     * </p>
     */
    @Test
    @DisplayName("concurrent readers and writers never see a torn entry or another key's answer")
    void concurrentAccessKeepsEntriesIntactAndCorrectlyKeyed() throws InterruptedException {
      final int threads = 8;
      final int keySpace = 512;
      final int iterations = 20_000;
      final HOTLookupCache cache = new HOTLookupCache(256); // deliberately smaller than the key space
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final AtomicLong hits = new AtomicLong();

      for (int t = 0; t < threads; t++) {
        final int seed = t;
        pool.execute(() -> {
          try {
            start.await();
            for (int i = 0; i < iterations; i++) {
              final int id = (seed * 31 + i) % keySpace;
              final byte[] bytes = key("concurrent-key-" + id);
              final HOTLookupKey probe =
                  HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
              final long[] hit = cache.get(probe);
              if (hit != null) {
                // The entry must be fully published AND belong to the key we asked for.
                assertEquals(2, hit.length, "torn or foreign entry for key " + id);
                assertEquals(id, hit[0], "entry for key " + id + " carries another key's answer");
                assertEquals(id + 1L, hit[1], "entry for key " + id + " is half-written");
                hits.incrementAndGet();
              } else {
                cache.put(probe.owned(), new long[] {id, id + 1L}, cache.generation());
              }
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        });
      }

      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers did not finish");
      if (failure.get() != null) {
        throw new AssertionError("concurrent access failed", failure.get());
      }
      assertTrue(hits.get() > 0, "no thread ever observed a cached entry, so nothing was exercised");
      assertTrue(cache.size() <= 256, "capacity was exceeded under concurrency: " + cache.size());
    }

    @Test
    @DisplayName("invalidation concurrent with lookups never yields a foreign answer")
    void invalidationDuringLookupsStaysCorrect() throws InterruptedException {
      // Sweeps run while readers are live — truncateTo does not stop the world — so nulling slots
      // must never be observable as a wrong value, only as a miss.
      final HOTLookupCache cache = new HOTLookupCache(256);
      final ExecutorService pool = Executors.newFixedThreadPool(4);
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      // Every assertion below sits inside `if (hit != null)`, so a cache that never hits would run
      // ZERO of them and pass. Count the hits and require some, exactly as the sibling test does.
      final AtomicLong hits = new AtomicLong();
      // The sweeper must stay live for the whole run rather than finishing in microseconds while the
      // readers grind on: a fixed 200-iteration sweeper left the overwhelming majority of reader
      // iterations running with no sweep in flight, i.e. not exercising the race the test is named
      // for. It now sweeps until the readers signal they are done.
      final CountDownLatch readersDone = new CountDownLatch(3);

      for (int t = 0; t < 3; t++) {
        pool.execute(() -> {
          try {
            start.await();
            for (int i = 0; i < 20_000; i++) {
              final int id = i % 256;
              final byte[] bytes = key("swept-key-" + id);
              final HOTLookupKey probe =
                  HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
              final long[] hit = cache.get(probe);
              if (hit == null) {
                cache.put(probe.owned(), new long[] {id}, cache.generation());
              } else {
                hits.incrementAndGet();
                assertEquals(1, hit.length);
                assertEquals(id, hit[0], "a swept slot produced another key's answer");
              }
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            readersDone.countDown();
          }
        });
      }
      pool.execute(() -> {
        try {
          start.await();
          while (!readersDone.await(1, TimeUnit.MILLISECONDS)) {
            cache.invalidateResource(DB, RESOURCE);
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      });

      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers did not finish");
      if (failure.get() != null) {
        throw new AssertionError("invalidation raced badly", failure.get());
      }
      assertTrue(hits.get() > 0, "no thread ever observed a cached entry, so nothing was exercised");
    }
  }

  @Nested
  @DisplayName("cache behaviour")
  final class CacheTests {

    @Test
    @DisplayName("a stored answer is found by an equal probe")
    void storedAnswerIsFoundByProbe() {
      final HOTLookupCache cache = new HOTLookupCache(64);
      final long[] nodeKeys = {3L, 9L, 27L};
      assertTrue(cache.put(probe(key("title")).owned(), nodeKeys, cache.generation()));

      assertArrayEquals(nodeKeys, cache.get(probe(key("title"))));
      assertNull(cache.get(probe(key("other"))));
    }

    @Test
    @DisplayName("an empty array is storable, so an ABSENT key can be memoized")
    void absentKeyIsStorable() {
      // The reader distinguishes "asked before, not there" from "never asked" by a zero-length
      // entry; a present key always has at least one node key.
      final HOTLookupCache cache = new HOTLookupCache(64);
      assertTrue(cache.put(probe(key("gone")).owned(), new long[0], cache.generation()));
      final long[] hit = cache.get(probe(key("gone")));
      assertNotNull(hit, "the ABSENT sentinel must come back as an entry, not as a miss");
      assertEquals(0, hit.length);
    }

    @Test
    @DisplayName("a borrowing probe key cannot be stored")
    void probeKeyIsRejectedForStorage() {
      // Storing a probe aliases a reused serialization buffer: the next lookup rewrites the stored
      // key's bytes while its hash stays frozen, after which the entry answers a DIFFERENT key.
      // Previously this contract was documented in three places and enforced in none.
      final HOTLookupCache cache = new HOTLookupCache(64);
      assertThrows(IllegalArgumentException.class,
          () -> cache.put(probe(key("borrowed")), new long[] {1L}, cache.generation()));
    }

    @Test
    @DisplayName("null arguments are rejected")
    void nullArgumentsAreRejected() {
      final HOTLookupCache cache = new HOTLookupCache(64);
      assertThrows(NullPointerException.class, () -> cache.get(null));
      assertThrows(NullPointerException.class, () -> cache.put(null, new long[] {1L}, cache.generation()));
      assertThrows(NullPointerException.class, () -> cache.put(probe(key("k")).owned(), null, cache.generation()));
    }

    @Test
    @DisplayName("the table built for a requested size is the largest power-of-two set count that fits")
    void capacityMatchesTheRequestedSize() {
      // The slot count used to round the SET count UP to a power of two, so a request for 32776
      // built 65536 slots — twice the operator's budget, against which the default's memory
      // estimate is computed. Only 256 and other exact powers hid it, which is what the previous
      // version of this test happened to use.
      //
      // Asserting the EXACT built capacity, not just `<= requested`: an upper bound is satisfied by
      // a constructor regression that built one 8-slot set for every requested size (highestOneBit
      // mistyped as numberOfTrailingZeros, say), silently shrinking the cache process-wide while
      // every entry in this list still passed.
      //
      // Read straight off capacity(), NOT inferred from full occupancy. Filling the table and
      // asserting on size() only proves the capacity if every one of up to 8192 sets happens to draw
      // at least WAYS of the probe keys, which makes an assertion about pure constructor arithmetic
      // depend on the distribution of HOTLookupKey#computeHash — so retuning the hash would fail
      // THIS test and point the reader at a constructor that never changed. It also cost ~525K puts
      // across nine tables, one of them 65536 slots, to observe a number capacity() returns directly.
      for (final int requested : new int[] {8, 64, 100, 200, 256, 1000, 1024, 32776, 65536}) {
        final int expectedSlots = Integer.highestOneBit(requested / 8) * 8;
        final HOTLookupCache cache = new HOTLookupCache(requested);
        assertTrue(cache.capacity() <= requested,
            "requested " + requested + " but built " + cache.capacity() + " slots");
        assertEquals(expectedSlots, cache.capacity(),
            "requested " + requested + " should build exactly " + expectedSlots + " slots");
      }
    }

    @Test
    @DisplayName("a request below the associativity still gets one full set, and says so")
    void requestBelowWaysGetsOneSet() {
      // The documented exception to the bound above: "A value below WAYS still gets one set, since a
      // set-associative table cannot be narrower than its associativity." That means a request of 1
      // to 7 OVERSHOOTS — occupancy reaches 8. It is reachable (BufferManagerImpl accepts any
      // maxEntries >= 0), so it is pinned here rather than left to a test list that starts at 8 and
      // a DisplayName claiming "any requested size".
      for (int requested = 1; requested < 8; requested++) {
        final HOTLookupCache cache = new HOTLookupCache(requested);
        assertEquals(8, cache.capacity(), "a request of " + requested + " should still build exactly one 8-way set");
      }
    }

    @Test
    @DisplayName("an absurd requested size is clamped, not allowed to overflow into a negative array")
    void absurdSizeIsClamped() {
      // sets * WAYS is int arithmetic reachable from a system property: without a ceiling,
      // maxEntries >= 1073741832 overflowed to Integer.MIN_VALUE and the constructor threw
      // NegativeArraySizeException out of BufferManagerImpl's constructor — one typo'd property
      // made every database in the JVM unopenable.
      final HOTLookupCache cache = new HOTLookupCache(Integer.MAX_VALUE);
      assertTrue(cache.put(probe(key("survives")).owned(), new long[] {1L}, cache.generation()));
      assertArrayEquals(new long[] {1L}, cache.get(probe(key("survives"))));
    }

    @Test
    @DisplayName("invalidateResource drops only that resource's entries")
    void invalidateResourceIsScoped() {
      // The reason this method exists: truncateTo re-issues committed revision numbers over
      // different content, and a dropped resource's id is reused — so entries keyed by
      // (db, resource, revision) can describe a history that no longer exists.
      final HOTLookupCache cache = new HOTLookupCache(1024);
      final byte[] bytes = key("shared-key-bytes");
      final HOTLookupKey mine =
          HOTLookupKey.probe(DB, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      final HOTLookupKey otherResource =
          HOTLookupKey.probe(DB, RESOURCE + 1, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      final HOTLookupKey otherDatabase =
          HOTLookupKey.probe(DB + 1, RESOURCE, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      cache.put(mine.owned(), new long[] {1L}, cache.generation());
      cache.put(otherResource.owned(), new long[] {2L}, cache.generation());
      cache.put(otherDatabase.owned(), new long[] {3L}, cache.generation());

      assertEquals(1, cache.invalidateResource(DB, RESOURCE));

      assertNull(cache.get(mine), "the invalidated resource's entry survived");
      assertArrayEquals(new long[] {2L}, cache.get(otherResource), "a sibling resource was swept");
      assertArrayEquals(new long[] {3L}, cache.get(otherDatabase), "another database was swept");
    }

    @Test
    @DisplayName("invalidateDatabase drops every resource of that database and nothing else")
    void invalidateDatabaseIsScoped() {
      final HOTLookupCache cache = new HOTLookupCache(1024);
      final byte[] bytes = key("shared-key-bytes");
      final HOTLookupKey res0 = HOTLookupKey.probe(DB, 0L, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      final HOTLookupKey res1 = HOTLookupKey.probe(DB, 1L, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      final HOTLookupKey otherDb =
          HOTLookupKey.probe(DB + 1, 0L, REVISION, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      cache.put(res0.owned(), new long[] {1L}, cache.generation());
      cache.put(res1.owned(), new long[] {2L}, cache.generation());
      cache.put(otherDb.owned(), new long[] {3L}, cache.generation());

      assertEquals(2, cache.invalidateDatabase(DB));

      assertNull(cache.get(res0));
      assertNull(cache.get(res1));
      assertArrayEquals(new long[] {3L}, cache.get(otherDb), "another database was swept");
    }

    @Test
    @DisplayName("a re-issued revision does not inherit the previous incarnation's answer")
    void reIssuedRevisionIsNotServedStaleAfterInvalidation() {
      // The end-to-end shape of the bug: revision 5 is memoized, truncateTo(4) rolls it back, the
      // next commit re-issues 5 over different content. Without the sweep the reader is served the
      // discarded history; with it, the key misses and recomputes.
      final HOTLookupCache cache = new HOTLookupCache(1024);
      final byte[] bytes = key("Vertigo");
      final HOTLookupKey atRevision5 =
          HOTLookupKey.probe(DB, RESOURCE, 5, IndexType.CAS, INDEX, bytes, 0, bytes.length);
      cache.put(atRevision5.owned(), new long[] {100L, 200L}, cache.generation());
      assertArrayEquals(new long[] {100L, 200L}, cache.get(atRevision5));

      cache.invalidateResource(DB, RESOURCE); // what truncateTo's sweep now does

      assertNull(cache.get(atRevision5), "a re-issued revision would be served the rolled-back answer");
    }

    @Test
    @DisplayName("a posting list beyond the bound is refused, not stored")
    void oversizedPostingListIsRefused() {
      final HOTLookupCache cache = new HOTLookupCache(64);
      final long[] tooBig = new long[HOTLookupCache.MAX_CACHED_NODE_KEYS + 1];
      assertFalse(cache.put(probe(key("popular")).owned(), tooBig, cache.generation()));
      assertNull(cache.get(probe(key("popular"))));

      final long[] atBound = new long[HOTLookupCache.MAX_CACHED_NODE_KEYS];
      assertTrue(cache.put(probe(key("borderline")).owned(), atBound, cache.generation()));
      assertSame(atBound, cache.get(probe(key("borderline"))));
    }

    @Test
    @DisplayName("the disabled cache reports every lookup as a miss")
    void disabledCacheNeverHits() {
      final HOTLookupCache cache = HOTLookupCache.disabled();
      assertFalse(cache.put(probe(key("title")).owned(), new long[] {1L}, cache.generation()));
      assertNull(cache.get(probe(key("title"))));
      assertEquals(0L, cache.size());
      cache.clear(); // must not throw
    }

    @Test
    @DisplayName("the disabled cache refuses a borrowing probe key rather than throwing at it")
    void disabledCacheRefusesProbeKeyWithoutThrowing() {
      // disabled() promises "refuses every admission, so callers need no null checks and no feature
      // flag". A no-op that throws for one class of argument is not a no-op — and the argument it
      // used to throw for, a borrowing probe key, is precisely what a caller taking that promise at
      // face value would hand it. The ownership check ran BEFORE the disabled-table check.
      final HOTLookupCache cache = HOTLookupCache.disabled();
      assertFalse(cache.put(probe(key("title")), new long[] {1L}, cache.generation()),
          "a disabled cache must decline, not reject");
      // The enabled cache still enforces the contract the check exists for.
      final HOTLookupCache enabled = new HOTLookupCache(64);
      assertThrows(IllegalArgumentException.class,
          () -> enabled.put(probe(key("title")), new long[] {1L}, enabled.generation()));
    }

    @Test
    @DisplayName("a non-positive size is rejected rather than silently disabling the cache")
    void nonPositiveSizeIsRejected() {
      assertThrows(IllegalArgumentException.class, () -> new HOTLookupCache(0));
      assertThrows(IllegalArgumentException.class, () -> new HOTLookupCache(-1));
    }

    @Test
    @DisplayName("clear drops everything")
    void clearDropsEverything() {
      final HOTLookupCache cache = new HOTLookupCache(64);
      cache.put(probe(key("a")).owned(), new long[] {1L}, cache.generation());
      cache.clear();
      assertNull(cache.get(probe(key("a"))));
    }

    @Test
    @DisplayName("admission always succeeds and capacity is never exceeded")
    void admissionNeverDeclinesAndStaysBounded() {
      // A set-associative table evicts rather than declining, because the whole point is that
      // admitting costs one store — a miss must never become more expensive than the walk it
      // replaces. The flip side is that occupancy has a hard ceiling no matter how much is offered.
      final int capacity = 256;
      final HOTLookupCache cache = new HOTLookupCache(capacity);
      for (int i = 0; i < capacity * 20; i++) {
        assertTrue(cache.put(probe(key("key-" + i)).owned(), new long[] {i}, cache.generation()),
            "admission declined at " + i);
      }
      assertTrue(cache.size() <= capacity, "occupancy " + cache.size() + " exceeded capacity " + capacity);
    }

    @Test
    @DisplayName("a re-admitted key is not duplicated across the ways of its set")
    void reAdmittingAKeyDoesNotDuplicateIt() {
      // Two entries for one key in the same set would waste a way and, if their answers ever
      // diverged, make which one is found depend on probe order.
      final HOTLookupCache cache = new HOTLookupCache(64);
      final long[] answer = {1L, 2L};
      for (int i = 0; i < 16; i++) {
        assertTrue(cache.put(probe(key("stable")).owned(), answer, cache.generation()));
      }
      assertEquals(1L, cache.size());
      assertArrayEquals(answer, cache.get(probe(key("stable"))));
    }

    @Test
    @DisplayName("entries surviving eviction still answer correctly")
    void survivorsRemainCorrect() {
      // Eviction is allowed to lose entries; it is NOT allowed to return one key's answer for
      // another, which is the failure a hash-indexed table with no per-entry key check would have.
      final HOTLookupCache cache = new HOTLookupCache(1024);
      for (int i = 0; i < 4096; i++) {
        cache.put(probe(key("k" + i)).owned(), new long[] {i, i + 1}, cache.generation());
      }
      int found = 0;
      for (int i = 0; i < 4096; i++) {
        final long[] hit = cache.get(probe(key("k" + i)));
        if (hit != null) {
          assertArrayEquals(new long[] {i, i + 1}, hit, "wrong answer for k" + i);
          found++;
        }
      }
      // Not `> 0`: that passes if a single key survived, so it would miss a degenerate set index or
      // a victim rotation stuck on one way. A 1024-slot table fed 4096 keys should retain most of
      // its capacity.
      assertTrue(found >= 512, "retained only " + found + " of 1024 slots — eviction is degenerate");
    }

    @Test
    @DisplayName("an admission that straddles a sweep is refused")
    void admissionStraddlingASweepIsRefused() {
      // The read-through race, deterministically: capture the generation the way a lookup does before
      // it starts reading pages, let a sweep run to completion in between, then admit. Ordering the
      // sweep's own steps cannot catch this — the answer was computed from content the sweep is
      // discarding, and without the generation it would be resurrected under a revision number
      // truncateTo re-issues over different content, then served indefinitely.
      final HOTLookupCache cache = new HOTLookupCache(256);
      final byte[] bytes = key("straddles");

      final long generation = cache.generation();
      cache.invalidateResource(DB, RESOURCE);

      assertFalse(cache.put(probe(bytes).owned(), new long[] {1L, 2L}, generation),
          "an answer computed before the sweep must not be admitted after it");
      assertNull(cache.get(probe(bytes)), "the refused entry must not be readable");
    }

    @Test
    @DisplayName("an admission that does not straddle a sweep still succeeds")
    void admissionWithoutASweepSucceeds() {
      // The control. Without it the test above passes against a put() that refuses everything.
      final HOTLookupCache cache = new HOTLookupCache(256);
      final byte[] bytes = key("clean");

      final long generation = cache.generation();
      assertTrue(cache.put(probe(bytes).owned(), new long[] {1L, 2L}, generation));
      assertArrayEquals(new long[] {1L, 2L}, cache.get(probe(bytes)));
    }

    @Test
    @DisplayName("a database sweep bumps the generation too")
    void databaseSweepAlsoBumpsTheGeneration() {
      // invalidateDatabase and clear share invalidateMatching's bump, but clear() has its own path,
      // so both scopes are pinned rather than assumed from the resource-scoped one.
      final HOTLookupCache cache = new HOTLookupCache(256);

      final long beforeDatabase = cache.generation();
      cache.invalidateDatabase(DB);
      assertNotEquals(beforeDatabase, cache.generation(), "invalidateDatabase must bump the generation");

      final long beforeClear = cache.generation();
      cache.clear();
      assertNotEquals(beforeClear, cache.generation(), "clear must bump the generation");
    }
  }
}
