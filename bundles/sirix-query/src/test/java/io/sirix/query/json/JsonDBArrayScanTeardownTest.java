package io.sirix.query.json;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Pins how a SEQUENTIAL scan of {@code AbstractJsonDBArray} ends.
 *
 * <p>Two properties, both invisible to a test that only checks the elements. The read-ahead
 * ({@link RecordPagePrefetcher}) opens a read-only transaction per worker thread, and nothing in
 * the JDM hands the array item a teardown hook: it is not {@code AutoCloseable} and brackit's
 * iterator {@code close()} is a no-op. Its unbox bounds the loop by {@code len()}, so a completed
 * scan never asks for the element past the end either — which means the last element is the only
 * point at which a finished walk is observable, and the only place those transactions can be
 * released. Left open they accumulate per scanned array, and each one pins its revision against
 * the page cache's watermark eviction.
 *
 * <p>The second property is what the overrun probe costs: a consumer that walks until {@code null}
 * instead of bounding by {@code len()} must not trigger a full materialization of the element list
 * — the very thing the sibling walk exists to avoid, and a list that is then retained for the rest
 * of the query.
 */
final class JsonDBArrayScanTeardownTest {

  private static final String COLL = "scanTeardownColl";
  private static final String RES = "scanTeardownRes";

  /**
   * Elements. The read-ahead declines a WALK spanning fewer than
   * {@code RecordPagePrefetcher.MIN_PAGES} record pages, so the array has to cover more than
   * {@code 8 * 1024} nodes for a prefetcher to exist at all.
   */
  private static final int ELEMENTS = 12_000;

  /** Outer elements of the nested corpus; enough that the RESOURCE clears the page threshold. */
  private static final int NESTED_OUTER_ELEMENTS = 4_000;

  private Path testDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    testDir = Files.createTempDirectory("sirix-json-scan-teardown-test");
    store = BasicJsonDBStore.newBuilder().location(testDir).build();
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
    if (testDir != null) {
      Databases.removeDatabase(testDir);
    }
  }

  private JsonDBArray loadArray(final String json) {
    store.create(COLL, RES, json);
    final JsonDBCollection coll = store.lookup(COLL);
    return (JsonDBArray) coll.getDocument(RES);
  }

  private static String numbers(final int count) {
    final StringBuilder json = new StringBuilder(count * 6 + 2);
    json.append('[');
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(i);
    }
    return json.append(']').toString();
  }

  /** {@code [[0,1,2],[0,1,2],...]} — a big resource whose INNER walks are three elements long. */
  private static String nestedTriples(final int outerCount) {
    final StringBuilder json = new StringBuilder(outerCount * 8 + 2);
    json.append('[');
    for (int i = 0; i < outerCount; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("[0,1,2]");
    }
    return json.append(']').toString();
  }

  private static Object valuesFieldOf(final JsonDBArray array) throws Exception {
    final Field values = AbstractJsonDBArray.class.getDeclaredField("values");
    values.setAccessible(true);
    return values.get(array);
  }

  private static Object prefetcherFieldOf(final AbstractJsonDBArray<?> array) throws Exception {
    final Field prefetcher = AbstractJsonDBArray.class.getDeclaredField("prefetcher");
    prefetcher.setAccessible(true);
    return prefetcher.get(array);
  }

  @Test
  @DisplayName("a completed sequential scan leaves no read-ahead transaction behind")
  void aCompletedScanReleasesThePrefetchTransactions() {
    assumeTrue(RecordPagePrefetcher.isEnabled(), "scan prefetching is switched off in this JVM");

    final JsonDBArray array = loadArray(numbers(ELEMENTS));
    final JsonResourceSession session = array.getResourceSession();
    final int baseline = session.activeTrxCount();

    final int len = array.len();
    assertEquals(ELEMENTS, len);

    // Walk the array exactly the way brackit's array unbox does: bounded by len(), so at(len) is
    // never called and the last element is the only signal that the walk is over.
    assertNotNull(array.at(0));
    assertNotNull(array.at(1), "the second step is what starts the read-ahead");
    assertTrue(awaitTransactionsAbove(session, baseline),
               "precondition: the read-ahead must have opened worker transactions, else this proves nothing");

    for (int i = 2; i < len; i++) {
      assertNotNull(array.at(i), "element " + i + " must be served by the sibling walk");
    }

    assertTrue(awaitTransactionsDownTo(session, baseline),
               "a completed scan must release every transaction the read-ahead opened");
  }

  /** Waits, bounded, for the read-ahead workers to have opened their transactions. */
  private static boolean awaitTransactionsAbove(final JsonResourceSession session, final int baseline) {
    return awaitTransactions(session, baseline, true);
  }

  /**
   * Waits, bounded, for the read-ahead's transactions to be gone again.
   *
   * <p>Bounded rather than immediate ON PURPOSE. Teardown hands the release to whichever thread
   * observes the last speculative decode finish — the query thread when the window is already
   * empty, the last worker otherwise — precisely so that closing at the last element cannot block
   * the query on work nothing will read. The guarantee is that a finished walk retains no
   * transaction, not that the release happens on the caller's stack.
   */
  private static boolean awaitTransactionsDownTo(final JsonResourceSession session, final int baseline) {
    return awaitTransactions(session, baseline, false);
  }

  private static boolean awaitTransactions(final JsonResourceSession session, final int baseline,
      final boolean above) {
    final long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      final int active = session.activeTrxCount();
      if (above ? active > baseline : active <= baseline) {
        return true;
      }
      Thread.onSpinWait();
    }
    return false;
  }

  @Test
  @DisplayName("probing past the end answers from the known count without materializing")
  void anOutOfRangeProbeDoesNotMaterializeTheElementList() throws Exception {
    final JsonDBArray array = loadArray(numbers(ELEMENTS));
    final JsonResourceSession session = array.getResourceSession();
    final int baseline = session.activeTrxCount();

    final int len = array.len();
    for (int i = 0; i < len; i++) {
      assertNotNull(array.at(i));
    }

    final Sequence overrun = array.at(len);
    assertNull(overrun, "the element after the last one does not exist");
    assertNull(valuesFieldOf(array),
               "an out-of-range probe must be answered from the child count, not by materializing "
                   + ELEMENTS + " elements");
    assertTrue(awaitTransactionsDownTo(session, baseline),
               "the probe that ends the walk must not leave a read-ahead transaction open");
  }

  @Test
  @DisplayName("a second sequential walk over the same array gets read-ahead again")
  void aSecondWalkRestartsTheReadAhead() throws Exception {
    assumeTrue(RecordPagePrefetcher.isEnabled(), "scan prefetching is switched off in this JVM");

    final JsonDBArray array = loadArray(numbers(ELEMENTS));
    final JsonResourceSession session = array.getResourceSession();
    final int baseline = session.activeTrxCount();
    final int len = array.len();

    for (int i = 0; i < len; i++) {
      assertNotNull(array.at(i));
    }
    assertNull(prefetcherFieldOf(array), "the last element ends the walk and its read-ahead");
    assertTrue(awaitTransactionsDownTo(session, baseline), "the first walk must release its transactions");

    // brackit binds ONE array item per variable, so `(count(for $x in $a[]...), count(for $y in
    // $a[]...))` walks this very object twice. The teardown at the last element must end the WALK,
    // not disable read-ahead on the item: a latch left set demoted every later scan to the cold
    // single-core decode path the prefetcher exists to hide.
    assertNotNull(array.at(0), "the second walk restarts at index 0");
    assertNotNull(array.at(1), "the second step is what restarts the read-ahead");
    assertNotNull(prefetcherFieldOf(array), "a second sequential walk must get read-ahead again");
    assertTrue(awaitTransactionsAbove(session, baseline),
               "the restarted read-ahead must actually open worker transactions again");

    for (int i = 2; i < len; i++) {
      assertNotNull(array.at(i));
    }
    assertTrue(awaitTransactionsDownTo(session, baseline), "the second walk must release them too");
  }

  @Test
  @DisplayName("a walk too short to amortize read-ahead never starts one")
  void aShortWalkInALargeResourceStartsNoPrefetcher() throws Exception {
    assumeTrue(RecordPagePrefetcher.isEnabled(), "scan prefetching is switched off in this JVM");

    final JsonDBArray outer = loadArray(nestedTriples(NESTED_OUTER_ELEMENTS));
    final JsonResourceSession session = outer.getResourceSession();
    final int baseline = session.activeTrxCount();

    // The OUTER walk spans the whole resource and is worth read-ahead.
    assertNotNull(outer.at(0));
    assertNotNull(outer.at(1));
    assertNotNull(prefetcherFieldOf(outer), "the long outer walk must still get read-ahead");

    // An INNER walk is three elements long. The resource is large either way, which is exactly why
    // gating on the resource admitted a full window of speculative decodes for a walk that ends two
    // elements later — a query like `for $m in jn:doc(...)[] return count($m.genres[])` pays that
    // per outer element.
    final AbstractJsonDBArray<?> inner = (AbstractJsonDBArray<?>) outer.at(2);
    assertNotNull(inner);
    assertEquals(3, inner.len());
    assertNotNull(inner.at(0));
    assertNotNull(inner.at(1));
    assertNull(prefetcherFieldOf(inner),
               "a three-element walk must never start a read-ahead window it would have to dispose of");
    assertNotNull(inner.at(2));
    assertNull(prefetcherFieldOf(inner));

    for (int i = 2; i < outer.len(); i++) {
      assertNotNull(outer.at(i));
    }
    assertTrue(awaitTransactionsDownTo(session, baseline), "the outer walk must release its transactions");
  }

  @Test
  @DisplayName("closing the read-ahead never blocks the query thread on outstanding work")
  void closingDoesNotWaitForOutstandingPrefetches() throws Exception {
    assumeTrue(RecordPagePrefetcher.isEnabled(), "scan prefetching is switched off in this JVM");

    final JsonDBArray array = loadArray(numbers(ELEMENTS));
    final RecordPagePrefetcher prefetcher =
        RecordPagePrefetcher.createOrNull(array.getTrx(), array.getNodeKey(), 1L, ELEMENTS);
    assertNotNull(prefetcher, "precondition: this walk is long enough to be admitted");

    // Hold one in-flight permit, which is exactly the state a still-running speculative decode
    // leaves behind. The old teardown did a bounded `inFlight.tryAcquire(WINDOW, 5s)` on the query
    // thread, so it could not return until every permit came back; here it must not wait at all.
    final Field inFlightField = RecordPagePrefetcher.class.getDeclaredField("inFlight");
    inFlightField.setAccessible(true);
    final Semaphore inFlight = (Semaphore) inFlightField.get(prefetcher);
    assertTrue(inFlight.tryAcquire(), "precondition: the window must have a permit to take");

    final long startNanos = System.nanoTime();
    prefetcher.close();
    final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    inFlight.release();
    assertTrue(elapsedMillis < 1_000L,
               "close() must cancel the outstanding window rather than wait for it, but took "
                   + elapsedMillis + " ms");
  }

  @Test
  @DisplayName("a negative or far-out index is still null, and random access still materializes")
  void boundsAndRandomAccessAreUnchanged() throws Exception {
    final JsonDBArray array = loadArray("[10,20,30]");

    assertNull(array.at(-1));
    assertNull(array.at(3));
    assertNull(array.at(99));
    assertNull(valuesFieldOf(array), "a bounds probe must not materialize a list either");

    // A jump, not a walk: this is the arm that legitimately materializes, and it must keep working.
    assertNotNull(array.at(2));
    assertNotNull(valuesFieldOf(array), "random access still memoizes the element list");
    assertEquals("10", ((Item) array.at(0)).atomize().stringValue());
  }
}
