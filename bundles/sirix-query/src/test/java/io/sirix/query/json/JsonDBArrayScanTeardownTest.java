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
   * Elements. The read-ahead declines resources smaller than
   * {@code RecordPagePrefetcher.MIN_PAGES} record pages, so the array has to span more than
   * {@code 8 * 1024} nodes for a prefetcher to exist at all.
   */
  private static final int ELEMENTS = 12_000;

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

  private static Object valuesFieldOf(final JsonDBArray array) throws Exception {
    final Field values = AbstractJsonDBArray.class.getDeclaredField("values");
    values.setAccessible(true);
    return values.get(array);
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

    assertEquals(baseline, session.activeTrxCount(),
                 "a completed scan must release every transaction the read-ahead opened");
  }

  /** Waits, bounded, for the read-ahead workers to have opened their transactions. */
  private static boolean awaitTransactionsAbove(final JsonResourceSession session, final int baseline) {
    final long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (session.activeTrxCount() > baseline) {
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
    assertEquals(baseline, session.activeTrxCount(),
                 "the probe that ends the walk must not leave a read-ahead transaction open");
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
