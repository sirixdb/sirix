/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The range-scan CONTRACT of the HOT cursor: a scan yields every key in the range, in ascending key
 * order, exactly once.
 *
 * <p>
 * <b>Why this test exists.</b> A projection index silently stopped serving above ~160 row groups
 * because its directory reader assumed the cursor yields keys ascending; the scan actually returned
 * {@code 1..159, 192..196, 160..191} — nothing lost, but not sorted. Order is not a cosmetic
 * property here: {@code HOTRangeCursor.advanceToValid} ends a BOUNDED scan at the first key past
 * {@code toKey}, so a single out-of-order page truncates the result and the caller sees a short
 * answer rather than an error.
 *
 * <p>
 * The pre-existing bounded-range coverage asserted only {@code rangeCount > 0}, which passes under
 * exactly that truncation. These tests assert EXACT counts and monotonicity instead, and use enough
 * distinct keys to force a multi-page trie — the defect is invisible while everything fits on one
 * leaf.
 */
@DisplayName("HOT range scan — ordering and completeness")
final class HOTRangeScanOrderTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE_NAME = "rangeOrderResource";

  /**
   * Distinct indexed values. Sized well past a single HOT leaf (512 entries): the 200k-record
   * projection that exposed the original defect spread over six pages, and a defect in page-to-page
   * traversal cannot show up until the trie actually has pages to traverse.
   */
  private static final int VALUE_COUNT = 20_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("An unbounded scan returns every key exactly once, in ascending order")
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void unboundedScanIsCompleteAndSorted() throws IOException {
    withCasIndexOverDistinctInts((hotReader, pcr) -> {
      final Iterator<Map.Entry<CASValue, NodeReferences>> it =
          hotReader.range(casKey(Integer.MIN_VALUE, pcr), casKey(Integer.MAX_VALUE, pcr));
      assertSortedAndComplete(it, VALUE_COUNT, "unbounded scan");
    });
  }

  @Test
  @DisplayName("A bounded scan returns exactly the keys inside its bounds, in ascending order")
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void boundedScanIsNotTruncated() throws IOException {
    withCasIndexOverDistinctInts((hotReader, pcr) -> {
      // A window deliberately spanning many pages, so a truncating scan comes up short rather
      // than merely returning a different page's worth of keys.
      final int lo = 25;
      final int hi = VALUE_COUNT - 25;
      final Iterator<Map.Entry<CASValue, NodeReferences>> it = hotReader.range(casKey(lo, pcr), casKey(hi, pcr));
      assertSortedAndComplete(it, hi - lo + 1, "bounded inclusive scan [" + lo + ", " + hi + "]");
    });
  }

  /**
   * Walks {@code it}, asserting keys ascend strictly and — when {@code expectedCount >= 0} — that
   * exactly that many arrive.
   *
   * @return the number of entries seen
   */
  private static int assertSortedAndComplete(final Iterator<Map.Entry<CASValue, NodeReferences>> it,
      final int expectedCount, final String what) {
    int seen = 0;
    CASValue previous = null;
    while (it.hasNext()) {
      final CASValue current = it.next().getKey();
      if (previous != null && previous.compareTo(current) >= 0) {
        fail(what + ": key #" + seen + " (" + current.getAtomicValue() + ") does not follow its " + "predecessor ("
            + previous.getAtomicValue() + ") — the cursor returned keys out "
            + "of order, which silently truncates any bounded scan");
      }
      previous = current;
      seen++;
    }
    if (expectedCount >= 0) {
      assertEquals(expectedCount, seen, what + " returned " + seen + " keys, expected " + expectedCount);
    }
    return seen;
  }

  private static CASValue casKey(final int value, final long pcr) {
    return new CASValue(new Int32(value), Type.INR, pcr);
  }

  @FunctionalInterface
  private interface ScanAssertion {
    void run(HOTIndexReader<CASValue> hotReader, long pcr);
  }

  /** Builds a resource whose CAS index holds {@link #VALUE_COUNT} distinct ints, then scans it. */
  private static void withCasIndexOverDistinctInts(final ScanAssertion assertion) throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        final var pathToValue = parse("/items/[]/value", io.brackit.query.util.path.PathParser.Type.JSON);
        final IndexDef casIndexDef =
            IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casIndexDef), wtx);

        // One object per value, values 0..VALUE_COUNT-1 so the expected key set is exactly known.
        final StringBuilder json = new StringBuilder(VALUE_COUNT * 24).append("{\"items\": [");
        for (int i = 0; i < VALUE_COUNT; i++) {
          if (i > 0) {
            json.append(',');
          }
          json.append("{\"value\": ").append(i).append('}');
        }
        json.append("]}");
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        wtx.commit();
        HOTInvariantValidator.validateIndex(wtx.getStorageEngineReader(), casIndexDef.getType(), casIndexDef.getID())
                             .assertOk();

        final var hotReader = HOTIndexReader.create(wtx.getStorageEngineReader(), CASKeySerializer.INSTANCE,
            casIndexDef.getType(), casIndexDef.getID());
        final long pcr = new JsonPCRCollector(wtx).getPCRsForPaths(casIndexDef.getPaths()).getPCRs().iterator().next();
        assertion.run(hotReader, pcr);
      }
    }
  }
}
