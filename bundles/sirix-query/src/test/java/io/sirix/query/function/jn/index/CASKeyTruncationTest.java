/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Pins that a CAS equality query stays exact for values the key encoding cannot represent.
 *
 * <p>
 * {@code CASKeySerializer} truncates a string value at {@code MAX_STRING_VALUE_BYTES} (246), so two
 * values sharing that prefix serialize to ONE key and share one posting list. The HOT equality
 * branch in {@code CASIndex#openHOTIndexWithFilter} used to return that list as-is, handing the
 * query both values' nodes for a probe that matches only one of them.
 * </p>
 *
 * <p>
 * That over-match is now corrected rather than merely documented. The fix does not change the key
 * format — the disambiguating bytes genuinely are not stored, so no index-only path can be exact.
 * Instead {@code CASKeySerializer#losesInformation} reports when a probe cannot be represented, and
 * only then does the seek re-check its candidates against the real node values. A value inside the
 * cap is decided by the seek alone and pays nothing.
 * </p>
 *
 * <p>
 * Both index shapes are covered because the two were easy to confuse while the defect stood.
 * Measured on this fixture, asking for one of the two long values: single-path returned 2 both
 * before and after the {@code pcrsAvailable} gate relaxation — it has always taken the seek, so the
 * relaxation could not have caused it. Multi-path returned 0 before (the scan compares the
 * truncated STORED value against the full probe and matches nothing) and 2 after. Both are 1 now.
 * </p>
 *
 * <p>
 * The third test is the control that keeps the other two honest: a value inside the cap must stay
 * exact, so a re-check that simply dropped everything — or a seek that stopped matching at all —
 * cannot pass this class.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class CASKeyTruncationTest extends AbstractJsonTest {

  /**
   * Exactly {@code CASKeySerializer.MAX_STRING_VALUE_BYTES}, so the two values differ only past it.
   */
  private static final String SHARED_PREFIX = "A".repeat(246);

  private static final String LONG_VALUE_ONE = SHARED_PREFIX + "X";

  private static final String LONG_VALUE_TWO = SHARED_PREFIX + "Y";

  private static final String DOC = "[{\"title\":\"" + LONG_VALUE_ONE + "\"},{\"title\":\"" + LONG_VALUE_TWO
      + "\"},{\"alias\":\"z\",\"title\":\"short\"}]";

  private static final String STORE = "jn:store('json-path1','mydoc.jn','" + DOC + "')";

  private static final String CREATE_SINGLE_PATH = "let $doc := jn:doc('json-path1','mydoc.jn') "
      + "let $i := jn:create-cas-index($doc,'xs:string','/[]/title') return sdb:commit($doc)";

  private static final String CREATE_MULTI_PATH = "let $doc := jn:doc('json-path1','mydoc.jn') "
      + "let $i := jn:create-cas-index($doc,'xs:string',('/[]/title','/[]/alias')) return sdb:commit($doc)";

  private static String countOfTitle(final String value) {
    return "let $doc := jn:doc('json-path1','mydoc.jn') return fn:count(jn:scan-cas-index($doc,"
        + "jn:find-cas-index($doc,'xs:string','/[]/title'),'" + value + "','==','/[]/title'))";
  }

  @Test
  @DisplayName("a single-path index does not return other values sharing the truncated prefix")
  void singlePathIndexIsExactPastTheTruncationCap() throws IOException {
    test(STORE, CREATE_SINGLE_PATH, countOfTitle(LONG_VALUE_ONE), "1");
  }

  @Test
  @DisplayName("a multi-path index does not either, having returned 0 before the gate relaxation and 2 after")
  void multiPathIndexIsExactPastTheTruncationCap() throws IOException {
    test(STORE, CREATE_MULTI_PATH, countOfTitle(LONG_VALUE_ONE), "1");
  }

  @Test
  @DisplayName("a value inside the bound is unaffected")
  void aValueShorterThanTheBoundIsExact() throws IOException {
    // The control: truncation only bites past the bound, so an ordinary value must still be exact.
    // Without this, a regression that broke equality outright would leave the two tests above green.
    test(STORE, CREATE_SINGLE_PATH, countOfTitle("short"), "1");
  }
}
