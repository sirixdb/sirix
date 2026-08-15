/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Pins that a CAS index over a NON-string content type answers the value the document holds.
 *
 * <p>
 * The two sides of a CAS index reach {@code CASKeySerializer} in different shapes, and each defect
 * below is one place where they encoded the same logical value to different keys — or two different
 * values to the same key. {@code CASIndexBuilder} stores every indexed value as a {@code Str} (it
 * builds one from the node's lexical form), while {@code ScanCASIndex} casts the query's argument
 * to the index's declared content type, so the probe arrives as a {@code Bool}, {@code Flt},
 * {@code Dbl} or {@code Dec}. Anything in the encoder that behaves differently for a {@code Str}
 * than for the typed atomic splits the two sides apart, and the query silently returns the wrong
 * rows.
 * </p>
 *
 * <p>
 * These are equality queries with unambiguous answers, so each test pins a count against a fixture
 * whose contents are visible above it. All three failed before the encoder fixes:
 * </p>
 * <ul>
 * <li><b>boolean</b> — the encoder called {@code Atomic#booleanValue()}, which is XQuery's
 * EFFECTIVE boolean value. On the stored {@code Str} that is "the string is non-empty", so
 * {@code "false"} encoded to byte 1 exactly as {@code "true"} did: both values shared one key and
 * one posting list. The probe, a real {@code Bool}, encoded {@code false()} to byte 0 and matched
 * nothing, so {@code = false()} returned 0 rows and {@code = true()} returned every boolean
 * node.</li>
 * <li><b>float</b> — the stored {@code Str} took the parse branch and gave
 * {@code Double.parseDouble("1.1")}, while the {@code Flt} probe gave {@code (double) 1.1f}. Those
 * differ in the low bits, so the keys differed and {@code = 1.1} found nothing at all.</li>
 * <li><b>decimal</b> — the equality re-check is gated on {@code CASKeySerializer#losesInformation},
 * which used to ask whether the PROBE was exactly a double. {@code 0.5} is, so the re-check was
 * switched off for it — while a stored value differing only past double precision encodes to that
 * same double and came back as a hit.</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class CASKeyTypedEncodingTest extends AbstractJsonTest {

  private static final String BOOL_DOC = "[{\"flag\":true},{\"flag\":false},{\"flag\":false}]";

  /**
   * {@code 1.1} is the point: it is not exactly representable in binary, so the float and double
   * parses of the same literal disagree. A dyadic value such as {@code 1.5} would have passed
   * throughout and proved nothing.
   */
  private static final String FLOAT_DOC = "[{\"m\":1.1},{\"m\":2.5},{\"m\":1.1}]";

  /**
   * {@code 0.5} is exactly a double — that is what made the old probe-only test report the key
   * lossless and skip the re-check. Its neighbour differs only past double precision, so the two
   * share one key: the index alone cannot separate them and only the re-check can.
   */
  private static final String DECIMAL_DOC = "[{\"p\":0.5},{\"p\":0.5000000000000000001},{\"p\":0.25}]";

  private static String store(final String doc) {
    return "jn:store('json-path1','mydoc.jn','" + doc + "')";
  }

  private static String createIndex(final String type, final String path) {
    return "let $doc := jn:doc('json-path1','mydoc.jn') " + "let $i := jn:create-cas-index($doc,'" + type + "','" + path
        + "') return sdb:commit($doc)";
  }

  private static String countMatching(final String type, final String path, final String probe) {
    return "let $doc := jn:doc('json-path1','mydoc.jn') return fn:count(jn:scan-cas-index($doc,"
        + "jn:find-cas-index($doc,'" + type + "','" + path + "')," + probe + ",'==','" + path + "'))";
  }

  @Test
  @DisplayName("a boolean index separates false from true, having merged both onto one key before")
  void aBooleanIndexSeparatesFalseFromTrue() throws IOException {
    test(store(BOOL_DOC), createIndex("xs:boolean", "/[]/flag"), countMatching("xs:boolean", "/[]/flag", "false()"),
        "2");
  }

  @Test
  @DisplayName("and finds the true one too, so the fix is not simply an inverted encoding")
  void aBooleanIndexStillFindsTrue() throws IOException {
    // The control. Mapping every value onto byte 0 instead of byte 1 would satisfy the test above on
    // its own, and would be just as wrong.
    test(store(BOOL_DOC), createIndex("xs:boolean", "/[]/flag"), countMatching("xs:boolean", "/[]/flag", "true()"),
        "1");
  }

  @Test
  @DisplayName("a float index finds a value whose float and double parses differ")
  void aFloatIndexFindsANonDyadicValue() throws IOException {
    test(store(FLOAT_DOC), createIndex("xs:float", "/[]/m"), countMatching("xs:float", "/[]/m", "xs:float('1.1')"),
        "2");
  }

  @Test
  @DisplayName("a decimal index does not return a neighbour that shares the probe's double")
  void aDecimalIndexIsExactAgainstADoubleCollision() throws IOException {
    // 0.5 and 0.5000000000000000001 encode to the same 8 key bytes, so the seek returns both and only
    // the value re-check can tell them apart. Answering 2 here is the collision going unchecked.
    test(store(DECIMAL_DOC), createIndex("xs:decimal", "/[]/p"),
        countMatching("xs:decimal", "/[]/p", "xs:decimal('0.5')"), "1");
  }

  @Test
  @DisplayName("a decimal index still matches a value that needs no disambiguation")
  void aDecimalIndexStillMatchesAnUncontestedValue() throws IOException {
    // The control for the re-check: 0.25 shares its key with nothing, so a re-check that dropped
    // candidates indiscriminately would show up here rather than in the test above.
    test(store(DECIMAL_DOC), createIndex("xs:decimal", "/[]/p"),
        countMatching("xs:decimal", "/[]/p", "xs:decimal('0.25')"), "1");
  }
}
