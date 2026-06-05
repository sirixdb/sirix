package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Regression tests for the JSON path-index optimizer rewrite of a context-item ({@code $$})
 * predicate applied to an unwrapped multi-segment array, e.g.
 * {@code jn:doc(...).store.book[][?$$.price gt 10]}.
 *
 * <p>The bug: {@code AbstractJsonPathWalker} fused the scalar predicate leaf ({@code price}, stored
 * as an {@code OBJECT_NAMED_NUMBER} path node) into the path and then looked that segment up in the
 * path summary as an {@code OBJECT_NAMED_OBJECT}. The kind mismatch made the match come back empty,
 * so {@code pathNodeKeys.isEmpty()} fired the destructive
 * {@code replaceAstNodeWithEmptySequenceAstNode} shortcut — replacing the whole {@code .store.book}
 * deref subtree with an {@code EmptySequenceType}. The optimized plan became
 * {@code ArrayAccess(EmptySequenceType, [])}, which unboxes nothing, so the query returned the empty
 * sequence regardless of the data. It fired UNCONDITIONALLY, even with no index defined.
 *
 * <p>These cases pin the correct rows for the predicate form in three index configurations:
 * <ul>
 *   <li>NO index defined (the shortcut must bail and let normal evaluation run),</li>
 *   <li>a CAS index on the predicate field (the legitimate index rewrite must still return the
 *       correct rows), and</li>
 *   <li>a path index covering the array (index acceleration applies, results stay correct).</li>
 * </ul>
 */
public final class PredicateOverUnwrappedArrayTest extends AbstractJsonTest {

  private static final String STORE =
      "jn:store('json-path1','mydoc.jn','" + "{\"store\":{\"book\":["
          + "{\"title\":\"A\",\"price\":12.5},"
          + "{\"title\":\"B\",\"price\":8.25},"
          + "{\"title\":\"C\",\"price\":42}]}}" + "')";

  /** {@code .store.book[][?$$.price gt 10]} keeps A (12.5) and C (42); drops B (8.25). */
  private static final String PREDICATE_QUERY =
      "jn:doc('json-path1','mydoc.jn').store.book[][?$$.price gt 10]";

  private static final String EXPECTED =
      "{\"title\":\"A\",\"price\":12.5} {\"title\":\"C\",\"price\":42}";

  @Test
  @DisplayName("context-item predicate over unwrapped array — NO index — returns matching rows")
  void predicateOverUnwrappedArrayWithoutIndex() throws IOException {
    test(STORE, PREDICATE_QUERY, EXPECTED);
  }

  @Test
  @DisplayName("context-item predicate over unwrapped array — CAS index on the field — same rows")
  void predicateOverUnwrappedArrayWithCasIndex() throws IOException {
    final String indexQuery =
        "let $doc := jn:doc('json-path1','mydoc.jn') "
            + "let $stats := jn:create-cas-index($doc, 'xs:decimal', '/store/book/[]/price') "
            + "return {\"revision\": sdb:commit($doc)}";
    test(STORE, indexQuery, PREDICATE_QUERY, EXPECTED);
  }

  @Test
  @DisplayName("context-item predicate over unwrapped array — path index on the array — same rows")
  void predicateOverUnwrappedArrayWithPathIndex() throws IOException {
    final String indexQuery =
        "let $doc := jn:doc('json-path1','mydoc.jn') "
            + "let $stats := jn:create-path-index($doc, ('/store/book', '/store/book/[]', '/store/book/[]/price')) "
            + "return {\"revision\": sdb:commit($doc)}";
    test(STORE, indexQuery, PREDICATE_QUERY, EXPECTED);
  }

  @Test
  @DisplayName("context-item predicate with equality on a string field over unwrapped array")
  void predicateStringEqualityOverUnwrappedArray() throws IOException {
    test(STORE, "jn:doc('json-path1','mydoc.jn').store.book[][?$$.title eq \"A\"]",
        "{\"title\":\"A\",\"price\":12.5}");
  }

  @Test
  @DisplayName("context-item predicate with ge keeps boundary row over unwrapped array")
  void predicateGreaterOrEqualOverUnwrappedArray() throws IOException {
    // 12.5 ge 12.5 -> keep A; 42 ge 12.5 -> keep C; 8.25 -> drop B.
    test(STORE, "jn:doc('json-path1','mydoc.jn').store.book[][?$$.price ge 12.5]", EXPECTED);
  }

  @Test
  @DisplayName("context-item predicate over unwrapped array — second revision, after append")
  void predicateOverUnwrappedArrayAfterAppend() throws IOException {
    final String append =
        "append json {\"title\":\"D\",\"price\":99} into jn:doc('json-path1','mydoc.jn').store.book";
    test(STORE, append, PREDICATE_QUERY,
        "{\"title\":\"A\",\"price\":12.5} {\"title\":\"C\",\"price\":42} {\"title\":\"D\",\"price\":99}");
  }
}
