package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Regression tests for the JSON path-index optimizer's array-index handling
 * ({@code AbstractJsonPathWalker}/{@code JsonCASStep}). It blindly cast the index AST value to
 * {@code Int32}, so any index that is not a small integer literal — a value ≥ 2^31, a decimal, a
 * function call, etc. — threw a {@code ClassCastException} during compilation (HTTP 500), even with
 * no index defined and for single-segment / top-level queries. The walker now bails out of the
 * rewrite for a non-literal-integer index and lets normal evaluation run.
 */
public final class ArrayIndexAccessOptimizerTest extends AbstractJsonTest {

  private static final String STORE = "jn:store('json-path1','mydoc.jn','{\"a\":{\"b\":[10,20,30,40,50]}}')";

  @Test
  @DisplayName("small integer literal index still works")
  void smallIntIndex() throws IOException {
    test(STORE, "jn:doc('json-path1','mydoc.jn').a.b[2]", "30");
  }

  @Test
  @DisplayName("index >= 2^31 must not crash — out of bounds yields empty")
  void largeIntIndex() throws IOException {
    test(STORE, "jn:doc('json-path1','mydoc.jn').a.b[3000000000]", "");
  }

  @Test
  @DisplayName("function-call index must not crash and selects the right element")
  void functionCallIndex() throws IOException {
    // count((1,2)) = 2 -> element at index 2 = 30
    test(STORE, "jn:doc('json-path1','mydoc.jn').a.b[count((1,2))]", "30");
  }

  @Test
  @DisplayName("top-level array with a large index must not crash")
  void topLevelLargeIndex() throws IOException {
    test("jn:store('json-path1','mydoc.jn','[10,20,30]')",
         "jn:doc('json-path1','mydoc.jn')[3000000000]", "");
  }
}
