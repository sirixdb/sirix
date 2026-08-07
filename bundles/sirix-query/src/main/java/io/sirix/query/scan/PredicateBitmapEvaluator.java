package io.sirix.query.scan;

import io.brackit.query.compiler.optimizer.PredicateNode;

import java.util.List;

/**
 * Evaluates a {@link PredicateNode} into a selection bitmap by bitmap algebra.
 *
 * <h2>Why a bitmap and not a count</h2>
 * A count cannot be composed — two counts cannot be ANDed — so every conjunction had to be fused by
 * hand into a kernel of its own, once per encoding and once more for the liveness-masked variant.
 * That is a cross-product, and it is why an encoding added to the region layer had to be wired into
 * every predicate shape before it could serve anything. A bitmap composes with {@code &},
 * {@code |} and {@code ~}, so the leaf matrix stays {@code (encoding × comparison)} — irreducible,
 * and the only place encoding knowledge belongs — while {@code And}, {@code Or} and {@code Not}
 * become encoding-agnostic and free.
 *
 * <h2>Still no decoding</h2>
 * Composition happening here does NOT mean values are materialised. Each leaf answers in its own
 * encoded domain — a FOR+bit-packed range is a range over the packed integers, a dictionary
 * equality is a code comparison, the boolean column IS a bitmap — which is the BtrBlocks/Umbra
 * position. The one scheme that cannot (ALP-RD splits bit patterns, so it is not order-preserving)
 * decodes that single tag behind {@link LeafSelector}, and nothing here can tell the difference.
 *
 * <h2>Fail-safe</h2>
 * Any leaf the page cannot answer makes the whole evaluation return {@code false}, and the caller
 * keeps its record path. Refusal is the default for an unrecognised node, so a predicate shape
 * nobody taught this evaluator is slow, never wrong.
 */
final class PredicateBitmapEvaluator {

  /**
   * Resolves one leaf of a predicate against a page's columns.
   *
   * <p>The page-specific half — field-to-tag resolution, record-ordinal alignment, completeness —
   * lives behind this, so the algebra above is testable without a page.
   */
  interface LeafSelector {

    /** Rows in the aligned window; every bitmap this evaluator handles covers exactly these. */
    int rows();

    /**
     * Write {@code leaf}'s selection into {@code out}, which is zeroed for {@link #rows()} bits.
     *
     * @return {@code false} when this page cannot answer the leaf, so the caller falls back
     */
    boolean select(PredicateNode leaf, long[] out);
  }

  /** Per-thread stack of scratch bitmaps, one per tree depth; the scan path allocates nothing. */
  private static final ThreadLocal<long[][]> SCRATCH = ThreadLocal.withInitial(() -> new long[0][]);

  private final LeafSelector leaves;
  private final int rows;
  private final int words;
  private long[][] stack;
  private int depth;

  PredicateBitmapEvaluator(final LeafSelector leaves) {
    this.leaves = leaves;
    this.rows = leaves.rows();
    this.words = (rows + 63) >>> 6;
  }

  /**
   * Evaluate {@code node} into {@code out}.
   *
   * @param out destination, at least {@code (rows + 63) / 64} words; contents are overwritten
   * @return {@code false} when the predicate cannot be answered from this page's columns
   */
  boolean evaluate(final PredicateNode node, final long[] out) {
    if (out.length < words) {
      throw new IllegalArgumentException(
          "selection bitmap too small: " + out.length + " words for " + rows + " rows");
    }
    stack = SCRATCH.get();
    depth = 0;
    final boolean ok = eval(node, out);
    SCRATCH.set(stack);  // keep whatever the deepest evaluation grew the pool to
    return ok;
  }

  private boolean eval(final PredicateNode node, final long[] out) {
    return switch (node) {
      case PredicateNode.And a -> combine(a.children(), out, true);
      case PredicateNode.Or o -> combine(o.children(), out, false);
      case PredicateNode.Not n -> {
        if (!eval(n.child(), out)) {
          yield false;
        }
        complement(out);
        yield true;
      }
      case PredicateNode.AlwaysTrue t -> {
        fillRows(out);
        yield true;
      }
      case PredicateNode.AlwaysFalse f -> {
        java.util.Arrays.fill(out, 0, words, 0L);
        yield true;
      }
      // Every remaining shape is a leaf over one field; the page resolves it.
      default -> {
        java.util.Arrays.fill(out, 0, words, 0L);
        yield leaves.select(node, out);
      }
    };
  }

  /**
   * Fold children into {@code out}: intersection for {@code And}, union for {@code Or}.
   *
   * <p>Short-circuits on an empty accumulator, which is where this pays for itself beyond code
   * shape. A conjunction whose first leaf selects nothing never touches the second field's column
   * at all — the same saving PREWHERE buys ClickHouse, falling out of composition rather than
   * needing a planner rule. The hand-fused kernel could not express it: it read every leaf's column
   * up front because the fusion required them all present.
   */
  private boolean combine(final List<PredicateNode> children, final long[] out,
      final boolean intersect) {
    if (children.isEmpty()) {
      // Vacuous: an empty conjunction holds everywhere, an empty disjunction nowhere.
      if (intersect) {
        fillRows(out);
      } else {
        java.util.Arrays.fill(out, 0, words, 0L);
      }
      return true;
    }
    if (!eval(children.get(0), out)) {
      return false;
    }
    if (children.size() == 1) {
      return true;
    }
    final long[] tmp = push();
    try {
      for (int i = 1; i < children.size(); i++) {
        if (intersect && isEmpty(out)) {
          return true;  // nothing can survive; later children are never read
        }
        if (!intersect && isFull(out)) {
          return true;  // everything already selected; later children cannot add
        }
        if (!eval(children.get(i), tmp)) {
          return false;
        }
        if (intersect) {
          for (int w = 0; w < words; w++) {
            out[w] &= tmp[w];
          }
        } else {
          for (int w = 0; w < words; w++) {
            out[w] |= tmp[w];
          }
        }
      }
      return true;
    } finally {
      pop();
    }
  }

  /** Complement within the window; bits past {@code rows} must stay clear. */
  private void complement(final long[] bits) {
    for (int w = 0; w < words; w++) {
      bits[w] = ~bits[w];
    }
    maskTail(bits);
  }

  private void fillRows(final long[] bits) {
    java.util.Arrays.fill(bits, 0, words, -1L);
    maskTail(bits);
  }

  /**
   * Clear the bits above {@code rows} in the last word.
   *
   * <p>Load-bearing rather than tidiness: a stray high bit survives a complement, then an AND, and
   * finally lands in a popcount as a row that does not exist.
   */
  private void maskTail(final long[] bits) {
    final int tail = rows & 63;
    if (tail != 0 && words > 0) {
      bits[words - 1] &= (1L << tail) - 1L;
    }
  }

  private boolean isEmpty(final long[] bits) {
    for (int w = 0; w < words; w++) {
      if (bits[w] != 0L) {
        return false;
      }
    }
    return true;
  }

  private boolean isFull(final long[] bits) {
    final int full = rows >>> 6;
    for (int w = 0; w < full; w++) {
      if (bits[w] != -1L) {
        return false;
      }
    }
    final int tail = rows & 63;
    return tail == 0 || bits[full] == (1L << tail) - 1L;
  }

  private long[] push() {
    if (depth == stack.length) {
      final long[][] grown = new long[depth + 1][];
      System.arraycopy(stack, 0, grown, 0, depth);
      grown[depth] = new long[words];
      stack = grown;
    } else if (stack[depth] == null || stack[depth].length < words) {
      stack[depth] = new long[words];
    }
    return stack[depth++];
  }

  private void pop() {
    depth--;
  }

  /** Rows selected by a finished bitmap. */
  static long popcount(final long[] bits, final int rows) {
    final int words = (rows + 63) >>> 6;
    long c = 0;
    for (int w = 0; w < words; w++) {
      c += Long.bitCount(bits[w]);
    }
    return c;
  }
}
