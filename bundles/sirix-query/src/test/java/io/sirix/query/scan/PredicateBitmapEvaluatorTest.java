package io.sirix.query.scan;

import io.brackit.query.compiler.optimizer.PredicateNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bitmap algebra, checked against a brute-force interpreter of the same predicate.
 *
 * <p>Deliberately free of pages and columns: {@link PredicateBitmapEvaluator.LeafSelector} is
 * stubbed with pre-decided per-row answers, so this pins the part that must be right for EVERY
 * encoding — composition — without any storage in the way. What the leaves do in their own encoded
 * domains is covered by the region-level equivalence tests.
 */
@DisplayName("predicate bitmap algebra")
final class PredicateBitmapEvaluatorTest {

  /**
   * A real leaf node, keyed by field name. {@link PredicateNode} is sealed — deliberately, since
   * its leaf kinds are the ones the engine must resolve — so the stub varies the ANSWER, not the
   * node type: the selector below decides each field's rows.
   */
  private static PredicateNode leaf(final String name) {
    return new PredicateNode.NumCmp(name, "gt", 0L);
  }

  private static String fieldOf(final PredicateNode node) {
    return ((PredicateNode.NumCmp) node).field();
  }

  private static final class Leaves implements PredicateBitmapEvaluator.LeafSelector {
    private final int rows;
    private final java.util.Map<String, boolean[]> answers = new java.util.HashMap<>();
    private final java.util.Set<String> refusing = new java.util.HashSet<>();
    private int selects;

    Leaves(final int rows) {
      this.rows = rows;
    }

    PredicateNode define(final String name, final boolean[] rows) {
      answers.put(name, rows);
      return leaf(name);
    }

    PredicateNode refuse(final String name) {
      refusing.add(name);
      return leaf(name);
    }

    boolean[] rowsOf(final String name) {
      return answers.get(name);
    }

    @Override
    public int rows() {
      return rows;
    }

    @Override
    public boolean select(final PredicateNode leaf, final long[] out) {
      selects++;
      final String name = fieldOf(leaf);
      if (refusing.contains(name)) {
        return false;
      }
      final boolean[] r = answers.get(name);
      for (int i = 0; i < rows; i++) {
        if (r[i]) {
          out[i >>> 6] |= 1L << (i & 63);
        }
      }
      return true;
    }
  }

  private static boolean[] randomRows(final Random rng, final int n, final double p) {
    final boolean[] r = new boolean[n];
    for (int i = 0; i < n; i++) {
      r[i] = rng.nextDouble() < p;
    }
    return r;
  }

  /** Brute-force truth for one row, mirroring the evaluator's semantics exactly. */
  private static boolean truth(final PredicateNode node, final int row, final Leaves leaves) {
    return switch (node) {
      case PredicateNode.And a -> {
        for (final PredicateNode c : a.children()) {
          if (!truth(c, row, leaves)) {
            yield false;
          }
        }
        yield true;
      }
      case PredicateNode.Or o -> {
        for (final PredicateNode c : o.children()) {
          if (truth(c, row, leaves)) {
            yield true;
          }
        }
        yield false;
      }
      case PredicateNode.Not n -> !truth(n.child(), row, leaves);
      case PredicateNode.AlwaysTrue t -> true;
      case PredicateNode.AlwaysFalse f -> false;
      default -> leaves.rowsOf(fieldOf(node))[row];
    };
  }

  private static void assertMatchesTruth(final PredicateNode node, final Leaves leaves) {
    final int rows = leaves.rows();
    final long[] out = new long[(rows + 63) >>> 6];
    assertTrue(new PredicateBitmapEvaluator(leaves).evaluate(node, out), "must be answerable");
    for (int i = 0; i < rows; i++) {
      final boolean got = (out[i >>> 6] & 1L << (i & 63)) != 0L;
      assertEquals(truth(node, i, leaves), got, "row " + i + " of " + node);
    }
  }

  @Test
  @DisplayName("random trees agree with a brute-force interpreter, row for row")
  void randomTreesMatchBruteForce() {
    final Random rng = new Random(0xBEEF);
    // 100 rows: not a multiple of 64, so every trial exercises the partial tail word.
    final int rows = 100;
    for (int trial = 0; trial < 500; trial++) {
      final Leaves leaves = new Leaves(rows);
      final PredicateNode tree = randomTree(rng, rows, 0, leaves, new int[] { 0 });
      assertMatchesTruth(tree, leaves);
    }
  }

  private PredicateNode randomTree(final Random rng, final int rows, final int depth,
      final Leaves leaves, final int[] seq) {
    if (depth >= 3 || rng.nextInt(3) == 0) {
      return switch (rng.nextInt(6)) {
        case 0 -> PredicateNode.AlwaysTrue.INSTANCE;
        case 1 -> PredicateNode.AlwaysFalse.INSTANCE;
        default -> leaves.define("f" + seq[0]++, randomRows(rng, rows, rng.nextDouble()));
      };
    }
    final int kids = 1 + rng.nextInt(3);
    final List<PredicateNode> children = new ArrayList<>(kids);
    for (int i = 0; i < kids; i++) {
      children.add(randomTree(rng, rows, depth + 1, leaves, seq));
    }
    return switch (rng.nextInt(3)) {
      case 0 -> PredicateNode.and(children);
      case 1 -> PredicateNode.or(children);
      default -> new PredicateNode.Not(children.get(0));
    };
  }

  @Test
  @DisplayName("a complement never invents rows past the window")
  void complementMasksTheTail() {
    // 5 rows in a 64-bit word: NOT(nothing) must select 5, not 64.
    final Leaves leaves = new Leaves(5);
    final long[] out = new long[1];
    final PredicateNode none = leaves.define("none", new boolean[5]);
    assertTrue(new PredicateBitmapEvaluator(leaves).evaluate(new PredicateNode.Not(none), out));
    assertEquals(5L, PredicateBitmapEvaluator.popcount(out, 5),
                 "complement leaked bits past the row window");
  }

  @Test
  @DisplayName("a refusing leaf refuses the whole predicate, at any depth")
  void refusalPropagates() {
    final Leaves leaves = new Leaves(64);
    final long[] out = new long[1];
    final PredicateNode refuse = leaves.refuse("refuse");
    final PredicateNode ok = leaves.define("ok", randomRows(new Random(1), 64, 0.5));
    for (final PredicateNode tree : List.of(refuse,
                                            new PredicateNode.Not(refuse),
                                            PredicateNode.and(List.of(ok, refuse)),
                                            PredicateNode.or(List.of(ok, refuse)),
                                            PredicateNode.and(List.of(PredicateNode.or(
                                                List.of(ok, refuse)), ok)))) {
      assertFalse(new PredicateBitmapEvaluator(leaves).evaluate(tree, out),
                  "refusal must propagate out of " + tree);
    }
  }

  @Test
  @DisplayName("an empty conjunction short-circuits before reading later leaves")
  void conjunctionShortCircuits() {
    // This is PREWHERE falling out of composition: the second field's column is never touched.
    final Leaves leaves = new Leaves(128);
    final long[] out = new long[2];
    final PredicateNode empty = leaves.define("empty", new boolean[128]);
    final PredicateNode other = leaves.define("other", randomRows(new Random(2), 128, 0.9));
    assertTrue(new PredicateBitmapEvaluator(leaves)
                   .evaluate(PredicateNode.and(List.of(empty, other)), out));
    assertEquals(0L, PredicateBitmapEvaluator.popcount(out, 128));
    assertEquals(1, leaves.selects, "the second leaf must not have been read");
  }

  @Test
  @DisplayName("a saturated disjunction short-circuits too")
  void disjunctionShortCircuits() {
    final Leaves leaves = new Leaves(128);
    final long[] out = new long[2];
    final boolean[] all = new boolean[128];
    java.util.Arrays.fill(all, true);
    final PredicateNode full = leaves.define("full", all);
    final PredicateNode other = leaves.define("other", randomRows(new Random(3), 128, 0.5));
    assertTrue(new PredicateBitmapEvaluator(leaves)
                   .evaluate(PredicateNode.or(List.of(full, other)), out));
    assertEquals(128L, PredicateBitmapEvaluator.popcount(out, 128));
    assertEquals(1, leaves.selects, "the second leaf must not have been read");
  }

  @Test
  @DisplayName("nesting reuses scratch rather than allocating per node")
  void deepNestingIsStable() {
    final Leaves leaves = new Leaves(200);
    final long[] out = new long[4];
    PredicateNode tree = leaves.define("l", randomRows(new Random(4), 200, 0.5));
    for (int i = 0; i < 12; i++) {
      tree = PredicateNode.and(
          List.of(tree, leaves.define("l" + i, randomRows(new Random(i), 200, 0.7))));
    }
    final PredicateBitmapEvaluator evaluator = new PredicateBitmapEvaluator(leaves);
    assertTrue(evaluator.evaluate(tree, out));
    final long[] first = out.clone();
    // A second run on the same evaluator must reuse the pool and reproduce the answer exactly.
    assertTrue(evaluator.evaluate(tree, out));
    assertArrayEquals(first, out, "scratch reuse changed the answer");
  }
}
