package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Hand-computed answers for single-equality nested semi/anti-joins, plus a deterministic work
 * bound.
 */
@Isolated
final class HashMembershipStageTest {
  @TempDir
  Path directory;

  private static final String ROWS = """
      declare variable $outer := [{"id":1,"pid":10},{"id":2,"pid":20},
        {"id":3,"pid":10},{"id":4,"pid":30},{"id":5},{"id":6,"pid":30}];
      declare variable $inner := [{"id":10},{"id":10},{"id":20},{}];
      """;

  @Test
  void antiJoinPreservesMissingKeysAndDuplicateOuterRows() throws Exception {
    assertOptimized("4 5 6", ROWS + "for $a in $outer[] where empty("
        + "for $b in $inner[] where $b.id eq $a.pid return $b.id) return $a.id");
  }

  @Test
  void semiJoinDoesNotMultiplyRowsForDuplicateMatches() throws Exception {
    assertOptimized("1 2 3",
        ROWS + "for $a in $outer[] where exists(" + "for $b in $inner[] where $a.pid eq $b.id return $b) return $a.id");
  }

  @Test
  void someAndNotSomeUseTheSameMembershipSemantics() throws Exception {
    assertOptimized("1 2 3",
        ROWS + "for $a in $outer[] where some $b in $inner[] satisfies $a.pid eq $b.id return $a.id");
    assertOptimized("4 5 6",
        ROWS + "for $a in $outer[] where not(some $b in $inner[] satisfies $b.id eq $a.pid) return $a.id");
  }

  @Test
  void emptyInnerKeepsEveryOuterRowAndEmptyOuterNeverReadsInner() throws Exception {
    assertOptimized("1 1 2",
        "let $inner := () for $a in (1,1,2)" + " where empty(for $b in $inner where $b eq $a return $b) return $a");
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence source = new CountingSequence(3);
      context.bind(new QNm("inner"), source);
      final Query query = new Query(chain, "declare variable $inner external; for $a in ()"
          + " where exists(for $b in $inner where $b eq $a return $b) return $a");
      assertEquals("", answer(query, context));
      assertEquals(0, source.visited);
    }
  }

  @Test
  void stringKeysAndValueComparisonPromotions() throws Exception {
    assertOptimized("b b", "let $inner := ('a', xs:untypedAtomic('a'), xs:anyURI('c'))"
        + " for $a in ('a','b','b','c') where empty(for $b in $inner where $b eq $a return $b) return $a");
    assertOptimized("1 2 3", "let $inner := (1,2,3) for $a in (xs:decimal('1'), xs:float('2'), xs:double('3'), 4)"
        + " where exists(for $b in $inner where $b eq $a return $b) return xs:integer($a)");
    assertOptimized("1 2 3", "let $inner := (xs:decimal('1'), xs:float('2'), xs:double('3'))"
        + " for $a in (1,2,3,4) where exists(for $b in $inner where $b eq $a return $b) return $a");
  }

  @Test
  void numericFallbackKeepsPrecision() throws Exception {
    assertOptimized("16777217 16777216", "let $inner := (xs:float('16777216'))"
        + " for $a in (16777217,16777216,16777218)" + " where some $b in $inner satisfies $b eq $a return $a");
    assertOptimized("9007199254740993",
        "let $inner := (9007199254740992)" + " for $a in (9007199254740992,9007199254740993)"
            + " where empty(for $b in $inner where $b eq $a return $b) return $a");
  }

  @Test
  void nanRetainsTheUnmodifiedEnginesFallback() throws Exception {
    // Brackit's existing numeric comparator treats NaN as equal to NaN. This stage must not
    // introduce its own incompatible equality rules for types it does not hash.
    final String text = "let $inner := (xs:double('NaN'), 1) for $a in (xs:double('NaN'), 1)"
        + " where some $b in $inner satisfies $b eq $a return $a";
    final String baseline;
    System.setProperty(HashMembershipStage.ENABLED_PROPERTY, "false");
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      baseline = answer(new Query(chain, text), context);
    } finally {
      System.clearProperty(HashMembershipStage.ENABLED_PROPERTY);
    }
    assertOptimized(baseline, text);
  }

  @Test
  void nullAndOtherAtomicKeysRetainValueComparison() throws Exception {
    assertOptimized("1", "let $inner := (null, xs:date('2024-01-01'))"
        + " for $a in (null) where some $b in $inner satisfies $b eq $a return 1");
    assertOptimized("2024-01-01",
        "let $inner := (xs:date('2024-01-01'))" + " for $a in (xs:date('2024-01-01'),xs:date('2024-01-02'))"
            + " where some $b in $inner satisfies $b eq $a return $a");
  }

  @Test
  void nullKeysMatchNullKeysAndNothingElse() throws Exception {
    assertOptimized("hit", "let $inner := (1, null, 2) for $a in (null)"
        + " where exists(for $b in $inner where $b eq $a return $b) return 'hit'");
    assertOptimized("", "let $inner := (1, 2) for $a in (null)"
        + " where exists(for $b in $inner where $b eq $a return $b) return 'hit'");
    assertOptimized("miss", "let $inner := (1, 2) for $a in (null)"
        + " where empty(for $b in $inner where $b eq $a return $b) return 'miss'");
    assertOptimized("a", "let $inner := ('b', null) for $a in ('a','b',null)"
        + " where empty(for $b in $inner where $b eq $a return $b) return $a");
    assertOptimized("1 2", "let $inner := (null, null) for $a in (1,2)"
        + " where empty(for $b in $inner where $b eq $a return $b) return $a");
  }

  @Test
  void explicitAndAbsentNullFieldsFollowTheValueComparison() throws Exception {
    final String rows = """
        declare variable $o := [{"id":1,"pid":10},{"id":2,"pid":null},{"id":3,"pid":20},{"id":4}];
        declare variable $i := [{"id":10},{"id":null},{}];
        """;
    assertOptimized("3 4",
        rows + "for $a in $o[] where empty(" + "for $b in $i[] where $b.id eq $a.pid return $b.id) return $a.id");
    assertOptimized("1 2",
        rows + "for $a in $o[] where exists(" + "for $b in $i[] where $b.id eq $a.pid return $b.id) return $a.id");
  }

  @Test
  void aNullInnerKeyKeepsTheHashRouteAndItsWorkBound() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(128, new QNm("id"), true);
      context.bind(new QNm("inner"), inner);
      final Query query = new Query(chain, "declare variable $inner external;"
          + " count(for $a in 1 to 256 where empty(for $b in $inner where $b.id eq $a return $b.id) return $a)");
      assertEquals("128", answer(query, context));
      assertEquals(129, inner.visited, "a null inner key must not revert to the per-row nested plan");
      assertEquals(1, inner.closed);
    }
  }

  @Test
  void aNullProbeKeyKeepsTheHashRouteAndItsWorkBound() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(128, new QNm("id"), true);
      context.bind(new QNm("inner"), inner);
      final Query query = new Query(chain,
          "declare variable $inner external;"
              + " declare variable $outer := [{\"pid\":null},{\"pid\":null},{\"pid\":null},{\"pid\":null}];"
              + " count(for $a in $outer[] where empty(for $b in $inner where $b.id eq $a.pid return $b.id)"
              + " return $a)");
      assertEquals("0", answer(query, context));
      assertEquals(129, inner.visited, "a null probe key must not revert to the per-row nested plan");
      assertEquals(1, inner.closed);
    }
  }

  @Test
  void valueComparisonErrorsAreNotSilentlyTurnedIntoNonMatches() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain,
          "let $inner := (1) for $a in ('one')" + " where some $b in $inner satisfies $b eq $a return $a");
      assertThrows(QueryException.class, () -> answer(query, context));
    }
  }

  @Test
  void speculativeBuildDoesNotExposeAnErrorAfterAnEarlierMatch() throws Exception {
    assertOptimized("1",
        "let $inner := (1, {'id':2}) for $a in (1)" + " where some $b in $inner satisfies $b eq $a return $a");
  }

  @Test
  void groupingAfterAntiJoinRemainsExact() throws Exception {
    assertOptimized("{\"key\":30,\"n\":2}",
        ROWS + "for $a in $outer[]" + " where empty(for $b in $inner[] where $b.id eq $a.pid return $b.id)"
            + " let $key := $a.pid group by $key let $n := count($a)"
            + " where exists($key) return {\"key\":$key,\"n\":$n}");
  }

  @Test
  void lookupIsRebuiltForEachEnclosingBinding() throws Exception {
    assertOptimized("2 3 1 3", "for $i in (1,2) let $inner := ($i) for $a in (1,2,3)"
        + " where empty(for $b in $inner where $b eq $a return $b) return $a");
  }

  @Test
  void generatedBindingCannotShadowAUserVariable() throws Exception {
    assertOptimized("42",
        "declare namespace x = 'https://sirix.io/optimizer/internal';"
            + " declare variable $x:membership0 := 42; let $inner := (1) for $a in (1,2)"
            + " where fn:not(fn:exists(for $b in $inner where $b eq $a return $b)) return $x:membership0");
  }

  @Test
  void multiItemValueKeysStillRaiseTheirCardinalityError() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain,
          "let $inner := ({'id':(1,2)}) for $a in (1)" + " where some $b in $inner satisfies $b.id eq $a return $a");
      assertThrows(QueryException.class, () -> answer(query, context));
    }
  }

  @Test
  void oneBuildPerEvaluationAndNoStateLeaksAcrossRepeatedExecutions() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain, "declare variable $inner external;"
          + " count(for $a in 1 to 256 where exists(for $b in $inner where $b eq $a return $b) return $a)");
      final CountingSequence first = new CountingSequence(128);
      context.bind(new QNm("inner"), first);
      assertEquals("128", answer(query, context));
      assertEquals(128, first.visited, "read each inner key once, independent of 256 outer rows");
      assertEquals(1, first.closed, "the build iterator closes immediately");
      final CountingSequence second = new CountingSequence(64);
      context.bind(new QNm("inner"), second);
      assertEquals("64", answer(query, context));
      assertEquals(64, second.visited, "a compiled query never reuses a prior evaluation's data");
      assertEquals(1, second.closed);
    }
  }

  @Test
  void unsafeShapesKeepTheOriginalPlan() throws Exception {
    assertUnchanged("1 2 3 4 5 6",
        ROWS + "for $a in $outer[] where empty(" + "for $b in $inner[] where $b.id eq $a.pid return ()) return $a.id");
    assertUnchanged("1 2 3 4 5 6", ROWS + "for $a in $outer[] where empty("
        + "for $b in $inner[] where $b.id eq $a.pid return $b.absent) return $a.id");
    assertUnchanged("1 2 3",
        ROWS + "for $a in $outer[] where exists(" + "for $b in $inner[] where $b.id = $a.pid return $b) return $a.id");
    assertUnchanged("1 2", "for $a in ([1],[2]) where some $b in $a[] satisfies $b eq $a[0] return $a[0]");
    assertUnchanged("1 2 3 4 5 6", "declare function local:empty($x) {true()};" + ROWS
        + "for $a in $outer[] where local:empty(for $b in $inner[] where $b.id eq $a.pid return $b) return $a.id");
  }

  private void assertOptimized(final String expected, final String text) throws Exception {
    check(expected, text, true);
  }

  private void assertUnchanged(final String expected, final String text) throws Exception {
    check(expected, text, false);
  }

  private void check(final String expected, final String text, final boolean optimized) throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain, text);
      assertEquals(expected, answer(query, context));
      assertEquals(optimized, containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  private BasicJsonDBStore store() {
    return BasicJsonDBStore.newBuilder().location(directory).build();
  }

  private static SirixCompileChain chain(final BasicJsonDBStore store) {
    return SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
  }

  private static String answer(final Query query, final SirixQueryContext context) throws Exception {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      query.serialize(context, writer);
    }
    return out.toString().trim();
  }

  private static boolean containsProbe(final AST node) {
    if (node.getType() == XQExt.MembershipProbeExpr) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (containsProbe(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private static final class CountingSequence extends LazySequence {
    private final int total;
    private final boolean trailingNull;
    private final QNm[] names;
    private int visited;
    private int closed;

    private CountingSequence(final int size) {
      this(size, null, false);
    }

    private CountingSequence(final int size, final QNm field, final boolean trailingNull) {
      this.total = trailingNull
          ? size + 1
          : size;
      this.trailingNull = trailingNull;
      this.names = field == null
          ? null
          : new QNm[] {field};
    }

    @Override
    public Iter iterate() {
      return new BaseIter() {
        private int position;

        @Override
        public Item next() {
          if (position == total) {
            return null;
          }
          visited++;
          position++;
          final Item key = trailingNull && position == total
              ? Null.INSTANCE
              : new Int32(position);
          return names == null
              ? key
              : new ArrayObject(names, new Sequence[] {key});
        }

        @Override
        public void close() {
          closed++;
        }
      };
    }
  }
}
