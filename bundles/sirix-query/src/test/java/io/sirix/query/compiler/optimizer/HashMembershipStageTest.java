package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.atomic.Bool;
import io.brackit.query.expr.BoundVariable;
import io.brackit.query.expr.DeclVariable;
import io.brackit.query.expr.PipeExpr;
import io.brackit.query.expr.SequenceExpr;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.operator.ForBind;
import io.brackit.query.operator.Start;
import io.brackit.query.operator.TupleImpl;
import io.sirix.query.compiler.operator.HashMembershipJoin;
import io.sirix.query.compiler.operator.HashMembershipJoin.Binding;
import org.junit.jupiter.api.Disabled;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.array.AbstractArray;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.AbstractObject;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.PrintWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Hand-computed answers for single-equality nested semi/anti-joins, plus a deterministic work
 * bound.
 */
@Isolated
final class HashMembershipStageTest {
  @TempDir
  Path directory;

  private static final String GROUP_BY_BUDGET_PROPERTY = "io.brackit.query.groupby.memory_budget";

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
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
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
    assertOptimized("hit", "let $inner := (null) for $a in (null)"
        + " where exists(for $b in $inner where $b eq $a return $b) return 'hit'");
    assertOptimized("", "let $inner := (null) for $a in (null)"
        + " where empty(for $b in $inner where $b eq $a return $b) return 'miss'");
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
      final CountingSequence inner = new CountingSequence(128, 1, new QNm("id"));
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
      final CountingSequence inner = new CountingSequence(128, 1, new QNm("id"));
      context.bind(new QNm("inner"), inner);
      final Query query = new Query(chain,
          "declare variable $inner external;"
              + " declare variable $outer := [{\"pid\":null},{\"pid\":null},{\"pid\":null},{\"pid\":null}];"
              + " count(for $a in $outer[] where empty(for $b in $inner where $b.id eq $a.pid return $b.id)"
              + " return $a)");
      assertEquals("0", answer(query, context));
      assertEquals(129, inner.visited, "a null probe key must not revert to the per-row nested plan");
      assertEquals(1, inner.closed, "the scan closes its iterator when the result iterator closes");
      assertEquals(inner.opened, inner.closed, "every inner iterator opened is closed");
    }
  }

  @Test
  void aNullOnlyBuildSideKeepsTheHashRouteAndItsWorkBound() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(0, 128, new QNm("id"));
      context.bind(new QNm("inner"), inner);
      final Query query = new Query(chain, "declare variable $inner external;"
          + " declare variable $outer := [{\"id\":1,\"pid\":null},{\"id\":2,\"pid\":7},"
          + "{\"id\":3,\"pid\":null},{\"id\":4}];"
          + " for $a in $outer[] where empty(for $b in $inner where $b.id eq $a.pid return $b.id)" + " return $a.id");
      assertEquals("2 4", answer(query, context));
      assertEquals(128, inner.visited, "a build side of only null keys must stay on the hash route");
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
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
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
  void aNamespacedGlobalIsUnaffectedByTheRewrite() throws Exception {
    assertOptimized("42",
        "declare namespace x = 'https://sirix.io/optimizer/internal';"
            + " declare variable $x:membership0 := 42; let $inner := (1) for $a in (1,2)"
            + " where fn:not(fn:exists(for $b in $inner where $b eq $a return $b)) return $x:membership0");
  }

  @Test
  void aSpillingGroupByAfterAnAntiJoinCompletes() throws Exception {
    // The lookup is not a serializable JDM item, so a tuple slot holding it would kill the query
    // here: SpillableGroupBy writes every slot of every tuple it partitions.
    System.setProperty(GROUP_BY_BUDGET_PROPERTY, "1");
    try {
      assertOptimized("3 4 5",
          "let $inner := (1,2) for $a in (1,2,3,4,5)" + " where empty(for $b in $inner where $b eq $a return $b)"
              + " let $key := $a group by $key order by $key return $key");
    } finally {
      System.clearProperty(GROUP_BY_BUDGET_PROPERTY);
    }
  }

  @Test
  void aSpillingGroupByKeepsExactCountsForManyGroups() throws Exception {
    System.setProperty(GROUP_BY_BUDGET_PROPERTY, "1");
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain,
          "let $inner := (1 to 64) for $a in 1 to 256" + " where empty(for $b in $inner where $b eq $a return $b)"
              + " let $key := $a mod 8 group by $key let $n := count($a) return $n");
      // 192 surviving rows (65..256) spread over the eight residue classes: 65..256 contains 24
      // members of each residue modulo 8.
      assertEquals("24 24 24 24 24 24 24 24", answer(query, context));
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    } finally {
      System.clearProperty(GROUP_BY_BUDGET_PROPERTY);
    }
  }

  @Test
  void aSpillingOrderByAfterAnAntiJoinCompletes() throws Exception {
    // Brackit sizes the sort budget as Runtime.maxMemory()/4 with no property override, so this
    // only runs under a focused small-heap invocation; see docs/QUERY_MEMBERSHIP_OPTIMIZATION.md.
    final long budget = Runtime.getRuntime().maxMemory() / 4;
    assumeTrue(budget <= 128L * 1024 * 1024,
        "needs -PtestHeapMin=128m -PtestHeapMax=256m so Runtime.maxMemory()/4 is reachable");
    final int rows = 1700;
    // TupleSerializer.estimateSize charges 4 + 2*length per string slot, so pad each surviving row
    // past the budget while the real Latin-1 payload stays about half of that.
    final int pad = (int) (budget / rows) + 4096;
    final String text = "declare variable $pad external;" + " let $inner := (1,2) for $a in 1 to " + rows
        + " where empty(for $b in $inner where $b eq $a return $b)"
        + " let $row := concat($pad, string($a)) order by $a descending return $a";
    final StringBuilder expected = new StringBuilder();
    for (int value = rows; value >= 3; value--) {
      expected.append(expected.isEmpty()
          ? ""
          : " ").append(value);
    }

    // The sort really spills: TupleSort creates its run files in java.io.tmpdir, so a directory it
    // cannot write to turns the spill, and only the spill, into a failure.
    final Path blocked = Files.createDirectories(directory.resolve("blocked-sort"));
    assertTrue(blocked.toFile().setWritable(false), "the sort directory must be made non-writable");
    // Root bypasses the permission bit, so it is the environment that gets skipped, not the engine.
    assumeTrue(!Files.isWritable(blocked), "the sort directory is still writable; running as root?");
    final String tmpdir = System.getProperty("java.io.tmpdir");
    System.setProperty("java.io.tmpdir", blocked.toString());
    try {
      // TupleSort.writeRun wraps the failing File.createTempFile as a QueryException over an
      // IOException, so only a real spill can produce this; any other failure is not a spill.
      final QueryException spill =
          assertThrows(QueryException.class, () -> orderBySpill(text, pad, null), "the sort did not spill");
      assertInstanceOf(IOException.class, rootCause(spill), "the failure did not come from the sort run file");
    } finally {
      System.setProperty("java.io.tmpdir", tmpdir);
      blocked.toFile().setWritable(true);
    }

    orderBySpill(text, pad, expected.toString());
  }

  private static Throwable rootCause(final Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  private void orderBySpill(final String text, final int pad, final String expected) throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      context.bind(new QNm("pad"), new Str("x".repeat(pad)));
      final Query query = new Query(chain, text);
      final String answer = answer(query, context);
      if (expected != null) {
        assertEquals(expected, answer);
        assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
      }
    }
  }

  @Test
  void anOrderByAfterAnAntiJoinCompletesAndKeepsTheRoute() throws Exception {
    assertOptimized("5 4 3", "let $inner := (1,2) for $a in (3,5,4)"
        + " where empty(for $b in $inner where $b eq $a return $b)" + " order by $a descending return $a");
  }

  @Test
  void multiItemValueKeysStillRaiseTheirCardinalityError() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain,
          "let $inner := ({'id':(1,2)}) for $a in (1)" + " where some $b in $inner satisfies $b.id eq $a return $a");
      assertThrows(QueryException.class, () -> answer(query, context));
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void outerKeyErrorsAreNotSwallowedWhenNoTypedKeysWereBuilt() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain, "let $inner := ({'x':1}) for $a in ({'id':(1,2)})"
          + " where exists(for $b in $inner where $b.id eq $a.id return $b.id) return 'hit'");
      assertThrows(QueryException.class, () -> answer(query, context));
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void oneBuildPerEvaluationAndNoStateLeaksAcrossRepeatedExecutions() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain, "declare variable $inner external;"
          + " count(for $a in 1 to 256 where empty(for $b in $inner where $b eq $a return $b) return $a)");
      final CountingSequence first = new CountingSequence(128);
      context.bind(new QNm("inner"), first);
      assertEquals("128", answer(query, context));
      assertEquals(128, first.visited, "read each inner key once, independent of 256 outer rows");
      assertEquals(1, first.closed, "the build iterator closes immediately");
      final CountingSequence second = new CountingSequence(64);
      context.bind(new QNm("inner"), second);
      assertEquals("192", answer(query, context));
      assertEquals(64, second.visited, "a compiled query never reuses a prior evaluation's data");
      assertEquals(1, second.closed);
      assertEquals(first.opened + second.opened, first.closed + second.closed, "no iterator is left open");
    }
  }

  @Test
  void aSemiJoinProbeStopsAtTheFirstMatchingInnerKey() throws Exception {
    // Without the incremental build this drains all 1,000,000 rows into a hash set before the first
    // probe can answer, where the unoptimized plan streams the inner side and stops at the match.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(1_000_000);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;" + " let $h := $src for $a in (1)"
          + " where exists(for $b in $h where $b eq $a return $b) return $a");
      assertEquals("1", answer(query, context));
      assertEquals(1, inner.visited, "the scan stops at the first matching key");
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void mixedHitAndMissSemiProbesNeverReadMoreThanTheOriginalPlan() throws Exception {
    // Inner keys are 1..8. Probe 3 reads rows 1..3 and stops at its match, retaining keys 1..3.
    // Probe 9 finishes the same scan. Every later probe is answered without reading a row.
    // Original: 3 + 8 + 1 + 8 + 8 + 5. Cursor-owned hash join: 3 + 5.
    final String text = "declare variable $src external;" + " let $h := $src for $a in (3, 9, 1, 8, 42, 5)"
        + " where exists(for $b in $h where $b eq $a return $b) return $a";
    final CountingSequence withRule = new CountingSequence(8);
    assertEquals("3 1 8 5", semiJoinAnswer(text, withRule, true));
    final CountingSequence withoutRule = new CountingSequence(8);
    assertEquals("3 1 8 5", semiJoinAnswer(text, withoutRule, false));
    assertEquals(33, withoutRule.visited, "the original plan scans until each probe's match");
    assertEquals(8, withRule.visited, "all probes share one resumable inner scan");
  }

  @Test
  void anEmptyInnerRelationIsReadOnceNotOncePerOuterRow() throws Exception {
    // The scan is entered at most once per lookup, so an empty inner side is opened once and every
    // later probe is answered from the terminal state without re-entering it.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(0);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;" + " count(let $h := $src for $a in 1 to 64"
          + " where empty(for $b in $h where $b eq $a return $b) return $a)");
      assertEquals("64", answer(query, context));
      assertEquals(1, inner.opened, "the empty inner side is iterated once for all 64 outer rows");
      assertEquals(1, inner.closed);
      assertEquals(0, inner.visited);
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void aFailedExecutionBuildsAfreshOnRetry() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final ThrowingCloseSequence inner = new ThrowingCloseSequence(64, 2);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;"
          + " for $a in (1,65,1) where empty(for $b in $src where $b eq $a return $b) return $a");
      final Exception failure = assertThrows(Exception.class, () -> answer(query, context));
      assertInstanceOf(IllegalStateException.class, rootCause(failure));
      assertEquals(1, inner.visited);
      assertEquals("65", answer(query, context));
      assertEquals(65, inner.visited, "the retry builds its own complete lookup");
      assertEquals(2, inner.opened);
    }
  }

  @Test
  void anAbruptArrayFailureBuildsAfreshOnRetry() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final FailingOnceArray inner = new FailingOnceArray(4, 2);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;" + " let $h := $src for $a in (1,2,3,4,5)"
          + " where empty(for $b in $h[] where $b eq $a return $b) return $a");
      final Exception failure = assertThrows(Exception.class, () -> answer(query, context));
      assertInstanceOf(IllegalStateException.class, rootCause(failure));
      assertEquals("5", answer(query, context));
      assertEquals(6, inner.reads, "the failed second read, then a fresh four-key build");
      assertTrue(containsProbe(chain.getOptimizedAST()));
    }
  }

  @Test
  void aMutableDatabaseBindingIsNeverReusedAcrossExecutions() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixQueryContext secondContext = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("membership", "record", "{\"id\":1}");
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession("record");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final JsonDBObject inner = new JsonDBObject(trx, collection);
        final QNm id = new QNm("id");
        context.bind(new QNm("src"), inner);
        secondContext.bind(new QNm("src"), inner);
        final String[] predicates = {"empty(for $b in $src where $b.id eq $a return $b.id)",
            "exists(for $b in $src where $b.id eq $a return $b.id)",
            "not(exists(for $b in $src where $b.id eq $a return $b.id))",
            "not(empty(for $b in $src where $b.id eq $a return $b.id))", "some $b in $src satisfies $b.id eq $a",
            "not(some $b in $src satisfies $b.id eq $a)"};
        final boolean[] anti = {true, false, true, false, false, true};
        try {
          for (int i = 0; i < predicates.length; i++) {
            inner.replace(id, Int32.ONE);
            final Query query = new Query(chain,
                "declare variable $src external; for $a in (1,2) where " + predicates[i] + " return $a");
            assertEquals(anti[i]
                ? "2"
                : "1", answer(query, context), predicates[i]);
            inner.replace(id, new Int32(2));
            assertEquals(anti[i]
                ? "1"
                : "2", answer(query, secondContext), predicates[i]);
            assertEquals(anti[i]
                ? "1"
                : "2", answer(query, context), predicates[i]);
          }
        } finally {
          trx.rollback();
        }
      }
    }
  }

  @Test
  @Disabled("Brackit Analyzer.checkCycle NPE compiling a declared FLWOR; covered through DeclVariable/PipeExpr directly")
  void aRetainedDeclaredLazyResultStartsAFreshCursorOnEveryIteration() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingRecords inner = new CountingRecords(1, new QNm("id"));
      final Query query =
          new Query(chain, "declare variable $src := ({'id':1});" + " declare variable $r := (for $a in (1,2)"
              + " where empty(for $b in $src where $b.id eq $a return $b.id) return $a); ($r, $r)");
      // Supply the same declared value through counted records to observe each physical build.
      context.bind(new QNm("src"), inner);
      assertEquals("2 2", answer(query, context));
      assertEquals(2, inner.keyReads, "each iteration of the retained lazy value builds afresh");
      assertEquals("2 2", answer(query, context));
      assertEquals(4, inner.keyReads);
    }
  }

  @Test
  void aRetainedDeclaredPipeSequenceCreatesIndependentCursors() {
    try (final BasicJsonDBStore store = store();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingRecords records = new CountingRecords(1, new QNm("id"));
      final DeclVariable source = new DeclVariable(new QNm("src"), SequenceType.ITEM_SEQUENCE);
      context.bind(source.getName(), records);
      final BoundVariable outer = new BoundVariable(new QNm("a"), 0);
      final ForBind loop = new ForBind(new Start(), new SequenceExpr(Int32.ONE, new Int32(2)), false);
      final HashMembershipJoin join =
          new HashMembershipJoin(loop, source, new Binding(source), outer, Bool.FALSE, new QNm("id"), true);
      final DeclVariable retained = new DeclVariable(new QNm("r"), SequenceType.ITEM_SEQUENCE);
      retained.setExpr(new PipeExpr(join, outer));
      final Sequence result = retained.evaluate(context, TupleImpl.EMPTY_TUPLE);
      assertEquals(List.of(new Int32(2)), items(result));
      assertEquals(List.of(new Int32(2)), items(result));
      assertEquals(2, records.keyReads, "each iteration opens its own cursor and builds its own table");
      records.offset = 1;
      assertEquals(List.of(Int32.ONE), items(retained.evaluate(context, TupleImpl.EMPTY_TUPLE)));
      assertEquals(3, records.keyReads, "the retained result cannot carry keys from an earlier iteration");
      try (final Iter abandoned = result.iterate()) {
        assertEquals(Int32.ONE, abandoned.next());
        records.offset = 0;
        assertEquals(List.of(new Int32(2)), items(result));
      }
      assertEquals(5, records.keyReads);
    }
  }

  @Test
  void aFunctionReturningTheJoinBuildsAgainOnEachInvocation() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingRecords inner = new CountingRecords(1, new QNm("id"));
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain,
          "declare variable $src external;" + " declare function local:filtered($s) {for $a in (1,2)"
              + " where empty(for $b in $s where $b.id eq $a return $b.id) return $a};"
              + " (local:filtered($src), local:filtered($src))");
      assertEquals("2 2", answer(query, context));
      assertEquals(2, inner.keyReads);
      inner.offset = 1;
      assertEquals("1 1", answer(query, context));
      assertEquals(4, inner.keyReads);
    }
  }

  private static List<Item> items(final Sequence sequence) {
    final List<Item> result = new ArrayList<>();
    try (final Iter iter = sequence.iterate()) {
      Item item;
      while ((item = iter.next()) != null) {
        result.add(item);
      }
    }
    return result;
  }

  @Test
  void nestedOuterLoopsShareAnIdentityStableBinding() throws Exception {
    assertOneInnerBuild(
        "declare variable $src external; count(let $inner := $src" + " for $x in 1 to 8 for $a in 129 to 132"
            + " where empty(for $b in $inner where $b eq $a return $b) return $a)",
        "32");
    assertOneInnerBuild("declare variable $src external; count(for $x in 1 to 8 for $a in 129 to 132"
        + " where empty(for $b in $src where $b eq $a return $b) return $a)", "32");
  }

  @Test
  void closingAPartiallyConsumedResultClosesItsInnerScan() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(1_000_000);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external; for $a in (1,2)"
          + " where exists(for $b in $src where $b eq $a return $b) return $a");
      try (final Iter result = query.execute(context).iterate()) {
        assertEquals(Int32.ONE, result.next());
        assertEquals(1, inner.visited);
        assertEquals(0, inner.closed, "the live cursor owns the resumable scan");
      }
      assertEquals(1, inner.closed);
      assertEquals("1 2", answer(query, context));
      assertEquals(3, inner.visited, "a new cursor starts its own scan");
      assertEquals(2, inner.closed);
    }
  }

  @Test
  void interleavedAndAbandonedExecutionsHaveIndependentLookups() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(64);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external; for $a in (65,66)"
          + " where empty(for $b in $src where $b eq $a return $b) return $a");
      try (final Iter first = query.execute(context).iterate(); final Iter second = query.execute(context).iterate()) {
        assertEquals(new Int32(65), first.next());
        assertEquals(new Int32(65), second.next());
        assertEquals(new Int32(66), first.next());
        assertEquals(128, inner.visited);
      }
      assertEquals("65 66", answer(query, context));
      assertEquals(192, inner.visited);
      assertEquals(3, inner.opened);
      assertEquals(3, inner.closed);
    }
  }

  @Test
  void anEmptyInnerStillSuppressesOuterFieldFailures() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingRecords outer = new CountingRecords(1, new QNm("id"));
      outer.failKeys = true;
      context.bind(new QNm("outer"), outer);
      context.bind(new QNm("inner"), new CountingSequence(0));
      final Query query = new Query(chain, "declare variable $outer external; declare variable $inner external;"
          + " for $a in $outer where empty(for $b in $inner where $b eq $a.id return $b) return 1");
      assertEquals("1", answer(query, context));
      context.bind(new QNm("inner"), new CountingSequence(1));
      assertThrows(IllegalStateException.class, () -> answer(query, context));
    }
  }

  @Test
  void unsupportedNumericProbesKeepTheOriginalEarlyExit() throws Exception {
    final String[] predicates = {"empty(for $b in $src where $b eq $a return $b)",
        "exists(for $b in $src where $b eq $a return $b)", "not(exists(for $b in $src where $b eq $a return $b))",
        "not(empty(for $b in $src where $b eq $a return $b))", "some $b in $src satisfies $b eq $a",
        "not(some $b in $src satisfies $b eq $a)"};
    for (final String predicate : predicates) {
      final String text = "declare variable $src external; for $a in xs:double(1) where " + predicate + " return 1";
      final CountingSequence original = new CountingSequence(1_000_000);
      final String expected = semiJoinAnswer(text, original, false);
      final CountingSequence optimized = new CountingSequence(1_000_000);
      assertEquals(expected, semiJoinAnswer(text, optimized, true), predicate);
      assertEquals(1, optimized.visited, predicate);
      assertEquals(original.visited, optimized.visited, predicate);
      assertEquals(optimized.opened, optimized.closed, predicate);
    }
  }

  @Test
  void arrayBindingsKeepNumericEarlyExitAndExecutionIsolation() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final FailingOnceArray inner = new FailingOnceArray(64, Integer.MAX_VALUE);
      context.bind(new QNm("src"), inner);
      final Query numeric = new Query(chain, "declare variable $src external; let $h := $src"
          + " for $a in (xs:double(1),1,1) where some $b in $h[] satisfies $b eq $a return 1");
      assertEquals("1 1 1", answer(numeric, context));
      assertEquals(2, inner.reads);
      final Query anti = new Query(chain, "declare variable $src external; let $h := $src"
          + " for $a in (1,65) where empty(for $b in $h[] where $b eq $a return $b) return $a");
      assertEquals("65", answer(anti, context));
      assertEquals(66, inner.reads);
      inner.replaceAt(0, new Int32(100));
      assertEquals("1 65", answer(anti, context));
      assertEquals(130, inner.reads);
    }
  }

  @Test
  void blockWorkersShareOneBuildPerExecution() throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createParallel(context.getNodeStore(), store)) {
      final CountingSequence inner = new CountingSequence(64);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external; count(for $a in 1 to 256"
          + " where empty(for $b in $src where $b eq $a return $b) return $a)");
      assertEquals("192", answer(query, context));
      assertEquals(64, inner.visited);
      assertEquals("192", answer(query, context));
      assertEquals(128, inner.visited);
      assertEquals(2, inner.opened);
      assertEquals(2, inner.closed);
    }
  }

  @Test
  void aSupportedProbeAfterAnUnsupportedProbeStillBuildsOnce() throws Exception {
    final String text = "declare variable $src external; for $a in (xs:double(1),65,66,65)"
        + " where empty(for $b in $src where $b eq $a return $b) return $a";
    final CountingSequence inner = new CountingSequence(64);
    assertEquals("65 66 65", semiJoinAnswer(text, inner, true));
    assertEquals(65, inner.visited, "one promoted comparison, then one shared supported-key build");
    assertEquals(2, inner.opened);
    assertEquals(2, inner.closed);
    final CountingSequence semi = new CountingSequence(64);
    assertEquals("1 1 1", semiJoinAnswer("declare variable $src external; for $a in (xs:double(1),1,1)"
        + " where some $b in $src satisfies $b eq $a return 1", semi, true));
    assertEquals(2, semi.visited);
    assertEquals(2, semi.opened);
    assertEquals(2, semi.closed);
  }

  @Test
  void anEarlyExitingSemiJoinClosesItsInnerIterator() throws Exception {
    // A semi join may finish without exhausting its inner scan. The result iterator must close it.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(1_000_000);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;" + " let $h := $src for $a in (1)"
          + " where exists(for $b in $h where $b eq $a return $b) return $a");
      assertEquals("1", answer(query, context));
      assertEquals(1, inner.visited, "the scan stops at the first matching key");
      assertEquals(1, inner.opened, "one inner iterator is opened");
      assertEquals(1, inner.closed, "and it is closed when the result iterator closes");
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void aLocalLetBindingAnswersTwoDifferentInputsCorrectly() throws Exception {
    // Two evaluations of one local let, each with a different multi-item inner relation:
    // changing the binding discards the previous scope's table.
    assertOptimized("3 1", "for $i in (1,2) let $inner := ($i, $i + 1)"
        + " for $a in (1,2,3) where empty(for $b in $inner where $b eq $a return $b) return $a");
  }

  @Test
  void aSingleOuterRowAntiJoinStopsAtItsMatch() throws Exception {
    // Owning the inner iterator allows anti joins to stop early too: later probes resume it,
    // and closing the result releases a scan that never needed to reach the end.
    final String text = "declare variable $src external;" + " let $h := $src for $a in (1)"
        + " where empty(for $b in $h where $b eq $a return $b) return $a";
    final CountingSequence withRule = new CountingSequence(8);
    assertEquals("", semiJoinAnswer(text, withRule, true));
    final CountingSequence withoutRule = new CountingSequence(8);
    assertEquals("", semiJoinAnswer(text, withoutRule, false));
    assertEquals(1, withoutRule.visited, "the original plan stops at the first matching inner row");
    assertEquals(1, withRule.visited, "the cursor stops as soon as this probe matches");
  }

  @Test
  void anAntiJoinStillReadsTheInnerSideExactlyOnce() throws Exception {
    // The mirror of the semi-join case: `empty` cannot answer until a probe exhausts the inner
    // side, and once one has, every later probe is served from the completed set.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(8);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, "declare variable $src external;" + " let $h := $src for $a in (3, 9, 1, 42)"
          + " where empty(for $b in $h where $b eq $a return $b) return $a");
      assertEquals("9 42", answer(query, context));
      assertEquals(8, inner.visited, "one pass over the inner side serves every probe");
      assertEquals(1, inner.closed);
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  @Test
  void aLocallyLetBoundInnerRelationIsBuiltOnce() throws Exception {
    // A local let slot holding a multi-item sequence is re-wrapped in a fresh TypedSequence on
    // every reference, so a memo keyed on the reference's value would rebuild per outer row.
    assertOneInnerBuild("declare variable $src external;" + " count(let $inner := $src for $a in 1 to 256"
        + " where empty(for $b in $inner where $b eq $a return $b) return $a)", "128");
  }

  @Test
  void aQ12StyleLocalSourceIsBuiltOnce() throws Exception {
    // Q12 binds its inner relation with `let $new := local:slice(...)`: a local let over a user
    // function call. Brackit materializes such a let, so re-iterating it no longer touches the
    // bound sequence; counting key reads on the records themselves survives that and is what
    // actually distinguishes one build from one build per outer row.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final QNm id = new QNm("id");
      final CountingRecords inner = new CountingRecords(128, id);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain,
          "declare variable $src external;" + " declare function local:slice($s) { $s };"
              + " count(let $new := local:slice($src) for $a in 1 to 256"
              + " where empty(for $b in $new where $b.id eq $a return $b.id) return $a)");
      assertEquals("128", answer(query, context));
      assertEquals(128, inner.keyReads, "the inner keys are hashed once, not once per outer row");
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
  }

  private void assertOneInnerBuild(final String text, final String expected) throws Exception {
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(128);
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, text);
      assertEquals(expected, answer(query, context));
      assertEquals(128, inner.visited, "the inner relation is read once, not once per outer row");
      assertEquals(1, inner.closed, "exactly one build iterator is opened and closed");
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
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

  private String semiJoinAnswer(final String text, final CountingSequence inner, final boolean enabled)
      throws Exception {
    if (!enabled) {
      System.setProperty(HashMembershipStage.ENABLED_PROPERTY, "false");
    }
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      context.bind(new QNm("src"), inner);
      final Query query = new Query(chain, text);
      final String answer = answer(query, context);
      assertEquals(enabled, containsProbe(chain.getOptimizedAST()), "membership route admission");
      return answer;
    } finally {
      System.clearProperty(HashMembershipStage.ENABLED_PROPERTY);
    }
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
    if (node.getType() == XQExt.HashMembershipJoin) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (containsProbe(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  /** Records whose key-field reads are counted, so rebuilds stay visible after materialization. */
  private static final class CountingRecords extends LazySequence {
    private final int size;
    private final QNm field;
    private int keyReads;
    private int offset;
    private boolean failKeys;

    private CountingRecords(final int size, final QNm field) {
      this.size = size;
      this.field = field;
    }

    @Override
    public Iter iterate() {
      return new BaseIter() {
        private int position;

        @Override
        public Item next() {
          if (position == size) {
            return null;
          }
          return new CountingRecord(new Int32(++position + offset));
        }

        @Override
        public void close() {}
      };
    }

    private final class CountingRecord extends AbstractObject {
      private final ArrayObject delegate;

      private CountingRecord(final Item value) {
        this.delegate = new ArrayObject(new QNm[] {field}, new Sequence[] {value});
      }

      @Override
      public Sequence get(final QNm name) {
        if (failKeys) {
          throw new IllegalStateException("outer field failed");
        }
        if (field.equals(name)) {
          keyReads++;
        }
        return delegate.get(name);
      }

      @Override
      public Sequence value(final IntNumeric index) {
        return delegate.value(index);
      }

      @Override
      public Sequence value(final int index) {
        return delegate.value(index);
      }

      @Override
      public Array names() {
        return delegate.names();
      }

      @Override
      public Array values() {
        return delegate.values();
      }

      @Override
      public QNm name(final IntNumeric index) {
        return delegate.name(index);
      }

      @Override
      public QNm name(final int index) {
        return delegate.name(index);
      }

      @Override
      public IntNumeric length() {
        return delegate.length();
      }

      @Override
      public int len() {
        return delegate.len();
      }

      @Override
      public Object replace(final QNm name, final Sequence value) {
        return delegate.replace(name, value);
      }

      @Override
      public Object rename(final QNm name, final QNm renamed) {
        return delegate.rename(name, renamed);
      }

      @Override
      public Object insert(final QNm name, final Sequence value) {
        return delegate.insert(name, value);
      }

      @Override
      public Object remove(final QNm name) {
        return delegate.remove(name);
      }

      @Override
      public Object remove(final IntNumeric index) {
        return delegate.remove(index);
      }

      @Override
      public Object remove(final int index) {
        return delegate.remove(index);
      }
    }
  }

  /** Fails its first read mid-iteration and its first close, then behaves. */
  private static final class ThrowingCloseSequence extends LazySequence {
    private final int size;
    private final int failAt;
    private boolean readFailed;
    private boolean closeFailed;
    private int visited;
    private int opened;

    private ThrowingCloseSequence(final int size, final int failAt) {
      this.size = size;
      this.failAt = failAt;
    }

    @Override
    public Iter iterate() {
      opened++;
      return new BaseIter() {
        private int position;

        @Override
        public Item next() {
          if (position == size) {
            return null;
          }
          position++;
          if (!readFailed && position == failAt) {
            readFailed = true;
            throw new IllegalStateException("inner read failed mid-scan");
          }
          visited++;
          return new Int32(position);
        }

        @Override
        public void close() {
          if (!closeFailed) {
            closeFailed = true;
            throw new IllegalStateException("inner close failed");
          }
        }
      };
    }
  }

  /** An array whose member access fails once, then behaves. */
  private static final class FailingOnceArray extends AbstractArray {
    private final Array delegate;
    private final int failAt;
    private boolean failed;
    private int reads;

    private FailingOnceArray(final int size, final int failAt) {
      final List<Sequence> values = new ArrayList<>(size);
      for (int value = 1; value <= size; value++) {
        values.add(new Int32(value));
      }
      this.delegate = new DArray(values);
      this.failAt = failAt;
    }

    @Override
    public Sequence at(final int index) {
      reads++;
      if (!failed && index == failAt - 1) {
        failed = true;
        throw new IllegalStateException("inner side failed mid-scan");
      }
      return delegate.at(index);
    }

    @Override
    public Sequence at(final IntNumeric index) {
      return at(index.intValue());
    }

    @Override
    public List<Sequence> values() {
      return delegate.values();
    }

    @Override
    public IntNumeric length() {
      return delegate.length();
    }

    @Override
    public int len() {
      return delegate.len();
    }

    @Override
    public Array replaceAt(final IntNumeric index, final Sequence value) {
      return delegate.replaceAt(index, value);
    }

    @Override
    public Array replaceAt(final int index, final Sequence value) {
      return delegate.replaceAt(index, value);
    }

    @Override
    public Array insert(final IntNumeric index, final Sequence value) {
      return delegate.insert(index, value);
    }

    @Override
    public Array insert(final int index, final Sequence value) {
      return delegate.insert(index, value);
    }

    @Override
    public Array append(final Sequence value) {
      return delegate.append(value);
    }

    @Override
    public Array remove(final int index) {
      return delegate.remove(index);
    }

    @Override
    public Array remove(final IntNumeric index) {
      return delegate.remove(index);
    }

    @Override
    public Array range(final IntNumeric from, final IntNumeric to) {
      return delegate.range(from, to);
    }
  }

  private static final class CountingSequence extends LazySequence {
    private final int typed;
    private final int total;
    private final QNm[] names;
    private int visited;
    private int opened;
    private int closed;

    private CountingSequence(final int size) {
      this(size, 0, null);
    }

    private CountingSequence(final int typed, final int nulls, final QNm field) {
      this.typed = typed;
      this.total = typed + nulls;
      this.names = field == null
          ? null
          : new QNm[] {field};
    }

    @Override
    public Iter iterate() {
      opened++;
      return new BaseIter() {
        private int position;

        @Override
        public Item next() {
          if (position == total) {
            return null;
          }
          visited++;
          position++;
          final Item key = position > typed
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
