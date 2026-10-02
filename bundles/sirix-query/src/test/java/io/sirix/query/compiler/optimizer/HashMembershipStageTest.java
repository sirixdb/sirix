package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
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
import io.brackit.query.jsonitem.object.AbstractObject;
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
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
      assertEquals(0, inner.closed, "the matching null key is the last row, so the scan stops before the end");
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
    final String tmpdir = System.getProperty("java.io.tmpdir");
    System.setProperty("java.io.tmpdir", blocked.toString());
    try {
      assertThrows(Exception.class, () -> orderBySpill(text, pad, null), "the sort did not spill");
    } finally {
      System.setProperty("java.io.tmpdir", tmpdir);
      blocked.toFile().setWritable(true);
    }

    orderBySpill(text, pad, expected.toString());
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
  void theLookupNeverOccupiesAPipelineTupleSlot() throws Exception {
    // Guards the order by and block GroupBy spill paths too: their budget is a fraction of the
    // heap rather than a property, so they cannot be forced to spill in this JVM. Every one of
    // them serializes whole tuples, so the lookup staying out of every slot is the invariant.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final Query query = new Query(chain,
          "let $inner := (10,20) for $a in (10,20,30,30,40)" + " where empty(for $b in $inner where $b eq $a return $b)"
              + " let $key := $a group by $key order by $key return count($a)");
      assertEquals("2 1", answer(query, context));
      final AST optimized = chain.getOptimizedAST();
      assertTrue(containsProbe(optimized), "membership route admission");
      assertEquals(0, boundLookups(optimized), "no variable may be bound to a membership lookup");
    }
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
  void mixedHitAndMissProbesReadTheInnerSideAtMostOnce() throws Exception {
    // Inner keys are 1..8. Probing 3 indexes rows 1..3 and stops; 9 exhausts the rest and completes
    // the set; 1, 8, 42 and 5 are then answered from it. Eight inner reads serve six probes.
    try (final BasicJsonDBStore store = store();
        final SirixCompileChain chain = chain(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final CountingSequence inner = new CountingSequence(8);
      context.bind(new QNm("src"), inner);
      final Query query =
          new Query(chain, "declare variable $src external;" + " let $h := $src for $a in (3, 9, 1, 8, 42, 5)"
              + " where exists(for $b in $h where $b eq $a return $b) return $a");
      assertEquals("3 1 8 5", answer(query, context));
      assertEquals(8, inner.visited, "the inner side is read once in total, not once per probe");
      assertEquals(1, inner.closed, "the single scan closes when it reaches the end");
      assertTrue(containsProbe(chain.getOptimizedAST()), "membership route admission");
    }
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

  /** Counts membership lookups reachable anywhere other than as their own probe's child. */
  private static int boundLookups(final AST node) {
    int count = 0;
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST child = node.getChild(i);
      if (child.getType() == XQExt.MembershipIndexExpr && node.getType() != XQExt.MembershipProbeExpr) {
        count++;
      }
      count += boundLookups(child);
    }
    return count;
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

  /** Records whose key-field reads are counted, so rebuilds stay visible after materialization. */
  private static final class CountingRecords extends LazySequence {
    private final int size;
    private final QNm field;
    private int keyReads;

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
          return new CountingRecord(new Int32(++position));
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

  private static final class CountingSequence extends LazySequence {
    private final int typed;
    private final int total;
    private final QNm[] names;
    private int visited;
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
