package io.sirix.query;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Broad differential between the auto-wired chain and the generic pipeline.
 *
 * <p>Auto-wiring is on by default, so the vectorized path now answers ordinary queries for everyone.
 * Its contract is that it is indistinguishable from the generic pipeline except in speed — and the
 * defects that got through before were all of one shape: the two paths quietly disagreeing on
 * something no test compared. A correct answer looks exactly like a wrong one unless something runs
 * both. This runs both, over a corpus built to be awkward, and reports EVERY disagreement in one
 * pass rather than stopping at the first.
 *
 * <p>A raised exception counts as a disagreement. That is deliberate: most vectorized entry points
 * are substituted at translate time with no generic pipeline behind them, so a kernel that declines
 * at run time fails the query. Now that the fast paths are a default, "the query used to return an
 * answer and now raises" is a regression as real as a wrong number.
 *
 * <p>The corpus deliberately carries what has broken this code before: sparse fields with disjoint
 * sparsity, a field that is a long on some records and a double on others, present-but-null values,
 * a nested object repeating an outer field name (path scoping), integers on the {@code 2^53}
 * plateau, non-shortest-form decimals, negative-hashing field names, and a second revision that both
 * updates and removes values so pages span commits.
 */
public final class AutoWiringDifferentialTest {

  private static final String DB = "json-path1";
  private static final String RES = "a.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final int N = 1_200;

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    query("jn:store('" + DB + "','" + RES + "','" + corpus() + "')");
    // A second revision so pages span commits: the column merge only runs on multi-fragment pages,
    // and "newest fragment defining a slot owns it" is the rule updates and removals both test.
    query("for $r in " + SRC + " where $r.id lt 120 return replace json value of $r.age with 4242");
    query("for $r in " + SRC + " where $r.id ge 120 and $r.id lt 200 return delete json $r.age");
  }

  @AfterEach
  public void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    JsonTestHelper.deleteEverything();
  }

  /**
   * Divergences that are known, diagnosed and tracked, keyed by a fragment of the query that
   * produces them. Listing one here is not forgiveness — it is a record, and the assertion below
   * fails in BOTH directions: a divergence nobody knew about fails, and so does a listed one that
   * has stopped diverging (delete it, and the bug note with it).
   */
  private static final String[][] KNOWN_DIVERGENCES = {
      // Empty, and worth keeping empty. Both entries this list has held were real defects — a
      // nested-deref predicate compiled to the wrong field, and a null-valued column answered where
      // the interpreter raises — and in both cases the assertion below is what announced the fix,
      // by failing once the divergence stopped happening.
  };

  @Test
  public void theAutoWiredChainAgreesWithTheGenericPipeline() throws IOException {
    final List<String> queries = queries();
    final List<String> unexpected = new ArrayList<>();
    final List<String> diverged = new ArrayList<>();

    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final SirixCompileChain autoWired = SirixCompileChain.createWithJsonStore(store)) {
      for (final String query : queries) {
        final String expected = attempt(generic, ctx, query);
        final String actual = attempt(autoWired, ctx, query);
        if (expected.equals(actual)) {
          continue;
        }
        diverged.add(query);
        if (knownReason(query) == null) {
          unexpected.add("  query     : " + query + "\n    generic   : " + expected + "\n    auto-wired: " + actual);
        }
      }
    }

    final List<String> stale = new ArrayList<>();
    for (final String[] known : KNOWN_DIVERGENCES) {
      if (diverged.stream().noneMatch(q -> q.contains(known[0]))) {
        stale.add(known[0]);
      }
    }

    assertTrue(unexpected.isEmpty(),
               unexpected.size() + " of " + queries.size() + " queries disagree with the generic pipeline for "
                   + "reasons nobody has written down:\n" + String.join("\n", unexpected));
    assertTrue(stale.isEmpty(),
               "these divergences are recorded as known but no longer happen — delete them from "
                   + "KNOWN_DIVERGENCES (and close the bug they describe): " + stale);
  }

  /** The recorded reason a query is allowed to disagree, or {@code null} if it is not. */
  private static String knownReason(final String query) {
    for (final String[] known : KNOWN_DIVERGENCES) {
      if (query.contains(known[0])) {
        return known[1];
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------------

  /** Every query shape the vectorized paths can intercept, over fields chosen to be awkward. */
  private static List<String> queries() {
    final List<String> out = new ArrayList<>();

    // --- whole-source counts and pure aggregates ---
    out.add("count(" + SRC + ")");
    for (final String field : new String[] { "age", "bonus", "score", "big", "dec", "id" }) {
      for (final String func : new String[] { "count", "sum", "min", "max", "avg" }) {
        out.add(func + "(for $u in " + SRC + " return $u." + field + ")");
      }
    }

    // --- predicate counts: numeric, string, boolean, and combinations ---
    final String[] predicates = {
        "$u.age gt 100",
        "$u.age le 4242",
        "$u.id ge 0",
        "$u.bonus gt 500",
        "$u.tier eq 'gold'",
        "$u.region eq 'east-r'",
        "$u.flag",
        "not($u.flag)",
        "$u.age gt 100 and $u.bonus gt 500",
        "$u.tier eq 'gold' or $u.region eq 'east-r'",
        "$u.age gt 100 and $u.tier eq 'gold'",
        "not($u.age gt 100)",
        "$u.score gt 10.5",
        "$u.big gt 9007199254740990",
        "$u.dec gt 2.5",
        // JSONiq gives null a TOTAL comparison order — equal only to itself, and smallest of all
        // for the ordering operators. Not a type error, which is what brackit used to raise, and
        // not the same as an absent field, which is what these kernels see. All six operators, over
        // a column that carries nulls, strings and absences at once.
        "$u.nully eq 'n1'",
        "$u.nully ne 'n1'",
        "$u.nully lt 'n1'",
        "$u.nully le 'n1'",
        "$u.nully gt 'n1'",
        "$u.nully ge 'n1'",
        "$u.nully eq 5",
        "$u.nully lt 5",
        "$u.nully ge 5",
        "$u.nully lt 5 and $u.age gt 100",
        "not($u.nully lt 5)",
        "$u.note eq 'quote\"inside'",
        "($u.age gt 100 or $u.bonus gt 500) and not($u.flag)",
    };
    for (final String predicate : predicates) {
      out.add("count(for $u in " + SRC + " where " + predicate + " return $u)");
      out.add("sum(for $u in " + SRC + " where " + predicate + " return $u.age)");
      out.add("min(for $u in " + SRC + " where " + predicate + " return $u.id)");
      out.add("max(for $u in " + SRC + " where " + predicate + " return $u.id)");
      out.add("avg(for $u in " + SRC + " where " + predicate + " return $u.id)");
    }

    // --- group-by: single key, over dense, sparse, typed and null-carrying fields ---
    for (final String key : new String[] { "tier", "region", "flag", "nully", "age", "score", "dec" }) {
      out.add("for $u in " + SRC + " let $k := $u." + key + " group by $k "
          + "return {\"k\": $k, \"n\": count($u)}");
      out.add("count(for $u in " + SRC + " let $k := $u." + key + " group by $k return $k)");
    }

    // --- group-by: multi key, including the both-sparse combination ---
    out.add("for $u in " + SRC + " let $t := $u.tier, $r := $u.region group by $t, $r "
        + "return {\"t\": $t, \"r\": $r, \"n\": count($u)}");
    out.add("for $u in " + SRC + " let $t := $u.tier, $f := $u.flag group by $t, $f "
        + "return {\"t\": $t, \"f\": $f, \"n\": count($u)}");
    out.add("for $u in " + SRC + " let $t := $u.tier, $r := $u.region, $f := $u.flag group by $t, $r, $f "
        + "return {\"t\": $t, \"r\": $r, \"f\": $f, \"n\": count($u)}");
    out.add("for $u in " + SRC + " let $t := $u.tier, $g := $u.ghost group by $t, $g "
        + "return {\"t\": $t, \"g\": $g, \"n\": count($u)}");

    // --- filtered group-by ---
    out.add("for $u in " + SRC + " where $u.age gt 100 let $t := $u.tier group by $t "
        + "return {\"t\": $t, \"n\": count($u)}");
    out.add("for $u in " + SRC + " where $u.flag let $t := $u.tier, $r := $u.region group by $t, $r "
        + "return {\"t\": $t, \"r\": $r, \"n\": count($u)}");

    // --- nested field references, which the annotation cannot express ---
    // A predicate on $u.inner.age once compiled to the byte-identical annotation as one on $u.age
    // — the nested prefix was dropped — so the executor compared the OUTER age and nothing was
    // left for Sirix to notice. Both shapes must now decline and be answered generically.
    out.add("sum(for $u in " + SRC + " return $u.inner.age)");
    out.add("count(for $u in " + SRC + " where $u.inner.age gt 5 return $u)");
    out.add("count(for $u in " + SRC + " where $u.inner.age gt 5 and $u.age gt 100 return $u)");
    out.add("sum(for $u in " + SRC + " where $u.inner.age gt 5 return $u.age)");
    out.add("for $u in " + SRC + " let $k := $u.inner.age group by $k "
        + "return {\"k\": $k, \"n\": count($u)}");

    return out;
  }

  /**
   * A corpus of everything that has caused a disagreement before. Seeded, so a divergence is
   * reproducible from the failure message alone.
   */
  private static String corpus() {
    final Random rng = new Random(0xD1FFL);
    final String[] tiers = { "gold", "silver", "bronze" };
    final String[] regions = { "east", "west", "north" };
    final StringBuilder sb = new StringBuilder(N * 160);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"id\":").append(i);
      // Dense long. 'amount' and 'active' hash negative; keep one such name in play.
      sb.append(",\"age\":").append(rng.nextInt(200));
      sb.append(",\"amount\":").append(rng.nextInt(1000));
      // Sparse long, ~70% present.
      if (i % 10 < 7) sb.append(",\"bonus\":").append(rng.nextInt(1000));
      // Sparse strings with DISJOINT sparsity — neither is a dense anchor.
      if (i % 3 != 0) sb.append(",\"tier\":\"").append(tiers[rng.nextInt(tiers.length)]).append('"');
      if (i % 2 == 1) sb.append(",\"region\":\"").append(regions[rng.nextInt(regions.length)]).append("-r\"");
      // Sparse boolean.
      if (i % 2 == 0) sb.append(",\"flag\":").append(rng.nextBoolean());
      // A field that is a LONG on most records and a DOUBLE on some — the type mixture that makes
      // a column fall back to the record path.
      if (i % 97 == 0) {
        sb.append(",\"score\":").append(rng.nextInt(100)).append(".25");
      } else {
        sb.append(",\"score\":").append(rng.nextInt(100));
      }
      // Present-but-null on some, a string on others, missing on the rest.
      if (i % 5 == 0) {
        sb.append(",\"nully\":null");
      } else if (i % 5 < 3) {
        sb.append(",\"nully\":\"n").append(i % 4).append('"');
      }
      // Integers on and around the 2^53 plateau, where long and double keys stop agreeing.
      if (i % 211 == 0) sb.append(",\"big\":").append(9007199254740993L + (i % 3));
      // Non-shortest-form decimals: 2.50 and 2.5 must group as one key.
      if (i % 53 == 0) sb.append(",\"dec\":").append(i % 106 == 0 ? "2.50" : "2.5");
      // A string with an embedded quote and a non-ASCII character.
      if (i % 7 == 0) sb.append(",\"note\":\"quote\\\"inside\"");
      else if (i % 7 == 1) sb.append(",\"note\":\"grüß-").append(i % 5).append('"');
      // A nested object repeating the outer field name — path scoping must keep them apart.
      sb.append(",\"inner\":{\"age\":").append(rng.nextInt(10)).append('}');
      sb.append('}');
    }
    sb.append(']');
    return sb.toString().replace("'", "''");
  }

  /** The answer, or a stable rendering of the failure — a raised query is a disagreement too. */
  private static String attempt(final SirixCompileChain chain, final SirixQueryContext ctx, final String query) {
    try {
      return normalize(evaluate(chain, ctx, query));
    } catch (final Exception e) {
      final Throwable root = rootCause(e);
      return "RAISED " + root.getClass().getSimpleName() + ": " + root.getMessage();
    }
  }

  private static Throwable rootCause(final Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  /** Group emission order is engine-specific — compare as sorted record lines. */
  private static String normalize(final String s) {
    return s.replace("} {", "}\n{").lines().map(String::strip).filter(l -> !l.isEmpty()).sorted()
            .reduce("", (a, b) -> a + "\n" + b);
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
         final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }

  private static BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
  }

  private static void query(final String query) {
    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }
}
