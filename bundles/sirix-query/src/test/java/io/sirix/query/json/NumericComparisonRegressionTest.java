package io.sirix.query.json;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression for comparing/ordering DOCUMENT-sourced numbers of DIFFERENT numeric kinds. XQuery
 * numeric comparison promotes to the least common type, so these must compare — not throw
 * {@code err:XPTY0004 Cannot compare ...}.
 *
 * <p>The interesting case is an integer stored as a brackit {@code Int64} (how the REST JSON
 * shredder stores every integer — Jackson parses JSON ints as {@code long}) compared against an
 * {@code xs:double}: brackit's {@code Int64.cmp} matches the {@code DblNumeric}/{@code FltNumeric}
 * operand by its CONCRETE {@code Dbl}/{@code Flt} class (it only checks the INTERFACE for
 * {@code DecNumeric}), so a document-sourced double — wrapped in {@link DblNumericJsonDBItem}, which
 * implements {@code DblNumeric} but is not the concrete {@code Dbl} — falls through to the throw.
 * {@link NumericJsonDBItem} therefore unwraps the other operand to its concrete brackit atomic
 * before delegating the comparison. (A small int parsed by {@code jn:store} is an {@code Int32},
 * whose {@code cmp} has a generic {@code Numeric} fallback, which is why this only bit the
 * REST-shredded demo.)
 */
public final class NumericComparisonRegressionTest {

  private static final String DB = "json-path1";
  private static final String RES = "mydoc.jn";

  /** A value &gt; 2^31 so brackit stores it as an Int64 (Long), like the REST shredder always does. */
  private static final String BIG_INT = "5000000000";

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String run(final String data, final String query) {
    try (final var store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + data + "')").evaluate(ctx);
      final var out = new ByteArrayOutputStream();
      try (final var pw = new PrintWriter(out, false, StandardCharsets.UTF_8)) {
        new Query(chain, query).serialize(ctx, pw);
      }
      return out.toString(StandardCharsets.UTF_8).trim().replaceAll("\\s+", " ");
    }
  }

  private static String doc() {
    return "jn:doc('" + DB + "','" + RES + "')";
  }

  @Test
  public void orderByInt64AndDouble() {
    assertEquals("3.7 5000000000", run("{\"vals\":[" + BIG_INT + ", 3.7e0]}",
        "for $v in " + doc() + ".vals[] order by $v return $v"));
  }

  @Test
  public void minMaxInt64AndDouble() {
    assertEquals("3.7", run("{\"vals\":[" + BIG_INT + ", 3.7e0]}", "min(" + doc() + ".vals[])"));
    assertEquals("5000000000", run("{\"vals\":[" + BIG_INT + ", 3.7e0]}", "max(" + doc() + ".vals[])"));
  }

  @Test
  public void compareInt64VsDouble() {
    assertEquals("false", run("{\"a\":" + BIG_INT + ",\"b\":3.7e0}", doc() + ".a lt " + doc() + ".b"));
    assertEquals("true", run("{\"a\":" + BIG_INT + ",\"b\":3.7e0}", doc() + ".a gt " + doc() + ".b"));
  }

  @Test
  public void compareInt64VsDecimalStillWorks() {
    // int64 vs xs:decimal already worked (Int64.cmp checks the DecNumeric interface) — guard it.
    assertEquals("true", run("{\"a\":" + BIG_INT + ",\"b\":3.7}", doc() + ".a gt " + doc() + ".b"));
  }

  @Test
  public void arithmeticInt64AndDouble() {
    // int64 + xs:double must promote, not throw: 5000000000 + 1.5 = 5000000001.5 > 4.9e9.
    assertEquals("true", run("{\"a\":" + BIG_INT + ",\"b\":1.5e0}",
        "(" + doc() + ".a + " + doc() + ".b) gt 4.9e9"));
  }
}
