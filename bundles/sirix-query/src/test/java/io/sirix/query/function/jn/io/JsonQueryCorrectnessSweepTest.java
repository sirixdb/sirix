package io.sirix.query.function.jn.io;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness sweep over a battery of JSONiq queries. Every case is executed; failures are
 * ACCUMULATED (no fail-fast). One line is printed per failure and the test only fails at the end
 * if at least one case produced a wrong result or threw. Results are compared robustly:
 * whitespace is normalized, the top-level space-joined sequence is split element-wise, and each
 * element is compared semantically when both sides parse as JSON (objects key-order-insensitive,
 * numbers by value); a top-level xs:string serialized without quotes by
 * {@code Query.serialize(ctx, writer)} is treated as equal to the same quoted literal.
 *
 * <p>All 59 cases COMPILE, EXECUTE and PASS — none are dropped for being invalid for this dialect.
 * Four cases historically surfaced genuine engine defects (NOT harness issues — each reproduced
 * with minimal standalone queries); all four are now FIXED and guarded here as regressions:
 * <ul>
 *   <li>{@code int-idiv-mod-div-type-distinction}: {@code $a mod $b} over DOCUMENT-sourced Int32
 *       operands used to return the DIVISION result ({@code 7 mod 2} -> {@code 3.5} instead of
 *       {@code 1}). FIXED by tagging document-sourced numbers with their specific brackit numeric
 *       sub-interface (IntNumeric/DecNumeric/...) so {@code instanceof} dispatch matches literals.</li>
 *   <li>{@code min-max-over-mixed-int-and-decimal}: {@code min(...)} over a sequence mixing
 *       xs:integer and xs:decimal used to throw {@code err:XPTY0004 Cannot compare}. FIXED by the
 *       same numeric-wrapper sub-typing.</li>
 *   <li>{@code sum-of-range-1-to-n-from-document-value}: {@code 1 to $d.n} with a document-sourced
 *       Int32 upper bound used to throw {@code err:XPTY0004}. FIXED by the same numeric-wrapper
 *       sub-typing (RangeExpr requires both bounds to be IntNumeric).</li>
 *   <li>{@code predicate-price-gt-over-unwrapped-elements}: {@code .book[][?$$.price gt 10]}
 *       COMPILED but returned the empty sequence. FIXED in {@code AbstractJsonPathWalker}: the
 *       path-index rewrite no longer destructively replaces the deref subtree with
 *       {@code EmptySequenceType} when the rightmost segment's path-summary lookup misses only on
 *       node kind (a scalar predicate-leaf field fused into the path) rather than genuinely not
 *       existing. See {@code PredicateOverUnwrappedArrayTest} for the with/without-index guard.</li>
 * </ul>
 */
public final class JsonQueryCorrectnessSweepTest {

  /** Database / collection name. Maps to {@link PATHS#PATH1}, the only path deleteEverything wipes. */
  private static final String DB = "json-path1";

  /** Resource name. */
  private static final String RES = "mydoc.jn";

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ---------------------------------------------------------------------------------------------
  // Case model
  // ---------------------------------------------------------------------------------------------

  private record Case(String name, String query, String data, List<String> revisions, String expected) {
    static Case single(String name, String data, String query, String expected) {
      return new Case(name, query, data, null, expected);
    }

    static Case revs(String name, String query, String expected, String... revisions) {
      return new Case(name, query, null, List.of(revisions), expected);
    }
  }

  private enum FailKind { WRONG_RESULT, QUERY_EXCEPTION, SHRED_EXCEPTION }

  private record Failure(String name, FailKind kind, String query, String expected, String actual) {}

  // ---------------------------------------------------------------------------------------------
  // The sweep
  // ---------------------------------------------------------------------------------------------

  @Test
  public void sweep() {
    final List<Case> cases = buildCases();
    final List<Failure> failures = new ArrayList<>();
    int ran = 0;

    for (final Case c : cases) {
      ran++;
      // Fresh DB+resource per case.
      JsonTestHelper.deleteEverything();
      try (final var store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
           final var ctx = SirixQueryContext.createWithJsonStore(store);
           final var chain = SirixCompileChain.createWithJsonStore(store)) {

        // (1) Build revision(s).
        try {
          if (c.revisions() != null) {
            buildRevisions(ctx, chain, c.revisions());
          } else {
            final String storeQuery =
                "jn:store('" + DB + "','" + RES + "','" + jsonStringLiteral(c.data()) + "')";
            new Query(chain, storeQuery).evaluate(ctx);
          }
        } catch (final Throwable t) {
          failures.add(new Failure(c.name(), FailKind.SHRED_EXCEPTION, c.query(), c.expected(),
              describe(t)));
          continue;
        }

        // (2) Substitute the real db/resource names into the query, run it, capture String.
        final String query = substitute(c.query());
        final String actual;
        try {
          actual = run(ctx, chain, query);
        } catch (final Throwable t) {
          failures.add(new Failure(c.name(), FailKind.QUERY_EXCEPTION, query, c.expected(),
              describe(t)));
          continue;
        }

        // (3) Compare robustly.
        if (!resultsEqual(c.expected(), actual)) {
          failures.add(new Failure(c.name(), FailKind.WRONG_RESULT, query, c.expected(), actual));
        }
      } catch (final Throwable t) {
        // Store / context lifecycle blew up — record as a shred exception for this case.
        failures.add(new Failure(c.name(), FailKind.SHRED_EXCEPTION, c.query(), c.expected(),
            describe(t)));
      }
    }

    // Report.
    System.out.println("=== JsonQueryCorrectnessSweepTest: " + ran + "/" + cases.size()
        + " cases ran, " + failures.size() + " failures ===");
    for (final Failure f : failures) {
      System.out.println("FAIL [" + f.kind() + "] " + f.name()
          + "\n    query   : " + f.query()
          + "\n    expected: " + f.expected()
          + "\n    actual  : " + f.actual());
    }

    assertTrue(failures.isEmpty(), failures.size() + " of " + cases.size()
        + " JSONiq correctness cases failed (see stdout for per-case detail)");
  }

  // ---------------------------------------------------------------------------------------------
  // Harness helpers
  // ---------------------------------------------------------------------------------------------

  /** Substitute the DB / RES placeholders (both the bare tokens and the literal 'mycol'/'myres'). */
  private static String substitute(final String query) {
    return query
        .replace("DB,RES", "'" + DB + "','" + RES + "'")
        .replace("'mycol','myres'", "'" + DB + "','" + RES + "'");
  }

  /** Escape a JSON document so it can be embedded inside a single-quoted jn:store string literal. */
  private static String jsonStringLiteral(final String json) {
    // Inside '...' the only thing we must guard against is an embedded single quote. The case
    // data here contains none, but be defensive.
    return json.replace("'", "&apos;");
  }

  /** Run a query and capture its serialized (compact-JSON) String exactly like DocIntegrationTest. */
  private static String run(final SirixQueryContext ctx, final SirixCompileChain chain, final String query) {
    try (final var out = new ByteArrayOutputStream(); final var pw = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, pw);
      pw.flush();
      return out.toString();
    } catch (final java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Build revisions from a list of full document states. Revision 1 is the shredded first state.
   * Each subsequent state is produced by IN-PLACE JSONiq updates (replace-value / insert / delete)
   * computed by diffing the previous state against the next, run as standalone updating queries
   * (each auto-commits => one new revision). In-place edits preserve the document-root node's
   * identity, which is required for the time-travel functions (jn:past/all-times/...) to see every
   * revision — a full remove+reinsert of the root content would break that identity.
   */
  private static void buildRevisions(final SirixQueryContext ctx, final SirixCompileChain chain,
      final List<String> states) {
    final String first = states.get(0);
    final String storeQuery = "jn:store('" + DB + "','" + RES + "','" + jsonStringLiteral(first) + "')";
    new Query(chain, storeQuery).evaluate(ctx);

    Object prev = parseOrThrow(first);
    for (int i = 1; i < states.size(); i++) {
      final Object next = parseOrThrow(states.get(i));
      final List<String> updates = new ArrayList<>();
      emitUpdates("jn:doc('" + DB + "','" + RES + "')", prev, next, updates);
      // Each standalone updating query auto-commits => exactly one new revision per state.
      for (final String u : updates) {
        new Query(chain, u).evaluate(ctx);
      }
      // If two consecutive states were identical no update would be emitted; force a revision by
      // committing a no-op so the revision count still advances.
      if (updates.isEmpty()) {
        new Query(chain, "sdb:commit(jn:doc('" + DB + "','" + RES + "'))").evaluate(ctx);
      }
      prev = next;
    }
  }

  private static Object parseOrThrow(final String json) {
    final Object v = tryParse(json.trim());
    if (v == SENTINEL_UNPARSED) {
      throw new IllegalArgumentException("case revision state is not valid JSON: " + json);
    }
    return v;
  }

  /**
   * Emit in-place JSONiq update statements transforming {@code oldVal} (currently stored at
   * {@code path}) into {@code newVal}. Recurses through matching object keys and equal-length
   * arrays; otherwise replaces the whole value at {@code path}.
   */
  @SuppressWarnings("unchecked")
  private static void emitUpdates(final String path, final Object oldVal, final Object newVal,
      final List<String> out) {
    if (jsonEqual(oldVal, newVal)) {
      return;
    }
    if (oldVal instanceof Map && newVal instanceof Map) {
      final Map<String, Object> oldM = (Map<String, Object>) oldVal;
      final Map<String, Object> newM = (Map<String, Object>) newVal;
      // Removed keys.
      for (final String k : oldM.keySet()) {
        if (!newM.containsKey(k)) {
          out.add("delete json " + path + "." + fieldAccess(k));
        }
      }
      // Added / changed keys.
      for (final Map.Entry<String, Object> en : newM.entrySet()) {
        final String k = en.getKey();
        if (!oldM.containsKey(k)) {
          out.add("insert json {" + jsonKey(k) + ": " + toJsonLiteral(en.getValue()) + "} into " + path);
        } else {
          emitUpdates(path + "." + fieldAccess(k), oldM.get(k), en.getValue(), out);
        }
      }
      return;
    }
    if (oldVal instanceof List && newVal instanceof List
        && ((List<Object>) oldVal).size() == ((List<Object>) newVal).size()) {
      final List<Object> oldL = (List<Object>) oldVal;
      final List<Object> newL = (List<Object>) newVal;
      for (int i = 0; i < oldL.size(); i++) {
        emitUpdates(path + "[" + i + "]", oldL.get(i), newL.get(i), out);
      }
      return;
    }
    // Scalar change, type change, or array length change: replace the whole value.
    out.add("replace json value of " + path + " with " + toJsonLiteral(newVal));
  }

  /** Render a field access segment. Keys here are simple identifiers; quote defensively otherwise. */
  private static String fieldAccess(final String key) {
    if (key.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return key;
    }
    return "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static String jsonKey(final String key) {
    return "\"" + key.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  /** Serialize a parsed JSON value (Map/List/BigDecimal/String/Boolean/null) back to a JSON literal. */
  @SuppressWarnings("unchecked")
  private static String toJsonLiteral(final Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof BigDecimal) {
      return ((BigDecimal) v).toPlainString();
    }
    if (v instanceof Boolean) {
      return v.toString();
    }
    if (v instanceof String) {
      return "\"" + ((String) v)
          .replace("\\", "\\\\")
          .replace("\"", "\\\"") + "\"";
    }
    if (v instanceof Map) {
      final StringBuilder sb = new StringBuilder("{");
      boolean first = true;
      for (final Map.Entry<String, Object> en : ((Map<String, Object>) v).entrySet()) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append(jsonKey(en.getKey())).append(":").append(toJsonLiteral(en.getValue()));
      }
      return sb.append("}").toString();
    }
    if (v instanceof List) {
      final StringBuilder sb = new StringBuilder("[");
      boolean first = true;
      for (final Object e : (List<Object>) v) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append(toJsonLiteral(e));
      }
      return sb.append("]").toString();
    }
    return v.toString();
  }

  private static String describe(final Throwable t) {
    Throwable cur = t;
    final StringBuilder sb = new StringBuilder();
    while (cur != null) {
      if (sb.length() > 0) {
        sb.append(" <- ");
      }
      sb.append(cur.getClass().getSimpleName());
      if (cur.getMessage() != null) {
        sb.append(": ").append(cur.getMessage());
      }
      cur = cur.getCause();
      if (sb.length() > 600) {
        break;
      }
    }
    String s = sb.toString();
    if (s.length() > 600) {
      s = s.substring(0, 600) + "...";
    }
    return s;
  }

  // ---------------------------------------------------------------------------------------------
  // Robust comparison
  // ---------------------------------------------------------------------------------------------

  static boolean resultsEqual(final String expected, final String actual) {
    final String e = expected == null ? "" : expected.trim();
    final String a = actual == null ? "" : actual.trim();
    if (e.equals(a)) {
      return true;
    }
    // Compare as space-joined sequences of top-level items.
    final List<String> es = splitTopLevel(e);
    final List<String> as = splitTopLevel(a);
    if (es.size() != as.size()) {
      return false;
    }
    for (int i = 0; i < es.size(); i++) {
      if (!itemsEqual(es.get(i), as.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare two serialized items robustly. Handles a serialization nuance: {@code
   * Query.serialize(ctx, writer)} renders a top-level/atomic xs:string WITHOUT surrounding quotes
   * (e.g. {@code bulk}), whereas the expected literals here carry the quotes (e.g. {@code
   * "bulk"}). So a quoted-string item is considered equal to the same text unquoted. Otherwise
   * both sides are parsed and compared as JSON (numbers by value, objects key-order-insensitive).
   */
  private static boolean itemsEqual(final String x, final String y) {
    final String xs = x.trim();
    final String ys = y.trim();
    if (xs.equals(ys)) {
      return true;
    }
    final String xStr = stringContentOrNull(xs);
    final String yStr = stringContentOrNull(ys);
    // One side is a quoted JSON string, the other is the same bare text (lost its quotes).
    if (xStr != null && xStr.equals(ys)) {
      return true;
    }
    if (yStr != null && yStr.equals(xs)) {
      return true;
    }
    // Both are quoted strings: compare decoded contents.
    if (xStr != null && yStr != null) {
      return xStr.equals(yStr);
    }
    final Object xv = tryParse(xs);
    final Object yv = tryParse(ys);
    if (xv == SENTINEL_UNPARSED || yv == SENTINEL_UNPARSED) {
      return xs.equals(ys);
    }
    return jsonEqual(xv, yv);
  }

  /** If {@code s} is a single JSON string literal, return its decoded content; else {@code null}. */
  private static String stringContentOrNull(final String s) {
    if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
      final Object v = tryParse(s);
      if (v instanceof String) {
        return (String) v;
      }
    }
    return null;
  }

  // -------- minimal JSON parser (objects/arrays/strings/numbers/bool/null) --------

  private static final Object SENTINEL_UNPARSED = new Object();

  private static Object tryParse(final String s) {
    try {
      final JsonParser p = new JsonParser(s);
      final Object v = p.parseValue();
      p.skipWs();
      if (!p.atEnd()) {
        return SENTINEL_UNPARSED;
      }
      return v;
    } catch (final RuntimeException ex) {
      return SENTINEL_UNPARSED;
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean jsonEqual(final Object x, final Object y) {
    if (x == null || y == null) {
      return x == y;
    }
    if (x instanceof BigDecimal && y instanceof BigDecimal) {
      return ((BigDecimal) x).compareTo((BigDecimal) y) == 0;
    }
    if (x instanceof Map && y instanceof Map) {
      final Map<String, Object> mx = (Map<String, Object>) x;
      final Map<String, Object> my = (Map<String, Object>) y;
      if (mx.size() != my.size()) {
        return false;
      }
      for (final Map.Entry<String, Object> en : mx.entrySet()) {
        if (!my.containsKey(en.getKey())) {
          return false;
        }
        if (!jsonEqual(en.getValue(), my.get(en.getKey()))) {
          return false;
        }
      }
      return true;
    }
    if (x instanceof List && y instanceof List) {
      final List<Object> lx = (List<Object>) x;
      final List<Object> ly = (List<Object>) y;
      if (lx.size() != ly.size()) {
        return false;
      }
      for (int i = 0; i < lx.size(); i++) {
        if (!jsonEqual(lx.get(i), ly.get(i))) {
          return false;
        }
      }
      return true;
    }
    if (x.getClass() != y.getClass()) {
      return false;
    }
    return x.equals(y);
  }

  /**
   * Split a serialized sequence into its top-level items. Items are joined by single spaces by the
   * Brackit StringSerializer, but spaces also occur inside string literals — so we only split on a
   * space when bracket depth is zero and we are not inside a string.
   */
  static List<String> splitTopLevel(final String s) {
    final List<String> out = new ArrayList<>();
    if (s.isEmpty()) {
      return out;
    }
    int depth = 0;
    boolean inStr = false;
    boolean esc = false;
    final StringBuilder cur = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      final char ch = s.charAt(i);
      if (inStr) {
        cur.append(ch);
        if (esc) {
          esc = false;
        } else if (ch == '\\') {
          esc = true;
        } else if (ch == '"') {
          inStr = false;
        }
        continue;
      }
      switch (ch) {
        case '"' -> {
          inStr = true;
          cur.append(ch);
        }
        case '{', '[' -> {
          depth++;
          cur.append(ch);
        }
        case '}', ']' -> {
          depth--;
          cur.append(ch);
        }
        case ' ' -> {
          if (depth == 0) {
            if (cur.length() > 0) {
              out.add(cur.toString());
              cur.setLength(0);
            }
          } else {
            cur.append(ch);
          }
        }
        default -> cur.append(ch);
      }
    }
    if (cur.length() > 0) {
      out.add(cur.toString());
    }
    return out;
  }

  /** Tiny recursive-descent JSON parser producing Map / List / BigDecimal / String / Boolean / null. */
  private static final class JsonParser {
    private final String s;
    private int i;

    JsonParser(final String s) {
      this.s = s;
    }

    boolean atEnd() {
      return i >= s.length();
    }

    void skipWs() {
      while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
        i++;
      }
    }

    Object parseValue() {
      skipWs();
      if (atEnd()) {
        throw new RuntimeException("eof");
      }
      final char c = s.charAt(i);
      return switch (c) {
        case '{' -> parseObject();
        case '[' -> parseArray();
        case '"' -> parseString();
        case 't', 'f' -> parseBool();
        case 'n' -> parseNull();
        default -> parseNumber();
      };
    }

    private Map<String, Object> parseObject() {
      final Map<String, Object> m = new LinkedHashMap<>();
      expect('{');
      skipWs();
      if (peek() == '}') {
        i++;
        return m;
      }
      while (true) {
        skipWs();
        final String key = parseString();
        skipWs();
        expect(':');
        final Object val = parseValue();
        m.put(key, val);
        skipWs();
        final char c = next();
        if (c == '}') {
          break;
        }
        if (c != ',') {
          throw new RuntimeException("expected , or } got " + c);
        }
      }
      return m;
    }

    private List<Object> parseArray() {
      final List<Object> l = new ArrayList<>();
      expect('[');
      skipWs();
      if (peek() == ']') {
        i++;
        return l;
      }
      while (true) {
        final Object val = parseValue();
        l.add(val);
        skipWs();
        final char c = next();
        if (c == ']') {
          break;
        }
        if (c != ',') {
          throw new RuntimeException("expected , or ] got " + c);
        }
      }
      return l;
    }

    private String parseString() {
      expect('"');
      final StringBuilder sb = new StringBuilder();
      while (true) {
        if (atEnd()) {
          throw new RuntimeException("unterminated string");
        }
        final char c = s.charAt(i++);
        if (c == '"') {
          break;
        }
        if (c == '\\') {
          final char e = s.charAt(i++);
          switch (e) {
            case '"' -> sb.append('"');
            case '\\' -> sb.append('\\');
            case '/' -> sb.append('/');
            case 'b' -> sb.append('\b');
            case 'f' -> sb.append('\f');
            case 'n' -> sb.append('\n');
            case 'r' -> sb.append('\r');
            case 't' -> sb.append('\t');
            case 'u' -> {
              final String hex = s.substring(i, i + 4);
              i += 4;
              sb.append((char) Integer.parseInt(hex, 16));
            }
            default -> throw new RuntimeException("bad escape \\" + e);
          }
        } else {
          sb.append(c);
        }
      }
      return sb.toString();
    }

    private Boolean parseBool() {
      if (s.startsWith("true", i)) {
        i += 4;
        return Boolean.TRUE;
      }
      if (s.startsWith("false", i)) {
        i += 5;
        return Boolean.FALSE;
      }
      throw new RuntimeException("bad boolean");
    }

    private Object parseNull() {
      if (s.startsWith("null", i)) {
        i += 4;
        return null;
      }
      throw new RuntimeException("bad null");
    }

    private BigDecimal parseNumber() {
      final int start = i;
      if (peek() == '-' || peek() == '+') {
        i++;
      }
      while (i < s.length()) {
        final char c = s.charAt(i);
        if ((c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
          i++;
        } else {
          break;
        }
      }
      final String num = s.substring(start, i);
      // Tokens like INF / -INF / NaN are NOT numbers here; let them be treated as unparsed.
      try {
        return new BigDecimal(num);
      } catch (final NumberFormatException ex) {
        throw new RuntimeException("bad number '" + num + "'");
      }
    }

    private char peek() {
      if (atEnd()) {
        throw new RuntimeException("eof");
      }
      return s.charAt(i);
    }

    private char next() {
      if (atEnd()) {
        throw new RuntimeException("eof");
      }
      return s.charAt(i++);
    }

    private void expect(final char c) {
      final char got = next();
      if (got != c) {
        throw new RuntimeException("expected " + c + " got " + got);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Cases
  // ---------------------------------------------------------------------------------------------

  private static List<Case> buildCases() {
    final List<Case> cs = new ArrayList<>();

    // --- arithmetic / aggregates ---
    cs.add(Case.single("sum-count-avg-with-decimal-mean-in-object",
        "{\"nums\":[1,2,3,4]}",
        "let $d := jn:doc(DB,RES) return {\"total\": sum($d.nums[]), \"n\": count($d.nums[]), \"mean\": avg($d.nums[])}",
        "{\"total\":10,\"n\":4,\"mean\":2.5}"));

    cs.add(Case.single("sum-of-decimals-lands-on-whole-number",
        "{\"prices\":[1.5,2.5,0.25,0.75]}",
        "let $d := jn:doc(DB,RES) return sum($d.prices[])",
        "5"));

    cs.add(Case.single("int-idiv-mod-div-type-distinction",
        "{\"a\":7,\"b\":2}",
        "let $d := jn:doc(DB,RES) return [$d.a idiv $d.b, $d.a mod $d.b, $d.a div $d.b]",
        "[3,1,3.5]"));

    cs.add(Case.single("min-max-over-mixed-int-and-decimal",
        "{\"vals\":[5,2.5,10,7.25]}",
        "let $d := jn:doc(DB,RES) return {\"min\": min($d.vals[]), \"max\": max($d.vals[])}",
        "{\"min\":2.5,\"max\":10}"));

    cs.add(Case.single("exists-empty-over-filtered-sequence-boolean-sequence",
        "{\"items\":[{\"k\":1},{\"k\":2},{\"k\":3}]}",
        "let $d := jn:doc(DB,RES) return (exists(for $i in $d.items[] where $i.k gt 5 return $i), empty(for $i in $d.items[] where $i.k gt 5 return $i), exists(for $i in $d.items[] where $i.k gt 1 return $i))",
        "false true true"));

    cs.add(Case.single("if-then-else-driven-by-count-comparison-returns-string",
        "{\"orders\":[10,20,30]}",
        "let $d := jn:doc(DB,RES) return if (count($d.orders[]) ge 3) then \"bulk\" else \"small\"",
        "\"bulk\""));

    cs.add(Case.single("sum-of-range-1-to-n-from-document-value",
        "{\"n\":10}",
        "let $d := jn:doc(DB,RES) return sum(1 to $d.n)",
        "55"));

    cs.add(Case.single("boolean-and-or-not-with-value-comparison",
        "{\"flag\":true,\"count\":0,\"name\":\"x\"}",
        "let $d := jn:doc(DB,RES) return [$d.flag and ($d.count gt 0), $d.flag or ($d.count gt 0), not($d.flag)]",
        "[false,true,false]"));

    cs.add(Case.single("sum-and-count-of-empty-filtered-sequence",
        "{\"nums\":[5,6,7]}",
        "let $d := jn:doc(DB,RES) return [sum(for $x in $d.nums[] where $x gt 100 return $x), count(for $x in $d.nums[] where $x gt 100 return $x)]",
        "[0,0]"));

    cs.add(Case.single("general-comparison-existential-over-array",
        "{\"codes\":[2,4,6,8]}",
        "let $d := jn:doc(DB,RES) return [$d.codes[] = 6, $d.codes[] = 5]",
        "[true,false]"));

    // --- navigation / indexing ---
    cs.add(Case.single("deep-object-chain-plus-array-index",
        "{\"store\":{\"book\":[{\"title\":\"A\",\"price\":12.5},{\"title\":\"B\",\"price\":8.25}],\"bicycle\":{\"color\":\"red\",\"price\":19.95}}}",
        "jn:doc(DB,RES).store.book[0].title",
        "\"A\""));

    cs.add(Case.single("predicate-price-gt-over-unwrapped-elements",
        "{\"store\":{\"book\":[{\"title\":\"A\",\"price\":12.5},{\"title\":\"B\",\"price\":8.25},{\"title\":\"C\",\"price\":42}]}}",
        "jn:doc(DB,RES).store.book[][?$$.price gt 10]",
        "{\"title\":\"A\",\"price\":12.5} {\"title\":\"C\",\"price\":42}"));

    cs.add(Case.single("negative-array-index-last-element",
        "[\"alpha\",\"beta\",\"gamma\",\"delta\",\"epsilon\"]",
        "jn:doc(DB,RES)[-1]",
        "\"epsilon\""));

    cs.add(Case.single("single-bracket-colon-slice-is-half-open",
        "[\"alpha\",\"beta\",\"gamma\",\"delta\",\"epsilon\"]",
        "jn:doc(DB,RES)[1:3]",
        "[\"beta\",\"gamma\"]"));

    cs.add(Case.single("unwrap-then-field-then-negindex-then-field",
        "[true,false,\"true\",{\"foo\":[\"tada\",{\"baz\":\"yes\"},{\"baz\":true}]}]",
        "jn:doc(DB,RES)[].foo[-1].baz",
        "true"));

    cs.add(Case.single("descendant-deref-preorder-doc-order",
        "[{\"baz\":[{\"test\":\"x\"}]},{\"foo\":[{\"test\":\"y\"}]}]",
        "jn:doc(DB,RES)=>>test",
        "\"x\" \"y\""));

    cs.add(Case.single("deref-past-scalar-yields-empty",
        "{\"a\":{\"b\":1}}",
        "jn:doc(DB,RES).a.b.c.d",
        ""));

    cs.add(Case.single("flwor-where-deep-equal-nested-field-projection",
        "[{\"generic\":1,\"location\":{\"state\":\"CA\",\"city\":\"Los Angeles\"}},{\"generic\":2,\"location\":{\"state\":\"NY\",\"city\":\"New York\"}},{\"generic\":1,\"location\":{\"state\":\"AL\",\"city\":\"Montgomery\"}}]",
        "for $i in jn:doc(DB,RES) where deep-equal($i.generic,1) return $i.location.state",
        "\"CA\" \"AL\""));

    cs.add(Case.single("chained-nested-array-indexing",
        "[\"foo\",[[\"bar\",\"baz\"]]]",
        "jn:doc(DB,RES)[1][0][1]",
        "\"baz\""));

    // --- order by / group by / joins ---
    cs.add(Case.single("order-by-multi-key-mixed-directions",
        "[{\"d\":\"A\",\"p\":2},{\"d\":\"B\",\"p\":1},{\"d\":\"A\",\"p\":1},{\"d\":\"B\",\"p\":2}]",
        "for $x in jn:doc('mycol','myres')[] order by $x.d ascending, $x.p descending return concat($x.d, $x.p)",
        "\"A2\" \"A1\" \"B2\" \"B1\""));

    cs.add(Case.single("order-by-lexicographic-string-numbers",
        "[\"10\",\"9\",\"100\",\"2\"]",
        "for $x in jn:doc('mycol','myres')[] order by $x return $x",
        "\"10\" \"100\" \"2\" \"9\""));

    cs.add(Case.single("group-by-string-key-with-count-first-appearance-order",
        "[{\"c\":\"x\"},{\"c\":\"y\"},{\"c\":\"x\"},{\"c\":\"z\"},{\"c\":\"y\"},{\"c\":\"x\"}]",
        "for $i in jn:doc('mycol','myres')[] let $k := $i.c group by $k return {$k: count($i)}",
        "{\"x\":3} {\"y\":2} {\"z\":1}"));

    cs.add(Case.single("flwor-equi-join-two-for-variables",
        "{\"orders\":[{\"oid\":1,\"cid\":10},{\"oid\":2,\"cid\":20},{\"oid\":3,\"cid\":10}],\"customers\":[{\"id\":10,\"name\":\"Al\"},{\"id\":20,\"name\":\"Bo\"}]}",
        "let $d := jn:doc('mycol','myres') for $o in $d.orders[], $c in $d.customers[] where $o.cid eq $c.id return {\"o\":$o.oid,\"n\":$c.name}",
        "{\"o\":1,\"n\":\"Al\"} {\"o\":2,\"n\":\"Bo\"} {\"o\":3,\"n\":\"Al\"}"));

    cs.add(Case.single("group-by-sum-order-by-aggregate-descending",
        "[{\"dep\":\"eng\",\"sal\":100},{\"dep\":\"sales\",\"sal\":50},{\"dep\":\"eng\",\"sal\":200},{\"dep\":\"sales\",\"sal\":40},{\"dep\":\"hr\",\"sal\":250}]",
        "for $e in jn:doc('mycol','myres')[] let $d := $e.dep group by $d let $t := sum($e.sal) order by $t descending return {\"dep\":$d,\"total\":$t}",
        "{\"dep\":\"eng\",\"total\":300} {\"dep\":\"hr\",\"total\":250} {\"dep\":\"sales\",\"total\":90}"));

    cs.add(Case.single("nested-flwor-cross-product-flattening",
        "{\"a\":[1,2],\"b\":[10,20]}",
        "let $d := jn:doc('mycol','myres') for $x in $d.a[] return (for $y in $d.b[] return $x + $y)",
        "11 21 12 22"));

    cs.add(Case.single("where-filter-numeric-ge-wrapped-in-count",
        "[{\"price\":10},{\"price\":50},{\"price\":100},{\"price\":200},{\"price\":49}]",
        "count(for $i in jn:doc('mycol','myres')[] where $i.price ge 50 return $i)",
        "3"));

    cs.add(Case.single("group-by-computed-key-mod-first-appearance-order",
        "[1,2,3,4,5,6,7]",
        "for $x in jn:doc('mycol','myres')[] let $g := $x mod 3 group by $g return {\"g\":$g,\"n\":count($x)}",
        "{\"g\":1,\"n\":3} {\"g\":2,\"n\":2} {\"g\":0,\"n\":2}"));

    cs.add(Case.single("group-by-avg-exact-integer-division",
        "[{\"t\":\"a\",\"v\":2},{\"t\":\"a\",\"v\":4},{\"t\":\"b\",\"v\":10},{\"t\":\"b\",\"v\":20},{\"t\":\"a\",\"v\":6}]",
        "for $i in jn:doc('mycol','myres')[] let $t := $i.t group by $t return {\"t\":$t,\"avg\":avg($i.v)}",
        "{\"t\":\"a\",\"avg\":4} {\"t\":\"b\",\"avg\":15}"));

    cs.add(Case.single("multi-variable-join-into-group-by-sum",
        "{\"stores\":[{\"sid\":1,\"region\":\"N\"},{\"sid\":2,\"region\":\"S\"},{\"sid\":3,\"region\":\"N\"}],\"sales\":[{\"sid\":1,\"amt\":100},{\"sid\":2,\"amt\":50},{\"sid\":3,\"amt\":30},{\"sid\":1,\"amt\":20}]}",
        "let $d := jn:doc('mycol','myres') for $s in $d.stores[], $sa in $d.sales[] where $s.sid eq $sa.sid let $r := $s.region group by $r return {\"region\":$r,\"total\":sum($sa.amt)}",
        "{\"region\":\"N\",\"total\":150} {\"region\":\"S\",\"total\":50}"));

    // --- string functions ---
    cs.add(Case.single("substring-start-lt-1-clamps",
        "{\"meta\":\"metadata\"}",
        "substring(jn:doc(DB,RES).meta, 0, 3)",
        "\"me\""));

    cs.add(Case.single("tokenize-keeps-trailing-empty-tokens",
        "{\"path\":\"a/b//c/\"}",
        "count(tokenize(jn:doc(DB,RES).path, \"/\"))",
        "5"));

    cs.add(Case.single("contains-empty-pattern-true-startswith-empty-string-false",
        "{\"t\":\"hello\"}",
        "(contains(jn:doc(DB,RES).t, \"\"), starts-with(jn:doc(DB,RES).t, \"\"), starts-with(\"\", \"x\"))",
        "true true false"));

    // --- numeric functions ---
    cs.add(Case.single("round-vs-round-half-to-even-on-ties",
        "{\"ignored\":true}",
        "(round(2.5), round-half-to-even(2.5), round-half-to-even(3.5))",
        "3 2 4"));

    cs.add(Case.single("floor-ceiling-round-of-negative-decimal-half",
        "{\"ignored\":true}",
        "(floor(-2.5), ceiling(-2.5), round(-2.5))",
        "-3 -2 -2"));

    // --- sequence functions ---
    cs.add(Case.single("distinct-values-preserves-first-occurrence-order",
        "{\"tags\":[\"x\",\"y\",\"x\",\"z\",\"y\",\"x\"]}",
        "distinct-values(jn:doc(DB,RES).tags[])",
        "\"x\" \"y\" \"z\""));

    cs.add(Case.single("index-of-returns-all-1-based-positions",
        "{\"a\":[\"a\",\"b\",\"c\",\"b\",\"a\"]}",
        "index-of(jn:doc(DB,RES).a[], \"b\")",
        "2 4"));

    cs.add(Case.single("subsequence-1-based-with-length-composed-with-reverse",
        "{\"a\":[\"a\",\"b\",\"c\",\"d\",\"e\"]}",
        "reverse(subsequence(jn:doc(DB,RES).a[], 2, 3))",
        "\"d\" \"c\" \"b\""));

    cs.add(Case.single("insert-before-position-lt-1-clamps-to-front",
        "{\"ignored\":true}",
        "insert-before((9, 8, 7), 0, 99)",
        "99 9 8 7"));

    // Per spec fn:string-length counts CHARACTERS (codepoints): Z + o + ë + 😀 = 4. (brackit
    // previously counted UTF-16 code units — 5, with the emoji as two — fixed 2026-06-10.)
    cs.add(Case.single("string-length-counts-codepoints",
        "{\"name\":\"Zoë😀\"}",
        "string-length(jn:doc(DB,RES).name)",
        "4"));

    // --- temporal ---
    cs.add(Case.revs("time-travel-open-past-revision-then-path",
        "jn:doc(DB,RES,1).items[-1]",
        "\"x\"",
        "{\"items\":[\"x\"],\"meta\":{\"v\":1}}",
        "{\"items\":[\"x\",\"y\",\"z\"],\"meta\":{\"v\":2}}"));

    cs.add(Case.revs("count-revisions-two-ways",
        "let $d := jn:doc(DB,RES) return (sdb:revision($d), count(jn:all-times($d)))",
        "3 3",
        "{\"v\":\"a\"}", "{\"v\":\"b\"}", "{\"v\":\"c\"}"));

    cs.add(Case.revs("jn-first-opens-oldest-revision-scalar",
        "jn:first(jn:doc(DB,RES)).price",
        "100",
        "{\"price\":100}", "{\"price\":200}", "{\"price\":300}"));

    cs.add(Case.revs("jn-last-opens-latest-revision-object",
        "jn:last(jn:doc(DB,RES))",
        "{\"a\":10,\"b\":2,\"c\":3}",
        "{\"a\":1,\"b\":2}", "{\"a\":10,\"b\":2}", "{\"a\":10,\"b\":2,\"c\":3}"));

    cs.add(Case.revs("jn-previous-of-opened-revision",
        "jn:previous(jn:doc(DB,RES,3))",
        "{\"v\":\"b\"}",
        "{\"v\":\"a\"}", "{\"v\":\"b\"}", "{\"v\":\"c\"}"));

    cs.add(Case.revs("jn-next-of-revision-1",
        "jn:next(jn:doc(DB,RES,1))",
        "{\"v\":\"b\"}",
        "{\"v\":\"a\"}", "{\"v\":\"b\"}", "{\"v\":\"c\"}"));

    cs.add(Case.revs("jn-past-without-include-self-newest-first",
        "jn:past(jn:doc(DB,RES,3))",
        "{\"v\":\"b\"} {\"v\":\"a\"}",
        "{\"v\":\"a\"}", "{\"v\":\"b\"}", "{\"v\":\"c\"}"));

    cs.add(Case.revs("jn-future-with-include-self-oldest-first",
        "jn:future(jn:doc(DB,RES,1),true())",
        "{\"v\":\"a\"} {\"v\":\"b\"} {\"v\":\"c\"}",
        "{\"v\":\"a\"}", "{\"v\":\"b\"}", "{\"v\":\"c\"}"));

    cs.add(Case.revs("jn-all-times-tracks-changed-value",
        "for $v in jn:all-times(jn:doc(DB,RES)) return {\"revision\": sdb:revision($v), \"level\": $v.level}",
        "{\"revision\":1,\"level\":\"low\"} {\"revision\":2,\"level\":\"high\"} {\"revision\":3,\"level\":\"critical\"}",
        "{\"name\":\"X\",\"level\":\"low\"}", "{\"name\":\"X\",\"level\":\"high\"}", "{\"name\":\"X\",\"level\":\"critical\"}"));

    cs.add(Case.revs("value-at-revision-plus-boolean-change-detection",
        "let $d := jn:doc(DB,RES) return (jn:doc(DB,RES,2)[0].price, jn:doc(DB,RES,3)[1].price ne jn:doc(DB,RES,1)[1].price)",
        "15 true",
        "[{\"id\":1,\"price\":10},{\"id\":2,\"price\":20}]",
        "[{\"id\":1,\"price\":15},{\"id\":2,\"price\":20}]",
        "[{\"id\":1,\"price\":15},{\"id\":2,\"price\":25}]"));

    // --- alpha13 number fidelity ---
    cs.add(Case.single("overflow-2e308-round-trips-as-bigdecimal-not-infinity",
        "{\"x\":2e308}",
        "jn:doc(DB,RES).x",
        "2" + "0".repeat(308)));

    cs.add(Case.single("subnormal-double-kept-faithful-no-underflow",
        "{\"x\":2.2250738585072014e-308}",
        "jn:doc(DB,RES).x",
        "2.2250738585072014E-308"));

    cs.add(Case.single("negative-zero-and-zero-exponent-normalize-to-zero",
        "{\"a\":-0,\"b\":-0.0,\"c\":0e0,\"d\":-0e10}",
        "jn:doc(DB,RES)",
        "{\"a\":0,\"b\":0,\"c\":0,\"d\":0}"));

    cs.add(Case.single("high-precision-bigdecimal-round-trips-exactly",
        "{\"tiny\":0.0000000000000000000000001,\"money\":99999999999999999999.99}",
        "jn:doc(DB,RES)",
        "{\"tiny\":0.0000000000000000000000001,\"money\":99999999999999999999.99}"));

    cs.add(Case.single("sum-of-two-max-doubles-overflows-to-INF",
        "[1e308,1e308]",
        "sum(jn:doc(DB,RES)[])",
        "INF"));

    cs.add(Case.single("decimal-addition-is-exact-unlike-binary-float",
        "{\"a\":0.1,\"b\":0.2,\"c\":0.3}",
        "jn:doc(DB,RES).a + jn:doc(DB,RES).b + jn:doc(DB,RES).c",
        "0.6"));

    cs.add(Case.single("clean-scientific-notation-fields-route-to-decimal-path",
        "{\"a\":1.25e7,\"b\":6.022e23,\"c\":1.602e-19}",
        "jn:doc(DB,RES)",
        "{\"a\":1.25E7,\"b\":6.022E23,\"c\":1.602E-19}"));

    // A non-BMP character is ONE character (codepoint), not two UTF-16 units.
    cs.add(Case.single("string-length-emoji-is-one-codepoint",
        "{\"x\":\"😀\"}",
        "string-length(jn:doc(DB,RES).x)",
        "1"));

    cs.add(Case.single("unicode-string-predicate-match-returns-value",
        "[{\"n\":\"café\",\"v\":1},{\"n\":\"tea\",\"v\":2}]",
        "for $i in jn:doc(DB,RES)[] where $i.n eq \"café\" return $i.v",
        "1"));

    cs.add(Case.single("null-handling-count-includes-null",
        "[1,null,3]",
        "count(jn:doc(DB,RES)[])",
        "3"));

    return cs;
  }
}
