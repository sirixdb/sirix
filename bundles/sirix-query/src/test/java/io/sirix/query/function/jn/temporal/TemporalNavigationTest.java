package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec tests for the temporal navigation functions on stored value items (issue #1429,
 * {@code docs/TEMPORAL_FUNCTIONS_SPEC.md}).
 *
 * <p>
 * History of resource {@code res} (one revision per step):
 * <ol>
 * <li>store: position 0 is an object with one field of each kind, position 1 an object kept
 * unchanged, positions 2-7 hold an integer, a boolean, {@code null}, a decimal, a long and a
 * string</li>
 * <li>update every value in place through the node API, whose setters keep the node key (null stays
 * null); {@code replace json value} of an array member would insert a new node instead</li>
 * <li>delete the string at position 7 (last, so no position shifts) and the field {@code name};
 * replace {@code none}'s null by 7</li>
 * <li>revert to revision 2 (the deleted nodes exist again)</li>
 * <li>{@code replace json value} of position 2 (new node), of {@code price} (same node) and of the
 * boolean at position 3 by a string (new node)</li>
 * </ol>
 *
 * <p>
 * The oracle never uses the temporal axes: it reads the node with {@code sdb:select-item} in each
 * revision and applies the spec's revision rules.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
final class TemporalNavigationTest {

  private static final String DB = "temporal-values";

  private static final String RES = "res";

  private static final int MOST_RECENT = 5;

  /** Value paths relative to the document: array positions and object field values. */
  private static final List<String> VALUE_PATHS = List.of("[2]", "[3]", "[4]", "[5]", "[6]", "[7]", "[0].price",
      "[0].name", "[0].ok", "[0].none", "[0].rate", "[0].big");

  /** (value path, revision) pairs without a node: [7] and [0].name in revision 3. */
  private static final int ABSENT_PATH_REVISIONS = 2;

  /** Field values whose node key is identical in every revision of the enclosing object. */
  private static final List<String> STABLE_FIELDS = List.of("price", "ok", "rate", "big");

  private BasicJsonDBStore store;

  private SirixQueryContext ctx;

  private SirixCompileChain chain;

  /** Revisions (1..MOST_RECENT) in which a node key exists, keyed by node key. */
  private final Map<Long, boolean[]> existence = new HashMap<>();

  @BeforeAll
  void createHistory(@TempDir final Path directory) {
    store = BasicJsonDBStore.newBuilder().location(directory).build();
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);

    evaluate("jn:store('" + DB + "','" + RES + "','["
        + "{\"price\":999,\"name\":\"a\",\"ok\":true,\"none\":null,\"rate\":1.5,\"big\":12345678901}, "
        + "{\"keep\":1}, 999, true, null, 1.5, 12345678901, \"a\"]')");
    final JsonResourceSession session =
        ((JsonDBItem) execute("jn:doc('" + DB + "','" + RES + "')")).getResourceSession();
    final JsonNodeTrx wtx = session.getNodeTrx().orElseGet(session::beginNodeTrx);
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.moveToRightSibling();
    wtx.moveToRightSibling();
    wtx.setNumberValue(899);
    wtx.moveToRightSibling();
    wtx.setBooleanValue(false);
    wtx.moveToRightSibling();
    wtx.moveToRightSibling();
    wtx.setNumberValue(2.5);
    wtx.moveToRightSibling();
    wtx.setNumberValue(98765432109L);
    wtx.moveToRightSibling();
    wtx.setStringValue("b");
    // Fused field records of [0], in document order: price, name, ok, none, rate, big.
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.setNumberValue(899);
    wtx.moveToRightSibling();
    wtx.setStringValue("b");
    wtx.moveToRightSibling();
    wtx.setBooleanValue(false);
    wtx.moveToRightSibling();
    wtx.moveToRightSibling();
    wtx.setNumberValue(2.5);
    wtx.moveToRightSibling();
    wtx.setNumberValue(98765432109L);
    wtx.commit();
    evaluate("""
        let $d := jn:doc('%s','%s')
        return (delete json $d[7], delete json $d[0].name, replace json value of $d[0].none with 7)""".formatted(DB,
        RES));

    final JsonNodeTrx reverter = session.getNodeTrx().orElseGet(session::beginNodeTrx);
    reverter.revertTo(2);
    reverter.commit();

    evaluate("""
        let $d := jn:doc('%s','%s')
        return (replace json value of $d[2] with 799, replace json value of $d[0].price with 799,
                replace json value of $d[3] with "now a string")""".formatted(DB, RES));

    assertEquals(String.valueOf(MOST_RECENT), query("sdb:revision(jn:doc('" + DB + "','" + RES + "'))"),
        "fixture must have exactly " + MOST_RECENT + " revisions");
  }

  @AfterAll
  void close() {
    if (chain != null) {
      chain.close();
    }
    if (ctx != null) {
      ctx.close();
    }
    if (store != null) {
      store.close();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Issue #1429: the exact shell reproduction.
  // ---------------------------------------------------------------------------------------------

  @Test
  void issue1429ShellReproduction() {
    evaluate("jn:store('" + DB + "','products','[{\"name\":\"Laptop\",\"price\":999}]')");
    evaluate("replace json value of jn:doc('" + DB + "','products')[0].price with 899");

    assertEquals("true", query("sdb:nodekey(jn:doc('" + DB + "','products', 1)[0].price) eq " + "sdb:nodekey(jn:doc('"
        + DB + "','products', 2)[0].price)"));
    assertEquals("999", query("jn:first(jn:doc('" + DB + "','products')[0].price)"));
    assertEquals("999", query("jn:previous(jn:doc('" + DB + "','products')[0].price)"));
    assertEquals("899", query("jn:last(jn:doc('" + DB + "','products', 1)[0].price)"));
    assertEquals("899", query("jn:next(jn:doc('" + DB + "','products', 1)[0].price)"));
    assertEquals("999 899", query("jn:all-times(jn:doc('" + DB + "','products')[0].price)"));
    assertEquals("999", query("jn:past(jn:doc('" + DB + "','products')[0].price)"));
    assertEquals("899", query("jn:future(jn:doc('" + DB + "','products', 1)[0].price)"));
  }

  // ---------------------------------------------------------------------------------------------
  // Spec invariants 1 and 2: every function x every value kind x array position / field value x
  // every source revision, against the select-item oracle.
  // ---------------------------------------------------------------------------------------------

  @TestFactory
  List<DynamicTest> everyFunctionOnEveryStoredValueMatchesTheOracle() {
    final List<DynamicTest> tests = new ArrayList<>();
    int covered = 0;
    for (int revision = 1; revision <= MOST_RECENT; revision++) {
      for (final String path : VALUE_PATHS) {
        final String source = doc(revision) + path;
        if (query("count(" + source + ")").equals("0")) {
          continue;
        }
        final long key = Long.parseLong(query("sdb:nodekey(" + source + ")"));
        for (final Fn fn : Fn.values()) {
          final int r = revision;
          tests.add(DynamicTest.dynamicTest(fn.call(source),
              () -> assertEquals(expected(fn.revisions(r, MOST_RECENT, existence(key)), key), actual(fn, source),
                  fn.call(source))));
          covered++;
        }
      }
    }
    // Guard against a vacuous matrix: every value path in every revision it exists in.
    assertEquals(Fn.values().length * (VALUE_PATHS.size() * MOST_RECENT - ABSENT_PATH_REVISIONS), covered);
    return tests;
  }

  @Test
  void resultsCarryTheValueAndTypeOfTheirRevision() {
    // integer, string, boolean, null, decimal and long values at an array position ...
    assertEquals("999 899 899 899", query("jn:all-times(" + doc(4) + "[2])"));
    assertEquals("\"a\"", query("jn:first(" + doc(2) + "[7])"));
    assertEquals("true", query("jn:first(" + doc(4) + "[3])"));
    assertEquals("false", query("jn:next(" + doc(1) + "[3])"));
    assertEquals("true", query("jn:first(" + doc(4) + "[3]) instance of xs:boolean"));
    assertEquals("true", query("jn:last(" + doc(1) + "[7]) instance of xs:string"));
    assertEquals("true", query("jn:first(" + doc(5) + "[0].price) instance of xs:integer"));
    assertEquals("null", query("jn:first(" + doc(5) + "[4])"));
    assertEquals("1.5", query("jn:first(" + doc(5) + "[5])"));
    assertEquals("12345678901", query("jn:first(" + doc(5) + "[6])"));
    // ... and as object field values.
    assertEquals("899 999", query("jn:past(" + doc(3) + "[0].price)"));
    assertEquals("\"a\"", query("jn:first(" + doc(2) + "[0].name)"));
    assertEquals("true", query("jn:first(" + doc(5) + "[0].ok)"));
    assertEquals("12345678901", query("jn:first(" + doc(5) + "[0].big)"));
    // Arithmetic on a navigated value uses the value of its revision.
    assertEquals("1000", query("jn:first(" + doc(4) + "[2]) + 1"));
  }

  @Test
  void absentNodeYieldsTheEmptySequence() {
    // The string at position 7 and the field "name" were deleted in revision 3, restored by the revert.
    assertEquals("", query("jn:next(" + doc(2) + "[7])"));
    assertEquals("", query("jn:next(" + doc(2) + "[0].name)"));
    assertEquals("", query("jn:previous(" + doc(4) + "[0].name)"));
    assertEquals("\"b\"", query("jn:last(" + doc(2) + "[0].name)"));
    assertEquals("\"b\"", query("jn:future(" + doc(2) + "[7], true())"));
    // The first revision has no predecessor; the most recent no successor.
    assertEquals("", query("jn:previous(" + doc(1) + "[2])"));
    assertEquals("", query("jn:next(" + doc(5) + "[2])"));
    assertEquals("", query("jn:past(" + doc(1) + "[2])"));
    assertEquals("", query("jn:future(" + doc(5) + "[2])"));
  }

  // ---------------------------------------------------------------------------------------------
  // Spec invariant 3: differential against navigating through the enclosing record.
  // ---------------------------------------------------------------------------------------------

  @TestFactory
  List<DynamicTest> valueRouteEqualsEnclosingRecordRoute() {
    final List<DynamicTest> tests = new ArrayList<>();
    for (final String field : STABLE_FIELDS) {
      // Precondition: the field's node key is the same in every revision, so both routes name the
      // same node (else the differential would compare different nodes).
      final String[] keys =
          query("for $o in jn:all-times(" + doc(1) + "[0]) return sdb:nodekey($o." + field + ")").split(" ");
      assertEquals(MOST_RECENT, keys.length, field + " must exist in every revision");
      assertEquals(1, Arrays.stream(keys).distinct().count(),
          "node key of " + field + " must be stable across all revisions: " + Arrays.toString(keys));
      for (int revision = 1; revision <= MOST_RECENT; revision++) {
        for (final Fn fn : Fn.values()) {
          final String value = fn.call(doc(revision) + "[0]." + field);
          final String viaRecord = "for $o in " + fn.call(doc(revision) + "[0]") + " return $o." + field;
          tests.add(DynamicTest.dynamicTest(value + " == " + viaRecord, () -> {
            final String expected = query(viaRecord);
            assertEquals(expected, query(value));
          }));
        }
      }
    }
    return tests;
  }

  // ---------------------------------------------------------------------------------------------
  // Field names: a fused record holds the name and the value; a name stays a name.
  // ---------------------------------------------------------------------------------------------

  @Test
  void fieldNamesNavigateAsNames() {
    final String names = "bit:fields(" + doc(5) + "[0])[]";
    assertEquals(query(names), query("for $n in " + names + " return jn:first($n)"),
        "jn:first of a field name must yield the name, not the field value");
    assertEquals("\"price\" \"price\" \"price\" \"price\" \"price\"",
        query("for $n in " + names + " where $n eq 'price' return jn:all-times($n)"));
    assertEquals("true", query("every $n in " + names + " satisfies jn:previous($n) instance of xs:string"));
  }

  // ---------------------------------------------------------------------------------------------
  // Exploratory: mixed types at one path, query shapes beyond a plain path, chained navigation.
  // ---------------------------------------------------------------------------------------------

  @Test
  void typeChangingFieldReplacementStartsANewHistory() {
    final String original = doc(2) + "[0].none";
    final String replacement = doc(3) + "[0].none";

    assertEquals("false", query("sdb:nodekey(" + original + ") eq sdb:nodekey(" + replacement + ")"));
    assertEquals("null null", query("jn:all-times(" + original + ")"));
    assertEquals("1 2", query("for $v in jn:all-times(" + original + ") return sdb:revision($v)"));
    assertEquals("7", query("jn:all-times(" + replacement + ")"));
    assertEquals("3", query("for $v in jn:all-times(" + replacement + ") return sdb:revision($v)"));
    assertEquals("", query("jn:first(" + replacement + ")"));
    assertEquals("7", query("jn:first-existing(" + replacement + ")"));
    assertEquals("", query("jn:previous(" + replacement + ")"));
    assertEquals("", query("jn:past(" + replacement + ")"));
    assertEquals("7", query("jn:past(" + replacement + ", true())"));
    assertEquals("", query("jn:next(" + original + ")"));
    assertEquals("", query("jn:future(" + original + ")"));
    assertEquals("null", query("jn:future(" + original + ", true())"));
    assertEquals("", query("jn:next(" + replacement + ")"));
    assertEquals("", query("jn:last(" + replacement + ")"));
    assertEquals("", query("jn:future(" + replacement + ")"));
    assertEquals("7", query("jn:future(" + replacement + ", true())"));
  }

  @Test
  void mixedTypesAtOnePathAcrossRevisions() {
    // [0].none: the null node is replaced by a new number node in revision 3, restored by the revert.
    final long key = Long.parseLong(query("sdb:nodekey(" + doc(1) + "[0].none)"));
    assertEquals(expected(Fn.ALL_TIMES.revisions(1, MOST_RECENT, existence(key)), key),
        actual(Fn.ALL_TIMES, doc(1) + "[0].none"));
    // [3]: a boolean node in revisions 1-4; revision 5 replaced it by a new string node.
    assertEquals("false", query("sdb:nodekey(" + doc(4) + "[3]) eq sdb:nodekey(" + doc(5) + "[3])"));
    assertEquals("false", query("sdb:nodekey(" + doc(4) + "[2]) eq sdb:nodekey(" + doc(5) + "[2])"));
    assertEquals("", query("jn:first(" + doc(5) + "[2])"));
    assertEquals("", query("jn:previous(" + doc(5) + "[2])"));
    assertEquals("799", query("jn:all-times(" + doc(5) + "[2])"));
    assertEquals("true false false false", query("jn:all-times(" + doc(1) + "[3])"));
    assertEquals("\"now a string\"", query("jn:all-times(" + doc(5) + "[3])"));
    assertEquals("true false false false \"now a string\"",
        query("for $s in 1 to 5 return jn:doc('" + DB + "','" + RES + "', $s)[3]"));
    assertEquals("4 4 4 4 1",
        query("for $s in 1 to 5 return count(jn:all-times(jn:doc('" + DB + "','" + RES + "', $s)[3]))"));
  }

  @Test
  void flworAndChainedNavigationAgreeWithPathNavigation() {
    assertEquals(query("jn:first(" + doc(5) + "[0].price)"), query("for $r in " + doc(5)
        + "[] where $r instance of object() and exists($r.price) " + "return jn:first($r.price)"));
    assertEquals(query("jn:first(" + doc(5) + "[2])"), query("let $v := " + doc(5) + "[2] return jn:first($v)"));
    assertEquals(query(doc(5) + "[0].price"), query("jn:last(jn:first(" + doc(5) + "[0].price))"));
    assertEquals("2", query("sdb:revision(jn:next(jn:first(" + doc(4) + "[2])))"));
    assertEquals("1 2 3 4", query("for $v in jn:all-times(" + doc(1) + "[2]) return sdb:revision($v)"));
  }

  @Test
  void navigationRepositionsTheSharedCursorOnTheItemsNode() {
    // Items of one array share its cursor; binding $other moves it away from $value's node, so
    // navigating $value must first move the cursor back (each value item's getTrx() does).
    for (final String[] pair : new String[][] {{"[7]", "[2]"}, {"[2]", "[7]"}, {"[3]", "[0].price"}}) {
      final String bound = "let $d := " + doc(4) + " let $value := $d" + pair[0] + " let $other := $d" + pair[1];
      assertEquals("1 " + query("[jn:first(" + doc(4) + pair[0] + "), jn:all-times(" + doc(4) + pair[0] + ")]"),
          query(bound + " return (count($other), [jn:first($value), jn:all-times($value)])"),
          pair[0] + " after " + pair[1]);
    }
  }

  @Test
  void objectsAndArraysKeepTheirResultViews() {
    assertEquals("{\"keep\":1}", query("jn:first(" + doc(5) + "[1])"));
    assertEquals(query(doc(1)), query("jn:first(" + doc(5) + ")"));
    assertEquals("true", query("jn:first(" + doc(5) + "[0]) instance of object()"));
  }

  // ---------------------------------------------------------------------------------------------
  // Errors: computed items never surface a ClassCastException.
  // ---------------------------------------------------------------------------------------------

  @TestFactory
  List<DynamicTest> computedItemsRaiseTheNotAStoredItemError() {
    final List<DynamicTest> tests = new ArrayList<>();
    for (final String computed : List.of("{\"a\": 1}", "[1, 2]", "{\"a\": 1}.a", "[1, 2][0]",
        "upper-case(" + doc(5) + "[0].name)", doc(5) + "[0].price + 1", "42", "\"x\"", "true()", "1.5", "jn:null()")) {
      for (final Fn fn : Fn.values()) {
        final String call = "count(" + fn.call(computed) + ")";
        tests.add(DynamicTest.dynamicTest(call, () -> {
          final String type = ((Item) execute(computed)).itemType().toString();
          final QueryException error = assertThrows(QueryException.class, () -> query(call));
          assertEquals(JNFun.ERR_NOT_A_STORED_ITEM, error.getCode(), error.getMessage());
          assertTrue(error.getMessage().contains(fn.name), error.getMessage());
          assertTrue(error.getMessage().contains("computed " + type), error.getMessage());
        }));
      }
    }
    return tests;
  }

  @TestFactory
  List<DynamicTest> emptyOrMultipleItemsRaiseTheCardinalityError() {
    final List<DynamicTest> tests = new ArrayList<>();
    for (final String argument : List.of("()", "(" + doc(5) + "[2], " + doc(5) + "[3])")) {
      for (final Fn fn : Fn.values()) {
        final String call = "count(" + fn.call(argument) + ")";
        tests.add(DynamicTest.dynamicTest(call, () -> {
          final QueryException error = assertThrows(QueryException.class, () -> query(call));
          assertEquals("XPTY0004", error.getCode().getLocalName(), error.getMessage());
        }));
      }
    }
    return tests;
  }

  // ---------------------------------------------------------------------------------------------
  // Oracle.
  // ---------------------------------------------------------------------------------------------

  /** The temporal functions and the spec's revision rule for each. */
  private enum Fn {
    FIRST("jn:first", ""), LAST("jn:last", ""), PREVIOUS("jn:previous", ""), NEXT("jn:next", ""), PAST("jn:past",
        ""), PAST_SELF("jn:past",
            ", true()"), FUTURE("jn:future", ""), FUTURE_SELF("jn:future", ", true()"), ALL_TIMES("jn:all-times", "");

    private final String name;

    private final String extraArgs;

    Fn(final String name, final String extraArgs) {
      this.name = name;
      this.extraArgs = extraArgs;
    }

    String call(final String argument) {
      return name + "(" + argument + extraArgs + ")";
    }

    /** Revisions the result covers, in result order, for an item read in {@code r}. */
    List<Integer> revisions(final int r, final int m, final boolean[] exists) {
      final List<Integer> result = new ArrayList<>();
      switch (this) {
        case FIRST -> addIf(result, 1, exists);
        case LAST -> addIf(result, m, exists);
        case PREVIOUS -> addIf(result, r - 1, exists);
        case NEXT -> addIf(result, r + 1, exists);
        case PAST, PAST_SELF -> {
          for (int s = this == PAST_SELF
              ? r
              : r - 1; s >= 1 && exists[s]; s--) {
            result.add(s);
          }
        }
        case FUTURE, FUTURE_SELF -> {
          for (int s = this == FUTURE_SELF
              ? r
              : r + 1; s <= m && exists[s]; s++) {
            result.add(s);
          }
        }
        case ALL_TIMES -> {
          int s = 1;
          while (s <= m && !exists[s]) {
            s++;
          }
          for (; s <= m && exists[s]; s++) {
            result.add(s);
          }
        }
      }
      return result;
    }

    private static void addIf(final List<Integer> result, final int revision, final boolean[] exists) {
      if (revision >= 1 && revision < exists.length && exists[revision]) {
        result.add(revision);
      }
    }
  }

  /** Revisions 1..MOST_RECENT in which {@code key} can be selected (index 0 unused). */
  private boolean[] existence(final long key) {
    return existence.computeIfAbsent(key, k -> {
      final boolean[] exists = new boolean[MOST_RECENT + 1];
      for (int s = 1; s <= MOST_RECENT; s++) {
        try {
          query("sdb:select-item(" + doc(s) + ", " + k + ")");
          exists[s] = true;
        } catch (final QueryException absent) {
          exists[s] = false;
        }
      }
      return exists;
    });
  }

  private String expected(final List<Integer> revisions, final long key) {
    if (revisions.isEmpty()) {
      return query("[]");
    }
    final StringJoiner list = new StringJoiner(", ", "(", ")");
    revisions.forEach(s -> list.add(String.valueOf(s)));
    return query("[ for $s in " + list + " let $v := sdb:select-item(jn:doc('" + DB + "','" + RES + "', $s), " + key
        + ") return " + describe("$v") + " ]");
  }

  private String actual(final Fn fn, final String source) {
    return query("[ for $v in " + fn.call(source) + " return " + describe("$v") + " ]");
  }

  private static String describe(final String var) {
    return "{\"rev\": sdb:revision(" + var + "), \"key\": sdb:nodekey(" + var + "), \"value\": " + var + "}";
  }

  private static String doc(final int revision) {
    return "jn:doc('" + DB + "','" + RES + "', " + revision + ")";
  }

  private void evaluate(final String query) {
    new Query(chain, query).evaluate(ctx);
  }

  private Sequence execute(final String query) {
    return new Query(chain, query).execute(ctx);
  }

  private String query(final String query) {
    final var buf = IOUtils.createBuffer();
    try (final var serializer = new StringSerializer(buf)) {
      serializer.serialize(execute(query));
    }
    return buf.toString();
  }
}
