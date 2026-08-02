package io.sirix.query.json;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the semantics of {@code JsonDBObject}'s field lookup after it stopped going through
 * {@code new FilterAxis<>(new ChildAxis(rtx), new JsonNameFilter(rtx, field))}.
 *
 * <p>That axis cost three objects plus a capturing lambda on every field access, and compared
 * {@link io.brackit.query.atomic.QNm} objects — materializing a name and doing string equality for
 * each child visited. A scan binds a FRESH object per record, so the memoizing map those
 * allocations filled was discarded immediately; allocation profiling of a 290k-record filter scan
 * attributed ~13 % of all allocations to this path. The replacement walks children directly and
 * compares the dictionary's raw UTF-8 name bytes against a needle encoded once per lookup, so no
 * name is materialized per child visited.
 *
 * <p>Hand-written cursor code has to reproduce three things the axis did for free, and each case
 * below pins one of them:
 * <ul>
 * <li>a match leaves the cursor ON the matching node, which under record fusion IS the value — so
 * a nested object or array must come back whole rather than collapsed to its first field;</li>
 * <li>a miss resets the cursor to the object, mirroring {@code AbstractAxis.resetToStartKey()};
 * without it the NEXT lookup on the same object starts from wherever the failed walk stopped;</li>
 * <li>a name no record in the resource carries must be a miss rather than a false match.</li>
 * </ul>
 */
public final class JsonDBObjectFieldLookupTest {

  private static final String DB = "json-path1";
  private static final String RES = "mydoc.jn";

  /** Mixed value kinds, a nested object and array, and a null — one record, several shapes. */
  private static final String DOC = """
      {"title":"Saleslady","year":1938,"active":true,"score":1.5,"href":null,\
      "nested":{"a":1,"b":2},"cast":["Anne Nagel","Weldon Heyburn"]}""";

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String run(final String query) {
    return run(query, DOC);
  }

  private static String run(final String query, final String doc) {
    try (final var store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + doc.replace("'", "''") + "')").evaluate(ctx);
      final var out = new ByteArrayOutputStream();
      try (final var pw = new PrintWriter(out, false, StandardCharsets.UTF_8)) {
        new Query(chain, query).serialize(ctx, pw);
      }
      return out.toString(StandardCharsets.UTF_8).strip();
    }
  }

  private static String field(final String expr) {
    return run("let $d := jn:doc('" + DB + "','" + RES + "') return " + expr);
  }

  private static String field(final String expr, final String doc) {
    return run("let $d := jn:doc('" + DB + "','" + RES + "') return " + expr, doc);
  }

  @Test
  @DisplayName("an object with no fields at all is a clean miss")
  void emptyObjectHasNoFields() {
    // Exercises the no-children branch of the first-child fast entry: the key captured at
    // construction is the NULL node key, so the walk must be skipped entirely and the null key
    // must never be handed to moveTo.
    assertEquals("", field("$d.anything", "{}"), "no field of an empty object may resolve");
    assertEquals("", field("($d.a, $d.b)", "{}"), "repeated misses on an empty object stay empty");
  }

  @Test
  @DisplayName("a single-field object resolves that field and nothing else")
  void singleFieldObject() {
    // First child is also last: the sibling walk must terminate without running off the chain.
    assertEquals("\"only\"", field("$d.a", "{\"a\":\"only\"}"), "the sole field must resolve");
    assertEquals("", field("$d.b", "{\"a\":\"only\"}"), "a different name must still miss");
    assertEquals("\"only\"", field("($d.b, $d.a)", "{\"a\":\"only\"}"),
                 "a miss before the sole field must not hide it");
  }

  @Test
  @DisplayName("primitive fields of every stored kind come back with the right value")
  void primitiveFieldsResolve() {
    assertEquals("\"Saleslady\"", field("$d.title"), "string field");
    assertEquals("1938", field("$d.year"), "integer field");
    assertEquals("true", field("$d.active"), "boolean field");
    assertEquals("1.5", field("$d.score"), "double field");
    assertEquals("null", field("$d.href"), "explicit JSON null must be the value, not a miss");
  }

  @Test
  @DisplayName("a nested object comes back whole, not collapsed to its first field")
  void nestedObjectIsNotUnwrapped() {
    // Under record fusion the cursor lands on OBJECT_NAMED_OBJECT and IS the value. Descending to
    // the first child here would return 1 instead of the object - the historical collapse bug.
    assertEquals("{\"a\":1,\"b\":2}", field("$d.nested"));
    assertEquals("1", field("$d.nested.a"), "nested field access must still work through the pair");
  }

  @Test
  @DisplayName("a nested array comes back whole")
  void nestedArrayIsNotUnwrapped() {
    assertEquals("[\"Anne Nagel\",\"Weldon Heyburn\"]", field("$d.cast"));
  }

  @Test
  @DisplayName("a missing field must not hide other fields of the same object")
  void missingFieldDoesNotPoisonOtherLookups() {
    assertEquals("", field("$d.nosuchfield"), "absent field must yield the empty sequence");

    // A MISSING field looked up FIRST must not poison later lookups on the same object. The
    // path-summary guard caches PathSummaryReader.match(field, level) per object; keying that
    // cache by path-class record alone let the first lookup's answer stand in for every later
    // one, so a leading miss cached an empty match and reported every existing field missing.
    // This ordering is the reproduction — with the miss LAST or in the middle it passes either
    // way, because a hit caches a non-empty match first.
    assertEquals("\"Saleslady\"",
                 field("($d.nosuchfield, $d.title)"),
                 "a miss looked up FIRST must not hide a field that exists");

    assertEquals("\"Saleslady\" 1938",
                 field("($d.nosuchfield, $d.title, $d.year)"),
                 "a leading miss must not hide any later field");

    assertEquals("\"Saleslady\"",
                 field("($d.nope1, $d.nope2, $d.title)"),
                 "two leading misses must still not hide an existing field");

    assertEquals("\"Saleslady\" 1938",
                 field("($d.title, $d.nosuchfield, $d.year)"),
                 "a hit AFTER a miss on the same object must still resolve");

    assertEquals("1938 1938",
                 field("($d.year, $d.mmmissing, $d.year)"),
                 "hits on either side of a miss must agree");

    assertEquals("\"Saleslady\" \"Saleslady\"",
                 field("($d.title, $d.nosuchfield, $d.title)"),
                 "a hit, a miss, then the same hit again must agree");
  }

  @Test
  @DisplayName("a name that exists in no resource dictionary is a miss, not a false match")
  void nameAbsentFromDictionaryIsAMiss() {
    // A name the resource never stored matches no child's stored name bytes, so the walk must run
    // off the end and report "no such field" — and must leave the cursor where the next lookup on
    // the same object can use it.
    assertEquals("", field("$d.zzzNeverStoredAnywhere"));
    assertEquals("\"Saleslady\" 1938", field("($d.title, $d.zzzNeverStoredAnywhere, $d.year)"),
                 "a miss must leave the cursor usable");
  }

  @Test
  @DisplayName("field names whose hashes collide each resolve to their own value")
  void hashCollidingFieldNamesResolveIndependently() {
    // "Aa" and "BB" both have String.hashCode() 2112, and that hash is exactly what
    // NamePageHash.generateHashForString (rtx.keyForName) returns. The key a record actually
    // STORES is the name dictionary's, which probes past collisions: "Aa" owns 2112 and "BB" 2113.
    //
    // Two distinct defects met here, and this case pins both closed:
    //
    //  * comparing rtx.getNameKey() against keyForName(field) matched Aa's record for a lookup of
    //    BB — a silent WRONG VALUE ($d.BB gave 1), with BB's own record unreachable. The walk now
    //    compares the dictionary's raw name bytes, which are per-name and cannot collide.
    //
    //  * the path summary collapsed colliding names into a single node, so
    //    PathSummaryReader.match("BB") was empty and the "this field cannot exist here" short
    //    circuit reported an EXISTING field as MISSING ($d.BB gave the empty sequence). The short
    //    circuit is gone from the lookup AND the summary keeps a node per name, so this case
    //    holds whichever of the two a later change touches.
    final String doc = "{\"Aa\":1,\"BB\":2}";
    assertEquals("1", field("$d.Aa", doc), "the name that won the collision must resolve");
    assertEquals("2", field("$d.BB", doc), "the name that lost the collision must resolve too");
    assertEquals("1 2", field("($d.Aa, $d.BB)", doc), "both must resolve in one query");
    assertEquals("2 1", field("($d.BB, $d.Aa)", doc), "and in either order");
    assertEquals("", field("$d.CC", doc), "a third name must still be a clean miss");
  }

  @Test
  @DisplayName("repeated access to the same field is stable")
  void repeatedAccessIsStable() {
    // The lookup memoizes on success; a stale or mis-keyed cache entry would show up here.
    assertEquals("1938 1938 1938", field("($d.year, $d.year, $d.year)"));
    assertEquals("\"Saleslady\" 1938 \"Saleslady\"", field("($d.title, $d.year, $d.title)"));
  }
}
