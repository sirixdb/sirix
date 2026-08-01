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
 * compares name KEYS as ints, resolved once per lookup.
 *
 * <p>Hand-written cursor code has to reproduce three things the axis did for free, and each case
 * below pins one of them:
 * <ul>
 * <li>a match leaves the cursor ON the matching node, which under record fusion IS the value — so
 * a nested object or array must come back whole rather than collapsed to its first field;</li>
 * <li>a miss resets the cursor to the object, mirroring {@code AbstractAxis.resetToStartKey()};
 * without it the NEXT lookup on the same object starts from wherever the failed walk stopped;</li>
 * <li>a name absent from the resource's dictionary resolves to key {@code -1}, which must mean
 * "no such field" rather than accidentally matching a child whose name key is unset.</li>
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
    try (final var store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile().getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + DOC.replace("'", "''") + "')").evaluate(ctx);
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
    // keyForName returns -1 for a name the resource never stored. That must short-circuit to "no
    // such field" rather than being compared against children's name keys.
    assertEquals("", field("$d.zzzNeverStoredAnywhere"));
    assertEquals("\"Saleslady\" 1938", field("($d.title, $d.zzzNeverStoredAnywhere, $d.year)"),
                 "the short-circuit must leave the cursor usable");
  }

  @Test
  @DisplayName("repeated access to the same field is stable")
  void repeatedAccessIsStable() {
    // The lookup memoizes on success; a stale or mis-keyed cache entry would show up here.
    assertEquals("1938 1938 1938", field("($d.year, $d.year, $d.year)"));
    assertEquals("\"Saleslady\" 1938 \"Saleslady\"", field("($d.title, $d.year, $d.title)"));
  }
}
