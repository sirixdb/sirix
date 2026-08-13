package io.sirix.query.function.jn.index;

import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * JSONiq-level differential tests for the HOT index backend.
 *
 * <h2>What these add over the unit tests</h2>
 * <p>
 * {@code CASIndexDifferentialTest} drives the index through the {@code IndexController} API. These
 * drive it through the QUERY LANGUAGE — the path a user actually takes — which pulls in layers the
 * unit tests bypass entirely: the index-scan functions, the sequence plumbing that wraps a
 * {@code NodeReferences} stream into a JSONiq sequence, and the optimizer that turns a comparison
 * into an index scan in the first place.
 *
 * <h2>How they assert</h2>
 * <p>
 * Not against hand-written answers. Each query computes BOTH the indexed answer and the same answer
 * by plain traversal of the same document, and returns whether they agree, so the expected string
 * is always {@code true}. A wrong answer therefore cannot be masked by an expectation someone
 * copied out of a failing run — which is how hand-written counts silently accepted the byte-window
 * and cross-path defects that three review rounds had to find by reading code.
 *
 * <p>
 * The fixture is deliberately the shape that broke those rounds: a prefix chain
 * ({@code car}/{@code carpet}/{@code carpeting}), a duplicated value so a group holds more than one
 * node, an empty string, and a second field whose values interleave the first's so a scan that
 * escapes its own path shows up in the ANSWER, not merely in the count.
 */
public final class HOTIndexJsoniqDifferentialTest extends AbstractJsonTest {

  private static final String DOC = "[" + "{\"title\":\"\",\"alias\":\"zulu\"},"
      + "{\"title\":\"a\",\"alias\":\"yankee\"}," + "{\"title\":\"car\",\"alias\":\"car\"},"
      + "{\"title\":\"car\",\"alias\":\"alpha\"}," + "{\"title\":\"carpet\",\"alias\":\"\"},"
      + "{\"title\":\"carpeting\",\"alias\":\"bravo\"}," + "{\"title\":\"zebra\",\"alias\":\"carpet\"}]";

  private static final String STORE = "jn:store('json-path1','mydoc.jn','" + DOC + "')";

  /**
   * The index-creation step. The {@code sdb:commit($doc)} is load-bearing: creating an index is a
   * WRITE, so without the commit the index definition dies with the query context and the following
   * scan reports {@code Index no 0 ... not found} — which looks exactly like a lost index but is only
   * an uncommitted transaction.
   */
  private static String create(final String paths, final String type) {
    return "let $doc := jn:doc('json-path1','mydoc.jn') let $idx := jn:create-cas-index($doc,'" + type + "'," + paths
        + ") return sdb:commit($doc)";
  }

  private static final String CREATE_TITLE = create("'/[]/title'", "xs:string");
  private static final String CREATE_BOTH = create("('/[]/title','/[]/alias')", "xs:string");
  private static final String CREATE_INSTANT = create("'/[]/t'", "xs:dateTime");

  /** Re-open the document for the scan step; {@code $d} is the same handle, for plain traversal. */
  private static final String HEAD = "let $doc := jn:doc('json-path1','mydoc.jn') let $d := $doc ";

  private static String idx(final String path, final String type) {
    return "jn:find-cas-index($doc,'" + type + "','" + path + "')";
  }

  /** Compare an index scan over {@code field} against the same question asked of the document. */
  private static String agree(final String scan, final String field, final String predicate) {
    return HEAD + "let $indexed := for $v in " + scan + " order by $v return $v "
        + "let $plain := for $o in $d[] where " + predicate + " order by $o." + field + " return $o." + field + " "
        // Joined rather than compared positionally: in JSONiq `$seq[$i]` is ARRAY access, so it
        // raises XPTY0004 on a sequence. Joining compares order and content in one go.
        + "return fn:string-join($indexed,'|') eq fn:string-join($plain,'|')";
  }

  private static String scan(final String value, final String op, final String path) {
    return "jn:scan-cas-index($doc," + idx(path, "xs:string") + ",'" + value + "','" + op + "','" + path + "')";
  }

  private static String range(final String min, final String max, final boolean incMin, final boolean incMax,
      final String path) {
    return "jn:scan-cas-index-range($doc," + idx(path, "xs:string") + ",'" + min + "','" + max + "'," + incMin + "(),"
        + incMax + "(),'" + path + "')";
  }

  /** A range with one end left unbounded, expressed as {@code ()}. */
  private static String openRange(final String bound, final boolean isMin, final boolean inclusive, final String path) {
    final String min = isMin
        ? "'" + bound + "'"
        : "()";
    final String max = isMin
        ? "()"
        : "'" + bound + "'";
    return "jn:scan-cas-index-range($doc," + idx(path, "xs:string") + "," + min + "," + max + "," + (isMin
        ? inclusive
        : true) + "(),"
        + (isMin
            ? true
            : inclusive)
        + "(),'" + path + "')";
  }

  private static String onTitle(final String scanExpr, final String predicate) {
    return agree(scanExpr, "title", predicate);
  }

  // ==================== the byte-window shapes ====================

  @Test
  @DisplayName("<= a value that is a strict PREFIX of other stored values")
  void lowerOrEqualOnAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("car", "<=", "/[]/title"), "$o.title le 'car'"), "true");
  }

  @Test
  @DisplayName("< a prefix value: the equal group goes, the extensions stay out")
  void lowerThanAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("car", "<", "/[]/title"), "$o.title lt 'car'"), "true");
  }

  @Test
  @DisplayName(">= a prefix value keeps the equal group and every extension")
  void greaterOrEqualOnAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("car", ">=", "/[]/title"), "$o.title ge 'car'"), "true");
  }

  @Test
  @DisplayName("> a prefix value drops only the equal group")
  void greaterThanAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("car", ">", "/[]/title"), "$o.title gt 'car'"), "true");
  }

  @Test
  @DisplayName("== a value stored on more than one node returns the whole group")
  void equalOnADuplicatedValue() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("car", "==", "/[]/title"), "$o.title eq 'car'"), "true");
  }

  @Test
  @DisplayName("== the empty string, whose key is a bare header")
  void equalOnTheEmptyString() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("", "==", "/[]/title"), "$o.title eq ''"), "true");
  }

  @Test
  @DisplayName(">= the empty string returns every entry")
  void greaterOrEqualOnTheEmptyString() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(scan("", ">=", "/[]/title"), "$o.title ge ''"), "true");
  }

  // ==================== range inclusivity ====================

  @Test
  @DisplayName("closed range ending on a prefix value")
  void closedRangeToAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE,
        onTitle(range("a", "car", true, true, "/[]/title"), "$o.title ge 'a' and $o.title le 'car'"), "true");
  }

  @Test
  @DisplayName("half-open range excluding a prefix value that is not the last group")
  void halfOpenRangeExcludingAPrefixValue() throws IOException {
    test(STORE, CREATE_TITLE,
        onTitle(range("a", "car", true, false, "/[]/title"), "$o.title ge 'a' and $o.title lt 'car'"), "true");
  }

  @Test
  @DisplayName("open range excluding a value that is neither first nor last")
  void openRangeExcludingAMiddleValue() throws IOException {
    test(STORE, CREATE_TITLE,
        onTitle(range("car", "zebra", false, false, "/[]/title"), "$o.title gt 'car' and $o.title lt 'zebra'"), "true");
  }

  @Test
  @DisplayName("range that straddles no stored value at all")
  void emptyRange() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(range("d", "e", true, true, "/[]/title"), "$o.title ge 'd' and $o.title le 'e'"),
        "true");
  }

  // ==================== the cross-path shapes ====================

  @Test
  @DisplayName("one-sided >= on ONE path of a two-path index must not return the other path's values")
  void oneSidedScanStaysOnItsPath() throws IOException {
    test(STORE, CREATE_BOTH, agree(scan("a", ">=", "/[]/title"), "title", "$o.title ge 'a'"), "true");
  }

  @Test
  @DisplayName("one-sided <= on the other path, whose open end points the other way")
  void oneSidedScanStaysOnItsPathDownwards() throws IOException {
    test(STORE, CREATE_BOTH, agree(scan("zz", "<=", "/[]/alias"), "alias", "$o.alias le 'zz'"), "true");
  }

  @Test
  @DisplayName("a two-sided range on one path of a two-path index")
  void twoSidedScanOnOnePathOfATwoPathIndex() throws IOException {
    test(STORE, CREATE_BOTH,
        agree(range("a", "zz", true, true, "/[]/title"), "title", "$o.title ge 'a' and $o.title le 'zz'"), "true");
  }

  // ==================== one-sided ranges, now expressible ====================

  @Test
  @DisplayName("min-only range on a single-path index")
  void minOnlyRange() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(openRange("car", true, true, "/[]/title"), "$o.title ge 'car'"), "true");
  }

  @Test
  @DisplayName("max-only range on a single-path index")
  void maxOnlyRange() throws IOException {
    test(STORE, CREATE_TITLE, onTitle(openRange("car", false, true, "/[]/title"), "$o.title le 'car'"), "true");
  }

  @Test
  @DisplayName("min-only range on ONE path of a two-path index must not leak the other path")
  void minOnlyRangeStaysOnItsPath() throws IOException {
    test(STORE, CREATE_BOTH, agree(openRange("a", true, true, "/[]/title"), "title", "$o.title ge 'a'"), "true");
  }

  @Test
  @DisplayName("max-only range on the other path, whose open end points the other way")
  void maxOnlyRangeStaysOnItsPath() throws IOException {
    test(STORE, CREATE_BOTH, agree(openRange("zz", false, true, "/[]/alias"), "alias", "$o.alias le 'zz'"), "true");
  }

  // ==================== the instant family, end to end ====================

  @Test
  @DisplayName("a dateTime range is chronological, not lexical, through the query layer")
  void dateTimeRangeIsChronological() throws IOException {
    // 14:00:00+02:00 is the same instant as 12:00:00Z written with an offset, so >= noon UTC is all
    // four. A lexical bound orders the offset spelling apart from the Z one and returns fewer.
    final String doc = "[{\"t\":\"2020-06-15T12:00:00Z\"},{\"t\":\"2020-06-15T12:00:00.5Z\"},"
        + "{\"t\":\"2020-06-15T14:00:00+02:00\"},{\"t\":\"2021-01-01T00:00:00Z\"}]";
    test("jn:store('json-path1','mydoc.jn','" + doc + "')", CREATE_INSTANT,
        HEAD + "return count(jn:scan-cas-index($doc," + idx("/[]/t", "xs:dateTime")
            + ",'2020-06-15T12:00:00Z','>=','/[]/t'))",
        "4");
  }

  @Test
  @DisplayName("a dateTime scan keeps the sub-second value a lexical bound would drop")
  void dateTimeScanKeepsSubSecondValues() throws IOException {
    // >= 12:00:00.500Z is {12:00:00.500001Z, 13:00:00Z}. A lexical bound drops the first, because
    // '.' (0x2E) sorts below 'Z' (0x5A).
    final String doc = "[{\"t\":\"2020-06-15T12:00:00Z\"},{\"t\":\"2020-06-15T12:00:00.500001Z\"},"
        + "{\"t\":\"2020-06-15T13:00:00Z\"}]";
    test("jn:store('json-path1','mydoc.jn','" + doc + "')", CREATE_INSTANT,
        HEAD + "return count(jn:scan-cas-index($doc," + idx("/[]/t", "xs:dateTime")
            + ",'2020-06-15T12:00:00.500Z','>=','/[]/t'))",
        "2");
  }
}
