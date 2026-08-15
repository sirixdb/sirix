/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.function.jn.index;

import io.sirix.access.Databases;
import io.sirix.cache.BufferManager;
import io.sirix.cache.HOTLookupCache;
import io.sirix.query.AbstractJsonTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JSONiq-level cover for the parts of the CAS index read path that unit tests cannot reach.
 *
 * <p>
 * The point-lookup memoization sits between {@code jn:scan-cas-index} and the trie, and its failure
 * mode is not an exception — it is a query that returns the right shape of answer with the wrong
 * contents: a previous revision's postings, or another key's. {@code HOTLookupCacheTest} pins the
 * data structure and {@code HOTLookupCacheInvalidationTest} pins the sweeps, but neither runs a
 * query, so neither would catch a reader that consults the cache with a key that fails to
 * discriminate what the query actually varies. These do, by asking the index a question and asking
 * the document the same question, exactly as {@link HOTIndexJsoniqDifferentialTest} does.
 * </p>
 *
 * <p>
 * The other half is encoding. A CAS key is bytes, and the paths that produce those bytes were
 * rewritten for speed — the ASCII fast path now writes through {@code String.getBytes} and falls
 * back for anything above {@code 0x7F}, and the comparators read eight bytes at a time. A value
 * that takes the fallback, or one whose length straddles the eight-byte stride, is where an
 * encoding regression shows up as "the query silently finds nothing".
 * </p>
 *
 * <p>
 * Every assertion here is pinned to a literal rather than to a second reading of the same query.
 * The natural shape for a memoization test — ask twice, compare the answers — is satisfied by a
 * cache that returns the same WRONG list both times, and by an encoding bug that makes both sides
 * empty, which are the two failures actually worth catching.
 * </p>
 *
 * <p>
 * The revision-discrimination claim was checked by mutation rather than assumed: dropping the
 * revision from the memoization key ({@code AbstractHOTIndexReader}) fails
 * {@link #insertAfterIndexCreationIsVisible()} (3 → 2) and
 * {@link #replaceMovesTheValueBetweenKeys()} while leaving the rest green. Those two carry the
 * weight; see {@link #deleteAfterIndexCreationIsVisible()} for why deletion structurally cannot.
 * </p>
 *
 * <p>
 * The key's INDEX NUMBER needs a test shaped deliberately to reach it, and
 * {@link #aCrossedProbeDoesNotBorrowTheOtherIndexsAnswer()} is that test. Two indexes are two
 * independent tries, so the field is what says which trie a memoized posting list came from — but a
 * scan phrased the obvious way never demonstrates it, because a CAS key already opens with the path
 * class record and matched index/path pairs therefore differ in their key bytes anyway. Crossing
 * the probe removes that second discriminator: asking the alias index a question phrased in title's
 * path class produces byte-identical keys with different answers (1 and 0), and dropping the index
 * number alone then fails that test and no other.
 * </p>
 *
 * <p>
 * Worth stating because the weaker arrangement is the tempting one to write: with matched pairs, a
 * mutation of the index number OR of the path class record alone leaves everything green, and only
 * dropping both together fails {@link #twoIndexesEachAnswerForTheirOwnPath()}. Read on its own that
 * looks like evidence the index number is inert. It is not — it is only evidence that those
 * particular queries never put it under load.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HOTIndexMemoizationJsoniqTest extends AbstractJsonTest {

  /**
   * Values chosen for what they exercise, not for realism: a non-ASCII value and an emoji take the
   * UTF-8 fallback out of the ASCII fast path; the {@code stride*} values sit at 7, 8 and 9 bytes so
   * the word-at-a-time comparators are driven either side of their stride boundary; two entries share
   * a value so the posting list has more than one node key.
   */
  private static final String DOC = "[" + "{\"title\":\"alpha\",\"n\":1}," + "{\"title\":\"alpha\",\"n\":2},"
      + "{\"title\":\"Ünïcödé\",\"n\":3}," + "{\"title\":\"日本語\",\"n\":4}," + "{\"title\":\"emoji 🚀 here\",\"n\":5},"
      + "{\"title\":\"stride7\",\"n\":6}," + "{\"title\":\"stride08\",\"n\":7}," + "{\"title\":\"stride009\",\"n\":8},"
      + "{\"title\":\"zulu\",\"n\":9}]";

  private static final String STORE = "jn:store('json-path1','mydoc.jn','" + DOC + "')";

  private static final String CREATE_TITLE =
      "let $doc := jn:doc('json-path1','mydoc.jn') let $idx := jn:create-cas-index($doc,'xs:string','/[]/title') "
          + "return sdb:commit($doc)";

  private static final String HEAD = "let $doc := jn:doc('json-path1','mydoc.jn') let $d := $doc ";

  private static final String IDX = "jn:find-cas-index($doc,'xs:string','/[]/title')";

  /** An equality scan for {@code value}. */
  private static String eq(final String value) {
    return "jn:scan-cas-index($doc," + IDX + ",'" + value + "','==','/[]/title')";
  }

  /**
   * The titles the index returns for {@code value}, ordered, joined — a comparable scalar.
   *
   * <p>
   * Deliberately a different expression shape from {@link #countFor(String)} even though both ask the
   * trie the same question. The memoized answer is only exercised when a second lookup actually
   * reaches the reader, so the two forms in a single query must not be foldable into one evaluation.
   * </p>
   */
  private static String titlesFor(final String value) {
    return "fn:string-join(for $v in " + eq(value) + " order by $v return $v,'|')";
  }

  /** How many entries the index returns for {@code value}. */
  private static String countFor(final String value) {
    return "fn:count(" + eq(value) + ")";
  }

  /** The same question asked of the document rather than the index. */
  private static String plainTitlesFor(final String value) {
    return "fn:string-join(for $o in $d[] where $o.title eq '" + value + "' order by $o.title return $o.title,'|')";
  }

  // ==================== a second document, for the multi-index cases ====================

  /**
   * {@code shared} deliberately occurs under BOTH indexed fields, with a different number of hits
   * under each — one title, two aliases. A key that failed to separate the two paths would answer one
   * scan with the other's postings, and only differing counts make that visible.
   */
  private static final String MULTI_DOC = "[" + "{\"title\":\"shared\",\"alias\":\"one\",\"n\":10},"
      + "{\"title\":\"other\",\"alias\":\"shared\",\"n\":20}," + "{\"title\":\"third\",\"alias\":\"shared\",\"n\":20}]";

  private static final String MULTI_STORE = "jn:store('json-path1','mydoc.jn','" + MULTI_DOC + "')";

  /** Two SEPARATE calls, so the resource carries two CAS indexes with distinct index numbers. */
  private static final String CREATE_TITLE_AND_ALIAS =
      "let $doc := jn:doc('json-path1','mydoc.jn') let $a := jn:create-cas-index($doc,'xs:string','/[]/title') "
          + "let $b := jn:create-cas-index($doc,'xs:string','/[]/alias') return sdb:commit($doc)";

  /** ONE index spanning both paths, so both values live in the same trie under one index number. */
  private static final String CREATE_SPANNING = "let $doc := jn:doc('json-path1','mydoc.jn') "
      + "let $idx := jn:create-cas-index($doc,'xs:string',('/[]/title','/[]/alias')) return sdb:commit($doc)";

  private static final String CREATE_N = "let $doc := jn:doc('json-path1','mydoc.jn') "
      + "let $idx := jn:create-cas-index($doc,'xs:integer','/[]/n') return sdb:commit($doc)";

  private static final String CREATE_NAMES = "let $doc := jn:doc('json-path1','mydoc.jn') "
      + "let $idx := jn:create-name-index($doc,('title','alias')) return sdb:commit($doc)";

  private static final String MULTI_HEAD = "let $doc := jn:doc('json-path1','mydoc.jn') ";

  /** An equality scan of the CAS index over {@code path} for a string {@code value}. */
  private static String eqOn(final String path, final String value) {
    return "jn:scan-cas-index($doc,jn:find-cas-index($doc,'xs:string','" + path + "'),'" + value + "','==','" + path
        + "')";
  }

  /**
   * An equality scan that probes the index built over {@code indexPath} using {@code probePath} to
   * form the key — deliberately crossed.
   *
   * <p>
   * The index number and the path are independent arguments to {@code jn:scan-cas-index}, so a scan
   * can ask one index a question phrased in another index's path class. The probe key is then
   * byte-for-byte what the other index would have been asked, which is the only arrangement in which
   * the index number of the memoization key is the sole thing telling the two lookups apart.
   * </p>
   */
  private static String eqCrossed(final String indexPath, final String probePath, final String value) {
    return "jn:scan-cas-index($doc,jn:find-cas-index($doc,'xs:string','" + indexPath + "'),'" + value + "','==','"
        + probePath + "')";
  }

  // ==================== the feature is actually switched on ====================

  @Test
  @DisplayName("a scan actually populates the memoization cache")
  void aScanPopulatesTheCache() throws IOException {
    // The one test here that fails if memoization is DEAD rather than wrong. Every other assertion in
    // this class compares query results, and a query is answered identically whether the cache served
    // it or the trie did — so wiring lookupCache to null (an inverted isEnabled(), an
    // EmptyBufferManager, hasTrxIntentLog() flipping for read-only readers) would leave the whole
    // class green while the feature does nothing. That also silently voids the mutation evidence in
    // the class javadoc, since a bypassed cache makes every key-corrupting mutation inert.
    query(STORE);
    query(CREATE_TITLE);
    query(HEAD + "return " + countFor("alpha"));

    final BufferManager bufferManager = Databases.peekGlobalBufferManager();
    assertNotNull(bufferManager, "no global buffer manager after a query — the wiring changed");
    final HOTLookupCache cache = bufferManager.getHOTLookupCache();
    assertNotNull(cache, "the buffer manager exposes no lookup cache");
    // Skipped rather than failed when the operator disabled the cache: sirix.hotLookupCache.maxEntries
    // = 0 is a documented setting the benchmark instructions actively use, and a disabled cache is not
    // a broken one.
    if (cache.isEnabled()) {
      assertTrue(cache.size() > 0, "a point lookup did not memoize anything — the cache is bypassed");
    }
  }

  // ==================== memoization seen through queries ====================

  @Test
  @DisplayName("asking the index the same question twice gives the same answer")
  void repeatedLookupIsStable() throws IOException {
    // The second lookup is answered from the memoization cache rather than the trie, so this is the
    // one query shape that compares the cached path against the computed one. A cache that returned
    // a stale or foreign posting list passes every unit test and fails here.
    //
    // Both readings are pinned to literals rather than to each other: "the same wrong answer twice"
    // would satisfy an equality between the two readings, and that is precisely the failure a
    // memoization bug produces.
    test(STORE, CREATE_TITLE, HEAD + "let $n := " + countFor("alpha") + " let $t := " + titlesFor("alpha")
        + " return $n eq 2 and $t eq 'alpha|alpha'", "true");
  }

  @Test
  @DisplayName("a memoized answer still agrees with the document")
  void repeatedLookupAgreesWithTheDocument() throws IOException {
    // The second reading is checked against the document itself, so this fails both when the cache
    // returns the wrong list and when index and document have drifted apart for any other reason.
    test(STORE, CREATE_TITLE, HEAD + "let $n := " + countFor("alpha") + " let $second := " + titlesFor("alpha")
        + " return $n eq 2 and $second eq " + plainTitlesFor("alpha") + " and $second eq 'alpha|alpha'", "true");
  }

  @Test
  @DisplayName("two different values do not share a memoized answer")
  void distinctValuesDoNotShareAnAnswer() throws IOException {
    // The cache is indexed by a hash of the serialized key; a set collision that skipped the
    // per-entry key comparison would surface exactly here, as one value answering for another.
    test(STORE, CREATE_TITLE,
        HEAD + "return " + titlesFor("alpha") + " eq 'alpha|alpha' and " + titlesFor("zulu") + " eq 'zulu'", "true");
  }

  @Test
  @DisplayName("a value that is absent stays absent when asked twice")
  void absentValueIsStableAndEmpty() throws IOException {
    // Absent keys are memoized too, under a zero-length sentinel that has to decode back to "no
    // matches" rather than to an empty-but-present posting list.
    // The trailing present-value check is a positive control: an index that answered nothing for
    // everything would satisfy the two zero counts on its own.
    test(STORE, CREATE_TITLE, HEAD + "let $a := " + countFor("nosuchvalue") + " let $b := " + titlesFor("nosuchvalue")
        + " return $a eq 0 and $b eq '' and " + countFor("alpha") + " eq 2", "true");
  }

  @Test
  @DisplayName("an absent lookup does not poison a neighbouring present one")
  void absentLookupDoesNotDisturbAPresentOne() throws IOException {
    test(STORE, CREATE_TITLE, HEAD + "let $missing := fn:count(" + eq("alphaX") + ") return $missing eq 0 and "
        + titlesFor("alpha") + " eq 'alpha|alpha'", "true");
  }

  // ==================== the answer must follow the revision ====================

  @Test
  @DisplayName("a value inserted after the index was built is found, not answered from the old revision")
  void insertAfterIndexCreationIsVisible() throws IOException {
    // The memoization key carries the revision number. If it did not, the pre-insert answer for
    // 'alpha' would still be served after the commit that adds a third one — which is why the read
    // below the update is deliberately preceded by one above it.
    query(STORE);
    query(CREATE_TITLE);
    query(HEAD + "return fn:count(" + eq("alpha") + ")"); // warms revision N
    query("let $doc := jn:doc('json-path1','mydoc.jn') return insert json {\"title\":\"alpha\",\"n\":99} into $doc");
    test(HEAD + "return fn:count(" + eq("alpha") + ")", "3");
  }

  @Test
  @DisplayName("a value deleted after the index was built disappears from the index")
  void deleteAfterIndexCreationIsVisible() throws IOException {
    // Unlike its insert and replace siblings this one does NOT discriminate a stale answer, and the
    // reason is worth writing down: a posting list is node keys, and the scan materializes each one
    // against the current revision. A stale list names a node that the delete removed, materialization
    // drops it, and the count collapses to the correct value anyway — the staleness is masked no
    // matter how the assertion is phrased. Verified by mutation: with the revision dropped from the
    // memoization key, insertAfterIndexCreationIsVisible and replaceMovesTheValueBetweenKeys both
    // fail and this one still passes. It stays because "the index follows deletions end to end" is
    // worth pinning on its own; it just is not the revision-discrimination test.
    query(STORE);
    query(CREATE_TITLE);
    query(HEAD + "return fn:count(" + eq("alpha") + ")");
    query("delete json jn:doc('json-path1','mydoc.jn')[0]");
    test(HEAD + "return fn:count(" + eq("alpha") + ")", "1");
  }

  @Test
  @DisplayName("a replaced value moves from its old key to its new one across revisions")
  void replaceMovesTheValueBetweenKeys() throws IOException {
    // Both keys are read BEFORE the update, so both are memoized for the old revision; the
    // assertions afterwards are what a revision-blind cache would get wrong in both directions.
    query(STORE);
    query(CREATE_TITLE);
    query(HEAD + "return fn:count(" + eq("zulu") + ")");
    query(HEAD + "return fn:count(" + eq("alpha") + ")");
    query("replace json value of jn:doc('json-path1','mydoc.jn')[0].title with \"zulu\"");
    test(HEAD + "return fn:count(" + eq("zulu") + ") eq 2 and fn:count(" + eq("alpha") + ") eq 1", "true");
  }

  // ==================== key encoding through the query stack ====================

  @Test
  @DisplayName("a non-ASCII value round-trips through the index")
  void nonAsciiValueIsFound() throws IOException {
    // Takes the UTF-8 fallback rather than the ASCII fast path: every char above 0x7F must make the
    // serializer abandon the byte-per-char write, or the stored bytes and the probe bytes diverge.
    //
    // Pinned to the literal as well as to the document: a serializer that mangled the value would
    // make BOTH sides of a pure index-vs-document comparison empty, and the test would pass blind.
    test(STORE, CREATE_TITLE, HEAD + "return " + titlesFor("Ünïcödé") + " eq 'Ünïcödé' and " + titlesFor("Ünïcödé")
        + " eq " + plainTitlesFor("Ünïcödé"), "true");
  }

  @Test
  @DisplayName("a multi-byte CJK value round-trips through the index")
  void cjkValueIsFound() throws IOException {
    test(STORE, CREATE_TITLE,
        HEAD + "return " + titlesFor("日本語") + " eq '日本語' and " + titlesFor("日本語") + " eq " + plainTitlesFor("日本語"),
        "true");
  }

  @Test
  @DisplayName("a value containing a surrogate pair round-trips through the index")
  void surrogatePairValueIsFound() throws IOException {
    // An emoji is two chars and four UTF-8 bytes, so it also proves the truncation cap counts BYTES
    // consistently on both the stored and the probe side.
    test(STORE, CREATE_TITLE, HEAD + "return " + titlesFor("emoji 🚀 here") + " eq 'emoji 🚀 here' and "
        + titlesFor("emoji 🚀 here") + " eq " + plainTitlesFor("emoji 🚀 here"), "true");
  }

  @Test
  @DisplayName("values either side of the eight-byte comparison stride are distinguished")
  void valuesAcrossTheWordStrideAreDistinguished() throws IOException {
    // The in-leaf comparators consume keys eight bytes at a time with a byte tail. Values of 7, 8
    // and 9 bytes land before, on and after that boundary, which is where an off-by-one in the tail
    // shows up as one value matching another.
    test(STORE, CREATE_TITLE, HEAD + "return " + titlesFor("stride7") + " eq 'stride7' and " + titlesFor("stride08")
        + " eq 'stride08' and " + titlesFor("stride009") + " eq 'stride009'", "true");
  }

  @Test
  @DisplayName("a prefix of a stored value does not match that value")
  void aPrefixDoesNotMatchTheLongerValue() throws IOException {
    // 'stride' prefixes three stored values. Equality must not be satisfied by a prefix compare that
    // forgot its length tie-break. The second clause is the positive control that keeps the zero
    // honest.
    test(STORE, CREATE_TITLE, HEAD + "return " + countFor("stride") + " eq 0 and " + countFor("stride7") + " eq 1",
        "true");
  }

  // ==================== more than one index over the same resource ====================

  @Test
  @DisplayName("one value under two paths of a single index keeps two separate answers")
  void oneIndexTwoPathsAreNotConflated() throws IOException {
    // ONE index, one trie, one index number, and the value 'shared' reachable under two paths. The
    // only thing separating the two probes is the path class record in the first eight bytes of the
    // key, so a key that lost it would answer the second scan with the first's postings — 1 where 2
    // is right. Verified by mutation: dropping those bytes from the memoization key fails this test.
    //
    // This test is also what found the multi-path full-scan bug. Until CASIndex#openHOTIndexWithFilter
    // stopped gating on how many path classes the INDEX spans, a spanning index never reached the
    // point lookup at all, and this stayed green under every corruption of the memoization key
    // because it was being served by the full scan instead.
    test(MULTI_STORE, CREATE_SPANNING, MULTI_HEAD + "let $t := fn:count(" + eqOn("/[]/title", "shared")
        + ") let $a := fn:count(" + eqOn("/[]/alias", "shared") + ") return $t eq 1 and $a eq 2", "true");
  }

  @Test
  @DisplayName("two indexes over one resource each answer for their own path")
  void twoIndexesEachAnswerForTheirOwnPath() throws IOException {
    // Two single-path indexes, so both scans DO take the memoized point-lookup branch, and the same
    // value 'shared' resolves to a different count under each. Note this is the WEAK arrangement:
    // matched index/path pairs already differ in their key bytes, so it takes the loss of both the
    // index number and the path class record to fail this. The crossed probe above is what puts the
    // index number under load on its own.
    test(MULTI_STORE, CREATE_TITLE_AND_ALIAS, MULTI_HEAD + "let $t := fn:count(" + eqOn("/[]/title", "shared")
        + ") let $a := fn:count(" + eqOn("/[]/alias", "shared") + ") return $t eq 1 and $a eq 2", "true");
  }

  @Test
  @DisplayName("an index asked in another index's path class does not answer for it")
  void aCrossedProbeDoesNotBorrowTheOtherIndexsAnswer() throws IOException {
    // The one arrangement that isolates the index number. Both scans probe the SAME key bytes —
    // title's path class record, the type id, then 'shared' — because the second one deliberately
    // asks the alias index a title-shaped question. The alias index holds nothing under title's path
    // class, so the honest answers are 1 and 0; the key bytes cannot tell those two lookups apart,
    // and neither can the path class record inside them. Only the index number can.
    //
    // Order matters here in a way it does not elsewhere: the absent answer is memoized under a
    // sentinel, so running the crossed probe FIRST is what makes a shared key poison the real
    // lookup rather than merely duplicate it.
    test(MULTI_STORE, CREATE_TITLE_AND_ALIAS,
        MULTI_HEAD + "let $crossed := fn:count(" + eqCrossed("/[]/alias", "/[]/title", "shared")
            + ") let $honest := fn:count(" + eqOn("/[]/title", "shared") + ") return $crossed eq 0 and $honest eq 1",
        "true");
  }

  @Test
  @DisplayName("the same holds when the second index is read first")
  void twoIndexesEachAnswerForTheirOwnPathReversed() throws IOException {
    // The mirror image: whichever scan runs first is the one that populates the cache, so both
    // orders are pinned rather than just the one that happened to be written first.
    test(MULTI_STORE, CREATE_TITLE_AND_ALIAS, MULTI_HEAD + "let $a := fn:count(" + eqOn("/[]/alias", "shared")
        + ") let $t := fn:count(" + eqOn("/[]/title", "shared") + ") return $a eq 2 and $t eq 1", "true");
  }

  @Test
  @DisplayName("a name-index lookup is memoized without changing its answer")
  void nameIndexLookupIsStable() throws IOException {
    // The name index reaches the trie through the same reader base class, so it shares the
    // memoization path with the CAS index while serializing an entirely different kind of key.
    // Nothing else in this class exercises that reader.
    test(MULTI_STORE, CREATE_NAMES, MULTI_HEAD
        + "let $n := fn:count(jn:scan-name-index($doc,jn:find-name-index($doc,'title'),'title')) "
        + "let $m := fn:count(for $x in jn:scan-name-index($doc,jn:find-name-index($doc,'title'),'title') return $x) "
        + "return $n eq 3 and $m eq 3", "true");
  }

  @Test
  @DisplayName("an integer-typed CAS lookup is memoized without changing its answer")
  void integerCasLookupIsStable() throws IOException {
    // A numeric CAS index serializes its keys through a different path from the string one, and a
    // memoized answer has to survive that too. 20 occurs twice and 10 once, so a shared answer
    // between the two probes would show up as a wrong count rather than as no result at all.
    test(MULTI_STORE, CREATE_N,
        MULTI_HEAD + "let $idx := jn:find-cas-index($doc,'xs:integer','/[]/n') "
            + "let $a := fn:count(jn:scan-cas-index($doc,$idx,20,'==','/[]/n')) "
            + "let $b := fn:count(for $x in jn:scan-cas-index($doc,$idx,20,'==','/[]/n') return $x) "
            + "let $c := fn:count(jn:scan-cas-index($doc,$idx,10,'==','/[]/n')) "
            + "return $a eq 2 and $b eq 2 and $c eq 1",
        "true");
  }

  @Test
  @DisplayName("a repeated lookup of a non-ASCII value is also stable")
  void repeatedNonAsciiLookupIsStable() throws IOException {
    // The memoization key is the SERIALIZED key, so a value whose encoding takes the fallback is
    // also the one most likely to hash or compare differently between the probe and the stored copy.
    test(STORE, CREATE_TITLE,
        HEAD + "let $n := " + countFor("日本語") + " let $t := " + titlesFor("日本語") + " return $n eq 1 and $t eq '日本語'",
        "true");
  }
}
