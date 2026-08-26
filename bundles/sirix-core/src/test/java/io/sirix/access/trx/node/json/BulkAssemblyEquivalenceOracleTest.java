/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The differential oracle for the bulk page-assembly loader — built and inversion-proven BEFORE the
 * assembler exists, per the campaign's discipline: a comparator that has never failed proves
 * nothing.
 *
 * <p>
 * The oracle's statement: two loaders fed the same JSON must produce resources whose CANONICAL
 * DUMPS are string-identical. The dump walks every node in document order through the public read
 * API and prints the complete structural identity the cursor loader guarantees — node key, kind,
 * name, value, parent/left-sibling/right-sibling/first-child keys, child count, path node key —
 * plus the resource-level max node key. Anything the future assembler gets wrong in key minting,
 * pointer wiring, name/path resolution or value bytes must surface as a first differing line.
 *
 * <p>
 * Self-proofs in this class: (1) DETERMINISM — the cursor loader run twice over the same input
 * yields identical dumps, so the oracle's ground truth is stable; (2) SENSITIVITY — a one-value
 * change, a field-order swap, and a nesting change each flip the dump, so the comparator can
 * actually say "no" along the value, order and structure axes it claims to cover. The assembler arm
 * plugs in later as a second {@link Consumer} loading the same input.
 */
final class BulkAssemblyEquivalenceOracleTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final String FLAT_RECORDS =
      "[{\"id\":1,\"name\":\"alpha\",\"ok\":true},{\"id\":2,\"name\":\"beta\",\"ok\":false},"
          + "{\"id\":3,\"name\":\"gamma\",\"nested\":{\"x\":1.5,\"y\":null}}]";

  private static final String EDGE_SHAPES =
      "{\"empty\":{},\"emptyArr\":[],\"uni\":\"\\u00e4\\u00df\\u2713\",\"esc\":\"a\\\"b\\\\c\","
          + "\"nums\":[0,-1,9007199254740993,1.0e-3],\"deep\":[[[[{\"leaf\":\"v\"}]]]]}";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("ground truth: the cursor loader is deterministic — same input, twice, identical dumps")
  void cursorLoaderIsDeterministic() {
    final String first = dumpAfterLoad("det-a", cursorLoader(FLAT_RECORDS));
    final String second = dumpAfterLoad("det-b", cursorLoader(FLAT_RECORDS));
    assertEquals(first, second, "two cursor loads of the same input must be structurally identical");
    assertTrue(first.lines().count() > 10, "the dump must actually enumerate nodes, not be vacuously empty");
  }

  @Test
  @DisplayName("ground truth holds for edge shapes (empty containers, unicode, escapes, deep nesting)")
  void cursorLoaderIsDeterministicOnEdgeShapes() {
    final String first = dumpAfterLoad("edge-a", cursorLoader(EDGE_SHAPES));
    final String second = dumpAfterLoad("edge-b", cursorLoader(EDGE_SHAPES));
    assertEquals(first, second);
    // Non-vacuity witnesses: the decoded unicode VALUE and the escaped quote must have survived the
    // shred into the dump — a comparator over dumps that dropped values would pass equality checks
    // while checking nothing.
    assertTrue(first.contains("äß✓"), "edge dump must carry the decoded unicode value");
    assertTrue(first.contains("a\"b\\c"), "edge dump must carry the unescaped string value");
  }

  @Test
  @DisplayName("sensitivity: a single changed VALUE flips the dump")
  void oracleSeesValueDifference() {
    final String base = dumpAfterLoad("val-a", cursorLoader(FLAT_RECORDS));
    final String mutated = dumpAfterLoad("val-b", cursorLoader(FLAT_RECORDS.replace("\"beta\"", "\"BETA\"")));
    assertNotEquals(base, mutated, "a one-value change must flip the canonical dump");
  }

  @Test
  @DisplayName("sensitivity: swapped FIELD ORDER flips the dump")
  void oracleSeesOrderDifference() {
    final String base = dumpAfterLoad("ord-a", cursorLoader("[{\"a\":1,\"b\":2}]"));
    final String swapped = dumpAfterLoad("ord-b", cursorLoader("[{\"b\":2,\"a\":1}]"));
    assertNotEquals(base, swapped, "sibling order is part of the structural identity");
  }

  @Test
  @DisplayName("sensitivity: a NESTING change flips the dump even when the scalar content matches")
  void oracleSeesStructureDifference() {
    final String flat = dumpAfterLoad("str-a", cursorLoader("[{\"a\":1,\"b\":2}]"));
    final String nested = dumpAfterLoad("str-b", cursorLoader("[{\"a\":1,\"n\":{\"b\":2}}]"));
    assertNotEquals(flat, nested);
  }

  /** The cursor arm every comparison anchors on. */
  static Consumer<JsonNodeTrx> cursorLoader(final String json) {
    return wtx -> wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
  }

  /**
   * The bulk-assembly arm under test. The cursor arm parses with Gson while this arm uses the
   * BulkJsonScanner, so every differential fixture is ALSO a tokenizer-equivalence test — escapes,
   * unicode, surrogates and number forms must decode identically or the dumps diverge.
   */
  static Consumer<JsonNodeTrx> bulkLoader(final String json) {
    return wtx -> BulkJsonTreeAssembler.assemble(wtx, new StringReader(json));
  }

  @Test
  @DisplayName("SCANNER EDGES: number-form zoo decodes identically to Gson + JsonNumber")
  void scannerNumberZooMatchesCursor() {
    final String json = "[-0,0,1,-1,2147483648,-2147483649,9223372036854775807,-9223372036854775808,"
        + "9223372036854775808,1.5,-0.5,1e3,1E-3,1.25e2,3.0e0,1e400]";
    assertEquals(dumpAfterLoad("z-cur", cursorLoader(json)), dumpAfterLoad("z-blk", bulkLoader(json)));
  }

  @Test
  @DisplayName("SCANNER EDGES: lone surrogate escapes encode like String.getBytes(UTF_8)")
  void scannerLoneSurrogateMatchesCursor() {
    final String json = "[\"a\\uD800b\",\"c\\uDC00d\",\"pair:\\uD83D\\uDE00!\"]";
    assertEquals(dumpAfterLoad("s-cur", cursorLoader(json)), dumpAfterLoad("s-blk", bulkLoader(json)));
  }

  @Test
  @DisplayName("SCANNER EDGES: a 7-char buffer forces refills inside every token, dumps unchanged")
  void scannerTinyBufferMatchesDefault() {
    final String json = EDGE_SHAPES;
    final String viaDefault = dumpAfterLoad("t-def", bulkLoader(json));
    final String viaTiny = dumpAfterLoad("t-tiny",
        wtx -> BulkJsonTreeAssembler.assemble(wtx, new BulkJsonScanner(new StringReader(json), 7)));
    assertEquals(viaDefault, viaTiny);
  }

  @Test
  @DisplayName("DIFFERENTIAL: bulk assembly ≡ cursor loader on flat records")
  void bulkMatchesCursorOnFlatRecords() {
    assertEquals(dumpAfterLoad("d-cur-flat", cursorLoader(FLAT_RECORDS)),
        dumpAfterLoad("d-blk-flat", bulkLoader(FLAT_RECORDS)));
  }

  @Test
  @DisplayName("DIFFERENTIAL: bulk assembly ≡ cursor loader on edge shapes")
  void bulkMatchesCursorOnEdgeShapes() {
    assertEquals(dumpAfterLoad("d-cur-edge", cursorLoader(EDGE_SHAPES)),
        dumpAfterLoad("d-blk-edge", bulkLoader(EDGE_SHAPES)));
  }

  @Test
  @DisplayName("DIFFERENTIAL: bulk assembly ≡ cursor loader on a deeply nested document")
  void bulkMatchesCursorOnDeepNesting() {
    // 200 levels: beyond the assembler's initial 64-frame stack (exercises growth) while under
    // Gson's own 255-deep tokenizer limit, which binds BOTH arms equally.
    final StringBuilder deep = new StringBuilder(1 << 13);
    for (int i = 0; i < 200; i++) {
      deep.append("{\"level").append(i).append("\":");
    }
    deep.append("\"bottom\"");
    deep.append("}".repeat(200));
    final String json = deep.toString();
    assertEquals(dumpAfterLoad("d-cur-deep", cursorLoader(json)), dumpAfterLoad("d-blk-deep", bulkLoader(json)));
  }

  @Test
  @DisplayName("DIFFERENTIAL: bulk assembly ≡ cursor loader on a top-level array of mixed elements")
  void bulkMatchesCursorOnTopLevelArray() {
    final String json = "[1,\"two\",true,null,{\"a\":[]},[3,4],{},[[5]],\"tail\"]";
    assertEquals(dumpAfterLoad("d-cur-arr", cursorLoader(json)), dumpAfterLoad("d-blk-arr", bulkLoader(json)));
  }

  /**
   * The parallel-import arm: same input, chunked through the coordinator/worker pipeline with a
   * DELIBERATELY tiny chunk budget so even small fixtures exercise many chunks, page-0 stitches and
   * cross-chunk sibling boundaries.
   */
  static Consumer<JsonNodeTrx> parallelLoader(final String json, final int chunkCharBudget) {
    return wtx -> ParallelBulkJsonImporter.assemble(wtx, new StringReader(json), chunkCharBudget);
  }

  @Test
  @DisplayName("PARALLEL DIFFERENTIAL: chunked import ≡ sequential bulk on flat records")
  void parallelMatchesBulkOnFlatRecords() {
    assertEquals(dumpAfterLoad("p-seq-flat", bulkLoader(FLAT_RECORDS)),
        dumpAfterLoad("p-par-flat", parallelLoader(FLAT_RECORDS, 48)));
  }

  @Test
  @DisplayName("PARALLEL DIFFERENTIAL: chunked import ≡ sequential bulk on a mixed top-level array")
  void parallelMatchesBulkOnMixedTopLevelArray() {
    final String json = "[1,\"two\",true,null,{\"a\":[]},[3,4],{},[[5]],\"tail\"]";
    assertEquals(dumpAfterLoad("p-seq-mix", bulkLoader(json)), dumpAfterLoad("p-par-mix", parallelLoader(json, 8)));
  }

  @Test
  @DisplayName("PARALLEL DIFFERENTIAL: chunked import ≡ sequential bulk across MULTIPLE record pages")
  void parallelMatchesBulkAcrossPages() {
    // ~600 members x ~5 nodes each = ~3000 nodes = 3 record pages: exercises whole-page adoption,
    // the held-tail protocol at chunk boundaries mid-page, and the page-0 prologue stitch.
    final StringBuilder json = new StringBuilder(1 << 16).append('[');
    for (int i = 0; i < 600; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":")
          .append(i)
          .append(",\"name\":\"row")
          .append(i)
          .append("\",\"flag\":")
          .append((i & 1) == 0)
          .append(",\"note\":null}");
    }
    final String corpus = json.append(']').toString();
    assertEquals(dumpAfterLoad("p-seq-pages", bulkLoader(corpus)),
        dumpAfterLoad("p-par-pages", parallelLoader(corpus, 1024)));
  }

  @Test
  @DisplayName("PARALLEL DIFFERENTIAL: single-chunk import (budget larger than input) still matches")
  void parallelMatchesBulkSingleChunk() {
    assertEquals(dumpAfterLoad("p-seq-one", bulkLoader(FLAT_RECORDS)),
        dumpAfterLoad("p-par-one", parallelLoader(FLAT_RECORDS, 1 << 20)));
  }

  @Test
  @DisplayName("PARALLEL DIFFERENTIAL: non-array top level falls back to the sequential assembler")
  void parallelFallsBackOnObjectTopLevel() {
    final String json = "{\"a\":1,\"b\":[true,false]}";
    assertEquals(dumpAfterLoad("p-seq-obj", bulkLoader(json)), dumpAfterLoad("p-par-obj", parallelLoader(json, 16)));
  }

  @Test
  @DisplayName("UNIFIED ARRAY PATH: sibling arrays share the first-child array's __array__ path class")
  void siblingArraysShareOnePathClass() {
    // [[1],[2]] — the second inner array is a SIBLING insert. Before the unification it resolved a
    // separate "array" path step; now both inner arrays carry the SAME path node key, and both
    // loaders agree on it.
    final String json = "[[1],[2],[3]]";
    final String cursor = dumpAfterLoad("u-cur", cursorLoader(json));
    final String bulk = dumpAfterLoad("u-blk", bulkLoader(json));
    assertEquals(cursor, bulk);
    // Witness the merge itself: the three inner ARRAY lines must carry ONE identical pathNodeKey.
    final long distinctInnerArrayPcrs = cursor.lines()
                                              .filter(line -> line.contains("|ARRAY|"))
                                              .map(line -> line.substring(line.lastIndexOf('|') + 1))
                                              .skip(1) // the outer array has its own class
                                              .distinct()
                                              .count();
    assertEquals(1, distinctInnerArrayPcrs, "all sibling inner arrays must share one path class");
  }

  @Test
  @DisplayName("DIFFERENTIAL: bulk ≡ cursor with intermediate async-flush EPOCHS firing mid-load")
  void bulkMatchesCursorAcrossEpochs() {
    // 400 small records under a 64-node auto-commit threshold: both arms rotate intermediate
    // async-flush epochs many times mid-load, so this pins the assembler's record-boundary epoch
    // accounting (bulkAccountMutations) against the cursor path's per-insert accounting.
    final StringBuilder many = new StringBuilder(1 << 14);
    many.append('[');
    for (int i = 0; i < 400; i++) {
      if (i > 0) {
        many.append(',');
      }
      many.append("{\"k").append(i % 7).append("\":").append(i).append(",\"s\":\"v").append(i).append("\"}");
    }
    many.append(']');
    final String json = many.toString();
    assertEquals(dumpAfterEpochLoad("e-cur", cursorLoader(json)), dumpAfterEpochLoad("e-blk", bulkLoader(json)));
  }

  /** As {@link #dumpAfterLoad} but with a 64-node KEEP_OPEN_ASYNC_FLUSH auto-commit transaction. */
  private static String dumpAfterEpochLoad(final String resourceName, final Consumer<JsonNodeTrx> loader) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useDeweyIDs(false)
                                                   .hashKind(HashType.NONE)
                                                   .storeNodeHistory(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(resourceName)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(64, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          loader.accept(wtx);
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          return canonicalDump(rtx) + summaryDump(session);
        }
      }
    }
  }

  /** Load {@code json} into a fresh resource via {@code loader}, then produce the canonical dump. */
  private static String dumpAfterLoad(final String resourceName, final Consumer<JsonNodeTrx> loader) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      // NONE hashes + no DeweyIDs: the bulk-import configuration both loaders are compared under
      // (and the shape the assembler's up-front refusal admits).
      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useDeweyIDs(false)
                                                   .hashKind(HashType.NONE)
                                                   .storeNodeHistory(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(resourceName)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          loader.accept(wtx);
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          return canonicalDump(rtx) + summaryDump(session);
        }
      }
    }
  }

  /**
   * The path summary's OWN structural identity, REFERENCE COUNTS included. The per-node dump below
   * cannot see reference counting — a loader that resolved path classes correctly but deferred or
   * dropped the per-occurrence reference increments would pass it silently. This section makes the
   * memoized/delta-counted bulk path falsifiable.
   */
  private static String summaryDump(final JsonResourceSession session) {
    final StringBuilder dump = new StringBuilder(1 << 10);
    dump.append("--- path summary ---\n");
    try (PathSummaryReader summary = session.openPathSummary()) {
      summary.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(summary, IncludeSelf.YES);
      while (axis.hasNext()) {
        axis.nextLong();
        if (summary.getNodeKey() == 0) {
          continue; // the summary root carries no path step
        }
        dump.append(summary.getNodeKey())
            .append('|')
            .append(summary.getName() == null
                ? "-"
                : summary.getName().getLocalName())
            .append('|')
            .append(summary.getReferences())
            .append('\n');
      }
    }
    return dump.toString();
  }

  /**
   * One line per node in document order, covering the full structural identity the assembler must
   * reproduce. Kept deliberately explicit — every field here is an invariant, and a field removed
   * from this dump is an invariant the oracle silently stops checking.
   */
  static String canonicalDump(final JsonNodeReadOnlyTrx rtx) {
    final StringBuilder dump = new StringBuilder(1 << 12);
    dump.append("maxNodeKey=").append(rtx.getMaxNodeKey()).append('\n');
    rtx.moveToDocumentRoot();
    final DescendantAxis axis = new DescendantAxis(rtx, IncludeSelf.YES);
    while (axis.hasNext()) {
      axis.nextLong();
      final NodeKind kind = rtx.getKind();
      dump.append(rtx.getNodeKey())
          .append('|')
          .append(kind)
          .append('|')
          .append(rtx.getName() == null
              ? "-"
              : rtx.getName().getLocalName())
          .append('|')
          .append(valueOf(rtx, kind))
          .append('|')
          .append(rtx.getParentKey())
          .append('|')
          .append(rtx.getLeftSiblingKey())
          .append('|')
          .append(rtx.getRightSiblingKey())
          .append('|')
          .append(rtx.getFirstChildKey())
          .append('|')
          .append(rtx.getChildCount())
          .append('|')
          .append(rtx.getDescendantCount())
          .append('|')
          .append(pathNodeKeyOf(rtx))
          .append('\n');
    }
    return dump.toString();
  }

  private static String valueOf(final JsonNodeReadOnlyTrx rtx, final NodeKind kind) {
    return switch (kind) {
      case STRING_VALUE, NUMBER_VALUE, BOOLEAN_VALUE, OBJECT_NAMED_STRING, OBJECT_NAMED_NUMBER, OBJECT_NAMED_BOOLEAN ->
        String.valueOf(rtx.getValue());
      case NULL_VALUE, OBJECT_NAMED_NULL -> "null";
      default -> "-";
    };
  }

  private static String pathNodeKeyOf(final JsonNodeReadOnlyTrx rtx) {
    try {
      return String.valueOf(rtx.getPathNodeKey());
    } catch (final RuntimeException unsupportedForThisKind) {
      return "-";
    }
  }
}
