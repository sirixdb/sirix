/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two string-region DICTIONARY-WALKING fast paths against a page written with the TEMPORAL LANE
 * armed.
 *
 * <p>
 * The lane stores a timestamp tag's dictionary as packed numbers rather than bytes, so
 * {@code StringRegion.decodeStringOffset} has no offset to hand back and refuses the tag. The
 * count-distinct kernel and the group-by-count kernel each walk a tag's dictionary reading exactly
 * that, so both must ask whether the tag stores inline bytes and decline to their slot walk when it
 * does not. Before they asked, arming the lane's own kill switch and querying a timestamp field
 * failed the query outright with an {@code IllegalStateException} raised inside a scan worker —
 * which is why the two kernels are driven DIRECTLY here rather than through query text that a
 * planner is free to route elsewhere.
 * </p>
 *
 * <p>
 * The answers are the assertion, not the route. Declining has to produce the same numbers the byte
 * path would have, so every kernel answer is checked against the INTERPRETER's over the same
 * document — that is the whole reason declining is the right escape rather than a narrowing.
 * </p>
 *
 * <h2>Why every test here first proves the lane was TAKEN</h2>
 *
 * <p>
 * {@code StringRegion.resolveTemporal} is ALL-OR-NOTHING: one dictionary entry it cannot encode — an
 * FSST-compressed one, a value off the canonical shape — and the whole tag keeps its bytes. A corpus
 * that quietly fails to convert leaves a tag that DOES store inline bytes, the guard is never reached,
 * and every answer assertion below passes with the guard deleted. So each test establishes
 * {@link #pagesWithATemporalTag()} first: a witness that is zero exactly when this class would
 * otherwise be proving nothing.
 * </p>
 */
final class TemporalLaneStringFastPathTest {

  private static final String DB = "temporal-lane-fast-path-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String[] SOURCE_PATH = {"[]"};

  /** Several pages of 1,024 slots, so a whole-page fast path has pages to be complete on. */
  private static final int N = 20_000;

  /** Few enough distinct values to sit inside the group-by kernel's 256-entry count scratch. */
  private static final int DISTINCT = 8;

  private Path dbDir;

  private static String timestamp(final int i) {
    return "2013-07-15 12:00:0" + (i % DISTINCT);
  }

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-temporal-lane-fast-path-");
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"ts\":\"").append(timestamp(i)).append("\"}");
    }
    sb.append(']');
    // Armed for the WRITE, which is the only side the lane gates: the tag is converted while the
    // region is encoded, and every read afterwards meets a page whose timestamps are numbers.
    StringRegion.setTemporalLaneEnabled(true);
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    } finally {
      StringRegion.clearTemporalLaneOverride();
    }
  }

  @AfterEach
  void tearDown() {
    StringRegion.clearTemporalLaneOverride();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  private interface ExecutorTask {
    void run(SirixVectorizedExecutor executor) throws Exception;
  }

  private void onExecutor(final ExecutorTask task) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES)) {
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
      try {
        task.run(executor);
      } finally {
        executor.close();
      }
    }
  }

  /**
   * Leaf pages whose PERSISTED string region carries a tag that took the temporal lane.
   *
   * <p>
   * Deliberately {@link KeyValueLeafPage#getStringRegionPayload()} and not
   * {@code getStringRegionHeader()}: the latter falls back to re-deriving the region from the
   * slotted page when none was persisted, and a derive is an ENCODE, so it consults
   * {@code temporalLaneEnabled()} at read time. Asking for the payload does not trigger that
   * fallback.
   * </p>
   *
   * <p>
   * The payload accessor alone is not a guarantee of provenance — a derive installs its result into
   * the same region table, so a page some earlier call already derived would answer from that. What
   * closes it here is that {@code setUp} disarms the switch before any witness runs: a derive under
   * a disarmed switch cannot produce a temporal tag, so this count can only be raised by tags that
   * were genuinely written.
   * </p>
   *
   * @return the number of leaf pages with at least one persisted temporal tag
   */
  private int pagesWithATemporalTag() throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES);
        var rtx = resourceSession.beginNodeReadOnlyTrx()) {
      final var reader = rtx.getStorageEngineReader();
      final long pages = (rtx.getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
      final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, rtx.getRevisionNumber());
      int converted = 0;
      for (long pk = 0; pk < pages; pk++) {
        final var res = reader.getRecordPage(key.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) {
          continue;
        }
        final MemorySegment payload = kv.getStringRegionPayload();
        if (payload == null || payload.byteSize() == 0) {
          continue;
        }
        final StringRegion.Header header = new StringRegion.Header().parseInto(payload);
        for (int tag = 0; tag < header.parentDictSize; tag++) {
          if (header.tagTemporal[tag]) {
            converted++;
            break;
          }
        }
      }
      return converted;
    }
  }

  /** Fails the test outright when nothing converted, so an answer assertion cannot stand in for it. */
  private void requireTheLaneWasTaken() throws Exception {
    assertTrue(pagesWithATemporalTag() > 0,
        "no page took the temporal lane, so the kernels never meet a tag without inline bytes and "
            + "this test would pass with the guard deleted");
  }

  /** The same question put to the interpreter, which reads the document's own bytes. */
  private String interpreted(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      return serialize(new Query(chain, query).evaluate(ctx));
    }
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new StringSerializer(writer).serialize(sequence);
    }
    return out.toString();
  }

  /** Group objects are order-independent; sort them so two agreeing answers compare equal. */
  private static List<String> normalizeGroups(final String serialized) {
    final List<String> groups = new ArrayList<>();
    for (final String part : serialized.split("(?<=\\})\\s*(?=\\{)")) {
      final String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        groups.add(trimmed);
      }
    }
    Collections.sort(groups);
    return groups;
  }

  @Test
  @DisplayName("WITNESS: the timestamp tag really took the temporal lane, so the rest of this class bites")
  void theTimestampTagActuallyTookTheTemporalLane() throws Exception {
    final int converted = pagesWithATemporalTag();
    assertTrue(converted > 0, "arming the lane converted no tag at all — the corpus does not exercise it");
    onExecutor(executor -> assertEquals(String.valueOf(DISTINCT),
        serialize(executor.executeCountDistinct(null, SOURCE_PATH, "ts")).trim(),
        "a converted tag must still answer, or the lane is not merely unexercised but broken"));
  }

  @Test
  @DisplayName("count-distinct over a temporal-lane tag answers, and agrees with the interpreter")
  void countDistinctOverATemporalTagAnswers() throws Exception {
    requireTheLaneWasTaken();
    final String expected = interpreted("count(distinct-values(for $r in " + DOC + " return $r.ts))");
    assertEquals(String.valueOf(DISTINCT), expected.trim(), "the interpreter must see every distinct timestamp");
    onExecutor(executor -> assertEquals(expected, serialize(executor.executeCountDistinct(null, SOURCE_PATH, "ts")),
        "the count-distinct kernel must answer a temporal tag, not fail on it"));
  }

  @Test
  @DisplayName("group-by-count over a temporal-lane tag answers, and agrees with the interpreter")
  void groupByCountOverATemporalTagAnswers() throws Exception {
    requireTheLaneWasTaken();
    // The kernel names the key after the group FIELD, so the interpreter's must be named the same
    // or the two agree on every number and compare unequal.
    final List<String> expected = normalizeGroups(interpreted("for $r in " + DOC
        + " let $ts := $r.ts group by $ts return {\"ts\": $ts, \"count\": count($r)}"));
    assertEquals(DISTINCT, expected.size(), "the interpreter must see every group");
    onExecutor(executor -> assertEquals(expected,
        normalizeGroups(serialize(executor.executeGroupByCount(null, SOURCE_PATH, "ts"))),
        "the group-by-count kernel must answer a temporal tag, not fail on it"));
  }

  @Test
  @DisplayName("the timestamps themselves survive the lane, so the counts count the right strings")
  void theTimestampsThemselvesReadBack() throws Exception {
    requireTheLaneWasTaken();
    final String answer = interpreted("for $r in " + DOC + " where $r.id lt 3 return $r.ts");
    for (int i = 0; i < 3; i++) {
      assertTrue(answer.contains(timestamp(i)), "timestamp " + timestamp(i) + " did not read back, got " + answer);
    }
  }
}
