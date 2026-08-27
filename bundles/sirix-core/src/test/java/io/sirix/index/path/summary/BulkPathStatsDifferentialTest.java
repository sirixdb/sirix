/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.BulkJsonTreeAssembler;
import io.sirix.access.trx.node.json.ChunkPathStatsPerturbation;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * DIFFERENTIAL ORACLE for bulk-built path statistics (the user-ordered feature): a resource loaded
 * by either bulk loader with {@code buildPathStatistics} enabled must carry per-path statistics
 * IDENTICAL to cursor insertion of the same data — field by field, after commit, close and reopen.
 *
 * <p>
 * Arms: cursor shredding (the only pre-existing truth), the sequential bulk assembler, the parallel
 * importer under its default chunking, and the parallel importer under an adversarial tiny chunking
 * (one-or-two-record chunks; the {@code late} field first occurs in the final chunks). Excluded
 * from exact comparison, per the documented merge contract
 * ({@link PathStatsAccumulator#mergeFrom}): {@code sumFraction}, the ONE order-dependent lane
 * (double accumulator, tolerance compare — never served, see {@code PathStats} and the executor's
 * doubleTyped decline) and the page-witness bitmap (leaf-page assignment is arm-specific by
 * construction; presence is compared).
 *
 * <p>
 * The perturbation test proves the oracle can FAIL: corrupting one chunk partial through the
 * production-code test seam must produce a detected divergence.
 */
final class BulkPathStatsDifferentialTest {

  private static final int RECORDS = 240;
  private static final long BIG = Long.MAX_VALUE / 3;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    ChunkPathStatsPerturbation.clear();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void bulkArmsMatchCursorFieldByField() {
    final String json = corpus();
    final Map<String, Snapshot> cursor = buildArmAndSnapshot(json, BulkPathStatsDifferentialTest::loadWithCursor);
    assertFalse(cursor.isEmpty(), "cursor arm produced no stats — vacuous oracle");
    assertTrue(cursor.keySet().stream().anyMatch(p -> p.contains("late")), "late path missing from cursor arm");

    final Map<String, Snapshot> sequential =
        buildArmAndSnapshot(json, (session, doc) -> loadWithSequentialBulk(session, doc));
    compareArms("sequential", cursor, sequential);

    final Map<String, Snapshot> parallelDefault =
        buildArmAndSnapshot(json, (session, doc) -> loadWithParallelBulk(session, doc, 1 << 20, 3));
    compareArms("parallel-default", cursor, parallelDefault);

    final Map<String, Snapshot> parallelTiny =
        buildArmAndSnapshot(json, (session, doc) -> loadWithParallelBulk(session, doc, 192, 3));
    compareArms("parallel-tiny-chunks", cursor, parallelTiny);

    final Map<String, Snapshot> cursorThreeCommits =
        buildArmAndSnapshot(json, BulkPathStatsDifferentialTest::loadWithCursorInThreeCommits);
    compareArms("cursor-three-commits", cursor, cursorThreeCommits);
  }

  @Test
  void perturbedChunkPartialIsCaughtByTheDifferential() {
    final String json = corpus();
    final Map<String, Snapshot> cursor = buildArmAndSnapshot(json, BulkPathStatsDifferentialTest::loadWithCursor);

    ChunkPathStatsPerturbation.addLongToEveryPartial(999_999L);
    try {
      final Map<String, Snapshot> perturbed =
          buildArmAndSnapshot(json, (session, doc) -> loadWithParallelBulk(session, doc, 192, 3));
      try {
        compareArms("perturbed", cursor, perturbed);
      } catch (final AssertionError expected) {
        return; // the oracle CAUGHT the corruption — the gate is non-vacuous
      }
      fail("a corrupted chunk partial passed the differential — the oracle is vacuous");
    } finally {
      ChunkPathStatsPerturbation.clear();
    }
  }

  // ==================== corpus ====================

  /**
   * Adversarial single-kind-per-path corpus: integral ids, an always-overflowing 64-bit id column
   * (same-sign — every arm must untrust its sum), a mixed-sign SWING column whose true total fits a
   * long although a prefix of it does not, non-integral doubles (fraction + doubleTyped), strings,
   * booleans, a null-only field on every third record, a numeric array (plain values on the enclosing
   * {@code __array__} class), and a LATE-APPEARING field on the last three records only (first
   * occurrence in the final chunks under tiny chunking).
   */
  private static String corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 160);
    json.append('[');
    final List<String> members = corpusMembers();
    for (int i = 0; i < members.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(members.get(i));
    }
    return json.append(']').toString();
  }

  /**
   * The corpus one member at a time, so the multi-commit arm cannot drift from the single-shot one.
   */
  private static List<String> corpusMembers() {
    final List<String> members = new ArrayList<>(RECORDS);
    for (int i = 0; i < RECORDS; i++) {
      final StringBuilder json = new StringBuilder(160);
      json.append("{\"id\":")
          .append(i)
          .append(",\"big\":")
          .append(BIG)
          .append(",\"score\":")
          .append(i)
          .append(".25")
          .append(",\"name\":\"n")
          .append(String.format("%03d", i % 37))
          .append('"')
          .append(",\"flag\":")
          .append((i & 1) == 0);
      if (i % 3 == 0) {
        json.append(",\"note\":null");
      }
      // A column whose TRUE total fits a long while a prefix of it does not: M, M, ... , -M.
      // Under three-commit ingestion the first batch alone is unrepresentable, so a per-flush
      // 64-bit fold persists a different sum and a different verdict here than a single-shot load.
      json.append(",\"swing\":")
          .append(i == 0 || i == 1
              ? Long.MAX_VALUE
              : i == RECORDS - 1
                  ? -Long.MAX_VALUE
                  : 0L);
      json.append(",\"nums\":[").append(i).append(',').append(i + 1).append(']');
      if (i >= RECORDS - 3) {
        json.append(",\"late\":\"L").append(i).append('"');
      }
      members.add(json.append('}').toString());
    }
    return members;
  }

  // ==================== arms ====================

  private interface ArmLoader {
    void load(JsonResourceSession session, String json);
  }

  private static void loadWithCursor(final JsonResourceSession session, final String json) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
      wtx.commit();
    }
  }

  /**
   * Same corpus, same order, but spread over THREE commits — so the deferred statistics flush three
   * times and the accumulator crosses two serialize/deserialize boundaries instead of none.
   *
   * <p>
   * This is the arm that pins order-independence END TO END rather than within one batch: the
   * {@code big} column's true total leaves {@code long} range, so a per-flush 64-bit fold would
   * persist a different sum and a different trust verdict here than in the single-commit arms, for
   * the very same values.
   */
  private static void loadWithCursorInThreeCommits(final JsonResourceSession session, final String ignoredJson) {
    final List<String> members = corpusMembers();
    final int firstCut = members.size() / 3;
    final int secondCut = 2 * members.size() / 3;
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(join(members.subList(0, firstCut))));
      wtx.commit();
    }
    appendMembers(session, members.subList(firstCut, secondCut));
    appendMembers(session, members.subList(secondCut, members.size()));
  }

  private static void appendMembers(final JsonResourceSession session, final List<String> members) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      final long arrayKey = wtx.getNodeKey();
      for (final String member : members) {
        wtx.moveTo(arrayKey);
        wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(member), JsonNodeTrx.Commit.NO);
      }
      wtx.commit();
    }
  }

  private static String join(final List<String> members) {
    final StringBuilder json = new StringBuilder(members.size() * 160);
    json.append('[');
    for (int i = 0; i < members.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(members.get(i));
    }
    return json.append(']').toString();
  }

  private static void loadWithSequentialBulk(final JsonResourceSession session, final String json) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      BulkJsonTreeAssembler.assemble(wtx, new StringReader(json));
      wtx.commit();
    }
  }

  private static void loadWithParallelBulk(final JsonResourceSession session, final String json,
      final int chunkCharBudget, final int parallelism) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      ParallelBulkJsonImporter.assemble(wtx, new StringReader(json), chunkCharBudget, parallelism);
      wtx.commit();
    }
  }

  /** Builds one arm in a fresh database, snapshots its stats AFTER close+reopen, then wipes. */
  private static Map<String, Snapshot> buildArmAndSnapshot(final String json, final ArmLoader loader) {
    JsonTestHelper.deleteEverything();
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                              .hashKind(HashType.NONE)
                                                              .storeNodeHistory(false)
                                                              .buildPathSummary(true)
                                                              .buildPathStatistics(true)
                                                              .build();
    try (Database<JsonResourceSession> database =
        JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config)) {
      try (JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        loader.load(session, json);
      }
    }
    // Reopen cold: the comparison must see what a fresh reader deserializes, not writer state.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        PathSummaryReader summary = session.openPathSummary()) {
      return snapshotAll(summary);
    }
  }

  // ==================== snapshot + comparison ====================

  private record Snapshot(long count, long nullCount, long sum, double sumFraction, long min, long max, byte[] minBytes,
      byte[] maxBytes, byte[] hllBytes, boolean minDirty, boolean maxDirty, boolean sumTrustworthy, boolean sumIntegral,
      boolean countDirty, boolean hasPageWitnesses) {
  }

  private static Map<String, Snapshot> snapshotAll(final PathSummaryReader summary) {
    final Map<String, Snapshot> byPath = new LinkedHashMap<>();
    // Deterministic full walk: collect every path node key first, then snapshot each.
    final List<Long> nodeKeys = new ArrayList<>();
    collectPathNodeKeys(summary, nodeKeys);
    for (final long nodeKey : nodeKeys) {
      assertTrue(summary.moveTo(nodeKey), "path node " + nodeKey + " must resolve");
      final PathNode node = summary.getPathNode();
      final String path = summary.getPath().toString();
      final PathStats stats = node.getStats();
      final Snapshot snapshot = new Snapshot(node.getStatsValueCount(), node.getStatsNullCount(), node.getStatsSum(),
          stats == null
              ? 0.0d
              : stats.sumFraction,
          node.getStatsMin(), node.getStatsMax(), node.getStatsMinBytes(), node.getStatsMaxBytes(),
          node.getHllSketch() == null
              ? null
              : node.getHllSketch().serialize(),
          node.isStatsMinDirty(), node.isStatsMaxDirty(), node.isStatsSumTrustworthy(), node.isStatsSumIntegral(),
          node.isStatsCountDirty(), stats != null && stats.pageKeysToArray() != null);
      final Snapshot previous = byPath.put(path, snapshot);
      assertTrue(previous == null, "duplicate path identity " + path + " — snapshot keying is broken");
    }
    return byPath;
  }

  private static void collectPathNodeKeys(final PathSummaryReader summary, final List<Long> out) {
    // The summary is small; a name-driven sweep over every known local name is the simplest
    // complete enumeration exposed by the reader API.
    for (final String name : new String[] {"id", "big", "swing", "score", "name", "flag", "note", "nums", "late",
        "__array__"}) {
      for (final PathNode node : summary.findPathsByLocalName(name)) {
        out.add(node.getNodeKey());
      }
    }
  }

  private static void compareArms(final String arm, final Map<String, Snapshot> cursor,
      final Map<String, Snapshot> bulk) {
    assertEquals(cursor.keySet(), bulk.keySet(), arm + ": path-class sets differ");
    for (final Map.Entry<String, Snapshot> entry : cursor.entrySet()) {
      final String path = entry.getKey();
      final Snapshot c = entry.getValue();
      final Snapshot b = Objects.requireNonNull(bulk.get(path));
      final String at = arm + " @ " + path;
      assertEquals(c.count, b.count, at + ": count");
      assertEquals(c.nullCount, b.nullCount, at + ": nullCount");
      assertEquals(c.sum, b.sum, at + ": sum");
      assertEquals(c.min, b.min, at + ": min");
      assertEquals(c.max, b.max, at + ": max");
      assertArrayEquals(c.minBytes, b.minBytes, at + ": minBytes");
      assertArrayEquals(c.maxBytes, b.maxBytes, at + ": maxBytes");
      assertArrayEquals(c.hllBytes, b.hllBytes, at + ": HLL sketch bytes");
      assertEquals(c.minDirty, b.minDirty, at + ": minDirty");
      assertEquals(c.maxDirty, b.maxDirty, at + ": maxDirty");
      // The trust verdict is a function of the observation multiset (128-bit integral accumulator),
      // so chunking may not move it in EITHER direction.
      assertEquals(c.sumTrustworthy, b.sumTrustworthy, at + ": sumTrustworthy");
      assertEquals(c.sumIntegral, b.sumIntegral, at + ": sumIntegral");
      assertEquals(c.countDirty, b.countDirty, at + ": countDirty");
      // sumFraction: double accumulator; never served (PathStats.sumFraction, executor's
      // doubleTyped decline) — tolerance compare per the documented merge contract.
      assertEquals(c.sumFraction, b.sumFraction, 1e-6, at + ": sumFraction beyond tolerance");
      // Page witnesses are arm-specific leaf-page assignments; presence must agree.
      assertEquals(c.hasPageWitnesses, b.hasPageWitnesses, at + ": page-witness presence");
    }
  }

  /**
   * Guard: the swing column really is the discriminating shape — its true total FITS (so it must stay
   * servable) while the prefix a three-commit load flushes first does not.
   */
  @Test
  void corpusSwingColumnTotalFitsButItsPrefixDoesNot() {
    final Map<String, Snapshot> cursor = buildArmAndSnapshot(corpus(), BulkPathStatsDifferentialTest::loadWithCursor);
    final Snapshot swing = cursor.entrySet()
                                 .stream()
                                 .filter(e -> e.getKey().contains("swing"))
                                 .map(Map.Entry::getValue)
                                 .findFirst()
                                 .orElse(null);
    assertNotNull(swing, "swing column path missing");
    assertTrue(swing.sumTrustworthy, "the swing column's true total fits a long and must stay servable");
    assertEquals(Long.MAX_VALUE, swing.sum, "M + M - M is exactly Long.MAX_VALUE");
    assertEquals(RECORDS, swing.count);
  }

  /** Guard: the corpus really exercises the untrusted-sum lane (non-vacuity of that assert). */
  @Test
  void corpusOverflowsTheBigColumn() {
    final Map<String, Snapshot> cursor = buildArmAndSnapshot(corpus(), BulkPathStatsDifferentialTest::loadWithCursor);
    final Snapshot big = cursor.entrySet()
                               .stream()
                               .filter(e -> e.getKey().contains("big"))
                               .map(Map.Entry::getValue)
                               .findFirst()
                               .orElse(null);
    assertNotNull(big, "big column path missing");
    assertFalse(big.sumTrustworthy, "the big column must overflow and untrust its sum");
    assertEquals(RECORDS, big.count, "overflow must not disturb the count");
  }
}
