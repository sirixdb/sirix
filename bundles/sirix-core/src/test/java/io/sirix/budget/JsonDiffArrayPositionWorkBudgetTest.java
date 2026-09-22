/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.IngestArrayPositionProbe;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.DiffFactory;
import io.sirix.diff.DiffTuple;
import io.sirix.diff.ArrayPositionCacheProbe;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Work budget for the update-diff sidecar's array positions: writing the sidecar costs one walk
 * over the array prefix a diff actually names, not one per element and not one over the whole
 * array.
 *
 * <p>
 * Diff storage is on by default, so this runs inside every ordinary commit. Resolving an element's
 * index by stepping left sibling by sibling, once per diff tuple, is quadratic in the array's
 * length: a bitemporal load spent forty minutes of full processor use inside {@code commitInternal}
 * producing about 24 KB of writes. Every way of getting this wrong writes the same sidecar bytes,
 * so only the sibling-move count tells the routes apart.
 *
 * <p>
 * There are two of them, and they fail different tests here. The per-tuple walk is quadratic and
 * blows the ceiling in {@link #everyPositionOfOneArrayCostsOneWalkOverIt(TupleOrder)}.
 * Pre-resolving the whole child list on the first lookup is linear in the <em>array</em> instead of
 * in the touched prefix, which passes that test and fails
 * {@link #oneElementCostsOnlyTheWalkItsOwnIndexNeeds()}: it makes the cheapest possible commit - a
 * single insert at the head of a large array - pay for the entire array in traversal and in cache
 * entries.
 *
 * <p>
 * Measured on the 10,000-element fixture: the memoized left walk does 9,999 sibling moves for all
 * 10,000 positions, 0 for the head element alone and 9,999 for the tail element alone. The
 * per-tuple walk does 49,995,000 / 0 / 9,999. The eager whole-array scan does 9,999 / 9,999 /
 * 9,999. The 100,000-element head-insert case retains 792 backing-array payload bytes across the
 * real commit and its counted serialization, a figure the array's length does not enter; adding
 * eager preallocation without changing the left walk leaves sibling moves at zero but sizes the
 * cache from the array's length instead, overshooting the independent 1,024-byte bound by three
 * orders of magnitude.
 *
 * <p>
 * That same case loads its array into the resource's bootstrap revision, which has no predecessor
 * to diff against and therefore writes no sidecar. Learning ingest ordinals there would build one
 * map entry per loaded element for a reader that never runs, so the load's own hint allocation is
 * budgeted at zero payload bytes, read while the transaction still holds the map. Capturing
 * unconditionally reports 3,145,740 bytes and 100,000 entries against that zero.
 *
 * <p>
 * The counter is a decorator over {@link JsonResourceSession}, the seam {@link JsonDiffSerializer}
 * already takes as a constructor argument, so it needs no engine counter and nothing global to
 * restore - hence a plain {@link WorkCounter} rather than a {@link WorkProbe}. The separate
 * {@link ArrayPositionCacheProbe} measures backing-array payload at the serialization boundary.
 * {@code hasLeftSibling()} does not move the cursor, so every counted move is a real one.
 */
@Isolated
final class JsonDiffArrayPositionWorkBudgetTest {

  private static final int LARGE_ARRAY_LENGTH = 10_000;

  private static final int HUGE_ARRAY_LENGTH = 100_000;

  /**
   * Amortized ceiling: a walk either stops on an already-resolved sibling, at most once per lookup,
   * or resolves the sibling it stepped onto, at most once per element. The quadratic route needs
   * 49,995,000.
   */
  private static final long AMORTIZED_MOVE_CEILING = 2L * LARGE_ARRAY_LENGTH;

  /** Every element but the first has to be stepped onto once, whatever the route. */
  private static final long MOVE_FLOOR = LARGE_ARRAY_LENGTH - 1L;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  enum TupleOrder {
    FORWARD, REVERSE, SHUFFLED
  }

  /**
   * On the unmodified baseline, 10k/20k elements in 1k batches required 54,990/209,980 sibling moves
   * versus 9,999/19,999 for one commit. Hints reduce the append replays to zero; the real commit must
   * also allocate no fallback cache. The baseline replay is the byte oracle.
   */
  @Test
  @Tag("heavy")
  void measureIncrementalAppendAcrossCommits() throws Exception {
    for (final int length : new int[] {10_000, 20_000}) {
      for (final int batchSize : new int[] {length, 1_000}) {
        incrementalAppend(length, batchSize);
      }
    }
  }

  @Test
  void incrementalAppendDoesNotRewalkEarlierCommits() throws Exception {
    incrementalAppend(2_048, 128);
  }

  @Test
  void unhintedTailStopsAtIngestedLeftSibling() throws Exception {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storageType(StorageType.FILE_CHANNEL).build();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      final long array = wtx.getNodeKey();
      wtx.commit();
      assertTrue(wtx.moveTo(array));
      wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(largeArray(128)), JsonNodeTrx.Commit.NO,
          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
      wtx.insertNumberValueAsRightSibling(128);
      final var hints = IngestArrayPositionProbe.snapshot(wtx);
      assertEquals(128, hints.size());
      final var diffs = IngestArrayPositionProbe.pendingDiffs(wtx, false);
      final ArrayPositionCacheProbe allocation = new ArrayPositionCacheProbe();
      final var commit = WorkCapture.of().with(allocation).call(wtx::commit);
      commit.work()
            .assertExactly(allocation.caches(), 2, "commit cache observer disconnected")
            .assertExactly(allocation.entries(), 1, "unhinted tail must stop at the known left sibling");
      final SiblingMoves moves = new SiblingMoves();
      final String fast =
          new JsonDiffSerializer(database.getName(), countingSession(session, moves), 1, 2, diffs).serializeSidecar(
              hints);
      final String baseline = new JsonDiffSerializer(database.getName(), session, 1, 2, diffs).serializeSidecar();
      assertEquals(baseline, fast);
      assertEquals(1, moves.leftMoves + moves.rightMoves, "one real move also proves the counter is live");
      assertArrayEquals(
          Files.readAllBytes(config.getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json")),
          fast.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void incrementalAppend(final int length, final int batchSize) throws Exception {
    JsonTestHelper.deleteEverything();
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storageType(StorageType.FILE_CHANNEL).build();
    assertTrue(config.storeDiffs());
    final SiblingMoves moves = new SiblingMoves();
    final SiblingMoves fastMoves = new SiblingMoves();
    final long started = System.nanoTime();
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      final long arrayKey = wtx.getNodeKey();
      wtx.commit();
      final JsonResourceSession counted = countingSession(session, moves);
      final JsonResourceSession fastCounted = countingSession(session, fastMoves);
      for (int start = 0; start < length; start += batchSize) {
        assertTrue(wtx.moveTo(arrayKey));
        wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(largeArray(batchSize)), JsonNodeTrx.Commit.NO,
            JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
        final int revision = wtx.getRevisionNumber();
        final var knownPositions = IngestArrayPositionProbe.snapshot(wtx);
        assertEquals(batchSize, knownPositions.size(), "hints must cover only this commit's appends");
        final ArrayPositionCacheProbe allocation = new ArrayPositionCacheProbe();
        final var committedWork = WorkCapture.of().with(allocation).call(wtx::commit);
        assertTrue(IngestArrayPositionProbe.snapshot(wtx).isEmpty(), "commit must drop ingest hints");
        final byte[] committed =
            Files.readAllBytes(config.getResource()
                                     .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                     .resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json"));
        final JsonObject document =
            JsonParser.parseString(new String(committed, StandardCharsets.UTF_8)).getAsJsonObject();
        final List<DiffTuple> diffs = new ArrayList<>();
        for (final var entry : document.getAsJsonArray("diffs")) {
          diffs.add(inserted(entry.getAsJsonObject().getAsJsonObject("insert").get("nodeKey").getAsLong()));
        }
        assertFalse(diffs.isEmpty());
        final String replay =
            new JsonDiffSerializer(database.getName(), counted, revision - 1, revision, diffs).serializeSidecar();
        assertArrayEquals(committed, replay.getBytes(StandardCharsets.UTF_8));
        final var fastReplay = WorkCapture.of(fastMoves.counters())
                                          .call(() -> new JsonDiffSerializer(database.getName(), fastCounted,
                                              revision - 1, revision, diffs).serializeSidecar(knownPositions));
        assertArrayEquals(committed, fastReplay.result().getBytes(StandardCharsets.UTF_8));
        fastReplay.work().assertZero(fastMoves.siblingMoves(), "append diff rewalking earlier committed prefixes");
        committedWork.work()
                     .assertExactly(allocation.caches(), 2, "real commit not observed")
                     .assertZero(allocation.entries(), "real commit failed to consume ingest hints")
                     .assertZero(allocation.backingBytes(), "append commit walked an already-known prefix");
        assertTrue(wtx.moveTo(arrayKey));
        assertEquals(start + batchSize, wtx.getChildCount());
      }
      assertTrue(wtx.moveToFirstChild());
      for (int index = 0; index < length; index++) {
        assertEquals(index % batchSize, wtx.getNumberValue().intValue());
        if (index + 1 < length) {
          assertTrue(wtx.moveToRightSibling());
        }
      }
    }
    final long commits = length / batchSize;
    assertEquals(batchSize * commits * (commits + 1) / 2 - commits, moves.leftMoves + moves.rightMoves,
        "the baseline is also the non-vacuity check on the counted cursor seam");
    assertEquals(0, fastMoves.leftMoves + fastMoves.rightMoves, "append diffs must not walk previous commits");
    System.out.printf(
        "APPEND-MEASUREMENT length=%d batch=%d commits=%d baselineMoves=%d fastMoves=%d runtimeSeconds=%.3f%n", length,
        batchSize, length / batchSize, moves.leftMoves + moves.rightMoves, fastMoves.leftMoves + fastMoves.rightMoves,
        (System.nanoTime() - started) / 1_000_000_000.0);
  }

  @ParameterizedTest
  @EnumSource(TupleOrder.class)
  void everyPositionOfOneArrayCostsOneWalkOverIt(final TupleOrder order) throws Exception {
    try (final var database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long[] elementKeys = shredLargeArray(wtx);

      final List<DiffTuple> diffs = new ArrayList<>(LARGE_ARRAY_LENGTH);
      for (final long elementKey : elementKeys) {
        diffs.add(inserted(elementKey));
      }
      switch (order) {
        case REVERSE -> Collections.reverse(diffs);
        case SHUFFLED -> Collections.shuffle(diffs, new Random(731));
        case FORWARD -> {
        }
      }

      final SiblingMoves moves = new SiblingMoves();
      final ArrayPositionCacheProbe allocation = new ArrayPositionCacheProbe();
      final JsonResourceSession countedSession = countingSession(session, moves);
      final WorkCapture.Captured<String> sidecar =
          WorkCapture.of(moves.counters())
                     .with(allocation)
                     .call(() -> serialize(database.getName(), countedSession, diffs));

      final JsonObject document = JsonParser.parseString(sidecar.result()).getAsJsonObject();
      assertEquals(LARGE_ARRAY_LENGTH, document.getAsJsonArray("diffs").size());
      for (int index = 0; index < LARGE_ARRAY_LENGTH; index++) {
        final int ordinal = document.getAsJsonArray("diffs")
                                    .get(index)
                                    .getAsJsonObject()
                                    .getAsJsonObject("insert")
                                    .get("data")
                                    .getAsInt();
        assertEquals("/[" + ordinal + "]", pathOf(document, index));
      }

      sidecar.work()
             .assertBetween(moves.siblingMoves(), MOVE_FLOOR, AMORTIZED_MOVE_CEILING,
                 "resolving each tuple's index by its own walk over the array prefix, which is quadratic "
                     + "in the array's length and ran inside commit");
      sidecar.work()
             .assertExactly(allocation.caches(), 2, "the allocation probe missing either revision")
             .assertExactly(allocation.entries(), LARGE_ARRAY_LENGTH, "caching more than the touched prefix")
             .assertBetween(allocation.backingBytes(), 1, 32L * LARGE_ARRAY_LENGTH,
                 "cache backing storage exceeding a linear bound on the touched prefix");
    }
  }

  @Test
  void singleHeadInsertIntoLargeArrayHasConstantWorkAndBackingAllocation() throws Exception {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).storageType(StorageType.FILE_CHANNEL).build();
    assertTrue(config.storeDiffs(), "this regression must exercise the default commit sidecar");
    try (
        final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final WorkCounter ingestHintBytes = ingestHintBackingBytes(wtx);
      final WorkCapture.Captured<Integer> load = WorkCapture.of(ingestHintBytes).call(() -> {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(largeArray(HUGE_ARRAY_LENGTH)),
            JsonNodeTrx.Commit.NO);
        return IngestArrayPositionProbe.snapshot(wtx).size();
      });
      load.work()
          .assertZero(ingestHintBytes,
              "a fresh resource's first commit sizing an ingest-ordinal map from the loaded array, for a sidecar "
                  + "that revision never writes");
      assertEquals(0, load.result().intValue(), "the bootstrap revision has no predecessor, so it learns no ordinals");
      wtx.commit();
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertEquals(HUGE_ARRAY_LENGTH, wtx.getChildCount());
      wtx.insertNumberValueAsFirstChild(-1);
      final long insertedKey = wtx.getNodeKey();

      final SiblingMoves moves = new SiblingMoves();
      final ArrayPositionCacheProbe allocation = new ArrayPositionCacheProbe();
      final JsonResourceSession countedSession = countingSession(session, moves);
      final WorkCapture.Captured<String> captured = WorkCapture.of(moves.counters()).with(allocation).call(() -> {
        wtx.commit();
        // Execute the very same serialization with counted cursors, and compare it to the bytes
        // from the actual default commit. The probe observes both real serialization invocations.
        return new JsonDiffSerializer(database.getName(), countedSession, 1, 2,
            List.of(inserted(insertedKey))).serializeSidecar();
      });
      final byte[] committed =
          Files.readAllBytes(config.getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json"));
      assertArrayEquals(committed, captured.result().getBytes(StandardCharsets.UTF_8));
      final JsonObject document = JsonParser.parseString(captured.result()).getAsJsonObject();
      assertEquals(1, document.getAsJsonArray("diffs").size());
      assertEquals("/[0]", pathOf(document, 0));

      captured.work()
              .assertZero(moves.siblingMoves(), "a single head insert scanning an untouched array suffix")
              .assertExactly(allocation.caches(), 4,
                  "either the real commit or the counted serialization bypassing the probe")
              .assertExactly(allocation.entries(), 2, "one head insert retaining ordinals of untouched siblings")
              .assertBetween(allocation.backingBytes(), 1, 1_024,
                  "preallocating cache storage from array length even when the sibling walk stays at zero");

      assertTrue(wtx.moveTo(insertedKey));
      assertTrue(wtx.moveToRightSibling());
      final long secondElementKey = wtx.getNodeKey();
      final WorkCapture.Captured<String> nonHead =
          WorkCapture.of(moves.counters())
                     .call(() -> new JsonDiffSerializer(database.getName(), countedSession, 1, 2,
                         List.of(inserted(secondElementKey))).serializeSidecar());
      assertEquals("/[1]", pathOf(JsonParser.parseString(nonHead.result()).getAsJsonObject(), 0));
      nonHead.work()
             .assertExactly(moves.siblingMoves(), 1,
                 "the counting session dropping off the serializer's cursor route, which would leave the "
                     + "head insert's zero above reading as no instrument rather than as no work");

      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      final WorkReport appended = WorkCapture.of(ingestHintBytes)
                                             .run(() -> wtx.insertSubtreeAsLastChild(
                                                 JsonShredder.createStringReader("[1,2,3]"), JsonNodeTrx.Commit.NO,
                                                 JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES));
      assertEquals(3, IngestArrayPositionProbe.snapshot(wtx).size());
      appended.assertAtLeast(ingestHintBytes, 1,
          "a revision that does emit a sidecar learning no ordinals, which would leave the load's zero above "
              + "reading as a dead counter rather than as no allocation");
      wtx.rollback();
    }
  }

  @Test
  void aDiffResolvingNoArrayPositionAllocatesNoCacheStorage() throws Exception {
    try (final var database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1}"), JsonNodeTrx.Commit.NO);
      wtx.commit();
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      final long recordKey = wtx.getNodeKey();

      final ArrayPositionCacheProbe allocation = new ArrayPositionCacheProbe();
      final WorkCapture.Captured<String> captured =
          WorkCapture.of()
                     .with(allocation)
                     .call(() -> serialize(database.getName(), session, List.of(inserted(recordKey))));

      assertEquals("/a", pathOf(JsonParser.parseString(captured.result()).getAsJsonObject(), 0));
      captured.work()
              .assertExactly(allocation.caches(), 2, "the allocation probe missing either revision")
              .assertZero(allocation.backingBytes(),
                  "allocating ordinal-cache backing storage for a serialization that resolves no array position");
    }
  }

  @Test
  void oneElementCostsOnlyTheWalkItsOwnIndexNeeds() throws Exception {
    try (final var database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long[] elementKeys = shredLargeArray(wtx);

      final SiblingMoves moves = new SiblingMoves();
      final JsonResourceSession countedSession = countingSession(session, moves);
      final WorkCapture capture = WorkCapture.of(moves.counters());

      final List<DiffTuple> headDiff = List.of(inserted(elementKeys[0]));
      final WorkCapture.Captured<String> head =
          capture.call(() -> serialize(database.getName(), countedSession, headDiff));
      assertEquals("/[0]", pathOf(JsonParser.parseString(head.result()).getAsJsonObject(), 0));
      head.work()
          .assertZero(moves.siblingMoves(),
              "pre-resolving the whole child list for one diff, which makes a single insert at an array's "
                  + "head traverse and cache the entire array");

      final List<DiffTuple> tailDiff = List.of(inserted(elementKeys[LARGE_ARRAY_LENGTH - 1]));
      final WorkCapture.Captured<String> tail =
          capture.call(() -> serialize(database.getName(), countedSession, tailDiff));
      assertEquals("/[" + (LARGE_ARRAY_LENGTH - 1) + "]",
          pathOf(JsonParser.parseString(tail.result()).getAsJsonObject(), 0));
      tail.work()
          .assertExactly(moves.siblingMoves(), MOVE_FLOOR,
              "a lookup costing more than its own index does, and - reading as a non-zero figure on the "
                  + "same seam - the head lookup's zero being an uncounted route rather than no work");
    }
  }

  private static Database<JsonResourceSession> openDatabase() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .storeDiffs(false)
                                                              .build();
    return JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config);
  }

  private static String serialize(final String databaseName, final JsonResourceSession session,
      final List<DiffTuple> diffs) {
    return new JsonDiffSerializer(databaseName, session, 1, 1, diffs).serializeSidecar();
  }

  private static String pathOf(final JsonObject document, final int diffIndex) {
    return document.getAsJsonArray("diffs")
                   .get(diffIndex)
                   .getAsJsonObject()
                   .getAsJsonObject("insert")
                   .get("path")
                   .getAsString();
  }

  /** Shreds {@code [0,1,...]} as the root array and returns its element keys in document order. */
  private static long[] shredLargeArray(final JsonNodeTrx wtx) {
    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(largeArray()), JsonNodeTrx.Commit.NO);
    wtx.commit();

    final long[] elementKeys = new long[LARGE_ARRAY_LENGTH];
    assertTrue(wtx.moveToDocumentRoot());
    assertTrue(wtx.moveToFirstChild());
    assertTrue(wtx.moveToFirstChild());
    for (int index = 0; index < LARGE_ARRAY_LENGTH; index++) {
      elementKeys[index] = wtx.getNodeKey();
      if (index + 1 < LARGE_ARRAY_LENGTH) {
        assertTrue(wtx.moveToRightSibling());
      }
    }
    return elementKeys;
  }

  private static String largeArray() {
    return largeArray(LARGE_ARRAY_LENGTH);
  }

  private static String largeArray(final int length) {
    final StringBuilder json = new StringBuilder(length * 8);
    json.append('[');
    for (int index = 0; index < length; index++) {
      if (index != 0) {
        json.append(',');
      }
      json.append(index);
    }
    return json.append(']').toString();
  }

  /**
   * Reads the transaction's transient ingest-ordinal map while it still holds one. Commit drops the
   * map, so a capture that closes after the commit would read zero whatever the load allocated.
   */
  private static WorkCounter ingestHintBackingBytes(final JsonNodeTrx wtx) {
    return WorkCounter.alwaysOn("ingestHintBackingBytes",
        "one allocated payload byte in the transaction's transient ingest-ordinal map",
        () -> IngestArrayPositionProbe.backingBytes(wtx));
  }

  private static DiffTuple inserted(final long nodeKey) {
    return new DiffTuple(DiffFactory.DiffType.INSERTED, nodeKey, 0, null);
  }

  private static JsonResourceSession countingSession(final JsonResourceSession delegate, final SiblingMoves moves) {
    return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
        new Class<?>[] {JsonResourceSession.class}, (proxy, method, arguments) -> {
          final Object result = invoke(delegate, method, arguments);
          if (result instanceof JsonNodeReadOnlyTrx rtx) {
            return countingTransaction(rtx, moves);
          }
          return result;
        });
  }

  private static JsonNodeReadOnlyTrx countingTransaction(final JsonNodeReadOnlyTrx delegate, final SiblingMoves moves) {
    return (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
        new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, arguments) -> {
          if (method.getName().equals("moveToLeftSibling")) {
            moves.leftMoves++;
          } else if (method.getName().equals("moveToRightSibling")) {
            moves.rightMoves++;
          }
          return invoke(delegate, method, arguments);
        });
  }

  private static Object invoke(final Object delegate, final Method method, final Object[] arguments) throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (final InvocationTargetException exception) {
      throw exception.getCause();
    }
  }

  /**
   * Running totals of the cursor moves the counting session saw. The directions are counted
   * separately so a failure names the way the route walked, and the budget is asserted on their sum
   * so it bounds the traversal rather than the direction of it.
   */
  private static final class SiblingMoves {
    private long leftMoves;

    private long rightMoves;

    private final WorkCounter siblingMoves = WorkCounter.alwaysOn("arrayPositionSiblingMoves",
        "one cursor move to a sibling while resolving a diff path's array indices", () -> leftMoves + rightMoves);

    private final List<WorkCounter> counters = List.of(siblingMoves,
        WorkCounter.alwaysOn("arrayPositionLeftMoves", "one move to a left sibling", () -> leftMoves),
        WorkCounter.alwaysOn("arrayPositionRightMoves", "one move to a right sibling", () -> rightMoves));

    private WorkCounter siblingMoves() {
      return siblingMoves;
    }

    private List<WorkCounter> counters() {
      return counters;
    }
  }
}
