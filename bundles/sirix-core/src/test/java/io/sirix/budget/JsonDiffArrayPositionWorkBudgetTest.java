/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
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
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(largeArray(HUGE_ARRAY_LENGTH)),
          JsonNodeTrx.Commit.NO);
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
      final WorkCapture.Captured<String> nonHead = WorkCapture.of(moves.counters())
                                                             .call(() -> new JsonDiffSerializer(database.getName(),
                                                                 countedSession, 1, 2,
                                                                 List.of(inserted(secondElementKey))).serializeSidecar());
      assertEquals("/[1]", pathOf(JsonParser.parseString(nonHead.result()).getAsJsonObject(), 0));
      nonHead.work()
             .assertExactly(moves.siblingMoves(), 1,
                 "the counting session dropping off the serializer's cursor route, which would leave the "
                     + "head insert's zero above reading as no instrument rather than as no work");
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
          WorkCapture.of().with(allocation).call(() -> serialize(database.getName(), session,
              List.of(inserted(recordKey))));

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
