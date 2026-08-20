/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */
package io.sirix.node.json;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct setter coverage for ObjectNode's allocation-free bound-field resize encoder. */
final class ObjectNodeResizeEncoderTest {

  private static final long NODE_KEY = 1_000L;
  private static final long HASH = 0xDEADBEEFCAFEL;
  private static final long FAR_KEY = NODE_KEY + (1L << 40);
  private static final long FAR_COUNT = 1L << 40;
  private static final int FAR_REVISION = 1_000_000;

  private static final ObjectState INITIAL_STATE = new ObjectState(
      999L,
      1_001L,
      Fixed.NULL_NODE_KEY.getStandardProperty(),
      1_002L,
      1_005L,
      0,
      1,
      HASH,
      4L,
      42L);

  private static final ObjectState FAR_STATE = new ObjectState(
      FAR_KEY + 1,
      FAR_KEY + 2,
      FAR_KEY + 3,
      FAR_KEY + 4,
      FAR_KEY + 5,
      FAR_REVISION,
      FAR_REVISION + 1,
      HASH,
      FAR_COUNT,
      FAR_COUNT + 1);

  private ResourceConfiguration config;
  private KeyValueLeafPage page;
  private ObjectNode objectNode;

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(1L << 30);
  }

  @BeforeEach
  void setUp() {
    config = ResourceConfiguration.newBuilder("object-node-resize-encoder").build();
    page = new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
    objectNode = detachedNode(INITIAL_STATE);
    objectNode.setWriteSingleton(true);
    page.serializeNewRecord(objectNode, NODE_KEY, 0);
    assertTrue(objectNode.isBound());
  }

  @AfterEach
  void tearDown() {
    if (page != null) {
      page.close();
    }
  }

  static Stream<Arguments> resizableFields() {
    return Stream.of(
        Arguments.of(NodeFieldLayout.OBJECT_PARENT_KEY, FAR_KEY + 1, "parentKey"),
        Arguments.of(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, FAR_KEY + 2, "rightSiblingKey"),
        Arguments.of(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, FAR_KEY + 3, "leftSiblingKey"),
        Arguments.of(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, FAR_KEY + 4, "firstChildKey"),
        Arguments.of(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, FAR_KEY + 5, "lastChildKey"),
        Arguments.of(NodeFieldLayout.OBJECT_PREV_REVISION, FAR_REVISION, "previousRevision"),
        Arguments.of(NodeFieldLayout.OBJECT_LAST_MOD_REVISION, FAR_REVISION + 1L, "lastModifiedRevision"),
        Arguments.of(NodeFieldLayout.OBJECT_CHILD_COUNT, FAR_COUNT, "childCount"),
        Arguments.of(NodeFieldLayout.OBJECT_DESCENDANT_COUNT, FAR_COUNT + 1, "descendantCount"));
  }

  @ParameterizedTest(name = "setter resize preserves every field: {2}")
  @MethodSource("resizableFields")
  void everySetterUsesThePrimitiveEncoder(final int fieldIndex, final long value,
      final String description) {
    mutate(fieldIndex, value);

    final ObjectState expected = INITIAL_STATE.withField(fieldIndex, value);
    assertTrue(objectNode.isBound(), description);
    assertState(expected);
    assertArrayEquals(logicalWire(detachedNode(expected)), logicalWire(objectNode), description);
  }

  @Test
  void growShrinkGrowAbaRetainsExactLogicalAndPageWire() {
    apply(FAR_STATE);
    final byte[] firstLogicalA = logicalWire(objectNode);
    final byte[] firstPageA = pageWire();

    apply(INITIAL_STATE);
    final byte[] logicalB = logicalWire(objectNode);
    final byte[] pageB = pageWire();
    assertFalse(Arrays.equals(firstLogicalA, logicalB));
    assertFalse(Arrays.equals(firstPageA, pageB));

    apply(FAR_STATE);
    assertState(FAR_STATE);
    assertArrayEquals(firstLogicalA, logicalWire(objectNode));
    assertArrayEquals(firstPageA, pageWire());
    assertTrue(objectNode.isBound());
  }

  @Test
  void failedResizeRetainsEveryGetterAndWireAndClearsScratchForNextMutation() {
    final byte[] logicalBefore = logicalWire(objectNode);
    final byte[] pageBefore = pageWire();
    final RuntimeException injected = new RuntimeException("injected resize failure");

    final RuntimeException failure = assertThrows(RuntimeException.class,
        () -> objectNode.exerciseFailingResizeForTest(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY,
            FAR_KEY, () -> {
              throw injected;
            }));

    assertSame(injected, failure);
    assertState(INITIAL_STATE);
    assertArrayEquals(logicalBefore, logicalWire(objectNode));
    assertArrayEquals(pageBefore, pageWire());
    assertTrue(objectNode.isBound());

    objectNode.setRightSiblingKey(FAR_KEY);
    final ObjectState expected = INITIAL_STATE.withField(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, FAR_KEY);
    assertState(expected);
    assertFalse(Arrays.equals(logicalBefore, logicalWire(objectNode)));
    assertFalse(Arrays.equals(pageBefore, pageWire()));

    objectNode.clearBinding();
    assertState(expected);
  }

  @Test
  void reentrantResizeIsRejectedAndSentinelIsCleared() {
    final byte[] logicalBefore = logicalWire(objectNode);
    final byte[] pageBefore = pageWire();

    final IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> objectNode.exerciseFailingResizeForTest(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY,
            FAR_KEY, () -> objectNode.setLastChildKey(FAR_KEY + 5)));

    assertTrue(failure.getMessage().contains("Reentrant ObjectNode field resize"));
    assertState(INITIAL_STATE);
    assertArrayEquals(logicalBefore, logicalWire(objectNode));
    assertArrayEquals(pageBefore, pageWire());

    objectNode.setLastChildKey(FAR_KEY + 5);
    objectNode.setRightSiblingKey(FAR_KEY);
    assertState(INITIAL_STATE
        .withField(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, FAR_KEY + 5)
        .withField(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, FAR_KEY));
  }

  private void apply(final ObjectState state) {
    objectNode.setParentKey(state.parentKey());
    objectNode.setRightSiblingKey(state.rightSiblingKey());
    objectNode.setLeftSiblingKey(state.leftSiblingKey());
    objectNode.setFirstChildKey(state.firstChildKey());
    objectNode.setLastChildKey(state.lastChildKey());
    objectNode.setPreviousRevision(state.previousRevision());
    objectNode.setLastModifiedRevision(state.lastModifiedRevision());
    objectNode.setChildCount(state.childCount());
    objectNode.setDescendantCount(state.descendantCount());
  }

  private void mutate(final int fieldIndex, final long value) {
    switch (fieldIndex) {
      case NodeFieldLayout.OBJECT_PARENT_KEY -> objectNode.setParentKey(value);
      case NodeFieldLayout.OBJECT_RIGHT_SIB_KEY -> objectNode.setRightSiblingKey(value);
      case NodeFieldLayout.OBJECT_LEFT_SIB_KEY -> objectNode.setLeftSiblingKey(value);
      case NodeFieldLayout.OBJECT_FIRST_CHILD_KEY -> objectNode.setFirstChildKey(value);
      case NodeFieldLayout.OBJECT_LAST_CHILD_KEY -> objectNode.setLastChildKey(value);
      case NodeFieldLayout.OBJECT_PREV_REVISION -> objectNode.setPreviousRevision((int) value);
      case NodeFieldLayout.OBJECT_LAST_MOD_REVISION -> objectNode.setLastModifiedRevision((int) value);
      case NodeFieldLayout.OBJECT_CHILD_COUNT -> objectNode.setChildCount(value);
      case NodeFieldLayout.OBJECT_DESCENDANT_COUNT -> objectNode.setDescendantCount(value);
      default -> throw new AssertionError("Unexpected test field " + fieldIndex);
    }
  }

  private void assertState(final ObjectState expected) {
    assertEquals(expected.parentKey(), objectNode.getParentKey(), "parentKey");
    assertEquals(expected.rightSiblingKey(), objectNode.getRightSiblingKey(), "rightSiblingKey");
    assertEquals(expected.leftSiblingKey(), objectNode.getLeftSiblingKey(), "leftSiblingKey");
    assertEquals(expected.firstChildKey(), objectNode.getFirstChildKey(), "firstChildKey");
    assertEquals(expected.lastChildKey(), objectNode.getLastChildKey(), "lastChildKey");
    assertEquals(expected.previousRevision(), objectNode.getPreviousRevisionNumber(), "previousRevision");
    assertEquals(expected.lastModifiedRevision(), objectNode.getLastModifiedRevisionNumber(),
        "lastModifiedRevision");
    assertEquals(expected.hash(), objectNode.getHash(), "hash");
    assertEquals(expected.childCount(), objectNode.getChildCount(), "childCount");
    assertEquals(expected.descendantCount(), objectNode.getDescendantCount(), "descendantCount");
  }

  private byte[] logicalWire(final ObjectNode node) {
    try (BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer(128)) {
      NodeKind.OBJECT.serialize(sink, node, config);
      return sink.toByteArray();
    }
  }

  private byte[] pageWire() {
    return page.getSlotAsByteArray(0);
  }

  private static ObjectNode detachedNode(final ObjectState state) {
    return new ObjectNode(NODE_KEY,
        state.parentKey(),
        state.previousRevision(),
        state.lastModifiedRevision(),
        state.rightSiblingKey(),
        state.leftSiblingKey(),
        state.firstChildKey(),
        state.lastChildKey(),
        state.childCount(),
        state.descendantCount(),
        state.hash(),
        LongHashFunction.xx3(),
        (byte[]) null);
  }

  private record ObjectState(long parentKey, long rightSiblingKey, long leftSiblingKey,
                             long firstChildKey, long lastChildKey, int previousRevision,
                             int lastModifiedRevision, long hash, long childCount,
                             long descendantCount) {

    private ObjectState withField(final int fieldIndex, final long value) {
      return switch (fieldIndex) {
        case NodeFieldLayout.OBJECT_PARENT_KEY -> new ObjectState(value, rightSiblingKey,
            leftSiblingKey, firstChildKey, lastChildKey, previousRevision, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_RIGHT_SIB_KEY -> new ObjectState(parentKey, value,
            leftSiblingKey, firstChildKey, lastChildKey, previousRevision, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_LEFT_SIB_KEY -> new ObjectState(parentKey, rightSiblingKey,
            value, firstChildKey, lastChildKey, previousRevision, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_FIRST_CHILD_KEY -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, value, lastChildKey, previousRevision, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_LAST_CHILD_KEY -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, firstChildKey, value, previousRevision, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_PREV_REVISION -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, firstChildKey, lastChildKey, (int) value, lastModifiedRevision,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_LAST_MOD_REVISION -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, firstChildKey, lastChildKey, previousRevision, (int) value,
            hash, childCount, descendantCount);
        case NodeFieldLayout.OBJECT_CHILD_COUNT -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, firstChildKey, lastChildKey, previousRevision, lastModifiedRevision,
            hash, value, descendantCount);
        case NodeFieldLayout.OBJECT_DESCENDANT_COUNT -> new ObjectState(parentKey, rightSiblingKey,
            leftSiblingKey, firstChildKey, lastChildKey, previousRevision, lastModifiedRevision,
            hash, childCount, value);
        default -> throw new AssertionError("Unexpected test field " + fieldIndex);
      };
    }
  }
}
