/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.StorageEngineWriter;
import io.sirix.node.NodeKind;
import io.sirix.node.json.StringNode;
import io.sirix.settings.StringCompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Correctness gates for the one-shot freshly-inserted write-cursor self-move. */
final class FreshlyInsertedCursorSelfMoveTest {

  private static final long NO_FRESH_CURSOR = Long.MIN_VALUE;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void approvedSelfMoveAndDirectMoveBothResetLogicalCursorState() throws ReflectiveOperationException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final long objectKey = trx.insertObjectAsFirstChild().getNodeKey();
      final AbstractNodeReadOnlyTrx<?, ?, ?> delegate = delegate(trx);

      poisonMoveSideEffects(delegate);
      assertTrue(trx.moveTo(objectKey), "freshly-inserted self-move must succeed");
      assertMoveSideEffectsApplied(delegate);
      assertEquals(NodeKind.OBJECT, trx.getKind());

      // The ordinary delegate entry point must retain the same logical side effects after their
      // prelude is shared with the optimized wrapper path.
      poisonMoveSideEffects(delegate);
      assertTrue(delegate.moveTo(objectKey), "direct physical self-move must succeed");
      assertMoveSideEffectsApplied(delegate);
      assertEquals(NodeKind.OBJECT, delegate.getKind());
      trx.commit();
    }
  }

  @Test
  void mutationSequenceRejectsAPreviouslyMarkedCursor() throws ReflectiveOperationException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      trx.insertObjectAsFirstChild();
      final long fieldKey = trx.insertObjectRecordAsFirstChild("value", new NumberValue(1)).getNodeKey();

      assertEquals(fieldKey, readLong(AbstractNodeTrxImpl.class, trx, "freshlyInsertedCursorNodeKey"));
      assertEquals(readLong(AbstractNodeTrxImpl.class, trx, "mutationSequence"),
          readLong(AbstractNodeTrxImpl.class, trx, "freshlyInsertedCursorMutationSequence"));

      trx.setNumberValue(2);
      assertTrue(
          readLong(AbstractNodeTrxImpl.class, trx, "mutationSequence") > readLong(AbstractNodeTrxImpl.class, trx,
              "freshlyInsertedCursorMutationSequence"),
          "a later mutation must invalidate the insertion epoch recorded by the marker");

      assertTrue(trx.moveTo(fieldKey));
      assertEquals(2, trx.getNumberValue().intValue());
      assertEquals(NO_FRESH_CURSOR, readLong(AbstractNodeTrxImpl.class, trx, "freshlyInsertedCursorNodeKey"),
          "the rejected one-shot marker must still be consumed");
      trx.commit();
    }
  }

  @Test
  void forwardedAwayAndBackNavigationLeavesAReusablePhysicalBinding() throws ReflectiveOperationException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final long objectKey = trx.insertObjectAsFirstChild().getNodeKey();
      final long fieldKey = trx.insertObjectRecordAsFirstChild("value", new NumberValue(1)).getNodeKey();

      // These forwarding methods navigate on the delegate and deliberately do not consume the
      // wrapper's marker. Returning through the normal delegate path rebinds the marked node.
      assertTrue(trx.moveToParent());
      assertEquals(objectKey, trx.getNodeKey());
      assertTrue(trx.moveToFirstChild());
      assertEquals(fieldKey, trx.getNodeKey());
      assertEquals(fieldKey, readLong(AbstractNodeTrxImpl.class, trx, "freshlyInsertedCursorNodeKey"));

      poisonMoveSideEffects(delegate(trx));
      assertTrue(trx.moveTo(fieldKey));
      assertMoveSideEffectsApplied(delegate(trx));
      assertEquals(1, trx.getNumberValue().intValue());
      trx.commit();
    }
  }

  @Test
  void rollbackWriterReplacementRejectsTheAbortedNodeMarker() throws ReflectiveOperationException {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final long objectKey = trx.insertObjectAsFirstChild().getNodeKey();
      trx.commit();
      assertTrue(trx.moveTo(objectKey));

      final long abortedFieldKey = trx.insertObjectRecordAsFirstChild("aborted", new NumberValue(1)).getNodeKey();
      final StorageEngineWriter markedWriter =
          (StorageEngineWriter) readObject(AbstractNodeTrxImpl.class, trx, "freshlyInsertedCursorWriter");

      trx.rollback();
      assertNotSame(markedWriter, readObject(AbstractNodeTrxImpl.class, trx, "storageEngineWriter"),
          "rollback must replace the writer identity captured by the marker");
      assertFalse(trx.moveTo(abortedFieldKey),
          "a marker owned by the aborted writer must not resurrect its uncommitted node");
      assertTrue(trx.moveTo(objectKey), "the committed parent must remain reachable after rollback");
    }
  }

  @Test
  void freshAllocationBindPreservesSameKindNodesAcrossDocumentPageBoundary() throws ReflectiveOperationException {
    final int nodeCount = 1_030;
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final long arrayKey;
      try (final var trx = session.beginNodeTrx()) {
        arrayKey = trx.insertArrayAsFirstChild().getNodeKey();
        for (int value = 0; value < nodeCount; value++) {
          assertTrue(trx.moveTo(arrayKey));
          trx.insertNumberValueAsFirstChild(value);
          assertEquals(value, trx.getNumberValue().intValue(),
              "fresh bind must expose the number just created, not a reused factory binding");

          if (value == nodeCount - 1) {
            final long insertedKey = trx.getNodeKey();
            final InternalNodeReadOnlyTrx<?> delegate =
                (InternalNodeReadOnlyTrx<?>) ((JsonNodeTrxImpl) trx).nodeReadOnlyTrxDelegate();
            final StorageEngineWriter writer =
                (StorageEngineWriter) readObject(AbstractNodeTrxImpl.class, trx, "storageEngineWriter");
            assertFalse(delegate.tryMoveToLastAllocatedDocumentNode(writer, insertedKey - 1),
                "a different expected key must not adopt the writer's latest slot");
            assertEquals(insertedKey, delegate.getNodeKey(),
                "a rejected allocation guard must leave the cursor unchanged");
            assertTrue(delegate.tryMoveToLastAllocatedDocumentNode(writer, insertedKey));
            assertEquals(value, trx.getNumberValue().intValue());
          }
        }
        trx.commit();
      }

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(arrayKey));
        assertEquals(nodeCount, rtx.getChildCount());
        assertTrue(rtx.moveToFirstChild());
        for (int expected = nodeCount - 1; expected >= 0; expected--) {
          assertEquals(expected, rtx.getNumberValue().intValue());
          if (expected > 0) {
            assertTrue(rtx.moveToRightSibling());
          }
        }
        assertFalse(rtx.hasRightSibling());
      }
    }
  }

  @Test
  void freshAllocationBindImmediatelyDecodesInsertTimeFsstString() {
    final String resource = "fresh-allocation-fsst";
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .hashKind(HashType.NONE)
                                                   .stringCompressionType(StringCompressionType.FSST)
                                                   .build());
      try (final var session = database.beginResourceSession(resource)) {
        try (final var trx = session.beginNodeTrx()) {
          trx.insertArrayAsFirstChild();
          for (int index = 0; index < 80; index++) {
            trx.insertStringValueAsFirstChild(compressibleString(index));
            assertTrue(trx.moveToParent());
          }
          trx.commit();
        }

        final String expected = compressibleString(10_000);
        try (final var trx = session.beginNodeTrx()) {
          assertTrue(trx.moveToFirstChild());
          trx.insertStringValueAsFirstChild(expected);
          assertEquals(expected, trx.getValue(),
              "the cursor singleton must receive the page's FSST table during the fresh-slot bind");

          final InternalNodeReadOnlyTrx<?> delegate =
              (InternalNodeReadOnlyTrx<?>) ((JsonNodeTrxImpl) trx).nodeReadOnlyTrxDelegate();
          assertTrue(((StringNode) delegate.getCurrentNode()).isCompressed(),
              "the gate must exercise insert-time FSST rather than an uncompressed fallback");
          trx.commit();
        }
      }
    }
  }

  private static String compressibleString(final int index) {
    return "tenant/region/common-prefix/common-prefix/common-prefix/metric/" + index + "/common-suffix/common-suffix";
  }

  private static AbstractNodeReadOnlyTrx<?, ?, ?> delegate(final Object trx) {
    return (AbstractNodeReadOnlyTrx<?, ?, ?>) ((JsonNodeTrxImpl) trx).nodeReadOnlyTrxDelegate();
  }

  private static void poisonMoveSideEffects(final AbstractNodeReadOnlyTrx<?, ?, ?> delegate)
      throws ReflectiveOperationException {
    final Field fusedMode = field(AbstractNodeReadOnlyTrx.class, "fusedSyntheticChildMode");
    fusedMode.setBoolean(delegate, true);
    final Field structuralKeys = field(AbstractNodeReadOnlyTrx.class, "structKeysCached");
    structuralKeys.setInt(delegate, 0b1111);
  }

  private static void assertMoveSideEffectsApplied(final AbstractNodeReadOnlyTrx<?, ?, ?> delegate)
      throws ReflectiveOperationException {
    assertFalse(delegate.isFusedSyntheticChild(), "every logical move must exit synthetic-child mode");
    assertEquals(0, field(AbstractNodeReadOnlyTrx.class, "structKeysCached").getInt(delegate),
        "every logical move must invalidate structural-key cache entries");
  }

  private static long readLong(final Class<?> owner, final Object target, final String name)
      throws ReflectiveOperationException {
    return field(owner, name).getLong(target);
  }

  private static Object readObject(final Class<?> owner, final Object target, final String name)
      throws ReflectiveOperationException {
    return field(owner, name).get(target);
  }

  private static Field field(final Class<?> owner, final String name) throws ReflectiveOperationException {
    final Field field = owner.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
