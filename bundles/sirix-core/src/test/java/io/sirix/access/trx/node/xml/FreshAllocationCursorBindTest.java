/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.xml;

import io.brackit.query.atomic.QNm;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.node.NodeKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Correctness gates for adopting a newly allocated XML document slot without a TIL lookup. */
final class FreshAllocationCursorBindTest {

  @BeforeEach
  void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    XmlTestHelper.deleteEverything();
  }

  @Test
  void sameKindFreshBindingsCrossDocumentPageBoundaryWithExactTopology() {
    final int nodeCount = 1_030;
    final QNm childName = new QNm("child");
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final long rootKey;
      try (final var trx = session.beginNodeTrx()) {
        rootKey = trx.insertElementAsFirstChild(new QNm("root")).getNodeKey();
        for (int index = 0; index < nodeCount; index++) {
          assertTrue(trx.moveTo(rootKey));
          trx.insertElementAsFirstChild(childName);
          assertEquals(NodeKind.ELEMENT, trx.getKind());
          assertEquals(childName, trx.getName(),
              "fresh bind must expose the element just created, not a rebound factory singleton");
          assertEquals(rootKey, trx.getParentKey());
        }
        trx.commit();
      }

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveTo(rootKey));
        assertEquals(nodeCount, rtx.getChildCount());
        assertTrue(rtx.moveToFirstChild());
        int visited = 0;
        do {
          assertEquals(childName, rtx.getName());
          assertEquals(rootKey, rtx.getParentKey());
          visited++;
        } while (rtx.moveToRightSibling());
        assertEquals(nodeCount, visited);
      }
    }
  }

  @Test
  void rejectedFreshAllocationGuardFallsBackBeforeXmlTextMerge() throws ReflectiveOperationException {
    try (final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final long rootKey = trx.insertElementAsFirstChild(new QNm("root")).getNodeKey();
      final long textKey = trx.insertTextAsFirstChild("left").getNodeKey();

      assertTrue(trx.moveTo(rootKey));
      trx.insertAttribute(new QNm("a"), "value");
      assertEquals(NodeKind.ATTRIBUTE, trx.getKind());

      // The attribute is now the writer's most recent allocation, so positioning on the older text
      // key must reject direct adoption and take the normal TIL-aware move fallback.
      invokeMoveToJustInsertedNode(trx, textKey);
      assertEquals(textKey, trx.getNodeKey());
      assertEquals(NodeKind.TEXT, trx.getKind());
      assertEquals("left", trx.getValue());

      trx.insertTextAsRightSibling("-right");
      assertEquals(textKey, trx.getNodeKey(), "adjacent text insertion must merge in place");
      assertEquals("left-right", trx.getValue());
      assertFalse(trx.hasRightSibling());
      trx.commit();
    }
  }

  private static void invokeMoveToJustInsertedNode(final Object trx, final long nodeKey)
      throws ReflectiveOperationException {
    final Method method = AbstractNodeTrxImpl.class.getDeclaredMethod("moveToJustInsertedNode", long.class);
    method.setAccessible(true);
    method.invoke(trx, nodeKey);
  }
}
