package org.sirix.access.node.json;

import org.brackit.xquery.atomic.QNm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.Axis;
import org.sirix.axis.DescendantAxis;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;
import org.sirix.settings.Fixed;

import static org.junit.jupiter.api.Assertions.*;

public final class PathSummaryTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createTestDocument();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testInsertedTestDocument() {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var pathSummary = manager.openPathSummary()) {
      final var pathSummaryAxis = new DescendantAxis(pathSummary);

      while (pathSummaryAxis.hasNext()) {
        pathSummaryAxis.nextLong();

        System.out.println("nodeKey: " + pathSummary.getNodeKey());
        System.out.println("path: " + pathSummary.getPath());
        System.out.println("references: " + pathSummary.getReferences());
        System.out.println("level: " + pathSummary.getLevel());
      }

      testInsertHelper(pathSummary);
    }
  }

  private void testInsertHelper(final PathSummaryReader summaryReader) {
    final var axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(8L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(6L, summary.getRightSiblingKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("tada"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.ARRAY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(11L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("__array__"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.ARRAY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(11L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(10L, summary.getRightSiblingKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("__array__"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(10L, summary.getNodeKey());
    assertEquals(11L, summary.getLeftSiblingKey());
    assertEquals(9L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("baz"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(10L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("foo"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("baz"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(1L, summary.getRightSiblingKey());
    assertEquals(5L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("bar"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    summary = next(axis);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("helloo"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(5L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("hello"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertEquals(NodeKind.OBJECT_KEY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(2L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("foo"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    summary = next(axis);
    assertEquals(NodeKind.ARRAY, summary.getPathKind());
    assertEquals(1, summary.getReferences());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    checkInMemoryNodes(summary);
    assertEquals(new QNm("__array__"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNull(summary);
  }

  private static void checkInMemoryNodes(PathSummaryReader summary) {
    if (summary.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      assertNull(summary.getPathNode().getFirstChild());
    } else {
      assertEquals(summary.getPathNode().getFirstChild().getNodeKey(), summary.getFirstChildKey());
    }
    if (summary.getLeftSiblingKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      assertNull(summary.getPathNode().getLeftSibling());
    } else {
      assertEquals(summary.getPathNode().getLeftSibling().getNodeKey(), summary.getLeftSiblingKey());
    }
    if (summary.getRightSiblingKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      assertNull(summary.getPathNode().getRightSibling());
    } else {
      assertEquals(summary.getPathNode().getRightSibling().getNodeKey(), summary.getRightSiblingKey());
    }
    if (summary.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      assertNull(summary.getPathNode().getParent());
    } else {
      assertEquals(summary.getPathNode().getParent().getNodeKey(), summary.getParentKey());
    }
  }

  /**
   * Get the next summary.
   *
   * @param axis the axis to use
   * @return the next path summary
   */
  private PathSummaryReader next(final Axis axis) {
    if (axis.hasNext()) {
      axis.nextLong();
      return (PathSummaryReader) axis.getCursor();
    }
    return null;
  }
}
