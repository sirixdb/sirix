package org.sirix.access.node.json;

import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.api.Axis;
import org.sirix.axis.DescendantAxis;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.NodeKind;

import static org.junit.Assert.*;

public class PathSummaryTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createTestDocument();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testInsertTestDocument() {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
        final var pathSummary = manager.openPathSummary()) {
//       final var pathSummaryAxis = new DescendantAxis(pathSummary);
//
//       while (pathSummaryAxis.hasNext()) {
//         pathSummaryAxis.next();
//
//         System.out.println("nodeKey: " + pathSummary.getNodeKey());
//         System.out.println("path: " + pathSummary.getPath());
//         System.out.println("references: " + pathSummary.getReferences());
//         System.out.println("level: " + pathSummary.getLevel());
//       }

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
    assertEquals(new QNm("__array__"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Get the next summary.
   *
   * @param axis the axis to use
   * @return the next path summary
   */
  private PathSummaryReader next(final Axis axis) {
    if (axis.hasNext()) {
      axis.next();
      return (PathSummaryReader) axis.getCursor();
    }
    return null;
  }
}
