package org.sirix.index.pathsummary.axis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.axis.pathsummary.ChildAxis;
import org.sirix.index.path.summary.PathNode;
import org.sirix.index.path.summary.PathSummaryReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class ChildAxisTest {
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
      pathSummary.moveToFirstChild();
      var axis = new ChildAxis(pathSummary.getPathNode());
      checkPathNodesForNodeKey7(pathSummary, axis);
      pathSummary.moveTo(8);

      axis = new ChildAxis(pathSummary.getPathNode());
      checkPathNodesForNodeKey8(pathSummary, axis);
    }
  }

  private static void checkPathNodesForNodeKey7(PathSummaryReader pathSummary,
      ChildAxis axis) {
    checkPathNode(pathSummary, axis, 8, "/tada/[]", 1, 2);
    assertFalse(axis.hasNext());
  }

  private static void checkPathNodesForNodeKey8(PathSummaryReader pathSummary, ChildAxis axis) {
    checkPathNode(pathSummary, axis, 11, "/tada/[]/[]", 1, 3);
    checkPathNode(pathSummary, axis, 10, "/tada/[]/baz", 1, 3);
    checkPathNode(pathSummary, axis, 9, "/tada/[]/foo", 1, 3);
    assertFalse(axis.hasNext());
  }

  private static void checkPathNode(PathSummaryReader pathSummary,
      ChildAxis pathSummaryAxis, long nodeKey, String path, int references, int level) {
    PathNode node = pathSummaryAxis.next();
    pathSummary.moveTo(node.getNodeKey());
    assertEquals(nodeKey, node.getNodeKey());
    assertEquals(path, pathSummary.getPath().toString());
    assertEquals(references, node.getReferences());
    assertEquals(level, node.getLevel());
  }
}
