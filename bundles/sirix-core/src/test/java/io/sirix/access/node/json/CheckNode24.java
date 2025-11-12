package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import org.junit.Test;

public class CheckNode24 {

  @Test
  public void check() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {
      
      if (rtx.hasNode(24)) {
        rtx.moveTo(24);
        System.out.println("Node 24: kind=" + rtx.getKind() + ", parent=" + rtx.getParentKey());
      } else {
        System.out.println("Node 24 doesn't exist!");
      }
      
      rtx.moveTo(16);
      System.out.println("\nNode 16 (array): childCount=" + rtx.getChildCount());
      if (rtx.hasFirstChild()) {
        rtx.moveToFirstChild();
        do {
          System.out.println("  Child: " + rtx.getNodeKey());
        } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
      }
    }
  }
}

