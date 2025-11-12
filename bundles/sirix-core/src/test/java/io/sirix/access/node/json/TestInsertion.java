package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import org.junit.Test;

public class TestInsertion {

  @Test
  public void testInsertObjectAsRightSibling() {
    JsonTestHelper.deleteEverything();
    
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      
      wtx.moveToDocumentRoot();
      wtx.insertObjectAsFirstChild();  // Node 1 (root object)
      
      long node1Key = wtx.getNodeKey();
      System.out.println("Created root object: node " + node1Key);
      
      wtx.insertObjectRecordAsFirstChild("tada", new io.sirix.service.json.shredder.ArrayValue());  // Creates array
      long arrayKey = wtx.getNodeKey();
      System.out.println("Created array (tada): node " + arrayKey + ", parent=" + wtx.getParentKey());
      
      // Now add children to the array
      long obj1Key = wtx.insertObjectAsFirstChild().getNodeKey();  // First object in array
      System.out.println("Created first object in array: node " + obj1Key + ", parent=" + wtx.getParentKey());
      wtx.moveToParent();  // Back to array
      
      long stringKey = wtx.insertStringValueAsRightSibling("boo").getNodeKey();  // String as sibling
      System.out.println("Created string as right sibling: node " + stringKey + ", parent=" + wtx.getParentKey());
      
      long emptyObjKey = wtx.insertObjectAsRightSibling().getNodeKey();  // Empty object
      System.out.println("Created empty object as right sibling: node " + emptyObjKey + ", parent=" + wtx.getParentKey());
      
      // Verify structure
      wtx.moveTo(arrayKey);
      System.out.println("\nArray node " + arrayKey + " children:");
      System.out.println("  childCount: " + wtx.getChildCount());
      System.out.println("  firstChild: " + wtx.getFirstChildKey());
      if (wtx.hasFirstChild()) {
        wtx.moveToFirstChild();
        do {
          System.out.println("    Child: node " + wtx.getNodeKey() + ", parent=" + wtx.getParentKey());
        } while (wtx.hasRightSibling() && wtx.moveToRightSibling());
      }
    }
  }
}

