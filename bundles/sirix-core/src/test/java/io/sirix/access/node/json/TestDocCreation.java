package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.Test;

public class TestDocCreation {

  @Test
  public void testCreation() {
    JsonTestHelper.deleteEverything();
    
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      
      JsonDocumentCreator.create(wtx);
      
      System.out.println("After creation, before commit:");
      System.out.println("  maxNodeKey: " + wtx.getMaxNodeKey());
      System.out.println("  hasNode(23): " + wtx.hasNode(23));
      System.out.println("  hasNode(24): " + wtx.hasNode(24));
      System.out.println("  hasNode(25): " + wtx.hasNode(25));
      
      wtx.commit();
      
      System.out.println("\nAfter commit:");
      System.out.println("  hasNode(23): " + wtx.hasNode(23));
      System.out.println("  hasNode(24): " + wtx.hasNode(24));
      System.out.println("  hasNode(25): " + wtx.hasNode(25));
    }
    
    // New read transaction
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      System.out.println("\nIn new read transaction:");
      System.out.println("  maxNodeKey: " + rtx.getMaxNodeKey());
      System.out.println("  hasNode(23): " + rtx.hasNode(23));
      System.out.println("  hasNode(24): " + rtx.hasNode(24));
      System.out.println("  hasNode(25): " + rtx.hasNode(25));
    }
  }
}

