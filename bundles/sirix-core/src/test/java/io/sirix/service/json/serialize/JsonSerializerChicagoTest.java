package io.sirix.service.json.serialize;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link JsonMaxLevelMaxNodesMaxChildNodesVisitor} using
 * a chicago-like dataset structure (object with "data" array containing many child arrays).
 */
public final class JsonSerializerChicagoTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  /**
   * Test serializing the "data" array with maxChildren limit.
   * The data array contains 101 child arrays (records).
   * With maxChildren=5, we should get exactly 5 child arrays.
   */
  @Test
  public void testSerializeDataArrayWithMaxChildren() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      
      // Shred the chicago subset JSON
      wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")));
      wtx.commit();

      // Find the "data" array node key
      // Structure: DocumentRoot -> Object -> ObjectKey("data") -> Array
      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild(); // Object
        
        // Find the "data" key
        long dataArrayNodeKey = -1;
        if (rtx.moveToFirstChild()) { // First ObjectKey
          do {
            if ("data".equals(rtx.getName().getLocalName())) {
              rtx.moveToFirstChild(); // Move to the array value
              dataArrayNodeKey = rtx.getNodeKey();
              break;
            }
          } while (rtx.moveToRightSibling());
        }
        
        assertTrue("Could not find 'data' array node", dataArrayNodeKey > 0);
        
        // Get the child count of the data array
        rtx.moveTo(dataArrayNodeKey);
        final long childCount = rtx.getChildCount();
        assertEquals("Expected 101 records in data array", 101, childCount);
        
        // Now serialize with maxChildren=5 and maxLevel=2
        try (final Writer writer = new StringWriter()) {
          final var serializer = new JsonSerializer.Builder(manager, writer)
              .startNodeKey(dataArrayNodeKey)
              .maxLevel(2)
              .maxChildren(5)
              .build();
          serializer.call();
          
          final String result = writer.toString();
          System.out.println("Result with maxChildren=5, maxLevel=2:");
          System.out.println(result);
          
          // Count the number of top-level arrays in the result
          // Each child array starts with "["
          // The result should be: [[...],[...],[...],[...],[...]]
          // So we should have exactly 5 child arrays
          int arrayCount = countTopLevelArrayChildren(result);
          assertEquals("Expected exactly 5 child arrays with maxChildren=5", 5, arrayCount);
        }
      }
    }
  }

  /**
   * Test serializing the "data" array with maxChildren=50.
   * Should return exactly 50 child arrays.
   */
  @Test
  public void testSerializeDataArrayWithMaxChildren50() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      
      wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")));
      wtx.commit();

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        
        long dataArrayNodeKey = -1;
        if (rtx.moveToFirstChild()) {
          do {
            if ("data".equals(rtx.getName().getLocalName())) {
              rtx.moveToFirstChild();
              dataArrayNodeKey = rtx.getNodeKey();
              break;
            }
          } while (rtx.moveToRightSibling());
        }
        
        assertTrue("Could not find 'data' array node", dataArrayNodeKey > 0);
        
        try (final Writer writer = new StringWriter()) {
          final var serializer = new JsonSerializer.Builder(manager, writer)
              .startNodeKey(dataArrayNodeKey)
              .maxLevel(2)
              .maxChildren(50)
              .build();
          serializer.call();
          
          final String result = writer.toString();
          System.out.println("Result with maxChildren=50, maxLevel=2:");
          System.out.println(result.substring(0, Math.min(500, result.length())) + "...");
          
          int arrayCount = countTopLevelArrayChildren(result);
          assertEquals("Expected exactly 50 child arrays with maxChildren=50", 50, arrayCount);
        }
      }
    }
  }

  /**
   * Test serializing the "data" array with metadata to match REST API behavior.
   */
  @Test
  public void testSerializeDataArrayWithMaxChildrenAndMetadata() throws IOException {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final JsonResourceSession manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      
      wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(JSON.resolve("chicago-subset.json")));
      wtx.commit();

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        
        long dataArrayNodeKey = -1;
        if (rtx.moveToFirstChild()) {
          do {
            if ("data".equals(rtx.getName().getLocalName())) {
              rtx.moveToFirstChild();
              dataArrayNodeKey = rtx.getNodeKey();
              break;
            }
          } while (rtx.moveToRightSibling());
        }
        
        assertTrue("Could not find 'data' array node", dataArrayNodeKey > 0);
        
        try (final Writer writer = new StringWriter()) {
          final var serializer = new JsonSerializer.Builder(manager, writer)
              .startNodeKey(dataArrayNodeKey)
              .maxLevel(2)
              .maxChildren(5)
              .withNodeKeyAndChildCountMetaData(true)
              .build();
          serializer.call();
          
          final String result = writer.toString();
          System.out.println("Result with maxChildren=5, maxLevel=2, metadata:");
          System.out.println(result);
          
          // With metadata, the structure is different:
          // {"metadata":{"nodeKey":...,"childCount":101},"value":[...]}
          // Count child arrays in the "value" section
          assertTrue("Result should contain metadata", result.contains("\"metadata\""));
          assertTrue("Result should contain childCount", result.contains("\"childCount\""));
          
          // Extract just the value array and count children
          int valueStart = result.indexOf("\"value\":[") + 9;
          String valueSection = result.substring(valueStart, result.length() - 2); // remove trailing ]}
          int arrayCount = countTopLevelArrayChildren("[" + valueSection + "]");
          assertEquals("Expected exactly 5 child arrays with maxChildren=5", 5, arrayCount);
        }
      }
    }
  }

  /**
   * Count the number of top-level array children in a JSON array string.
   * For example: [[1,2],[3,4],[5,6]] has 3 children.
   */
  private int countTopLevelArrayChildren(String jsonArray) {
    if (jsonArray == null || jsonArray.length() < 2) {
      return 0;
    }
    
    // Remove leading/trailing whitespace
    jsonArray = jsonArray.trim();
    
    // Should start with [ and end with ]
    if (!jsonArray.startsWith("[") || !jsonArray.endsWith("]")) {
      return 0;
    }
    
    // Handle empty array
    String content = jsonArray.substring(1, jsonArray.length() - 1).trim();
    if (content.isEmpty()) {
      return 0;
    }
    
    int count = 0;
    int depth = 0;
    boolean inString = false;
    boolean escaped = false;
    
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      
      if (escaped) {
        escaped = false;
        continue;
      }
      
      if (c == '\\') {
        escaped = true;
        continue;
      }
      
      if (c == '"') {
        inString = !inString;
        continue;
      }
      
      if (inString) {
        continue;
      }
      
      if (c == '[' || c == '{') {
        if (depth == 0) {
          count++;
        }
        depth++;
      } else if (c == ']' || c == '}') {
        depth--;
      }
    }
    
    return count;
  }
}
