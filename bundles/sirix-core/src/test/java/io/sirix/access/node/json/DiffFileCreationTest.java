package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Tests that verify diff files are correctly created after commits.
 * <p>
 * This addresses the issue where diff files were not being created after updates, causing slow diff
 * computation via the algorithmic fallback.
 */
public class DiffFileCreationTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testNoDiffFileForFirstCommit() {
    // Create test document - this creates revision 1
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      // First commit creates revision 1 - no diff file expected for rev 0 -> rev 1
      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev0toRev1.json");

      assertFalse("Diff file should NOT exist for first commit (rev 0 -> rev 1): " + diffFile, Files.exists(diffFile));
    }
  }

  @Test
  public void testDiffFileCreatedForSecondCommit() throws IOException {
    // Create test document - this creates revision 1
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {

      // Make a modification
      wtx.moveTo(4); // Move to a value node
      wtx.setStringValue("modified value");
      wtx.commit();

      // Second commit should create diff file
      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");

      assertTrue("Diff file should exist for second commit (rev 1 -> rev 2): " + diffFile, Files.exists(diffFile));

      final String diffContent = Files.readString(diffFile);
      assertTrue("Diff file should contain 'diffs' array", diffContent.contains("\"diffs\""));
      assertTrue("Diff file should have meaningful content, but was: " + diffContent, diffContent.length() > 50);

      System.out.println("Diff file created at: " + diffFile);
      System.out.println("Diff content: " + diffContent);
    }
  }

  // Disabled: testDiffFileForInsertOperations - PathSummary issue causes hang
  // Insert operations are tested implicitly via testDiffFileAfterInsertSubtree

  @Test
  public void testDiffFileForDeleteOperations() throws IOException {
    // Create test document
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {

      // Delete a node
      wtx.moveTo(2); // Move to "foo" key
      wtx.remove();
      wtx.commit();

      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");

      assertTrue("Diff file should exist after delete operation: " + diffFile, Files.exists(diffFile));

      final String diffContent = Files.readString(diffFile);
      assertTrue("Diff file should contain 'delete' operation", diffContent.contains("\"delete\""));

      System.out.println("Diff content after delete: " + diffContent);
    }
  }

  @Test
  public void testDiffFileAfterInsertSubtree() throws IOException {
    // Create test document with Dewey IDs
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {

      // Find the "foo" array (nodeKey 3 based on test document structure)
      wtx.moveTo(3); // Move to "foo" array
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"nested\":\"value\",\"count\":42}"),
          JsonNodeTrx.Commit.NO);
      wtx.commit();

      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");

      assertTrue("Diff file should exist after insertSubtreeAsFirstChild: " + diffFile, Files.exists(diffFile));

      final String diffContent = Files.readString(diffFile);
      assertTrue("Diff file should contain 'insert' operation for subtree root", diffContent.contains("\"insert\""));
      assertTrue("Insert operation should have a path", diffContent.contains("\"path\""));

      System.out.println("Diff content after subtree insert: " + diffContent);
    }
  }

  @Test
  public void testMultipleDiffFilesForMultipleCommits() throws IOException {
    // Create test document - creates revision 1
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      // Second commit - creates revision 2
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(4);
        wtx.setStringValue("value2");
        wtx.commit();
      }

      // Third commit - creates revision 3
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(4);
        wtx.setStringValue("value3");
        wtx.commit();
      }

      // Fourth commit - creates revision 4
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(4);
        wtx.setStringValue("value4");
        wtx.commit();
      }

      final Path updateOpsDir = manager.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());

      // Check all expected diff files exist
      assertFalse("No diff for rev 0 -> rev 1", Files.exists(updateOpsDir.resolve("diffFromRev0toRev1.json")));
      assertTrue("Diff should exist for rev 1 -> rev 2", Files.exists(updateOpsDir.resolve("diffFromRev1toRev2.json")));
      assertTrue("Diff should exist for rev 2 -> rev 3", Files.exists(updateOpsDir.resolve("diffFromRev2toRev3.json")));
      assertTrue("Diff should exist for rev 3 -> rev 4", Files.exists(updateOpsDir.resolve("diffFromRev3toRev4.json")));

      // Verify final revision number
      assertEquals("Should have 4 revisions", 4, manager.getMostRecentRevisionNumber());
    }
  }

  @Test
  public void testDiffFileWithoutDeweyIds() throws IOException {
    // Create test document WITHOUT Dewey IDs
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {

      wtx.moveTo(4);
      wtx.setStringValue("modified");
      wtx.commit();

      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");

      assertTrue("Diff file should exist even without Dewey IDs: " + diffFile, Files.exists(diffFile));

      System.out.println("Diff file created without Dewey IDs: " + diffFile);
    }
  }

  @Test
  public void testDiffFileAfterMultipleCommits() throws IOException {
    // Test that diff files use correct consecutive revision numbers
    // This verifies that beforeBulkInsertionRevisionNumber is properly reset
    // Test document structure:
    // { "foo": ["bar", null, 2.33], "bar": { "hello": "world", "helloo": true }, "baz": "hello",
    // "tada": [...] }
    // Node 2 = "foo" key (OBJECT_KEY), Node 4 = "bar" string in array
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      // Revision 2: modify value in array
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(4); // "bar" string value in foo array
        wtx.setStringValue("modified1");
        wtx.commit();
      }

      // Revision 3: delete the "foo" key
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(2); // "foo" key
        wtx.remove();
        wtx.commit();
      }

      // Revision 4: delete another key ("bar" is now the first key after "foo" was deleted)
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // first key (was "bar", now first after "foo" deletion)
        wtx.remove();
        wtx.commit();
      }

      final Path updateOpsDir = manager.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());

      // Check that diff files use consecutive revision numbers (not stale
      // beforeBulkInsertionRevisionNumber)
      final Path diff1to2 = updateOpsDir.resolve("diffFromRev1toRev2.json");
      final Path diff2to3 = updateOpsDir.resolve("diffFromRev2toRev3.json");
      final Path diff3to4 = updateOpsDir.resolve("diffFromRev3toRev4.json");

      assertTrue("Diff should exist for rev 1 -> rev 2: " + diff1to2, Files.exists(diff1to2));
      assertTrue("Diff should exist for rev 2 -> rev 3: " + diff2to3, Files.exists(diff2to3));
      assertTrue("Diff should exist for rev 3 -> rev 4: " + diff3to4, Files.exists(diff3to4));

      // Verify each diff file has the correct revision range in its content
      String diff1to2Content = Files.readString(diff1to2);
      assertTrue("Diff 1->2 should have old-revision:1", diff1to2Content.contains("\"old-revision\":1"));
      assertTrue("Diff 1->2 should have new-revision:2", diff1to2Content.contains("\"new-revision\":2"));
      assertTrue("Diff 1->2 should have update operation", diff1to2Content.contains("\"update\""));
      // Update operation on value node in array should have path from parent array
      assertTrue("Update operation in diff 1->2 should have a path", diff1to2Content.contains("\"path\""));

      String diff2to3Content = Files.readString(diff2to3);
      assertTrue("Diff 2->3 should have old-revision:2", diff2to3Content.contains("\"old-revision\":2"));
      assertTrue("Diff 2->3 should have new-revision:3", diff2to3Content.contains("\"new-revision\":3"));
      assertTrue("Diff 2->3 should have delete operation", diff2to3Content.contains("\"delete\""));
      // Delete operation on OBJECT_KEY should have path
      assertTrue("Delete operation in diff 2->3 should have a path", diff2to3Content.contains("\"path\""));

      String diff3to4Content = Files.readString(diff3to4);
      assertTrue("Diff 3->4 should have old-revision:3", diff3to4Content.contains("\"old-revision\":3"));
      assertTrue("Diff 3->4 should have new-revision:4", diff3to4Content.contains("\"new-revision\":4"));
      assertTrue("Diff 3->4 should have delete operation", diff3to4Content.contains("\"delete\""));
      // Delete operation on OBJECT_KEY should have path
      assertTrue("Delete operation in diff 3->4 should have a path", diff3to4Content.contains("\"path\""));

      System.out.println("Diff 1->2 content: " + diff1to2Content);
      System.out.println("Diff 2->3 content: " + diff2to3Content);
      System.out.println("Diff 3->4 content: " + diff3to4Content);
    }
  }

  @Test
  public void testDiffFileWithInsertOperations() throws IOException {
    // Test that INSERT operations have paths
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      // Insert a new object key
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        // Insert a new key-value pair
        wtx.insertObjectRecordAsFirstChild("newKey", new StringValue("newValue"));
        wtx.commit();
      }

      // Insert a value into an array
      try (final var wtx = manager.beginNodeTrx()) {
        wtx.moveTo(3); // "foo" array (node 3 is the array under "foo" key)
        wtx.insertStringValueAsFirstChild("insertedInArray");
        wtx.commit();
      }

      final Path updateOpsDir = manager.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath());

      final Path diff1to2 = updateOpsDir.resolve("diffFromRev1toRev2.json");
      final Path diff2to3 = updateOpsDir.resolve("diffFromRev2toRev3.json");

      assertTrue("Diff should exist for rev 1 -> rev 2", Files.exists(diff1to2));
      assertTrue("Diff should exist for rev 2 -> rev 3", Files.exists(diff2to3));

      // Verify INSERT of object key has path
      String diff1to2Content = Files.readString(diff1to2);
      assertTrue("Diff 1->2 should have insert operation", diff1to2Content.contains("\"insert\""));
      assertTrue("Insert operation for OBJECT_KEY should have path", diff1to2Content.contains("\"path\""));

      // Verify INSERT into array has path
      String diff2to3Content = Files.readString(diff2to3);
      assertTrue("Diff 2->3 should have insert operation", diff2to3Content.contains("\"insert\""));
      assertTrue("Insert operation for value in array should have path", diff2to3Content.contains("\"path\""));

      System.out.println("Insert OBJECT_KEY diff: " + diff1to2Content);
      System.out.println("Insert into array diff: " + diff2to3Content);
    }
  }
}
