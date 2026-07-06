package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.serialize.JsonSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for {@code AbstractNodeTrxImpl#rollback()} (issue #1060).
 * <p>
 * Before the fix, rollback did not re-instantiate the node-hashing writer (the first mutation
 * after a rollback on a hash-enabled resource hit a closed page transaction) and did not clear
 * the recorded update operations (a later commit serialized phantom diff tuples for the
 * rolled-back changes).
 */
public final class RollbackStateTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testInsertAndCommitAfterRollbackWithDefaultHashType() {
    // Default resource configuration => HashType.ROLLING.
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.rollback();

      // Pre-fix this threw from the stale nodeHashing writer (assertNotClosed on the closed
      // page transaction).
      wtx.moveToDocumentRoot();
      wtx.insertObjectAsFirstChild();
      wtx.commit();

      assertEquals(1, session.getMostRecentRevisionNumber());

      // Verify the committed content survived (the rolled-back array must be gone).
      final var writer = new StringWriter();
      JsonSerializer.newBuilder(session, writer).build().call();
      assertEquals("{}", writer.toString());
    }
  }

  @Test
  public void testRolledBackUpdateOperationsDoNotLeakIntoNextDiff() throws IOException {
    // Creates revision 1 with the standard test document (Dewey IDs enabled, diffs stored).
    JsonTestHelper.createTestDocumentWithDeweyIdsEnabled();

    try (final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {

      // Navigate to the "bar" string value inside the "foo" array.
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild()); // top-level object
      assertTrue(wtx.moveToFirstChild()); // "foo" fused array
      final long arrayKey = wtx.getNodeKey();
      assertTrue(wtx.moveToFirstChild()); // "bar" string value

      // Record an update op, then abandon it.
      wtx.setStringValue("phantom-rolled-back");
      wtx.rollback();

      // Perform a DIFFERENT change (an insert, so the only legitimate diff op is "insert").
      assertTrue(wtx.moveTo(arrayKey));
      wtx.insertStringValueAsFirstChild("committed-insert");
      wtx.commit();

      final Path diffFile = manager.getResourceConfig()
                                   .getResource()
                                   .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                   .resolve("diffFromRev1toRev2.json");

      assertTrue("Diff file should exist for second commit: " + diffFile, Files.exists(diffFile));

      final String diffContent = Files.readString(diffFile);
      assertTrue("Diff must contain the committed insert, but was: " + diffContent,
          diffContent.contains("\"insert\""));
      assertFalse("Rolled-back update op must not leak into the diff, but was: " + diffContent,
          diffContent.contains("\"update\""));
      assertFalse("Rolled-back value must not appear in the diff, but was: " + diffContent,
          diffContent.contains("phantom-rolled-back"));
    }
  }
}
