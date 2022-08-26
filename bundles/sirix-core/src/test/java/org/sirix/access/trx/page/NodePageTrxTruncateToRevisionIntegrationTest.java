package org.sirix.access.trx.page;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.trx.node.json.objectvalue.BooleanValue;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.utils.JsonDocumentCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Johannes Lichtenberger
 */
public final class NodePageTrxTruncateToRevisionIntegrationTest {

  private static final Path RESOURCE_DATA_FILE = JsonTestHelper.PATHS.PATH1.getFile()
                                                                           .resolve("resources")
                                                                           .resolve(JsonTestHelper.RESOURCE)
                                                                           .resolve("data")
                                                                           .resolve("sirix.data");

  private Database<JsonResourceManager> database;

  private JsonResourceManager resourceManager;

  private long fileSize;

  @BeforeEach
  public void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceManager = database.openResourceManager(JsonTestHelper.RESOURCE);

    try (final var wtx = resourceManager.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
      fileSize = Files.size(RESOURCE_DATA_FILE);
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("b", new StringValue("value"));
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertObjectRecordAsFirstChild("a", new BooleanValue(false));
      wtx.commit();
      assertTrue(Files.size(RESOURCE_DATA_FILE) > fileSize);
      assertEquals(4, wtx.getRevisionNumber());
    }
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test_when_sirix_is_setup_with_3_revisions_truncate_to_first_revision() throws IOException {
    try (final var pageWtx = resourceManager.beginPageTrx()) {
      pageWtx.truncateTo(1);
    }

    assertEquals(fileSize, Files.size(RESOURCE_DATA_FILE));
  }
}
