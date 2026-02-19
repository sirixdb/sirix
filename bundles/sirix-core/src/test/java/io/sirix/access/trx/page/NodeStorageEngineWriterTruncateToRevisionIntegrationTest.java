package io.sirix.access.trx.page;

import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.utils.JsonDocumentCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Johannes Lichtenberger
 */
public final class NodeStorageEngineWriterTruncateToRevisionIntegrationTest {

  private static final Path RESOURCE_DATA_FILE = JsonTestHelper.PATHS.PATH1.getFile()
                                                                           .resolve("resources")
                                                                           .resolve(JsonTestHelper.RESOURCE)
                                                                           .resolve("data")
                                                                           .resolve("sirix.data");

  private Database<JsonResourceSession> database;

  private JsonResourceSession resourceSession;

  private long fileSize;

  @BeforeEach
  public void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);

    try (final var wtx = resourceSession.beginNodeTrx()) {
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
    try (final var storageEngineWriter = resourceSession.beginStorageEngineWriter()) {
      storageEngineWriter.truncateTo(1);
    }

    assertEquals(fileSize, Files.size(RESOURCE_DATA_FILE));
  }
}
