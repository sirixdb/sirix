package org.sirix.service.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.trx.node.json.objectvalue.StringValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public final class BasicJsonDiffTest {
  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(15);
      final var nodeKey = wtx.insertObjectRecordAsRightSibling("hereIAm", new StringValue("yeah")).getParentKey();
      wtx.commit();
      wtx.moveTo(nodeKey);
      wtx.insertObjectRecordAsRightSibling("111hereIAm", new StringValue("111yeah"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff().generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev2.json")), diffRev1Rev2);

      final String diffRev1Rev3 = new BasicJsonDiff().generateDiff(manager, 1, 3);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev3.json")), diffRev1Rev3);
    }
  }
}
