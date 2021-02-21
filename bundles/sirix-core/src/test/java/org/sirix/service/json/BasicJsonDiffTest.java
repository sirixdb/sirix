package org.sirix.service.json;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
import org.sirix.service.json.shredder.JsonShredder;

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
  public void test_whenMultipleRevisionsExist_thenDiff1() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(15);
      final var nodeKey = wtx.insertObjectRecordAsRightSibling("hereIAm", new StringValue("yeah")).getParentKey();
      wtx.commit();
      wtx.moveTo(nodeKey);
      wtx.insertObjectRecordAsRightSibling("111hereIAm", new StringValue("111yeah"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev2.json")), diffRev1Rev2);

      final String diffRev1Rev3 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 3);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("diffRev1Rev3.json")), diffRev1Rev3);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff2() throws IOException {
    JsonTestHelper.createTestDocument();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));
      wtx.moveTo(4);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"new\":\"stuff\"}"));

      final String diffRev1Rev4 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 4);
      System.out.println(diffRev1Rev4);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff3() throws IOException {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.insertArrayAsFirstChild();
      wtx.commit();
      wtx.insertObjectAsFirstChild();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("emptyObjectDiffRev1Rev2.json")),
                   diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff4() throws IOException {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"test\", \"test\"]"));
      wtx.moveTo(2);
      wtx.remove();
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff5() throws IOException {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"baz\":\"hello\"}"));
      wtx.moveTo(2);
      wtx.replaceObjectRecordValue(new StringValue("test"));
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 1, 2);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace1.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff6() throws IOException {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
                                                                           {"data":"data"}
                                                                        """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
                                                                            {"data":"data"}
                                                                        """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
                                                                            {"data":"data"}
                                                                        """.strip()));
      wtx.moveTo(2);
      wtx.remove();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 2, 5);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("deletion-at-eof.json")), diffRev1Rev2);
    }
  }

  @Test
  public void test_whenMultipleRevisionsExist_thenDiff7() throws IOException {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    assert database != null;
    final var databaseName = database.getName();
    try (final var manager = database.openResourceManager(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
                                                                            {"data":"data"}
                                                                        """.strip()));
      wtx.moveTo(1);
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
                                                                            {"data":"data"}
                                                                        """.strip()));
      wtx.moveTo(2);
      wtx.remove();
      wtx.commit();

      final String diffRev1Rev2 = new BasicJsonDiff(databaseName).generateDiff(manager, 2, 4);
      assertEquals(Files.readString(JSON.resolve("basicJsonDiffTest").resolve("replace2.json")), diffRev1Rev2);
    }
  }
}
