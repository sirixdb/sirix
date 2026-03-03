package io.sirix.diff.algorithm;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for the JSON FMSE (Fast Match / Edit Script) algorithm. Verifies that the algorithm
 * correctly transforms old JSON revisions into new ones via update, insert, delete, and move
 * operations.
 */
public final class JsonFMSETest {

  private static final String RESOURCE = "shredded";

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testSameDocument() throws Exception {
    final String json = "{\"name\":\"Alice\",\"age\":30}";
    testDiff(json, json, json);
  }

  @Test
  public void testInsertObjectKey() throws Exception {
    testDiff(
        "{\"name\":\"Alice\"}",
        "{\"name\":\"Alice\",\"age\":30}",
        "{\"name\":\"Alice\",\"age\":30}");
  }

  @Test
  public void testInsertArrayElement() throws Exception {
    testDiff(
        "[1,2]",
        "[1,2,3]",
        "[1,2,3]");
  }

  @Test
  public void testDeleteObjectKey() throws Exception {
    testDiff(
        "{\"name\":\"Alice\",\"age\":30}",
        "{\"name\":\"Alice\"}",
        "{\"name\":\"Alice\"}");
  }

  @Test
  public void testDeleteArrayElement() throws Exception {
    testDiff(
        "[1,2,3]",
        "[1,3]",
        "[1,3]");
  }

  @Test
  public void testUpdateStringValue() throws Exception {
    testDiff(
        "{\"name\":\"Alice\"}",
        "{\"name\":\"Bob\"}",
        "{\"name\":\"Bob\"}");
  }

  @Test
  public void testUpdateNumberValue() throws Exception {
    testDiff(
        "{\"count\":10}",
        "{\"count\":42}",
        "{\"count\":42}");
  }

  @Test
  public void testUpdateBooleanValue() throws Exception {
    testDiff(
        "{\"active\":true}",
        "{\"active\":false}",
        "{\"active\":false}");
  }

  @Test
  public void testNestedObjects() throws Exception {
    testDiff(
        "{\"a\":{\"b\":{\"c\":1}}}",
        "{\"a\":{\"b\":{\"c\":2,\"d\":3}}}",
        "{\"a\":{\"b\":{\"c\":2,\"d\":3}}}");
  }

  @Test
  public void testEmptyContainers() throws Exception {
    testDiff("{}", "{}", "{}");
  }

  @Test
  public void testEmptyArrays() throws Exception {
    testDiff("[]", "[]", "[]");
  }

  @Test
  public void testArrayAllDeleted() throws Exception {
    testDiff(
        "[1,2,3]",
        "[]",
        "[]");
  }

  @Test
  public void testMixedOperations() throws Exception {
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":3}",
        "{\"a\":10,\"c\":3,\"d\":4}",
        "{\"a\":10,\"c\":3,\"d\":4}");
  }

  @Test
  public void testNullValues() throws Exception {
    testDiff(
        "{\"x\":null}",
        "{\"x\":null}",
        "{\"x\":null}");
  }

  @Test
  public void testValueTypeChange() throws Exception {
    testDiff(
        "{\"a\":\"hello\"}",
        "{\"a\":42}",
        "{\"a\":42}");
  }

  @Test
  public void testObjectToArrayInKey() throws Exception {
    testDiff(
        "{\"data\":{\"x\":1}}",
        "{\"data\":[1,2,3]}",
        "{\"data\":[1,2,3]}");
  }

  @Test
  public void testDeepNesting() throws Exception {
    testDiff(
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":\"deep\"}}}}",
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":\"deeper\",\"l5\":true}}}}",
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":\"deeper\",\"l5\":true}}}}");
  }

  @Test
  public void testArrayWithMixedTypes() throws Exception {
    testDiff(
        "[1,\"two\",true,null]",
        "[1,\"TWO\",false,null,5]",
        "[1,\"TWO\",false,null,5]");
  }

  // ==================== Move Operations ====================

  @Test
  public void testMoveArrayElement() throws Exception {
    // Reorder array elements: [1,2,3] → [3,1,2]
    testDiff(
        "[1,2,3]",
        "[3,1,2]",
        "[3,1,2]");
  }

  @Test
  public void testMoveObjectKeyBetweenObjects() throws Exception {
    // Move key "x" from object "a" to object "b"
    testDiff(
        "{\"a\":{\"x\":1},\"b\":{}}",
        "{\"a\":{},\"b\":{\"x\":1}}",
        "{\"a\":{},\"b\":{\"x\":1}}");
  }

  @Test
  public void testReverseArrayOrder() throws Exception {
    testDiff(
        "[1,2,3,4,5]",
        "[5,4,3,2,1]",
        "[5,4,3,2,1]");
  }

  // ==================== Insert Variants ====================

  @Test
  public void testInsertMultipleObjectKeys() throws Exception {
    testDiff(
        "{\"a\":1}",
        "{\"a\":1,\"b\":2,\"c\":3,\"d\":4}",
        "{\"a\":1,\"b\":2,\"c\":3,\"d\":4}");
  }

  @Test
  public void testInsertNestedObject() throws Exception {
    testDiff(
        "{\"root\":{}}",
        "{\"root\":{\"child\":{\"grandchild\":\"value\"}}}",
        "{\"root\":{\"child\":{\"grandchild\":\"value\"}}}");
  }

  @Test
  public void testInsertIntoEmptyArray() throws Exception {
    testDiff(
        "[]",
        "[1,2,3]",
        "[1,2,3]");
  }

  @Test
  public void testInsertNestedArrays() throws Exception {
    testDiff(
        "[[1]]",
        "[[1],[2,3]]",
        "[[1],[2,3]]");
  }

  // ==================== Delete Variants ====================

  @Test
  public void testDeleteAllObjectKeys() throws Exception {
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":3}",
        "{}",
        "{}");
  }

  @Test
  public void testDeleteNestedObject() throws Exception {
    testDiff(
        "{\"root\":{\"child\":{\"grandchild\":\"value\"}}}",
        "{\"root\":{}}",
        "{\"root\":{}}");
  }

  @Test
  public void testDeleteMiddleArrayElement() throws Exception {
    testDiff(
        "[1,2,3,4,5]",
        "[1,3,5]",
        "[1,3,5]");
  }

  // ==================== Update Variants ====================

  @Test
  public void testUpdateMultipleValues() throws Exception {
    testDiff(
        "{\"a\":1,\"b\":\"old\",\"c\":true}",
        "{\"a\":2,\"b\":\"new\",\"c\":false}",
        "{\"a\":2,\"b\":\"new\",\"c\":false}");
  }

  @Test
  public void testUpdateNullToValue() throws Exception {
    testDiff(
        "{\"x\":null}",
        "{\"x\":42}",
        "{\"x\":42}");
  }

  @Test
  public void testUpdateValueToNull() throws Exception {
    testDiff(
        "{\"x\":42}",
        "{\"x\":null}",
        "{\"x\":null}");
  }

  @Test
  public void testUpdateStringToBoolean() throws Exception {
    testDiff(
        "{\"val\":\"text\"}",
        "{\"val\":true}",
        "{\"val\":true}");
  }

  @Test
  public void testUpdateNumberToString() throws Exception {
    testDiff(
        "{\"val\":123}",
        "{\"val\":\"onetwothree\"}",
        "{\"val\":\"onetwothree\"}");
  }

  // ==================== Complex / Mixed ====================

  @Test
  public void testComplexMixedOperations() throws Exception {
    testDiff(
        "{\"users\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}],\"count\":2}",
        "{\"users\":[{\"name\":\"Alice\",\"age\":31},{\"name\":\"Charlie\",\"age\":28}],\"count\":2,\"active\":true}",
        "{\"users\":[{\"name\":\"Alice\",\"age\":31},{\"name\":\"Charlie\",\"age\":28}],\"count\":2,\"active\":true}");
  }

  @Test
  public void testObjectWithNestedArrayChange() throws Exception {
    testDiff(
        "{\"data\":{\"items\":[1,2,3],\"label\":\"old\"}}",
        "{\"data\":{\"items\":[1,4,3,5],\"label\":\"new\"}}",
        "{\"data\":{\"items\":[1,4,3,5],\"label\":\"new\"}}");
  }

  @Test
  public void testDeeplyNestedChange() throws Exception {
    testDiff(
        "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"old\"}}}}}",
        "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"new\",\"f\":true}}}}}",
        "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"new\",\"f\":true}}}}}");
  }

  // ==================== Edge Cases ====================

  @Test
  public void testSingleValueObject() throws Exception {
    testDiff(
        "{\"only\":\"one\"}",
        "{\"only\":\"two\"}",
        "{\"only\":\"two\"}");
  }

  @Test
  public void testSingleElementArray() throws Exception {
    testDiff(
        "[42]",
        "[99]",
        "[99]");
  }

  @Test
  public void testNullToNullSame() throws Exception {
    testDiff(
        "{\"a\":null,\"b\":null}",
        "{\"a\":null,\"b\":null}",
        "{\"a\":null,\"b\":null}");
  }

  @Test
  public void testBooleanValues() throws Exception {
    testDiff(
        "{\"t\":true,\"f\":false}",
        "{\"t\":false,\"f\":true}",
        "{\"t\":false,\"f\":true}");
  }

  @Test
  public void testEmptyObjectToPopulated() throws Exception {
    testDiff(
        "{}",
        "{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":null}",
        "{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":null}");
  }

  @Test
  public void testPopulatedToEmpty() throws Exception {
    testDiff(
        "{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":null}",
        "{}",
        "{}");
  }

  @Test
  public void testArrayOfObjects() throws Exception {
    testDiff(
        "[{\"id\":1},{\"id\":2},{\"id\":3}]",
        "[{\"id\":1},{\"id\":3}]",
        "[{\"id\":1},{\"id\":3}]");
  }

  @Test
  public void testArrayOfObjectsWithUpdates() throws Exception {
    testDiff(
        "[{\"id\":1,\"val\":\"a\"},{\"id\":2,\"val\":\"b\"}]",
        "[{\"id\":1,\"val\":\"x\"},{\"id\":2,\"val\":\"y\"}]",
        "[{\"id\":1,\"val\":\"x\"},{\"id\":2,\"val\":\"y\"}]");
  }

  @Test
  public void testNestedArraysUpdate() throws Exception {
    testDiff(
        "[[1,2],[3,4]]",
        "[[1,2],[3,5]]",
        "[[1,2],[3,5]]");
  }

  @Test
  public void testObjectKeyRename() throws Exception {
    // Key rename where names are similar enough (>0.7 Levenshtein)
    testDiff(
        "{\"old_name\":1}",
        "{\"new_name\":1}",
        "{\"new_name\":1}");
  }

  @Test
  public void testLargeNumberValues() throws Exception {
    testDiff(
        "{\"big\":999999999}",
        "{\"big\":1000000000}",
        "{\"big\":1000000000}");
  }

  @Test
  public void testDecimalNumberValues() throws Exception {
    testDiff(
        "{\"pi\":3.14}",
        "{\"pi\":3.14159}",
        "{\"pi\":3.14159}");
  }

  @Test
  public void testEmptyStringValue() throws Exception {
    testDiff(
        "{\"s\":\"\"}",
        "{\"s\":\"notempty\"}",
        "{\"s\":\"notempty\"}");
  }

  @Test
  public void testSpecialCharactersInStrings() throws Exception {
    testDiff(
        "{\"msg\":\"hello world\"}",
        "{\"msg\":\"hello\\nnewline\"}",
        "{\"msg\":\"hello\\nnewline\"}");
  }

  @Test
  public void testArrayInsertAtBeginning() throws Exception {
    testDiff(
        "[2,3,4]",
        "[1,2,3,4]",
        "[1,2,3,4]");
  }

  @Test
  public void testArrayInsertAtEnd() throws Exception {
    testDiff(
        "[1,2,3]",
        "[1,2,3,4]",
        "[1,2,3,4]");
  }

  @Test
  public void testArrayInsertInMiddle() throws Exception {
    testDiff(
        "[1,3,5]",
        "[1,2,3,4,5]",
        "[1,2,3,4,5]");
  }

  @Test
  public void testMultipleNullValues() throws Exception {
    testDiff(
        "[null,null,null]",
        "[null,null]",
        "[null,null]");
  }

  @Test
  public void testObjectWithArrayValues() throws Exception {
    testDiff(
        "{\"a\":[1,2],\"b\":[3,4]}",
        "{\"a\":[1,2,3],\"b\":[4]}",
        "{\"a\":[1,2,3],\"b\":[4]}");
  }

  @Test
  public void testReplaceContainerType() throws Exception {
    // Replace object with array under same key
    testDiff(
        "{\"data\":{\"inner\":1}}",
        "{\"data\":[1,2]}",
        "{\"data\":[1,2]}");
  }

  @Test
  public void testCompleteReplacement() throws Exception {
    // Complete document content change
    testDiff(
        "{\"x\":1}",
        "{\"y\":2}",
        "{\"y\":2}");
  }

  @Test
  public void testObjectKeysReordered() throws Exception {
    // Object keys are reordered (JSON objects are unordered, so this should work)
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":3}",
        "{\"c\":3,\"a\":1,\"b\":2}",
        "{\"c\":3,\"a\":1,\"b\":2}");
  }

  @Test
  public void testSameEmptyObject() throws Exception {
    testDiff("{}", "{}", "{}");
  }

  @Test
  public void testSameEmptyArray() throws Exception {
    testDiff("[]", "[]", "[]");
  }

  @Test
  public void testSameLargerDocument() throws Exception {
    final String json = "{\"users\":[{\"name\":\"Alice\",\"age\":30,\"active\":true},"
        + "{\"name\":\"Bob\",\"age\":25,\"active\":false}],"
        + "\"metadata\":{\"version\":1,\"count\":2}}";
    testDiff(json, json, json);
  }

  // ==================== Helper Methods ====================

  /**
   * Tests the FMSE diff by shredding the old JSON, then diffing with the new JSON, and verifying
   * the result matches the expected JSON.
   *
   * @param oldJson    the original JSON document
   * @param newJson    the target JSON document
   * @param expected   the expected result after applying the diff
   */
  private void testDiff(final String oldJson, final String newJson, final String expected)
      throws Exception {

    // 1. Shred old JSON into PATH1 database.
    final var dbConfOld = new DatabaseConfiguration(PATHS.PATH1.getFile());
    Databases.removeDatabase(PATHS.PATH1.getFile());
    Databases.createJsonDatabase(dbConfOld);

    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      dbOld.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgrOld = dbOld.beginResourceSession(RESOURCE);
          final var wtxOld = resMgrOld.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtxOld,
            JsonShredder.createStringReader(oldJson),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }

    // 2. Shred new JSON into PATH2 database.
    final var dbConfNew = new DatabaseConfiguration(PATHS.PATH2.getFile());
    Databases.removeDatabase(PATHS.PATH2.getFile());
    Databases.createJsonDatabase(dbConfNew);

    try (final var dbNew = Databases.openJsonDatabase(PATHS.PATH2.getFile())) {
      dbNew.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgrNew = dbNew.beginResourceSession(RESOURCE);
          final var wtxNew = resMgrNew.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtxNew,
            JsonShredder.createStringReader(newJson),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }

    // 3. Run FMSE diff: transform old into new.
    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var dbNew = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var resMgrOld = dbOld.beginResourceSession(RESOURCE);
        final var resMgrNew = dbNew.beginResourceSession(RESOURCE);
        final var wtx = resMgrOld.beginNodeTrx();
        final var rtx = resMgrNew.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // 4. Serialize the result and compare.
    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var resMgr = dbOld.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(resMgr, writer).build();
      serializer.call();
      JSONAssert.assertEquals(expected, writer.toString(), true);
    }
  }
}
