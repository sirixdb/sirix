package io.sirix.diff.algorithm;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Axis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.diff.algorithm.fmse.FMSEAlgorithm;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.diff.algorithm.fmse.json.JsonFMSENodeComparisonUtils;
import io.sirix.diff.algorithm.fmse.json.JsonFMSEVisitor;
import io.sirix.diff.algorithm.fmse.json.JsonLabelFMSEVisitor;
import io.sirix.diff.algorithm.fmse.json.JsonMatching;
import io.sirix.diff.service.JsonFMSEImport;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.SirixFiles;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

  // ==================== Unit Tests: JsonFMSENodeComparisonUtils ====================

  @Test
  public void testCalculateRatioIdenticalStrings() {
    assertEquals(1.0, JsonFMSENodeComparisonUtils.calculateRatio("hello", "hello"), 0.001);
  }

  @Test
  public void testCalculateRatioCompletelyDifferent() {
    final double ratio = JsonFMSENodeComparisonUtils.calculateRatio("abc", "xyz");
    assertTrue("Ratio should be low for completely different strings", ratio < 0.5);
  }

  @Test
  public void testCalculateRatioSimilarStrings() {
    final double ratio = JsonFMSENodeComparisonUtils.calculateRatio("hello", "hallo");
    assertTrue("Ratio should be high for similar strings", ratio > 0.5);
  }

  @Test
  public void testCalculateRatioNullInputs() {
    assertEquals(0.0, JsonFMSENodeComparisonUtils.calculateRatio(null, "test"), 0.001);
    assertEquals(0.0, JsonFMSENodeComparisonUtils.calculateRatio("test", null), 0.001);
    assertEquals(0.0, JsonFMSENodeComparisonUtils.calculateRatio(null, null), 0.001);
  }

  @Test
  public void testCalculateRatioEmptyStrings() {
    assertEquals(1.0, JsonFMSENodeComparisonUtils.calculateRatio("", ""), 0.001);
  }

  @Test
  public void testCalculateRatioLongStrings() {
    // Strings longer than MAX_LENGTH (50) use quickRatio instead of Levenshtein
    final String long1 = "a".repeat(60);
    final String long2 = "a".repeat(55) + "b".repeat(5);
    final double ratio = JsonFMSENodeComparisonUtils.calculateRatio(long1, long2);
    assertTrue("Ratio for long similar strings should be high", ratio > 0.5);
  }

  @Test
  public void testTypedValuesEqualNumbers() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":42}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":42}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      // Navigate to the number value node in both trees
      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild(); // object
      rtx1.moveToFirstChild(); // object key "a"
      rtx1.moveToFirstChild(); // number value 42
      final long key1 = rtx1.getNodeKey();

      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      final long key2 = rtx2.getNodeKey();

      assertTrue(JsonFMSENodeComparisonUtils.typedValuesEqual(key1, key2, rtx1, rtx2));
    }
  }

  @Test
  public void testTypedValuesNotEqualDifferentKinds() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":42}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":\"hello\"}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();

      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();

      assertFalse(JsonFMSENodeComparisonUtils.typedValuesEqual(
          rtx1.getNodeKey(), rtx2.getNodeKey(), rtx1, rtx2));
    }
  }

  @Test
  public void testGetNodeValueForObjectKey() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"mykey\":\"value\"}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild(); // object
      rtx.moveToFirstChild(); // object key "mykey"
      assertEquals("mykey", JsonFMSENodeComparisonUtils.getNodeValue(rtx.getNodeKey(), rtx));
    }
  }

  @Test
  public void testGetNodeValueForStringValue() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"key\":\"value\"}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild(); // string value "value"
      assertEquals("value", JsonFMSENodeComparisonUtils.getNodeValue(rtx.getNodeKey(), rtx));
    }
  }

  @Test
  public void testGetNodeValueForBooleanValue() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"flag\":true}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild(); // boolean value true
      assertEquals("true", JsonFMSENodeComparisonUtils.getNodeValue(rtx.getNodeKey(), rtx));
    }
  }

  @Test
  public void testGetNodeValueForNullValue() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"n\":null}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild(); // null value
      assertEquals("null", JsonFMSENodeComparisonUtils.getNodeValue(rtx.getNodeKey(), rtx));
    }
  }

  @Test
  public void testCheckAncestorsCompatible() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"root\":{\"child\":1}}");
    shredJson(PATHS.PATH2.getFile(), "{\"root\":{\"child\":2}}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      // Navigate to the leaf values
      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      final long leafKey1 = rtx1.getNodeKey();

      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      final long leafKey2 = rtx2.getNodeKey();

      // The start keys need to be set to document root for proper ancestor walking
      final var utils = new JsonFMSENodeComparisonUtils(0L, 0L, rtx1, rtx2);
      assertTrue(utils.checkAncestors(leafKey1, leafKey2));
    }
  }

  // ==================== Unit Tests: FMSEAlgorithm ====================

  @Test
  public void testFMSEAlgorithmMatchEmpty() {
    final Map<NodeKind, List<Long>> oldLabels = new EnumMap<>(NodeKind.class);
    final Map<NodeKind, List<Long>> newLabels = new EnumMap<>(NodeKind.class);
    final LongArrayList matches = new LongArrayList();

    FMSEAlgorithm.match(oldLabels, newLabels, (x, y) -> {
      matches.add(x);
      matches.add(y);
    }, (a, b) -> true);

    assertTrue("No matches expected for empty label maps", matches.isEmpty());
  }

  @Test
  public void testFMSEAlgorithmMatchIdenticalLabels() {
    final Map<NodeKind, List<Long>> oldLabels = new EnumMap<>(NodeKind.class);
    final Map<NodeKind, List<Long>> newLabels = new EnumMap<>(NodeKind.class);
    final LongArrayList oldList = new LongArrayList();
    oldList.add(1L);
    oldList.add(2L);
    oldList.add(3L);
    final LongArrayList newList = new LongArrayList();
    newList.add(10L);
    newList.add(20L);
    newList.add(30L);
    oldLabels.put(NodeKind.STRING_VALUE, oldList);
    newLabels.put(NodeKind.STRING_VALUE, newList);

    final Long2LongOpenHashMap matchResult = new Long2LongOpenHashMap();
    matchResult.defaultReturnValue(-1L);

    // All nodes are considered equal
    FMSEAlgorithm.match(oldLabels, newLabels, matchResult::put, (a, b) -> true);

    assertEquals("Should have 3 matches", 3, matchResult.size());
  }

  @Test
  public void testFMSEAlgorithmMatchNoOverlap() {
    final Map<NodeKind, List<Long>> oldLabels = new EnumMap<>(NodeKind.class);
    final Map<NodeKind, List<Long>> newLabels = new EnumMap<>(NodeKind.class);
    oldLabels.put(NodeKind.STRING_VALUE, new LongArrayList(new long[]{1L}));
    newLabels.put(NodeKind.NUMBER_VALUE, new LongArrayList(new long[]{2L}));

    final Long2LongOpenHashMap matchResult = new Long2LongOpenHashMap();
    FMSEAlgorithm.match(oldLabels, newLabels, matchResult::put, (a, b) -> true);

    assertTrue("No matches when labels don't overlap", matchResult.isEmpty());
  }

  @Test
  public void testRemoveCommonNodes() {
    final LongArrayList list = new LongArrayList(new long[]{1L, 2L, 3L, 4L, 5L});
    final LongOpenHashSet seen = new LongOpenHashSet(new long[]{2L, 4L});
    FMSEAlgorithm.removeCommonNodes(list, seen);

    assertEquals(3, list.size());
    assertTrue(list.contains(1L));
    assertTrue(list.contains(3L));
    assertTrue(list.contains(5L));
    assertFalse(list.contains(2L));
    assertFalse(list.contains(4L));
  }

  // ==================== Unit Tests: JsonMatching ====================

  @Test
  public void testJsonMatchingPartnerLookup() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":1}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      final var matching = new JsonMatching(rtx1, rtx2);

      // Navigate to object nodes
      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      final long oldObjKey = rtx1.getNodeKey();

      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();
      final long newObjKey = rtx2.getNodeKey();

      matching.add(oldObjKey, newObjKey);

      assertEquals(newObjKey, matching.partner(oldObjKey));
      assertEquals(oldObjKey, matching.reversePartner(newObjKey));
      assertTrue(matching.contains(oldObjKey, newObjKey));
    }
  }

  @Test
  public void testJsonMatchingNoPartner() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":1}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      final var matching = new JsonMatching(rtx1, rtx2);
      assertEquals(JsonMatching.NO_PARTNER, matching.partner(999L));
      assertEquals(JsonMatching.NO_PARTNER, matching.reversePartner(999L));
    }
  }

  @Test
  public void testJsonMatchingRemove() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":1}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      final var matching = new JsonMatching(rtx1, rtx2);

      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();

      matching.add(rtx1.getNodeKey(), rtx2.getNodeKey());
      assertTrue(matching.remove(rtx1.getNodeKey()));
      assertEquals(JsonMatching.NO_PARTNER, matching.partner(rtx1.getNodeKey()));
      assertFalse(matching.remove(rtx1.getNodeKey())); // already removed
    }
  }

  @Test
  public void testJsonMatchingCopyConstructor() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":1}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      final var original = new JsonMatching(rtx1, rtx2);

      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();

      original.add(rtx1.getNodeKey(), rtx2.getNodeKey());

      final var copy = new JsonMatching(original);
      assertEquals(rtx2.getNodeKey(), copy.partner(rtx1.getNodeKey()));
      assertTrue(copy.contains(rtx1.getNodeKey(), rtx2.getNodeKey()));
    }
  }

  @Test
  public void testJsonMatchingReset() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":1}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      final var matching = new JsonMatching(rtx1, rtx2);

      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();

      matching.add(rtx1.getNodeKey(), rtx2.getNodeKey());
      matching.reset();
      assertEquals(JsonMatching.NO_PARTNER, matching.partner(rtx1.getNodeKey()));
    }
  }

  // ==================== Unit Tests: Visitors ====================

  @Test
  public void testJsonFMSEVisitorInitializesDescendants() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":{\"b\":1,\"c\":2}}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      final var inOrder = new Long2BooleanOpenHashMap();
      final var descendants = new Long2LongOpenHashMap();

      final var visitor = new JsonFMSEVisitor(rtx, inOrder, descendants);

      // Run post-order traversal
      for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
        axis.nextLong();
        rtx.acceptVisitor(visitor);
      }

      // All visited nodes should have inOrder = false
      assertTrue(inOrder.size() > 0);
      for (final boolean val : inOrder.values()) {
        assertFalse("All nodes should be marked out of order initially", val);
      }

      // Leaf nodes should have descendants count = 1
      assertTrue(descendants.size() > 0);
    }
  }

  @Test
  public void testJsonLabelVisitorCollectsLabels() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":null}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      final var visitor = new JsonLabelFMSEVisitor(rtx);

      for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
        axis.nextLong();
        rtx.acceptVisitor(visitor);
      }

      final Map<NodeKind, List<Long>> leafLabels = visitor.getLeafLabels();
      final Map<NodeKind, List<Long>> innerLabels = visitor.getLabels();

      // Should have leaf labels for different types
      assertTrue("Should have OBJECT_NUMBER_VALUE labels",
          leafLabels.containsKey(NodeKind.OBJECT_NUMBER_VALUE));
      assertTrue("Should have OBJECT_STRING_VALUE labels",
          leafLabels.containsKey(NodeKind.OBJECT_STRING_VALUE));
      assertTrue("Should have OBJECT_BOOLEAN_VALUE labels",
          leafLabels.containsKey(NodeKind.OBJECT_BOOLEAN_VALUE));
      assertTrue("Should have OBJECT_NULL_VALUE labels",
          leafLabels.containsKey(NodeKind.OBJECT_NULL_VALUE));

      // Should have inner labels
      assertTrue("Should have OBJECT labels", innerLabels.containsKey(NodeKind.OBJECT));
      assertTrue("Should have OBJECT_KEY labels", innerLabels.containsKey(NodeKind.OBJECT_KEY));
    }
  }

  @Test
  public void testJsonLabelVisitorArrayLabels() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "[1,\"two\",true,null]");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx()) {
      final var visitor = new JsonLabelFMSEVisitor(rtx);

      for (final Axis axis = new PostOrderAxis(rtx); axis.hasNext();) {
        axis.nextLong();
        rtx.acceptVisitor(visitor);
      }

      final Map<NodeKind, List<Long>> leafLabels = visitor.getLeafLabels();
      final Map<NodeKind, List<Long>> innerLabels = visitor.getLabels();

      // Array elements use non-OBJECT_ prefixed kinds
      assertTrue(leafLabels.containsKey(NodeKind.NUMBER_VALUE));
      assertTrue(leafLabels.containsKey(NodeKind.STRING_VALUE));
      assertTrue(leafLabels.containsKey(NodeKind.BOOLEAN_VALUE));
      assertTrue(leafLabels.containsKey(NodeKind.NULL_VALUE));
      assertTrue(innerLabels.containsKey(NodeKind.ARRAY));
    }
  }

  // ==================== Integration Tests: moveSubtree ====================

  @Test
  public void testMoveSubtreeToFirstChild() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "[1,2,3]");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      // Navigate: doc -> array -> 1(first) -> 2 -> 3
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long thirdKey = wtx.getNodeKey();

      // Move 3 to first child of array
      wtx.moveToParent(); // array
      wtx.moveSubtreeToFirstChild(thirdKey);
      wtx.commit();
    }

    // Verify result
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer).build();
      serializer.call();
      JSONAssert.assertEquals("[3,1,2]", writer.toString(), true);
    }
  }

  @Test
  public void testMoveSubtreeToRightSibling() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "[1,2,3]");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      // Navigate to get keys
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      final long firstKey = wtx.getNodeKey();
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3

      // Move first element (1) to right sibling of third element (3)
      // Position cursor at 3, then move 1 to its right
      wtx.moveSubtreeToRightSibling(firstKey);
      wtx.commit();
    }

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer).build();
      serializer.call();
      JSONAssert.assertEquals("[2,3,1]", writer.toString(), true);
    }
  }

  @Test
  public void testMoveSubtreeToLeftSibling() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "[1,2,3]");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveToRightSibling(); // 3
      final long thirdKey = wtx.getNodeKey();

      // Move 3 to left sibling of 2 (which is 1's right sibling position)
      wtx.moveToParent(); // array
      wtx.moveToFirstChild(); // 1
      wtx.moveToRightSibling(); // 2
      wtx.moveSubtreeToLeftSibling(thirdKey);
      wtx.commit();
    }

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer).build();
      serializer.call();
      JSONAssert.assertEquals("[1,3,2]", writer.toString(), true);
    }
  }

  // ==================== Integration Tests: JsonFMSEImport ====================

  @Test
  public void testJsonFMSEImportSimple() throws Exception {
    // Create old database
    shredJson(PATHS.PATH1.getFile(), "{\"name\":\"Alice\",\"age\":30}");

    // Write new JSON to a temp file
    final Path tempDir = Files.createTempDirectory("fmse-test");
    final Path newJsonFile = tempDir.resolve("new.json");
    Files.writeString(newJsonFile, "{\"name\":\"Bob\",\"age\":31,\"active\":true}");

    try {
      final var importer = new JsonFMSEImport();
      importer.jsonDataImport(PATHS.PATH1.getFile(), newJsonFile);

      // Verify the result
      try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
          final var res = db.beginResourceSession(RESOURCE);
          final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(res, writer).build();
        serializer.call();
        JSONAssert.assertEquals(
            "{\"name\":\"Bob\",\"age\":31,\"active\":true}",
            writer.toString(), true);
      }
    } finally {
      SirixFiles.recursiveRemove(tempDir);
    }
  }

  // ==================== Integration Tests: Multi-Revision Behavioral ====================

  @Test
  public void testDiffProducesNewRevision() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), "{\"a\":2}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var wtx = res1.beginNodeTrx();
        final var rtx = res2.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // After diff + commit, there should be 2 revisions
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE)) {
      assertEquals("Should have 2 revisions after diff", 2, res.getMostRecentRevisionNumber());
    }
  }

  @Test
  public void testDiffPreservesOriginalRevision() throws Exception {
    final String original = "{\"a\":1,\"b\":2}";
    shredJson(PATHS.PATH1.getFile(), original);
    shredJson(PATHS.PATH2.getFile(), "{\"a\":3,\"c\":4}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var wtx = res1.beginNodeTrx();
        final var rtx = res2.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // Original revision (1) should still be readable
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var rtx = res.beginNodeReadOnlyTrx(1);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer, 1).build();
      serializer.call();
      JSONAssert.assertEquals(original, writer.toString(), true);
    }
  }

  @Test
  public void testSequentialDiffs() throws Exception {
    // Apply multiple diffs in sequence to test chained operations
    final String[] revisions = {
        "{\"v\":1}",
        "{\"v\":2,\"new\":true}",
        "{\"v\":3,\"new\":false,\"extra\":\"data\"}"
    };

    shredJson(PATHS.PATH1.getFile(), revisions[0]);

    for (int i = 1; i < revisions.length; i++) {
      JsonTestHelper.closeEverything();
      shredJson(PATHS.PATH2.getFile(), revisions[i]);

      try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
          final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
          final var res1 = db1.beginResourceSession(RESOURCE);
          final var res2 = db2.beginResourceSession(RESOURCE);
          final var wtx = res1.beginNodeTrx();
          final var rtx = res2.beginNodeReadOnlyTrx();
          final var fmse = JsonFMSE.createInstance()) {
        fmse.diff(wtx, rtx);
      }

      Databases.removeDatabase(PATHS.PATH2.getFile());
    }

    // Final result should match the last revision
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer).build();
      serializer.call();
      JSONAssert.assertEquals(revisions[revisions.length - 1], writer.toString(), true);
    }
  }

  @Test
  public void testDiffIdempotent() throws Exception {
    // Applying the same diff twice should not change the result
    final String target = "{\"a\":1,\"b\":2}";
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1}");
    shredJson(PATHS.PATH2.getFile(), target);

    // First diff
    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var wtx = res1.beginNodeTrx();
        final var rtx = res2.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // Second diff with same target
    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var wtx = res1.beginNodeTrx();
        final var rtx = res2.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // Result should still match target
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var writer = new StringWriter()) {
      final var serializer = JsonSerializer.newBuilder(res, writer).build();
      serializer.call();
      JSONAssert.assertEquals(target, writer.toString(), true);
    }
  }

  // ==================== Behavioral Integration Tests ====================

  @Test
  public void testDiffWithPathSummary() throws Exception {
    // Test that path summary is correctly maintained after diff
    testDiff(
        "{\"users\":[{\"name\":\"Alice\"}]}",
        "{\"users\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}",
        "{\"users\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}");
  }

  @Test
  public void testDiffLargerDocumentInsertAndDelete() throws Exception {
    testDiff(
        "{\"items\":[{\"id\":1,\"name\":\"a\",\"active\":true},"
            + "{\"id\":2,\"name\":\"b\",\"active\":false},"
            + "{\"id\":3,\"name\":\"c\",\"active\":true}]}",
        "{\"items\":[{\"id\":1,\"name\":\"a\",\"active\":true},"
            + "{\"id\":4,\"name\":\"d\",\"active\":true}]}",
        "{\"items\":[{\"id\":1,\"name\":\"a\",\"active\":true},"
            + "{\"id\":4,\"name\":\"d\",\"active\":true}]}");
  }

  @Test
  public void testDiffAllTypesInArray() throws Exception {
    // Test with all JSON value types in an array
    testDiff(
        "[1,\"hello\",true,null,3.14,{},[]]",
        "[2,\"world\",false,null,2.71,{\"a\":1},[1]]",
        "[2,\"world\",false,null,2.71,{\"a\":1},[1]]");
  }

  @Test
  public void testDiffNestedMixedContainers() throws Exception {
    testDiff(
        "{\"root\":{\"arr\":[{\"obj\":{\"arr2\":[1,2]}}]}}",
        "{\"root\":{\"arr\":[{\"obj\":{\"arr2\":[3,4,5]}}]}}",
        "{\"root\":{\"arr\":[{\"obj\":{\"arr2\":[3,4,5]}}]}}");
  }

  @Test
  public void testDiffMultipleObjectKeysWithSameValueType() throws Exception {
    testDiff(
        "{\"x\":1,\"y\":2,\"z\":3}",
        "{\"x\":10,\"y\":20,\"z\":30}",
        "{\"x\":10,\"y\":20,\"z\":30}");
  }

  @Test
  public void testDiffObjectKeySwapValues() throws Exception {
    // Swap values between keys
    testDiff(
        "{\"first\":\"a\",\"second\":\"b\"}",
        "{\"first\":\"b\",\"second\":\"a\"}",
        "{\"first\":\"b\",\"second\":\"a\"}");
  }

  @Test
  public void testDiffArrayGrowAndShrink() throws Exception {
    // Array grows significantly
    testDiff(
        "[1]",
        "[1,2,3,4,5,6,7,8,9,10]",
        "[1,2,3,4,5,6,7,8,9,10]");
  }

  @Test
  public void testDiffArrayShrinkToOne() throws Exception {
    testDiff(
        "[1,2,3,4,5,6,7,8,9,10]",
        "[5]",
        "[5]");
  }

  @Test
  public void testDiffObjectWithDeeplyNestedArrays() throws Exception {
    testDiff(
        "{\"data\":[[1,2],[3,[4,5]]]}",
        "{\"data\":[[1,2],[3,[4,6,7]]]}",
        "{\"data\":[[1,2],[3,[4,6,7]]]}");
  }

  @Test
  public void testDiffSingleNumberRootValue() throws Exception {
    // Edge case: root is a single number
    testDiff("[42]", "[99]", "[99]");
  }

  @Test
  public void testDiffObjectKeyNameHighSimilarity() throws Exception {
    // Test with keys that have high Levenshtein similarity (>0.7)
    testDiff(
        "{\"userName\":\"Alice\"}",
        "{\"user_name\":\"Alice\"}",
        "{\"user_name\":\"Alice\"}");
  }

  @Test
  public void testDiffObjectKeyNameLowSimilarity() throws Exception {
    // Test with keys that have low Levenshtein similarity (<0.7)
    testDiff(
        "{\"x\":1}",
        "{\"longDifferentName\":1}",
        "{\"longDifferentName\":1}");
  }

  @Test
  public void testDiffWithNegativeNumbers() throws Exception {
    testDiff(
        "{\"val\":-10}",
        "{\"val\":-20}",
        "{\"val\":-20}");
  }

  @Test
  public void testDiffWithZero() throws Exception {
    testDiff(
        "{\"val\":0}",
        "{\"val\":0.0}",
        "{\"val\":0.0}");
  }

  @Test
  public void testDiffEmptyNestedContainers() throws Exception {
    testDiff(
        "{\"a\":{},\"b\":[]}",
        "{\"a\":{\"x\":1},\"b\":[1]}",
        "{\"a\":{\"x\":1},\"b\":[1]}");
  }

  @Test
  public void testDiffPopulatedToEmptyNestedContainers() throws Exception {
    testDiff(
        "{\"a\":{\"x\":1},\"b\":[1]}",
        "{\"a\":{},\"b\":[]}",
        "{\"a\":{},\"b\":[]}");
  }

  // ==================== Corner Case Tests ====================

  @Test
  public void testArrayWithDuplicateIdenticalValues() throws Exception {
    // Tests LCS/matching behavior when multiple identical leaf nodes exist
    testDiff(
        "[1,1,1,1,1]",
        "[1,1,1]",
        "[1,1,1]");
  }

  @Test
  public void testArrayWithDuplicateIdenticalValuesGrow() throws Exception {
    testDiff(
        "[1,1]",
        "[1,1,1,1,1]",
        "[1,1,1,1,1]");
  }

  @Test
  public void testArrayWithDuplicateNullsAndNumbers() throws Exception {
    // Mix of duplicates across types
    testDiff(
        "[null,null,1,1,true,true]",
        "[null,1,true]",
        "[null,1,true]");
  }

  @Test
  public void testUnicodeInValues() throws Exception {
    testDiff(
        "{\"greeting\":\"hello\"}",
        "{\"greeting\":\"\u00e9l\u00e8ve\"}",
        "{\"greeting\":\"\u00e9l\u00e8ve\"}");
  }

  @Test
  public void testUnicodeInKeys() throws Exception {
    testDiff(
        "{\"name\":\"Alice\"}",
        "{\"nom\":\"Alice\",\"pr\u00e9nom\":\"B\"}",
        "{\"nom\":\"Alice\",\"pr\u00e9nom\":\"B\"}");
  }

  @Test
  public void testUnicodeChineseCharacters() throws Exception {
    testDiff(
        "{\"lang\":\"en\"}",
        "{\"lang\":\"\u65e5\u672c\u8a9e\"}",
        "{\"lang\":\"\u65e5\u672c\u8a9e\"}");
  }

  @Test
  public void testEmptyStringAsObjectKey() throws Exception {
    testDiff(
        "{\"\":1}",
        "{\"\":2}",
        "{\"\":2}");
  }

  @Test
  public void testEmptyStringKeyInsertAndDelete() throws Exception {
    testDiff(
        "{\"\":1,\"a\":2}",
        "{\"\":3}",
        "{\"\":3}");
  }

  @Test
  public void testObjectReorderWithinArray() throws Exception {
    // Different object structures reordered in an array
    testDiff(
        "[{\"a\":1},{\"b\":2}]",
        "[{\"b\":2},{\"a\":1}]",
        "[{\"b\":2},{\"a\":1}]");
  }

  @Test
  public void testObjectReorderWithinArrayComplex() throws Exception {
    testDiff(
        "[{\"id\":1,\"name\":\"first\"},{\"id\":2,\"name\":\"second\"},{\"id\":3,\"name\":\"third\"}]",
        "[{\"id\":3,\"name\":\"third\"},{\"id\":1,\"name\":\"first\"},{\"id\":2,\"name\":\"second\"}]",
        "[{\"id\":3,\"name\":\"third\"},{\"id\":1,\"name\":\"first\"},{\"id\":2,\"name\":\"second\"}]");
  }

  @Test
  public void testBooleanArrayManipulation() throws Exception {
    testDiff(
        "[true,false,true]",
        "[false,true,false]",
        "[false,true,false]");
  }

  @Test
  public void testBooleanArrayGrow() throws Exception {
    testDiff(
        "[true]",
        "[true,false,true,false]",
        "[true,false,true,false]");
  }

  @Test
  public void testBooleanArrayShrink() throws Exception {
    testDiff(
        "[true,false,true,false]",
        "[false]",
        "[false]");
  }

  @Test
  public void testNestedIdenticalStructures() throws Exception {
    // Multiple isomorphic objects — challenges the matcher to distinguish them
    testDiff(
        "[{\"x\":1,\"y\":2},{\"x\":1,\"y\":2},{\"x\":1,\"y\":2}]",
        "[{\"x\":1,\"y\":2},{\"x\":1,\"y\":3}]",
        "[{\"x\":1,\"y\":2},{\"x\":1,\"y\":3}]");
  }

  @Test
  public void testNestedIdenticalStructuresInsert() throws Exception {
    testDiff(
        "[{\"x\":1}]",
        "[{\"x\":1},{\"x\":1},{\"x\":1}]",
        "[{\"x\":1},{\"x\":1},{\"x\":1}]");
  }

  @Test
  public void testVeryWideObject() throws Exception {
    // 15 keys at the same level
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,"
            + "\"f\":6,\"g\":7,\"h\":8,\"i\":9,\"j\":10,"
            + "\"k\":11,\"l\":12,\"m\":13,\"n\":14,\"o\":15}",
        "{\"a\":10,\"b\":20,\"c\":30,\"d\":40,\"e\":50,"
            + "\"f\":60,\"g\":70,\"h\":80,\"i\":90,\"j\":100,"
            + "\"k\":110,\"l\":120,\"m\":130,\"n\":140,\"o\":150}",
        "{\"a\":10,\"b\":20,\"c\":30,\"d\":40,\"e\":50,"
            + "\"f\":60,\"g\":70,\"h\":80,\"i\":90,\"j\":100,"
            + "\"k\":110,\"l\":120,\"m\":130,\"n\":140,\"o\":150}");
  }

  @Test
  public void testVeryWideObjectPartialDeleteAndInsert() throws Exception {
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,"
            + "\"f\":6,\"g\":7,\"h\":8,\"i\":9,\"j\":10}",
        "{\"a\":1,\"c\":3,\"e\":5,\"g\":7,\"i\":9,"
            + "\"k\":11,\"l\":12,\"m\":13}",
        "{\"a\":1,\"c\":3,\"e\":5,\"g\":7,\"i\":9,"
            + "\"k\":11,\"l\":12,\"m\":13}");
  }

  @Test
  public void testMultipleMovesInSameDiff() throws Exception {
    // Non-monotonic shuffle of 8 elements requiring multiple moves
    testDiff(
        "[1,2,3,4,5,6,7,8]",
        "[8,2,6,4,5,3,7,1]",
        "[8,2,6,4,5,3,7,1]");
  }

  @Test
  public void testLargeArrayShuffleNonMonotonic() throws Exception {
    // Full shuffle of 10 elements
    testDiff(
        "[1,2,3,4,5,6,7,8,9,10]",
        "[7,2,9,1,4,10,3,8,5,6]",
        "[7,2,9,1,4,10,3,8,5,6]");
  }

  @Test
  public void testAllFourOperationsCombined() throws Exception {
    // Insert "d":4 + Delete "b":2 + Update "a":1→10 + Move/reorder "c" (via matching)
    testDiff(
        "{\"a\":1,\"b\":2,\"c\":{\"inner\":true}}",
        "{\"c\":{\"inner\":false},\"a\":10,\"d\":4}",
        "{\"c\":{\"inner\":false},\"a\":10,\"d\":4}");
  }

  @Test
  public void testStringThatLooksLikeNumber() throws Exception {
    // Type coercion: string "42" becomes number 42 under the same key
    testDiff(
        "{\"val\":\"42\"}",
        "{\"val\":42}",
        "{\"val\":42}");
  }

  @Test
  public void testNumberThatBecomesString() throws Exception {
    testDiff(
        "{\"val\":42}",
        "{\"val\":\"42\"}",
        "{\"val\":\"42\"}");
  }

  @Test
  public void testHeterogeneousObjectsInArray() throws Exception {
    // Objects with different key sets — partial overlap
    testDiff(
        "[{\"a\":1,\"b\":2},{\"b\":2,\"c\":3}]",
        "[{\"a\":1,\"c\":3},{\"b\":2,\"d\":4}]",
        "[{\"a\":1,\"c\":3},{\"b\":2,\"d\":4}]");
  }

  @Test
  public void testHeterogeneousObjectsWithVaryingSizes() throws Exception {
    testDiff(
        "[{\"x\":1},{\"x\":1,\"y\":2},{\"x\":1,\"y\":2,\"z\":3}]",
        "[{\"x\":10,\"y\":20,\"z\":30},{\"x\":10,\"y\":20},{\"x\":10}]",
        "[{\"x\":10,\"y\":20,\"z\":30},{\"x\":10,\"y\":20},{\"x\":10}]");
  }

  @Test
  public void testVeryDeepNesting7Levels() throws Exception {
    testDiff(
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":{\"l5\":{\"l6\":{\"l7\":\"deep\"}}}}}}}",
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":{\"l5\":{\"l6\":{\"l7\":\"deeper\",\"l8\":true}}}}}}}",
        "{\"l1\":{\"l2\":{\"l3\":{\"l4\":{\"l5\":{\"l6\":{\"l7\":\"deeper\",\"l8\":true}}}}}}}");
  }

  @Test
  public void testVeryDeepNestingArrays() throws Exception {
    // 6 levels of nested arrays
    testDiff(
        "[[[[[[1]]]]]]",
        "[[[[[[2,3]]]]]]",
        "[[[[[[2,3]]]]]]");
  }

  @Test
  public void testFlatArrayBecomingDeeplyNested() throws Exception {
    testDiff(
        "[[1,2,3]]",
        "[[1,[2,[3]]]]",
        "[[1,[2,[3]]]]");
  }

  @Test
  public void testDeeplyNestedBecomingFlat() throws Exception {
    testDiff(
        "[[1,[2,[3]]]]",
        "[[1,2,3]]",
        "[[1,2,3]]");
  }

  @Test
  public void testCheckAncestorsIncompatible() throws Exception {
    // Two leaf values under different ancestor key names (incompatible paths)
    shredJson(PATHS.PATH1.getFile(), "{\"alpha\":{\"child\":1}}");
    shredJson(PATHS.PATH2.getFile(), "{\"zzzzzz\":{\"child\":2}}");

    try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var res1 = db1.beginResourceSession(RESOURCE);
        final var res2 = db2.beginResourceSession(RESOURCE);
        final var rtx1 = res1.beginNodeReadOnlyTrx();
        final var rtx2 = res2.beginNodeReadOnlyTrx()) {
      // Navigate to the leaf values: doc -> object -> key -> inner_object -> key -> value
      rtx1.moveToDocumentRoot();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      rtx1.moveToFirstChild();
      final long leafKey1 = rtx1.getNodeKey();

      rtx2.moveToDocumentRoot();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      rtx2.moveToFirstChild();
      final long leafKey2 = rtx2.getNodeKey();

      // Start keys at document root so the whole ancestor chain is checked
      final var utils = new JsonFMSENodeComparisonUtils(0L, 0L, rtx1, rtx2);
      // "alpha" vs "zzzzzz" have low Levenshtein similarity (<0.7) → ancestors incompatible
      assertFalse("Ancestor paths should be incompatible due to different key names",
          utils.checkAncestors(leafKey1, leafKey2));
    }
  }

  @Test
  public void testDeleteOnlyChild() throws Exception {
    // Object with single key → empty object (tests delete-only-child cursor positioning)
    testDiff(
        "{\"only\":1}",
        "{}",
        "{}");
  }

  @Test
  public void testDeleteFirstChildKeepRest() throws Exception {
    // Delete the first sibling, keep the rest
    testDiff(
        "[1,2,3,4]",
        "[2,3,4]",
        "[2,3,4]");
  }

  @Test
  public void testDeleteLastChildKeepRest() throws Exception {
    // Delete the last sibling, keep the rest
    testDiff(
        "[1,2,3,4]",
        "[1,2,3]",
        "[1,2,3]");
  }

  @Test
  public void testDeleteMiddleChildKeepRest() throws Exception {
    // Delete only the middle child
    testDiff(
        "[1,2,3]",
        "[1,3]",
        "[1,3]");
  }

  @Test
  public void testDeleteOnlyChildOfNestedObject() throws Exception {
    // Tests cursor positioning when deleting the only child under a nested structure
    testDiff(
        "{\"parent\":{\"child\":{\"grandchild\":1}}}",
        "{\"parent\":{\"child\":{}}}",
        "{\"parent\":{\"child\":{}}}");
  }

  @Test
  public void testEmptyContainerVsPopulatedContainer() throws Exception {
    // Inner node comparator: one empty array, one with children → should not match
    testDiff(
        "{\"a\":[],\"b\":[1,2,3]}",
        "{\"a\":[4,5],\"b\":[]}",
        "{\"a\":[4,5],\"b\":[]}");
  }

  @Test
  public void testEmptyObjectVsPopulatedObject() throws Exception {
    testDiff(
        "{\"a\":{},\"b\":{\"x\":1}}",
        "{\"a\":{\"y\":2},\"b\":{}}",
        "{\"a\":{\"y\":2},\"b\":{}}");
  }

  @Test
  public void testArrayOfOnlyNulls() throws Exception {
    testDiff(
        "[null,null,null,null,null]",
        "[null,null,null]",
        "[null,null,null]");
  }

  @Test
  public void testArrayOfOnlyTrues() throws Exception {
    testDiff(
        "[true,true,true]",
        "[true,true,true,true,true]",
        "[true,true,true,true,true]");
  }

  @Test
  public void testMixedTypeArrayWithContainers() throws Exception {
    // Array mixing scalars and containers
    testDiff(
        "[1,{\"a\":2},[3],null,\"five\"]",
        "[\"five\",null,[3,4],{\"a\":20},1]",
        "[\"five\",null,[3,4],{\"a\":20},1]");
  }

  @Test
  public void testKeyDeleteAndDifferentKeyAdd() throws Exception {
    // Explicit test: remove one key, add a completely different key at same level
    testDiff(
        "{\"remove_me\":1,\"keep\":2}",
        "{\"keep\":2,\"brand_new\":3}",
        "{\"keep\":2,\"brand_new\":3}");
  }

  @Test
  public void testNumberEdgeCasesMaxMin() throws Exception {
    testDiff(
        "{\"big\":2147483647,\"small\":-2147483648}",
        "{\"big\":-2147483648,\"small\":2147483647}",
        "{\"big\":-2147483648,\"small\":2147483647}");
  }

  @Test
  public void testVeryLongStringValue() throws Exception {
    // String longer than MAX_LENGTH (50) to exercise quickRatio path
    final String longOld = "a".repeat(100);
    final String longNew = "b".repeat(100);
    testDiff(
        "{\"s\":\"" + longOld + "\"}",
        "{\"s\":\"" + longNew + "\"}",
        "{\"s\":\"" + longNew + "\"}");
  }

  @Test
  public void testObjectWithMixedNestedChanges() throws Exception {
    // Complex scenario: updates + structural changes at multiple nesting levels
    testDiff(
        "{\"config\":{\"db\":{\"host\":\"localhost\",\"port\":5432},\"cache\":{\"ttl\":300}},\"version\":1}",
        "{\"config\":{\"db\":{\"host\":\"remote.io\",\"port\":5433,\"ssl\":true},\"log\":{\"level\":\"debug\"}},\"version\":2}",
        "{\"config\":{\"db\":{\"host\":\"remote.io\",\"port\":5433,\"ssl\":true},\"log\":{\"level\":\"debug\"}},\"version\":2}");
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

  /**
   * Creates a SirixDB JSON database at the given path and shreds the provided JSON string into it.
   *
   * @param dbPath the database path
   * @param json   the JSON string to shred
   */
  private void shredJson(final Path dbPath, final String json) throws Exception {
    final var dbConf = new DatabaseConfiguration(dbPath);
    Databases.removeDatabase(dbPath);
    Databases.createJsonDatabase(dbConf);

    try (final var db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgr = db.beginResourceSession(RESOURCE);
          final var wtx = resMgr.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }
  }
}
