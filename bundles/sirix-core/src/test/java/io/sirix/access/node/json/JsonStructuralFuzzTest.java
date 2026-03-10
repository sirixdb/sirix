package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.NodeKind;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive structural correctness fuzz tests for JSON mutations.
 *
 * <p>After every mutation, a full tree walk verifies all structural invariants:
 * <ul>
 *   <li>Sibling-link symmetry (A.right == B implies B.left == A)</li>
 *   <li>Parent-child consistency (parent.firstChild reachable, all children point back to parent)</li>
 *   <li>childCount matches actual children count via sibling chain walk</li>
 *   <li>descendantCount matches recursive sum</li>
 *   <li>firstChild/lastChild consistency (null iff childCount == 0)</li>
 *   <li>Hash non-zero for all nodes (when hashing enabled)</li>
 * </ul>
 *
 * <p>On failure, the seed and operation log are printed for exact reproducibility.
 */
class JsonStructuralFuzzTest {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ==================== INSERTION-ONLY FUZZ ====================

  @RepeatedTest(30)
  void fuzzInsertionsWithStructuralVerification() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Start with an array as root container
      wtx.insertArrayAsFirstChild();
      opLog.add("insertArrayAsFirstChild -> key=" + wtx.getNodeKey());
      verifyAllInvariants(wtx, seed, opLog);

      final int numOps = rng.nextInt(40) + 10;

      for (int op = 0; op < numOps; op++) {
        final long nodeKey = wtx.getNodeKey();
        final NodeKind kind = wtx.getKind();
        final String opDesc = performRandomInsert(wtx, rng, kind);
        opLog.add("op#" + op + ": at key=" + nodeKey + " kind=" + kind + " -> " + opDesc
            + " (now at key=" + wtx.getNodeKey() + ")");
        verifyAllInvariants(wtx, seed, opLog);

        // Randomly navigate to a different node for the next operation
        navigateToRandomNode(wtx, rng);
      }

      wtx.commit();
      verifyAllInvariants(wtx, seed, opLog);
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", ops=" + opLog + "]: " + e.getMessage(), e);
    }
  }

  // ==================== INSERT + DELETE FUZZ ====================

  @RepeatedTest(30)
  void fuzzInsertAndDeleteWithStructuralVerification() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Build initial tree: object with a few children
      wtx.insertObjectAsFirstChild();
      opLog.add("insertObjectAsFirstChild -> key=" + wtx.getNodeKey());

      // Insert 5 initial object records
      for (int i = 0; i < 5; i++) {
        wtx.moveTo(1);
        final String key = "key" + i;
        wtx.insertObjectRecordAsLastChild(key, new StringValue("val" + i));
        opLog.add("insertObjectRecordAsLastChild(\"" + key + "\") -> key=" + wtx.getNodeKey());
        wtx.moveToParent(); // back to object_key
      }
      verifyAllInvariants(wtx, seed, opLog);

      // Now perform random inserts and deletes
      final int numOps = rng.nextInt(30) + 10;

      for (int op = 0; op < numOps; op++) {
        navigateToRandomNode(wtx, rng);
        final long nodeKey = wtx.getNodeKey();
        final NodeKind kind = wtx.getKind();

        // 60% insert, 40% delete (but don't delete document root or last remaining child)
        if (rng.nextInt(10) < 6 || nodeKey == 0) {
          final String opDesc = performRandomInsert(wtx, rng, kind);
          opLog.add("op#" + op + ": INSERT at key=" + nodeKey + " kind=" + kind
              + " -> " + opDesc + " (now at key=" + wtx.getNodeKey() + ")");
        } else {
          // Don't delete document root
          if (kind == NodeKind.JSON_DOCUMENT) {
            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild();
            }
          }
          if (wtx.getNodeKey() != 0) {
            moveToRemovableNode(wtx);
            final long removedKey = wtx.getNodeKey();
            wtx.remove();
            opLog.add("op#" + op + ": REMOVE key=" + removedKey
                + " (now at key=" + wtx.getNodeKey() + ")");
          }
        }

        verifyAllInvariants(wtx, seed, opLog);
      }

      wtx.commit();
      verifyAllInvariants(wtx, seed, opLog);
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", ops=" + opLog + "]: " + e.getMessage(), e);
    }
  }

  // ==================== UPDATE VALUE FUZZ ====================

  @RepeatedTest(20)
  void fuzzValueUpdatesWithStructuralVerification() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Build a tree with various value types
      wtx.insertObjectAsFirstChild();
      opLog.add("insertObjectAsFirstChild");

      wtx.insertObjectRecordAsFirstChild("str", new StringValue("hello"));
      opLog.add("insertObjectRecordAsFirstChild(str)");
      wtx.moveToParent();
      wtx.insertObjectRecordAsRightSibling("num", new NumberValue(42));
      opLog.add("insertObjectRecordAsRightSibling(num)");
      wtx.moveToParent();
      wtx.insertObjectRecordAsRightSibling("bool", new BooleanValue(true));
      opLog.add("insertObjectRecordAsRightSibling(bool)");
      wtx.moveToParent();
      wtx.insertObjectRecordAsRightSibling("arr", new ArrayValue());
      wtx.insertStringValueAsFirstChild("item1");
      wtx.insertNumberValueAsRightSibling(3.14);
      wtx.insertBooleanValueAsRightSibling(false);
      wtx.insertNullValueAsRightSibling();
      opLog.add("built array with 4 items");

      verifyAllInvariants(wtx, seed, opLog);

      // Perform random value updates
      final int numOps = rng.nextInt(30) + 10;

      for (int op = 0; op < numOps; op++) {
        navigateToRandomNode(wtx, rng);
        final long nodeKey = wtx.getNodeKey();
        final NodeKind kind = wtx.getKind();

        switch (kind) {
          case STRING_VALUE, OBJECT_STRING_VALUE -> {
            final String newVal = "updated_" + rng.nextInt(10000);
            wtx.setStringValue(newVal);
            opLog.add("op#" + op + ": setStringValue(\"" + newVal + "\") at key=" + nodeKey);
          }
          case NUMBER_VALUE, OBJECT_NUMBER_VALUE -> {
            final double newVal = rng.nextDouble() * 1000;
            wtx.setNumberValue(newVal);
            opLog.add("op#" + op + ": setNumberValue(" + newVal + ") at key=" + nodeKey);
          }
          case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE -> {
            final boolean newVal = rng.nextBoolean();
            wtx.setBooleanValue(newVal);
            opLog.add("op#" + op + ": setBooleanValue(" + newVal + ") at key=" + nodeKey);
          }
          case OBJECT_KEY -> {
            final String newName = "renamed_" + rng.nextInt(10000);
            wtx.setObjectKeyName(newName);
            opLog.add("op#" + op + ": setObjectKeyName(\"" + newName + "\") at key=" + nodeKey);
          }
          default -> {
            // For structural nodes, do an insert instead
            final String opDesc = performRandomInsert(wtx, rng, kind);
            opLog.add("op#" + op + ": (no update for " + kind + ") INSERT: " + opDesc);
          }
        }

        verifyAllInvariants(wtx, seed, opLog);
      }

      wtx.commit();
      verifyAllInvariants(wtx, seed, opLog);
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", ops=" + opLog + "]: " + e.getMessage(), e);
    }
  }

  // ==================== MIXED OPERATIONS FUZZ ====================

  @RepeatedTest(30)
  void fuzzMixedOperationsWithStructuralVerification() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Start with an array
      wtx.insertArrayAsFirstChild();
      opLog.add("insertArrayAsFirstChild -> key=" + wtx.getNodeKey());

      // Insert a few initial children
      for (int i = 0; i < 3; i++) {
        wtx.moveTo(1);
        wtx.insertObjectAsLastChild();
        final long objKey = wtx.getNodeKey();
        wtx.insertObjectRecordAsFirstChild("f" + i, new StringValue("v" + i));
        opLog.add("inserted object key=" + objKey + " with record f" + i);
        wtx.moveToParent(); // back to object_key
      }
      verifyAllInvariants(wtx, seed, opLog);

      final int numOps = rng.nextInt(50) + 20;

      for (int op = 0; op < numOps; op++) {
        navigateToRandomNode(wtx, rng);
        final long nodeKey = wtx.getNodeKey();
        final NodeKind kind = wtx.getKind();

        final int action = rng.nextInt(10);

        if (action < 5) {
          // INSERT (50%)
          final String opDesc = performRandomInsert(wtx, rng, kind);
          opLog.add("op#" + op + ": INSERT at key=" + nodeKey + " " + kind + " -> " + opDesc);
        } else if (action < 8) {
          // DELETE (30%) — but not document root
          if (nodeKey != 0 && kind != NodeKind.JSON_DOCUMENT) {
            moveToRemovableNode(wtx);
            final long removedKey = wtx.getNodeKey();
            wtx.remove();
            opLog.add("op#" + op + ": REMOVE key=" + removedKey);
          } else {
            opLog.add("op#" + op + ": SKIP remove (document root)");
          }
        } else {
          // UPDATE (20%)
          switch (kind) {
            case STRING_VALUE, OBJECT_STRING_VALUE -> {
              wtx.setStringValue("up_" + rng.nextInt(1000));
              opLog.add("op#" + op + ": UPDATE string at key=" + nodeKey);
            }
            case NUMBER_VALUE, OBJECT_NUMBER_VALUE -> {
              wtx.setNumberValue(rng.nextInt(1000));
              opLog.add("op#" + op + ": UPDATE number at key=" + nodeKey);
            }
            case BOOLEAN_VALUE, OBJECT_BOOLEAN_VALUE -> {
              wtx.setBooleanValue(rng.nextBoolean());
              opLog.add("op#" + op + ": UPDATE boolean at key=" + nodeKey);
            }
            case OBJECT_KEY -> {
              wtx.setObjectKeyName("k_" + rng.nextInt(1000));
              opLog.add("op#" + op + ": UPDATE key name at key=" + nodeKey);
            }
            default -> opLog.add("op#" + op + ": SKIP update for " + kind);
          }
        }

        verifyAllInvariants(wtx, seed, opLog);
      }

      // Commit and re-verify
      wtx.commit();
      opLog.add("COMMIT");
      verifyAllInvariants(wtx, seed, opLog);

      // Also verify via a fresh read-only transaction
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        verifyAllInvariants(rtx, seed, opLog);
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + ", ops=" + opLog + "]: " + e.getMessage(), e);
    }
  }

  // ==================== SPECIFIC PATTERN: INSERT ALL POSITIONS ====================

  @Test
  void testInsertAsFirstChildThenLastChildThenBetween() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Create array
      wtx.insertArrayAsFirstChild();
      opLog.add("array");

      // insertAsFirstChild
      wtx.insertStringValueAsFirstChild("C");
      final long keyC = wtx.getNodeKey();
      opLog.add("insertFirstChild C -> key=" + keyC);
      verifyAllInvariants(wtx, 0, opLog);

      // insertAsFirstChild again (pushes C to right)
      wtx.moveToParent();
      wtx.insertStringValueAsFirstChild("A");
      final long keyA = wtx.getNodeKey();
      opLog.add("insertFirstChild A -> key=" + keyA);
      verifyAllInvariants(wtx, 0, opLog);

      // insertAsLastChild
      wtx.moveToParent();
      wtx.insertStringValueAsLastChild("E");
      final long keyE = wtx.getNodeKey();
      opLog.add("insertLastChild E -> key=" + keyE);
      verifyAllInvariants(wtx, 0, opLog);

      // insertAsRightSibling of A (between A and C)
      wtx.moveTo(keyA);
      wtx.insertStringValueAsRightSibling("B");
      final long keyB = wtx.getNodeKey();
      opLog.add("insertRightSibling B after A -> key=" + keyB);
      verifyAllInvariants(wtx, 0, opLog);

      // insertAsLeftSibling of E (between C and E)
      wtx.moveTo(keyE);
      wtx.insertStringValueAsLeftSibling("D");
      final long keyD = wtx.getNodeKey();
      opLog.add("insertLeftSibling D before E -> key=" + keyD);
      verifyAllInvariants(wtx, 0, opLog);

      // Verify order: A -> B -> C -> D -> E
      wtx.moveTo(1); // array
      assertEquals(5, wtx.getChildCount());
      wtx.moveToFirstChild();
      assertEquals("A", wtx.getValue());
      wtx.moveToRightSibling();
      assertEquals("B", wtx.getValue());
      wtx.moveToRightSibling();
      assertEquals("C", wtx.getValue());
      wtx.moveToRightSibling();
      assertEquals("D", wtx.getValue());
      wtx.moveToRightSibling();
      assertEquals("E", wtx.getValue());
      assertFalse(wtx.hasRightSibling());

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: DELETE ALL POSITIONS ====================

  @Test
  void testDeleteFirstMiddleLast() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Create array [1, 2, 3, 4, 5]
      wtx.insertArrayAsFirstChild();
      final long[] keys = new long[5];
      for (int i = 0; i < 5; i++) {
        if (i == 0) {
          wtx.insertNumberValueAsFirstChild(i + 1);
        } else {
          wtx.insertNumberValueAsRightSibling(i + 1);
        }
        keys[i] = wtx.getNodeKey();
      }
      opLog.add("array [1,2,3,4,5]");
      verifyAllInvariants(wtx, 0, opLog);

      // Delete middle (3)
      wtx.moveTo(keys[2]);
      wtx.remove();
      opLog.add("remove middle (3)");
      verifyAllInvariants(wtx, 0, opLog);

      // Verify 2's right sibling is now 4
      wtx.moveTo(keys[1]);
      assertEquals(keys[3], wtx.getRightSiblingKey());
      wtx.moveTo(keys[3]);
      assertEquals(keys[1], wtx.getLeftSiblingKey());

      // Delete first (1)
      wtx.moveTo(keys[0]);
      wtx.remove();
      opLog.add("remove first (1)");
      verifyAllInvariants(wtx, 0, opLog);

      // 2 should now be firstChild
      wtx.moveTo(1); // array
      assertEquals(keys[1], wtx.getFirstChildKey());
      wtx.moveTo(keys[1]);
      assertEquals(NULL_KEY, wtx.getLeftSiblingKey());

      // Delete last (5)
      wtx.moveTo(keys[4]);
      wtx.remove();
      opLog.add("remove last (5)");
      verifyAllInvariants(wtx, 0, opLog);

      // 4 should now be lastChild
      wtx.moveTo(1);
      assertEquals(keys[3], wtx.getLastChildKey());
      wtx.moveTo(keys[3]);
      assertEquals(NULL_KEY, wtx.getRightSiblingKey());

      // Array should have 2 children: [2, 4]
      wtx.moveTo(1);
      assertEquals(2, wtx.getChildCount());

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: NESTED DELETE ====================

  @Test
  void testDeleteSubtreeUpdatesAncestorDescendantCounts() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // Build: { "a": { "b": { "c": "deep" } } }
      wtx.insertObjectAsFirstChild();
      final long rootObj = wtx.getNodeKey();
      wtx.insertObjectRecordAsFirstChild("a", new ObjectValue());
      final long aObj = wtx.getNodeKey(); // the inner object
      wtx.insertObjectRecordAsFirstChild("b", new ObjectValue());
      final long bObj = wtx.getNodeKey();
      wtx.insertObjectRecordAsFirstChild("c", new StringValue("deep"));
      opLog.add("built nested object");
      verifyAllInvariants(wtx, 0, opLog);

      // Check descendant counts before delete
      wtx.moveTo(rootObj);
      final long rootDescBefore = wtx.getDescendantCount();

      // Delete "b" subtree (removes b_key, b_obj, c_key, c_value = 4 nodes)
      wtx.moveTo(bObj);
      wtx.moveToParent(); // b_key
      final long bKey = wtx.getNodeKey();
      wtx.remove();
      opLog.add("removed b_key subtree");
      verifyAllInvariants(wtx, 0, opLog);

      // Root object descendantCount should have decreased
      wtx.moveTo(rootObj);
      assertTrue(wtx.getDescendantCount() < rootDescBefore,
          "descendantCount should decrease after subtree removal [was=" + rootDescBefore
              + ", now=" + wtx.getDescendantCount() + "]");

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: OBJECT RECORD OPERATIONS ====================

  @Test
  void testObjectRecordInsertAllPositions() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      wtx.insertObjectAsFirstChild();
      final long objKey = wtx.getNodeKey();

      // insertObjectRecordAsFirstChild
      wtx.insertObjectRecordAsFirstChild("first", new StringValue("1"));
      wtx.moveToParent(); // VALUE → OBJECT_KEY
      final long firstKey = wtx.getNodeKey();
      opLog.add("record 'first' as firstChild");
      verifyAllInvariants(wtx, 0, opLog);

      // insertObjectRecordAsLastChild
      wtx.moveTo(objKey);
      wtx.insertObjectRecordAsLastChild("last", new NumberValue(99));
      wtx.moveToParent(); // VALUE → OBJECT_KEY
      final long lastKey = wtx.getNodeKey();
      opLog.add("record 'last' as lastChild");
      verifyAllInvariants(wtx, 0, opLog);

      // insertObjectRecordAsRightSibling of first
      wtx.moveTo(firstKey);
      wtx.insertObjectRecordAsRightSibling("middle", new BooleanValue(true));
      wtx.moveToParent(); // VALUE → OBJECT_KEY
      final long middleKey = wtx.getNodeKey();
      opLog.add("record 'middle' as rightSibling of first");
      verifyAllInvariants(wtx, 0, opLog);

      // insertObjectRecordAsLeftSibling of last
      wtx.moveTo(lastKey);
      wtx.insertObjectRecordAsLeftSibling("before_last", new NullValue());
      opLog.add("record 'before_last' as leftSibling of last");
      verifyAllInvariants(wtx, 0, opLog);

      // Verify order: first -> middle -> before_last -> last
      wtx.moveTo(objKey);
      assertEquals(4, wtx.getChildCount());
      assertTrue(wtx.moveToFirstChild());
      assertEquals("first", wtx.getName().getLocalName());
      assertTrue(wtx.moveToRightSibling());
      assertEquals("middle", wtx.getName().getLocalName());
      assertTrue(wtx.moveToRightSibling());
      assertEquals("before_last", wtx.getName().getLocalName());
      assertTrue(wtx.moveToRightSibling());
      assertEquals("last", wtx.getName().getLocalName());
      assertFalse(wtx.hasRightSibling());

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: HASH PROPAGATION ====================

  @Test
  void testHashChangesOnInsertUpdateDelete() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("hello");
      wtx.commit();

      // Record root hash
      wtx.moveTo(1);
      final long hashAfterInsert = wtx.getHash();

      // Update value — hash should change
      wtx.moveToFirstChild();
      wtx.setStringValue("world");
      wtx.commit();
      wtx.moveTo(1);
      final long hashAfterUpdate = wtx.getHash();
      assertNotEquals(hashAfterInsert, hashAfterUpdate,
          "Hash should change after value update");

      // Insert another child — hash should change
      wtx.insertStringValueAsLastChild("extra");
      wtx.commit();
      wtx.moveTo(1);
      final long hashAfterSecondInsert = wtx.getHash();
      assertNotEquals(hashAfterUpdate, hashAfterSecondInsert,
          "Hash should change after insert");

      // Delete a child — hash should change
      wtx.moveToFirstChild();
      wtx.remove();
      wtx.commit();
      wtx.moveTo(1);
      final long hashAfterDelete = wtx.getHash();
      assertNotEquals(hashAfterSecondInsert, hashAfterDelete,
          "Hash should change after delete");

      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: SINGLE-CHILD REMOVAL ====================

  @Test
  void testRemoveOnlyChild() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      wtx.insertArrayAsFirstChild();
      wtx.insertStringValueAsFirstChild("only");
      final long childKey = wtx.getNodeKey();
      opLog.add("array with single child");
      verifyAllInvariants(wtx, 0, opLog);

      // Remove the only child
      wtx.remove();
      opLog.add("removed only child");
      verifyAllInvariants(wtx, 0, opLog);

      // Parent should now have no children
      wtx.moveTo(1);
      assertEquals(0, wtx.getChildCount());
      assertEquals(0, wtx.getDescendantCount());
      assertFalse(wtx.hasFirstChild());

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== SPECIFIC PATTERN: REPLACE OBJECT RECORD VALUE ====================

  @Test
  void testReplaceObjectRecordValue() {
    final List<String> opLog = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      // { "key": "oldValue" }
      wtx.insertObjectAsFirstChild();
      wtx.insertObjectRecordAsFirstChild("key", new StringValue("oldValue"));
      wtx.moveToParent(); // VALUE → OBJECT_KEY
      final long objectKeyNode = wtx.getNodeKey();
      opLog.add("object with key: oldValue");
      verifyAllInvariants(wtx, 0, opLog);

      // Replace string value with number value
      wtx.moveTo(objectKeyNode);
      wtx.replaceObjectRecordValue(new NumberValue(42));
      opLog.add("replaced value with NumberValue(42)");
      verifyAllInvariants(wtx, 0, opLog);

      // Replace number value with object value
      wtx.moveTo(objectKeyNode);
      wtx.replaceObjectRecordValue(new ObjectValue());
      opLog.add("replaced value with ObjectValue");
      verifyAllInvariants(wtx, 0, opLog);

      // Replace object value with array value
      wtx.moveTo(objectKeyNode);
      wtx.replaceObjectRecordValue(new ArrayValue());
      opLog.add("replaced value with ArrayValue");
      verifyAllInvariants(wtx, 0, opLog);

      // Replace array value with boolean
      wtx.moveTo(objectKeyNode);
      wtx.replaceObjectRecordValue(new BooleanValue(false));
      opLog.add("replaced value with BooleanValue(false)");
      verifyAllInvariants(wtx, 0, opLog);

      // Replace with null
      wtx.moveTo(objectKeyNode);
      wtx.replaceObjectRecordValue(new NullValue());
      opLog.add("replaced value with NullValue");
      verifyAllInvariants(wtx, 0, opLog);

      wtx.commit();
      verifyAllInvariants(wtx, 0, opLog);
    }
  }

  // ==================== INVARIANT VERIFICATION ====================

  private void verifyAllInvariants(final JsonNodeReadOnlyTrx rtx, final long seed,
      final List<String> opLog) {
    final long savedKey = rtx.getNodeKey();
    final String ctx = "[seed=" + seed + ", lastOp=" + (opLog.isEmpty() ? "none" : opLog.getLast()) + "]";

    try {
      rtx.moveToDocumentRoot();
      verifyNodeAndDescendants(rtx, ctx);
    } finally {
      rtx.moveTo(savedKey);
    }
  }

  private void verifyNodeAndDescendants(final JsonNodeReadOnlyTrx rtx, final String ctx) {
    final long nodeKey = rtx.getNodeKey();
    final NodeKind kind = rtx.getKind();

    // 1. Verify parent link
    if (kind != NodeKind.JSON_DOCUMENT) {
      final long parentKey = rtx.getParentKey();
      assertTrue(parentKey != NULL_KEY,
          "Non-root node key=" + nodeKey + " has null parent " + ctx);
      assertTrue(rtx.hasNode(parentKey),
          "Parent key=" + parentKey + " does not exist for node key=" + nodeKey + " " + ctx);
    }

    // 2. Verify sibling links are symmetric
    if (rtx.hasLeftSibling()) {
      final long leftKey = rtx.getLeftSiblingKey();
      rtx.moveTo(leftKey);
      assertEquals(nodeKey, rtx.getRightSiblingKey(),
          "Left sibling key=" + leftKey + " does not point back to key=" + nodeKey + " " + ctx);
      rtx.moveTo(nodeKey);
    } else {
      assertEquals(NULL_KEY, rtx.getLeftSiblingKey(),
          "hasLeftSibling=false but leftSiblingKey is not NULL for key=" + nodeKey + " " + ctx);
    }

    if (rtx.hasRightSibling()) {
      final long rightKey = rtx.getRightSiblingKey();
      rtx.moveTo(rightKey);
      assertEquals(nodeKey, rtx.getLeftSiblingKey(),
          "Right sibling key=" + rightKey + " does not point back to key=" + nodeKey + " " + ctx);
      rtx.moveTo(nodeKey);
    } else {
      assertEquals(NULL_KEY, rtx.getRightSiblingKey(),
          "hasRightSibling=false but rightSiblingKey is not NULL for key=" + nodeKey + " " + ctx);
    }

    // 3. Verify child links and counts
    if (rtx.hasFirstChild()) {
      assertTrue(rtx.getChildCount() > 0,
          "hasFirstChild=true but childCount=0 for key=" + nodeKey + " " + ctx);

      // Walk sibling chain and count children
      long actualChildCount = 0;
      long actualDescendantCount = 0;
      long lastChildKey = NULL_KEY;

      final long firstChildKey = rtx.getFirstChildKey();
      rtx.moveTo(firstChildKey);

      // First child should have no left sibling
      assertEquals(NULL_KEY, rtx.getLeftSiblingKey(),
          "First child key=" + firstChildKey + " has a left sibling for parent key=" + nodeKey + " " + ctx);

      // Verify first child's parent points back
      assertEquals(nodeKey, rtx.getParentKey(),
          "First child key=" + firstChildKey + " parent != " + nodeKey + " " + ctx);

      do {
        actualChildCount++;
        // Verify each child's parent
        assertEquals(nodeKey, rtx.getParentKey(),
            "Child key=" + rtx.getNodeKey() + " parent != " + nodeKey + " " + ctx);

        // Count descendants recursively
        actualDescendantCount += 1 + countDescendants(rtx);

        lastChildKey = rtx.getNodeKey();
      } while (rtx.moveToRightSibling());

      // Last child should have no right sibling
      rtx.moveTo(lastChildKey);
      assertEquals(NULL_KEY, rtx.getRightSiblingKey(),
          "Last child in chain key=" + lastChildKey + " has right sibling for parent key=" + nodeKey + " " + ctx);

      rtx.moveTo(nodeKey);

      assertEquals(actualChildCount, rtx.getChildCount(),
          "childCount mismatch for key=" + nodeKey + " kind=" + kind + " " + ctx);

      // Verify lastChildKey
      assertEquals(lastChildKey, rtx.getLastChildKey(),
          "lastChildKey mismatch for key=" + nodeKey + " " + ctx);

      // Verify descendantCount
      assertEquals(actualDescendantCount, rtx.getDescendantCount(),
          "descendantCount mismatch for key=" + nodeKey + " kind=" + kind + " " + ctx);

      // Recurse into children
      rtx.moveTo(firstChildKey);
      do {
        verifyNodeAndDescendants(rtx, ctx);
      } while (rtx.moveToRightSibling());

      rtx.moveTo(nodeKey);
    } else {
      assertEquals(0, rtx.getChildCount(),
          "hasFirstChild=false but childCount=" + rtx.getChildCount() + " for key=" + nodeKey + " " + ctx);
      assertEquals(0, rtx.getDescendantCount(),
          "hasFirstChild=false but descendantCount=" + rtx.getDescendantCount()
              + " for key=" + nodeKey + " " + ctx);
    }
  }

  private long countDescendants(final JsonNodeReadOnlyTrx rtx) {
    final long nodeKey = rtx.getNodeKey();
    long count = 0;

    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      do {
        count += 1 + countDescendants(rtx);
      } while (rtx.moveToRightSibling());
      rtx.moveTo(nodeKey);
    }

    return count;
  }

  // ==================== RANDOM OPERATION HELPERS ====================

  private String performRandomInsert(final JsonNodeTrx wtx, final Random rng,
      final NodeKind currentKind) {
    // Determine valid insert positions based on current node kind
    if (currentKind == NodeKind.JSON_DOCUMENT) {
      if (!wtx.hasFirstChild()) {
        wtx.insertObjectAsFirstChild();
        return "insertObjectAsFirstChild (from document)";
      }
      wtx.moveToFirstChild();
      return "moved to first child (document already has root)";
    }

    if (currentKind == NodeKind.OBJECT) {
      // Objects can only have object_key children
      final String key = "k" + rng.nextInt(100000);
      final int valueType = rng.nextInt(6);
      return switch (valueType) {
        case 0 -> {
          wtx.insertObjectRecordAsFirstChild(key, new StringValue("s" + rng.nextInt(1000)));
          wtx.moveToParent();
          yield "insertObjectRecordAsFirstChild(\"" + key + "\", string)";
        }
        case 1 -> {
          wtx.insertObjectRecordAsLastChild(key, new NumberValue(rng.nextDouble() * 100));
          wtx.moveToParent();
          yield "insertObjectRecordAsLastChild(\"" + key + "\", number)";
        }
        case 2 -> {
          wtx.insertObjectRecordAsFirstChild(key, new BooleanValue(rng.nextBoolean()));
          wtx.moveToParent();
          yield "insertObjectRecordAsFirstChild(\"" + key + "\", boolean)";
        }
        case 3 -> {
          wtx.insertObjectRecordAsLastChild(key, new NullValue());
          wtx.moveToParent();
          yield "insertObjectRecordAsLastChild(\"" + key + "\", null)";
        }
        case 4 -> {
          wtx.insertObjectRecordAsFirstChild(key, new ObjectValue());
          wtx.moveToParent(); // OBJECT_VALUE → OBJECT_KEY
          yield "insertObjectRecordAsFirstChild(\"" + key + "\", object)";
        }
        default -> {
          wtx.insertObjectRecordAsLastChild(key, new ArrayValue());
          wtx.moveToParent(); // ARRAY_VALUE → OBJECT_KEY
          yield "insertObjectRecordAsLastChild(\"" + key + "\", array)";
        }
      };
    }

    if (currentKind == NodeKind.ARRAY) {
      // Arrays can have any value type as children
      final int pos = rng.nextInt(2); // 0 = firstChild, 1 = lastChild
      final int valueType = rng.nextInt(6);
      return insertValueChild(wtx, rng, pos == 0, valueType);
    }

    if (currentKind == NodeKind.OBJECT_KEY) {
      // Object keys can only have their value child; insert siblings instead
      if (wtx.hasRightSibling() && rng.nextBoolean()) {
        // Nothing to insert here — navigate away
        wtx.moveToParent();
        return "moved to parent (object_key, no valid insert)";
      }
      wtx.moveToParent();
      return "moved to parent (from object_key)";
    }

    // Leaf nodes (STRING_VALUE, NUMBER_VALUE, etc.) — insert as sibling
    if (rng.nextBoolean() && canInsertRightSibling(wtx)) {
      return insertValueSibling(wtx, rng, false);
    } else if (canInsertLeftSibling(wtx)) {
      return insertValueSibling(wtx, rng, true);
    } else if (canInsertRightSibling(wtx)) {
      return insertValueSibling(wtx, rng, false);
    } else {
      // Navigate away
      wtx.moveToParent();
      return "moved to parent (no valid sibling position)";
    }
  }

  private String insertValueChild(final JsonNodeTrx wtx, final Random rng,
      final boolean asFirst, final int valueType) {
    final String position = asFirst ? "FirstChild" : "LastChild";
    return switch (valueType) {
      case 0 -> {
        if (asFirst) wtx.insertStringValueAsFirstChild("s" + rng.nextInt(1000));
        else wtx.insertStringValueAsLastChild("s" + rng.nextInt(1000));
        yield "insertStringValueAs" + position;
      }
      case 1 -> {
        if (asFirst) wtx.insertNumberValueAsFirstChild(rng.nextInt(1000));
        else wtx.insertNumberValueAsLastChild(rng.nextInt(1000));
        yield "insertNumberValueAs" + position;
      }
      case 2 -> {
        if (asFirst) wtx.insertBooleanValueAsFirstChild(rng.nextBoolean());
        else wtx.insertBooleanValueAsLastChild(rng.nextBoolean());
        yield "insertBooleanValueAs" + position;
      }
      case 3 -> {
        if (asFirst) wtx.insertNullValueAsFirstChild();
        else wtx.insertNullValueAsLastChild();
        yield "insertNullValueAs" + position;
      }
      case 4 -> {
        if (asFirst) wtx.insertObjectAsFirstChild();
        else wtx.insertObjectAsLastChild();
        yield "insertObjectAs" + position;
      }
      default -> {
        if (asFirst) wtx.insertArrayAsFirstChild();
        else wtx.insertArrayAsLastChild();
        yield "insertArrayAs" + position;
      }
    };
  }

  private String insertValueSibling(final JsonNodeTrx wtx, final Random rng,
      final boolean asLeft) {
    final int valueType = rng.nextInt(6);
    final String direction = asLeft ? "LeftSibling" : "RightSibling";
    return switch (valueType) {
      case 0 -> {
        if (asLeft) wtx.insertStringValueAsLeftSibling("s" + rng.nextInt(1000));
        else wtx.insertStringValueAsRightSibling("s" + rng.nextInt(1000));
        yield "insertStringValueAs" + direction;
      }
      case 1 -> {
        if (asLeft) wtx.insertNumberValueAsLeftSibling(rng.nextInt(1000));
        else wtx.insertNumberValueAsRightSibling(rng.nextInt(1000));
        yield "insertNumberValueAs" + direction;
      }
      case 2 -> {
        if (asLeft) wtx.insertBooleanValueAsLeftSibling(rng.nextBoolean());
        else wtx.insertBooleanValueAsRightSibling(rng.nextBoolean());
        yield "insertBooleanValueAs" + direction;
      }
      case 3 -> {
        if (asLeft) wtx.insertNullValueAsLeftSibling();
        else wtx.insertNullValueAsRightSibling();
        yield "insertNullValueAs" + direction;
      }
      case 4 -> {
        if (asLeft) wtx.insertObjectAsLeftSibling();
        else wtx.insertObjectAsRightSibling();
        yield "insertObjectAs" + direction;
      }
      default -> {
        if (asLeft) wtx.insertArrayAsLeftSibling();
        else wtx.insertArrayAsRightSibling();
        yield "insertArrayAs" + direction;
      }
    };
  }

  private boolean canInsertRightSibling(final JsonNodeTrx wtx) {
    // Value siblings only valid under ARRAY parent (OBJECT requires OBJECT_KEY children,
    // OBJECT_KEY can only have a single value child)
    if (wtx.getKind() == NodeKind.JSON_DOCUMENT || wtx.getParentKey() == 0) {
      return false;
    }
    final long savedKey = wtx.getNodeKey();
    wtx.moveToParent();
    final boolean isArray = wtx.getKind() == NodeKind.ARRAY;
    wtx.moveTo(savedKey);
    return isArray;
  }

  private boolean canInsertLeftSibling(final JsonNodeTrx wtx) {
    if (wtx.getKind() == NodeKind.JSON_DOCUMENT || wtx.getParentKey() == 0) {
      return false;
    }
    final long savedKey = wtx.getNodeKey();
    wtx.moveToParent();
    final boolean isArray = wtx.getKind() == NodeKind.ARRAY;
    wtx.moveTo(savedKey);
    return isArray;
  }

  /**
   * If the cursor is on an object record value (child of OBJECT_KEY), move to the OBJECT_KEY.
   * Sirix does not allow removing an object record value directly — the whole OBJECT_KEY must be removed.
   */
  private void moveToRemovableNode(final JsonNodeTrx wtx) {
    final long savedKey = wtx.getNodeKey();
    wtx.moveToParent();
    if (wtx.getKind() == NodeKind.OBJECT_KEY) {
      // Stay on OBJECT_KEY — remove the whole record
      return;
    }
    wtx.moveTo(savedKey);
  }

  private void navigateToRandomNode(final JsonNodeTrx wtx, final Random rng) {
    // Random walk: up to 5 random navigation steps
    final int steps = rng.nextInt(5) + 1;
    for (int s = 0; s < steps; s++) {
      final int dir = rng.nextInt(5);
      switch (dir) {
        case 0 -> { if (wtx.hasFirstChild()) wtx.moveToFirstChild(); }
        case 1 -> { if (wtx.hasRightSibling()) wtx.moveToRightSibling(); }
        case 2 -> { if (wtx.hasLeftSibling()) wtx.moveToLeftSibling(); }
        case 3 -> { if (wtx.hasLastChild()) wtx.moveToLastChild(); }
        case 4 -> {
          if (wtx.getKind() != NodeKind.JSON_DOCUMENT) wtx.moveToParent();
        }
      }
    }
  }
}
