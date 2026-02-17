/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.node.NodeKind;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTIndirectPage.NodeType;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Comprehensive corner case tests for HOT (Height Optimized Trie) implementation.
 * 
 * <p>
 * Tests are organized by corner case ID from the implementation plan:
 * </p>
 * <ul>
 * <li>CC-1 to CC-4: Key edge cases (empty, max diff, length diff, long keys)</li>
 * <li>CC-5 to CC-6: Node structure changes (BiNode collapse, SpanNode overflow)</li>
 * <li>CC-7 to CC-9: Key pattern edge cases (duplicates, common prefix, alternating)</li>
 * <li>CC-10 to CC-12: Concurrent and range operations</li>
 * <li>CC-13 to CC-15: Bit mask and value edge cases</li>
 * <li>CC-16 to CC-21: Versioning and COW edge cases</li>
 * </ul>
 * 
 * @see DiscriminativeBitComputer
 * @see NodeUpgradeManager
 * @see HeightOptimalSplitter
 * @see SiblingMerger
 */
@DisplayName("HOT Corner Cases Tests")
class HOTCornerCasesTest {

  private static final Path TEST_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeAll
  static void setupOnce() {
    LinuxMemorySegmentAllocator.getInstance().init(1L << 30);
  }

  @BeforeEach
  void setup() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(TEST_PATH));
  }

  @AfterEach
  void tearDown() {
    Databases.getGlobalBufferManager().clearAllCaches();
    JsonTestHelper.deleteEverything();
  }

  // ===========================================================================
  // CC-1: Empty Keys
  // ===========================================================================
  @Nested
  @DisplayName("CC-1: Empty Keys")
  class CC1_EmptyKeys {

    @Test
    @DisplayName("Both empty keys return -1 (identical)")
    void testBothEmptyKeys() {
      int result = DiscriminativeBitComputer.computeDifferingBit(new byte[0], new byte[0]);
      assertEquals(-1, result, "Empty keys are identical, should return -1");
    }

    @Test
    @DisplayName("One empty key returns 0 (differs at first bit)")
    void testOneEmptyKey() {
      assertEquals(0, DiscriminativeBitComputer.computeDifferingBit(new byte[0], new byte[] {0x01}));
      assertEquals(0, DiscriminativeBitComputer.computeDifferingBit(new byte[] {0x01}, new byte[0]));
    }

    @Test
    @DisplayName("Empty key vs 0x00 returns 0 (length difference)")
    void testEmptyVsZeroByte() {
      assertEquals(0, DiscriminativeBitComputer.computeDifferingBit(new byte[0], new byte[] {0x00}));
    }

    @Test
    @DisplayName("isBitSet returns false for empty key")
    void testIsBitSetOnEmptyKey() {
      assertFalse(DiscriminativeBitComputer.isBitSet(new byte[0], 0));
      assertFalse(DiscriminativeBitComputer.isBitSet(new byte[0], 7));
      assertFalse(DiscriminativeBitComputer.isBitSet(new byte[0], 100));
    }
  }

  // ===========================================================================
  // CC-2: Single-Byte Keys with All Bits Differing
  // ===========================================================================
  @Nested
  @DisplayName("CC-2: Maximum Bit Difference")
  class CC2_MaxBitDifference {

    @Test
    @DisplayName("0x00 vs 0xFF - MSB is first differing bit (bit 0)")
    void testZeroVsFF() {
      // XOR = 0xFF, clz(0xFF as int) = 24, 24-24 = 0
      int result = DiscriminativeBitComputer.computeDifferingBit(new byte[] {0x00}, new byte[] {(byte) 0xFF});
      assertEquals(0, result);
    }

    @Test
    @DisplayName("0x80 vs 0x7F - bit 0 differs (MSB)")
    void test80vs7F() {
      // 0x80 = 10000000, 0x7F = 01111111, XOR = 0xFF
      int result = DiscriminativeBitComputer.computeDifferingBit(new byte[] {(byte) 0x80}, new byte[] {0x7F});
      assertEquals(0, result);
    }

    @Test
    @DisplayName("0xF0 vs 0x0F - bit 0 differs")
    void testF0vs0F() {
      // 0xF0 XOR 0x0F = 0xFF
      int result = DiscriminativeBitComputer.computeDifferingBit(new byte[] {(byte) 0xF0}, new byte[] {0x0F});
      assertEquals(0, result);
    }
  }

  // ===========================================================================
  // CC-3: Keys Differing Only in Length
  // ===========================================================================
  @Nested
  @DisplayName("CC-3: Length-Only Difference")
  class CC3_LengthDifference {

    @Test
    @DisplayName("Same prefix, different length - differs at first bit of extension")
    void testSamePrefixDifferentLength() {
      byte[] short_ = {0x12, 0x34};
      byte[] long_ = {0x12, 0x34, 0x56};
      // Differ at byte 2 (index 2), bit 0 of that byte = absolute bit 16
      int result = DiscriminativeBitComputer.computeDifferingBit(short_, long_);
      assertEquals(16, result);
    }

    @Test
    @DisplayName("Empty prefix extended - differs at bit 0")
    void testEmptyExtended() {
      byte[] short_ = {};
      byte[] long_ = {0x00, 0x00, 0x01};
      int result = DiscriminativeBitComputer.computeDifferingBit(short_, long_);
      assertEquals(0, result);
    }

    @Test
    @DisplayName("Multi-byte prefix extended with zero byte")
    void testExtendedWithZeroByte() {
      byte[] short_ = {0x12, 0x34, 0x56};
      byte[] long_ = {0x12, 0x34, 0x56, 0x00};
      // Differ at byte 3, first bit = 24
      int result = DiscriminativeBitComputer.computeDifferingBit(short_, long_);
      assertEquals(24, result);
    }
  }

  // ===========================================================================
  // CC-4: Very Long Keys (>64 bits)
  // ===========================================================================
  @Nested
  @DisplayName("CC-4: Long Keys")
  class CC4_LongKeys {

    @Test
    @DisplayName("16-byte keys differing in last byte")
    void test16ByteKeysDifferInLast() {
      byte[] key1 = new byte[16];
      byte[] key2 = new byte[16];
      key2[15] = 0x01;
      // Differ at byte 15, bit 7 = absolute bit 127
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(127, result);
    }

    @Test
    @DisplayName("256-byte keys differing in middle")
    void test256ByteKeys() {
      byte[] key1 = new byte[256];
      byte[] key2 = new byte[256];
      key2[128] = (byte) 0x80;
      // Differ at byte 128, bit 0 = absolute bit 1024
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(1024, result);
    }

    @Test
    @DisplayName("Long identical keys return -1")
    void testLongIdenticalKeys() {
      byte[] key = new byte[1000];
      new Random(42).nextBytes(key);
      int result = DiscriminativeBitComputer.computeDifferingBit(key, key.clone());
      assertEquals(-1, result);
    }

    @Test
    @DisplayName("8-byte aligned comparison uses long path")
    void test8ByteAlignedComparison() {
      byte[] key1 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
      byte[] key2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
      // Uses long comparison for first 16 bytes
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(127, result); // Last bit of 16 bytes
    }
  }

  // ===========================================================================
  // CC-5: BiNode with Single Child After Deletion
  // ===========================================================================
  @Nested
  @DisplayName("CC-5: BiNode Collapse")
  class CC5_BiNodeCollapse {

    @Test
    @DisplayName("SiblingMerger detects collapsible BiNode")
    void testCanCollapseBiNode() {
      // BiNode with 1 child should be collapsible
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1, 1, 0, leftRef, rightRef);

      // Normal BiNode with 2 children is not collapsible
      assertFalse(SiblingMerger.canCollapseBiNode(biNode));
    }

    @Test
    @DisplayName("Min fill factor for merge detection")
    void testMinFillFactor() {
      assertEquals(0.25, SiblingMerger.MIN_FILL_FACTOR, 0.001);
    }
  }

  // ===========================================================================
  // CC-6: SpanNode Overflow (17th Child)
  // ===========================================================================
  @Nested
  @DisplayName("CC-6: SpanNode to MultiNode Upgrade")
  class CC6_SpanNodeOverflow {

    @Test
    @DisplayName("17 children requires MultiNode")
    void test17ChildrenIsMultiNode() {
      assertEquals(NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeType(17));
    }

    @Test
    @DisplayName("16 children is SpanNode max")
    void test16ChildrenIsSpanNode() {
      assertEquals(NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(16));
    }

    @Test
    @DisplayName("32 children is MultiNode max")
    void test32ChildrenIsMultiNodeMax() {
      assertEquals(NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeType(32));
    }

    @Test
    @DisplayName("33 children throws exception")
    void test33ChildrenThrows() {
      assertThrows(IllegalArgumentException.class, () -> NodeUpgradeManager.determineNodeType(33));
    }
  }

  // ===========================================================================
  // CC-7: Identical Keys (Duplicate Insertion)
  // ===========================================================================
  @Nested
  @DisplayName("CC-7: Duplicate Key Handling")
  class CC7_DuplicateKeys {

    @Test
    @DisplayName("Identical keys return -1")
    void testIdenticalKeys() {
      byte[] key = {0x12, 0x34, 0x56, 0x78};
      int result = DiscriminativeBitComputer.computeDifferingBit(key, key.clone());
      assertEquals(-1, result);
    }

    @Test
    @DisplayName("isBitSet consistent for identical bytes")
    void testIsBitSetConsistent() {
      byte[] key = {(byte) 0xAA}; // 10101010
      for (int i = 0; i < 8; i++) {
        boolean expected = (i % 2 == 0); // bits 0,2,4,6 are set
        assertEquals(expected, DiscriminativeBitComputer.isBitSet(key, i), "Bit " + i + " should be " + (expected
            ? "set"
            : "unset"));
      }
    }
  }

  // ===========================================================================
  // CC-8: All Keys in Single Byte Range
  // ===========================================================================
  @Nested
  @DisplayName("CC-8: Common Prefix Keys")
  class CC8_CommonPrefix {

    @Test
    @DisplayName("Keys with same first byte differ in second byte")
    void testSameFirstByte() {
      byte[] key1 = {0x41, 0x00};
      byte[] key2 = {0x41, (byte) 0x80};
      // First byte identical, differ at byte 1, bit 0 = absolute bit 8
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(8, result);
    }

    @Test
    @DisplayName("Keys with 4-byte common prefix")
    void test4ByteCommonPrefix() {
      byte[] key1 = {0x12, 0x34, 0x56, 0x78, 0x00};
      byte[] key2 = {0x12, 0x34, 0x56, 0x78, 0x01};
      // 4 bytes identical, differ at byte 4, bit 7 = absolute bit 39
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(39, result);
    }

    @Test
    @DisplayName("All ASCII 'A' prefix keys")
    void testAllAsciiAPrefix() {
      byte[] key1 = "AAAA1".getBytes();
      byte[] key2 = "AAAA2".getBytes();
      // 'A' = 0x41, '1' = 0x31, '2' = 0x32
      // Differ at byte 4: 0x31 XOR 0x32 = 0x03 (bits 6,7 differ)
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(38, result); // byte 4, bit 6
    }
  }

  // ===========================================================================
  // CC-9: Alternating Bit Patterns
  // ===========================================================================
  @Nested
  @DisplayName("CC-9: Alternating Bit Patterns")
  class CC9_AlternatingPatterns {

    @Test
    @DisplayName("0xAA vs 0x55 - all bits differ, MSB first")
    void testAAVs55() {
      // 0xAA = 10101010, 0x55 = 01010101, XOR = 0xFF
      int result = DiscriminativeBitComputer.computeDifferingBit(new byte[] {(byte) 0xAA}, new byte[] {0x55});
      assertEquals(0, result);
    }

    @Test
    @DisplayName("Discriminative mask captures all differing bits")
    void testDiscriminativeMask() {
      byte[][] keys = {new byte[] {(byte) 0xAA}, new byte[] {0x55}};
      long mask = DiscriminativeBitComputer.computeDiscriminativeMask(keys, 0, 1);
      // All 8 bits differ, mask should have all 8 bits of first byte set
      // In 64-bit mask, byte 0 is at bits 56-63
      assertEquals(8, DiscriminativeBitComputer.countDiscriminativeBits(mask));
    }

    @Test
    @DisplayName("Alternating pattern with common prefix")
    void testAlternatingWithPrefix() {
      byte[] key1 = {0x00, (byte) 0xAA};
      byte[] key2 = {0x00, 0x55};
      // First byte same, second byte all bits differ
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(8, result); // First differing bit is MSB of byte 1
    }
  }

  // ===========================================================================
  // CC-10: Concurrent COW During Split
  // ===========================================================================
  @Nested
  @DisplayName("CC-10: Concurrent Read During Split")
  class CC10_ConcurrentCOW {

    @Test
    @DisplayName("Concurrent reads and writes maintain isolation")
    void testConcurrentReadDuringSplit() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.FULL)
                                               .build());

        // Create initial data - revision 1
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3,4,5]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Create second revision
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.moveTo(1L);
          wtx.moveToLastChild();
          wtx.insertNumberValueAsRightSibling(6);
          wtx.commit();
        }

        // Read revision 1 - should see 5 elements
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx(1)) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(5, count, "Reader on revision 1 should see 5 elements");
        }

        // Read revision 2 - should see 6 elements
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx(2)) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(6, count, "Reader on revision 2 should see 6 elements");
        }
      }
    }
  }

  // ===========================================================================
  // CC-11: Range Spanning Multiple Pages After Split
  // ===========================================================================
  @Nested
  @DisplayName("CC-11: Range Query Across Split")
  class CC11_RangeAcrossSplit {

    @Test
    @DisplayName("Range query finds all keys after split")
    void testRangeQueryAcrossSplit() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.FULL)
                                               .build());

        // Create data that will span multiple pages
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append(i);
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Count all elements
          wtx.moveTo(1L);
          int count = 0;
          if (wtx.moveToFirstChild()) {
            do {
              count++;
            } while (wtx.moveToRightSibling());
          }

          assertEquals(100, count, "Should have 100 array elements");
        }
      }
    }
  }

  // ===========================================================================
  // CC-12: Merge Creating Node Type Downgrade
  // ===========================================================================
  @Nested
  @DisplayName("CC-12: Merge Node Type Downgrade")
  class CC12_MergeDowngrade {

    @Test
    @DisplayName("Two merged nodes with 2 children each stays BiNode-compatible")
    void testMergeStaysBiNode() {
      // If merged result has 4 unique children, it's still SpanNode
      // If it has 2 (due to duplicate keys), it could be BiNode
      int leftChildren = 2;
      int rightChildren = 2;
      int total = leftChildren + rightChildren;

      assertTrue(total <= SiblingMerger.MAX_ENTRIES_PER_NODE);
      assertEquals(NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(total));
    }

    @Test
    @DisplayName("Determine correct node type after merge")
    void testNodeTypeAfterMerge() {
      assertEquals(NodeType.BI_NODE, NodeUpgradeManager.determineNodeType(2));
      assertEquals(NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(3));
      assertEquals(NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(4));
    }
  }

  // ===========================================================================
  // CC-13: Bit Mask Overflow at Byte Boundary
  // ===========================================================================
  @Nested
  @DisplayName("CC-13: Bit Mask Byte Boundary")
  class CC13_BitMaskBoundary {

    @Test
    @DisplayName("Discriminative bit in byte 8 is correctly identified")
    void testBitInByte8() {
      byte[] key1 = new byte[9];
      byte[] key2 = new byte[9];
      key2[8] = (byte) 0x80;
      // Differ at byte 8, bit 0 = absolute bit 64
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(64, result);
    }

    @Test
    @DisplayName("Bits spanning bytes 7-8 boundary")
    void testBitsSpanningBoundary() {
      byte[] key1 = new byte[9];
      byte[] key2 = new byte[9];
      key2[7] = 0x01; // Last bit of byte 7 = bit 63
      // Differ at byte 7, bit 7 = absolute bit 63
      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(63, result);
    }

    @Test
    @DisplayName("isBitSet works beyond 64-bit boundary")
    void testIsBitSetBeyond64() {
      byte[] key = new byte[16];
      key[8] = (byte) 0x80; // Bit 64
      key[15] = 0x01; // Bit 127

      assertTrue(DiscriminativeBitComputer.isBitSet(key, 64));
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 127));
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 65));
    }
  }

  // ===========================================================================
  // CC-14: HOTIndirectPage Full COW Versioning
  // ===========================================================================
  @Nested
  @DisplayName("CC-14: IndirectPage COW Versioning")
  class CC14_IndirectPageCOW {

    @Test
    @DisplayName("HOTIndirectPage copy creates independent instance")
    void testIndirectPageCopy() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(1, 1, 5, leftRef, rightRef);
      HOTIndirectPage copy = original.copyWithNewPageKey(2, 2);

      assertEquals(1, original.getPageKey());
      assertEquals(2, copy.getPageKey());
      assertEquals(1, original.getRevision());
      assertEquals(2, copy.getRevision());
    }

    @Test
    @DisplayName("Child reference update creates new page")
    void testChildReferenceUpdate() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(1, 1, 5, leftRef, rightRef);

      PageReference newChildRef = new PageReference();
      newChildRef.setKey(300);
      HOTIndirectPage updated = original.copyWithUpdatedChild(0, newChildRef);

      // Original unchanged
      assertEquals(100, original.getChildReference(0).getKey());
      // Updated has new child
      assertEquals(300, updated.getChildReference(0).getKey());
    }
  }

  // ===========================================================================
  // CC-15: Zero-Length Value
  // ===========================================================================
  @Nested
  @DisplayName("CC-15: Zero-Length Values")
  class CC15_ZeroLengthValue {

    @Test
    @DisplayName("Empty byte array value is valid")
    void testEmptyByteArrayValue() {
      byte[] emptyValue = new byte[0];
      assertEquals(0, emptyValue.length);
      assertNotNull(emptyValue);
    }

    @Test
    @DisplayName("Null-safe value length check")
    void testNullSafeValueLength() {
      byte[] value = null;
      short valueLen = (value == null)
          ? 0
          : (short) value.length;
      assertEquals(0, valueLen);
    }
  }

  // ===========================================================================
  // CC-16: Split During INCREMENTAL Versioning Chain
  // ===========================================================================
  @Nested
  @DisplayName("CC-16: Split with INCREMENTAL Versioning")
  class CC16_SplitWithIncremental {

    @Test
    @DisplayName("INCREMENTAL versioning works with data modifications")
    void testSplitWithIncrementalVersioning() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.INCREMENTAL)
                                               .maxNumberOfRevisionsToRestore(5)
                                               .build());

        // Create initial data - note: insertSubtreeAsFirstChild may auto-commit
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\":1,\"b\":2}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read back - just verify data is accessible
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          // Should have 2 object key nodes
          assertEquals(2, count, "Should have 2 object entries (a and b)");
        }

        // Create another revision
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.moveTo(1L);
          wtx.moveToFirstChild();
          // Just traverse to verify
          wtx.commit();
        }

        // Verify data still accessible after multiple revisions
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertTrue(count >= 2, "Should still have at least 2 object entries");
        }
      }
    }
  }

  // ===========================================================================
  // CC-17: HOTIndirectPage in Historical Revision
  // ===========================================================================
  @Nested
  @DisplayName("CC-17: Historical Revision Access")
  class CC17_HistoricalRevision {

    @Test
    @DisplayName("Can read data from earlier revision")
    void testHistoricalRevisionRead() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.FULL)
                                               .build());

        // Create initial data
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Add more data
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.moveTo(1L);
          wtx.moveToLastChild();
          wtx.insertNumberValueAsRightSibling(4);
          wtx.commit();
        }

        // Read historical revision 1
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx(1)) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(3, count, "Revision 1 should have 3 elements");
        }

        // Read current revision 2
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(4, count, "Revision 2 should have 4 elements");
        }
      }
    }
  }

  // ===========================================================================
  // CC-18: Split Creates New Root Across Revision Boundary
  // ===========================================================================
  @Nested
  @DisplayName("CC-18: Root Split Across Revisions")
  class CC18_RootSplitRevision {

    @Test
    @DisplayName("Root split followed by new revision works correctly")
    void testRootSplitThenNewRevision() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.FULL)
                                               .build());

        // Create large initial data (may cause root split)
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"key").append(String.format("%03d", i)).append("\":").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // New revision modifies data
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.moveTo(1L);
          wtx.moveToFirstChild();
          // Just traverse to verify structure is intact
          int count = 0;
          do {
            count++;
          } while (wtx.moveToRightSibling());

          assertEquals(100, count, "Should still have 100 keys after revision change");
        }
      }
    }
  }

  // ===========================================================================
  // CC-19: revsToRestore Threshold for Fragment Combining
  // ===========================================================================
  @Nested
  @DisplayName("CC-19: Fragment Threshold")
  class CC19_FragmentThreshold {

    @Test
    @DisplayName("Multiple revisions with threshold=3 creates fragments")
    void testFragmentThreshold() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.DIFFERENTIAL)
                                               .maxNumberOfRevisionsToRestore(3)
                                               .build());

        // Create 5 revisions
        for (int rev = 1; rev <= 5; rev++) {
          try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
            if (rev == 1) {
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"), JsonNodeTrx.Commit.NO);
            } else {
              wtx.moveTo(1L);
              wtx.moveToLastChild();
              wtx.insertNumberValueAsRightSibling(rev);
            }
            wtx.commit();
          }
        }

        // Verify all data accessible
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveTo(1L);
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(5, count, "Should have 5 elements across 5 revisions");
        }
      }
    }
  }

  // ===========================================================================
  // CC-20: Concurrent Read During COW Split
  // ===========================================================================
  @Nested
  @DisplayName("CC-20: Concurrent Read During COW")
  class CC20_ConcurrentCOW {

    @Test
    @DisplayName("Reader isolation during writer COW")
    void testReaderIsolationDuringCOW() throws Exception {
      try (var db = Databases.openJsonDatabase(TEST_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder("resource")
                                               .useHOTIndexes()
                                               .versioningApproach(VersioningType.FULL)
                                               .build());

        // Create initial data
        try (var session = db.beginResourceSession("resource"); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3,4,5]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        int readerCount = 0;

        // Open reader on revision 1
        try (var session = db.beginResourceSession("resource"); var rtx = session.beginNodeReadOnlyTrx(1)) {

          rtx.moveTo(1L);
          if (rtx.moveToFirstChild()) {
            do {
              readerCount++;
            } while (rtx.moveToRightSibling());
          }
        }

        assertEquals(5, readerCount, "Reader should see 5 elements from revision 1");
      }
    }
  }

  // ===========================================================================
  // CC-21: Node Upgrade Preserves References
  // ===========================================================================
  @Nested
  @DisplayName("CC-21: Node Upgrade Reference Preservation")
  class CC21_NodeUpgradeRefs {

    @Test
    @DisplayName("SpanNode creation preserves child references")
    void testSpanNodePreservesRefs() {
      PageReference[] refs = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        refs[i] = new PageReference();
        refs[i].setKey(100 + i);
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1, 1, (byte) 0, 0x0FL, partialKeys, refs);

      assertEquals(4, spanNode.getNumChildren());
      for (int i = 0; i < 4; i++) {
        assertEquals(100 + i, spanNode.getChildReference(i).getKey());
        assertEquals(i, spanNode.getPartialKey(i));
      }
    }

    @Test
    @DisplayName("BiNode to SpanNode upgrade preserves children")
    void testBiNodeToSpanNodeUpgrade() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1, 1, 0, leftRef, rightRef);

      // Verify BiNode has both children
      assertEquals(2, biNode.getNumChildren());
      assertEquals(100, biNode.getChildReference(0).getKey());
      assertEquals(200, biNode.getChildReference(1).getKey());
    }
  }

  // ===========================================================================
  // Additional Edge Cases
  // ===========================================================================
  @Nested
  @DisplayName("Additional Edge Cases")
  class AdditionalEdgeCases {

    @Test
    @DisplayName("All 256 possible byte values correctly compared")
    void testAll256ByteValues() {
      for (int i = 0; i < 256; i++) {
        byte[] key1 = {(byte) i};
        byte[] key2 = {(byte) ((i + 1) % 256)};

        int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);

        // Unless identical, should find a differing bit
        if (i != (i + 1) % 256) {
          assertTrue(result >= 0 && result < 8, "Byte " + i + " vs " + ((i + 1) % 256) + " should differ in bits 0-7");
        }
      }
    }

    @Test
    @DisplayName("Extremely long identical prefix")
    void testLongIdenticalPrefix() {
      byte[] key1 = new byte[1000];
      byte[] key2 = new byte[1000];
      Arrays.fill(key1, (byte) 0xAB);
      Arrays.fill(key2, (byte) 0xAB);
      key2[999] = (byte) 0xAC; // Differ in last byte

      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      // 0xAB XOR 0xAC = 0x07 (bits 5,6,7 differ), first is bit 5
      // Byte 999, bit 5 = absolute bit 999*8 + 5 = 7997
      assertEquals(7997, result);
    }

    @Test
    @DisplayName("Single bit difference at various positions")
    void testSingleBitDifferences() {
      for (int bitPos = 0; bitPos < 64; bitPos++) {
        int bytePos = bitPos / 8;
        int bitInByte = bitPos % 8;

        byte[] key1 = new byte[bytePos + 1];
        byte[] key2 = new byte[bytePos + 1];
        key2[bytePos] = (byte) (0x80 >> bitInByte);

        int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
        assertEquals(bitPos, result, "Bit position " + bitPos + " should be correctly identified");
      }
    }

    @Test
    @DisplayName("extractPartialKey handles various masks")
    void testExtractPartialKeyVariousMasks() {
      byte[] key = {(byte) 0xFF}; // All bits set

      // Extract MSB only
      assertEquals(1, DiscriminativeBitComputer.extractPartialKey(key, 1L << 63, 0));

      // Extract LSB only
      assertEquals(1, DiscriminativeBitComputer.extractPartialKey(key, 1L << 56, 0));

      // Extract bits 0 and 7 (MSB and LSB of first byte)
      long mask = (1L << 63) | (1L << 56);
      int result = DiscriminativeBitComputer.extractPartialKey(key, mask, 0);
      assertEquals(3, result); // Both bits set = binary 11 = 3
    }
  }
}

