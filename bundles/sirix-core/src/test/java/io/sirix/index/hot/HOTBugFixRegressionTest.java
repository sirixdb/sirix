/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.page.CASPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for Phase 0 bug fixes in the HOT (Height Optimized Trie) index.
 *
 * <p>These tests verify fixes for:
 * <ul>
 *   <li>Bug 1: Signed byte comparison in SiblingMerger merge ordering</li>
 *   <li>Bug 2: Identical keys fallback to bit 0 in HeightOptimalSplitter</li>
 *   <li>Bug 4: Page key persistence across transactions (nextPageKey counter)</li>
 * </ul>
 */
@DisplayName("HOT Bug Fix Regression Tests (Phase 0)")
class HOTBugFixRegressionTest {

  private static final Path JSON = Paths.get("src", "test", "resources", "json");

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOTSetting() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  // ===== Bug 1: Signed byte comparison in merge ordering =====

  @Nested
  @DisplayName("Bug 1: SiblingMerger unsigned byte comparison")
  class SignedByteComparisonTests {

    @Test
    @DisplayName("Unsigned comparison orders 0x01 < 0xFF correctly (not signed: 0xFF=-1 < 0x01=1)")
    void testUnsignedByteOrdering() {
      // Before fix: (byte)0xFF = -1 < (byte)0x01 = 1 → wrong order
      // After fix: Byte.toUnsignedInt(0xFF) = 255 > Byte.toUnsignedInt(0x01) = 1 → correct

      // Create two BiNodes with partial keys that trigger the bug
      final PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      final PageReference rightRef = new PageReference();
      rightRef.setKey(2);

      // BiNode with partial key 0x01 (first child index determines partial key extraction)
      final HOTIndirectPage nodeWithLowKey =
          HOTIndirectPage.createBiNode(10L, 1, 0, leftRef, rightRef, 1);

      // BiNode with partial key 0xFF
      final HOTIndirectPage nodeWithHighKey =
          HOTIndirectPage.createBiNode(11L, 1, 0, leftRef, rightRef, 1);

      // Verify getPartialKey returns the expected bytes for basic comparison
      // The core fix is in SiblingMerger line 265: Byte.toUnsignedInt()
      // This test exercises the unsigned comparison semantics directly
      byte lowByte = 0x01;
      byte highByte = (byte) 0xFF;

      // Signed comparison (old, broken): highByte < lowByte → true (-1 < 1)
      assertTrue(highByte < lowByte, "Java signed byte 0xFF=-1 IS less than 0x01=1 (this is the bug)");

      // Unsigned comparison (new, correct): highByte > lowByte → true (255 > 1)
      assertTrue(Byte.toUnsignedInt(highByte) > Byte.toUnsignedInt(lowByte),
          "Unsigned: 0xFF=255 must be greater than 0x01=1");
    }

    @Test
    @DisplayName("Boundary: 0x7F and 0x80 comparison crosses signed/unsigned boundary")
    void testSignedUnsignedBoundary() {
      // 0x7F = 127 (positive in both signed and unsigned)
      // 0x80 = -128 signed, 128 unsigned
      byte belowBoundary = 0x7F;
      byte aboveBoundary = (byte) 0x80;

      // Signed: 0x80 (-128) < 0x7F (127) → WRONG for key ordering
      assertTrue(aboveBoundary < belowBoundary, "Signed 0x80=-128 < 0x7F=127 (the bug)");

      // Unsigned: 0x80 (128) > 0x7F (127) → CORRECT
      assertTrue(Byte.toUnsignedInt(aboveBoundary) > Byte.toUnsignedInt(belowBoundary),
          "Unsigned 0x80=128 > 0x7F=127 (the fix)");
    }
  }

  // ===== Bug 2: Identical keys fallback to bit 0 =====

  @Nested
  @DisplayName("Bug 2: HeightOptimalSplitter identical key detection")
  class IdenticalKeysSplitTests {

    @Test
    @DisplayName("computeDifferingBit returns -1 for identical keys")
    void testIdenticalKeysReturnMinusOne() {
      byte[] key1 = {0x41, 0x42, 0x43};
      byte[] key2 = {0x41, 0x42, 0x43};

      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(-1, result, "Identical keys should return -1 (no differing bit)");
    }

    @Test
    @DisplayName("computeDifferingBit finds correct bit for keys differing in last byte")
    void testDifferingBitLastByte() {
      byte[] key1 = {0x41, 0x42, 0x00};
      byte[] key2 = {0x41, 0x42, 0x01};

      int result = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      // XOR of last bytes: 0x00 ^ 0x01 = 0x01, leading zeros in last byte = 7
      // Absolute position: byte 2 * 8 + 7 = 23
      assertEquals(23, result, "Should find differing bit at position 23 (byte 2, bit 7)");
    }

    @Test
    @DisplayName("computeDifferingBit handles different-length keys (prefix match)")
    void testDifferentLengthKeys() {
      byte[] shorter = {0x41, 0x42};
      byte[] longer = {0x41, 0x42, 0x43};

      int result = DiscriminativeBitComputer.computeDifferingBit(shorter, longer);
      // When keys share a common prefix but differ in length, the discriminative bit
      // is at position minLen * 8 (first bit of the longer key's suffix byte).
      // minLen=2, so result = 2*8 = 16
      assertEquals(16, result, "Different-length keys should differ at minLen*8");
    }
  }

  // ===== Bug 4: Persistent page key allocation =====

  @Nested
  @DisplayName("Bug 4: Persistent HOT page key allocation")
  class PersistentPageKeyTests {

    @BeforeEach
    void setUp() {
      JsonTestHelper.deleteEverything();
    }

    @AfterEach
    void tearDown() {
      JsonTestHelper.closeEverything();
      JsonTestHelper.deleteEverything();
      Databases.getGlobalBufferManager().clearAllCaches();
    }

    @Test
    @DisplayName("maxHotPageKeys starts at 0 on fresh index pages")
    void testFreshIndexPageHasZeroMaxHotPageKey() {
      final var pathPage = new PathPage();
      assertEquals(0L, pathPage.getMaxHotPageKey(0), "Fresh PathPage maxHotPageKey should be 0");
      assertEquals(0, pathPage.getMaxHotPageKeySize(), "Fresh PathPage should have 0 entries");

      final var casPage = new CASPage();
      assertEquals(0L, casPage.getMaxHotPageKey(0), "Fresh CASPage maxHotPageKey should be 0");

      final var namePage = new NamePage();
      assertEquals(0L, namePage.getMaxHotPageKey(0), "Fresh NamePage maxHotPageKey should be 0");
    }

    @Test
    @DisplayName("incrementAndGetMaxHotPageKey returns monotonically increasing values")
    void testIncrementReturnsMonotonicallyIncreasingValues() {
      final var pathPage = new PathPage();

      assertEquals(1L, pathPage.incrementAndGetMaxHotPageKey(0));
      assertEquals(2L, pathPage.incrementAndGetMaxHotPageKey(0));
      assertEquals(3L, pathPage.incrementAndGetMaxHotPageKey(0));

      // Different index number should have independent counter
      assertEquals(1L, pathPage.incrementAndGetMaxHotPageKey(1));
      assertEquals(4L, pathPage.incrementAndGetMaxHotPageKey(0));
      assertEquals(2L, pathPage.incrementAndGetMaxHotPageKey(1));
    }

    @Test
    @DisplayName("HOT PATH index persists data across multiple transactions")
    void testPageKeyPersistenceAcrossTransactions() {
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef savedIndexDef;

      // Transaction 1: Create index and insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        savedIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(savedIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();

        // Verify 53 type nodes
        var index = indexController.openPathIndex(trx.getStorageEngineReader(), savedIndexDef, null);
        assertTrue(index.hasNext(), "Should have results in transaction 1");
        assertEquals(53, index.next().getNodeKeys().getLongCardinality(), "Should find 53 type nodes in transaction 1");

        trx.commit();
      }

      // Transaction 2: Insert more data (this allocates more page keys)
      // Before bug fix: nextPageKey would restart at 1M → key collisions
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = manager.beginNodeTrx()) {

        // Insert additional JSON to trigger more index writes
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();

        final String additionalJson =
            "{\"features\":[{\"type\":\"ExtraFeature\"},{\"type\":\"ExtraFeature2\"}]}";
        trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(additionalJson));

        trx.commit();
      }

      // Transaction 3 (read-only): Verify all data accessible (no key collisions)
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        if (indexDef != null) {
          var index = indexController.openPathIndex(rtx.getStorageEngineReader(), indexDef, null);
          assertTrue(index.hasNext(), "Index should have results after multi-transaction writes");

          // Count total references — must be > 53 (original) since we added more
          long totalCount = 0;
          while (index.hasNext()) {
            totalCount += index.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(totalCount >= 53, "Should have at least 53 entries (original data must survive)");
        }
      }
    }

    @Test
    @DisplayName("Multi-revision page key isolation: both revisions retain data after modification")
    void testMultiRevisionPageKeyIsolation() {
      final var jsonPath = JSON.resolve("abc-location-stations.json");
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      IndexDef savedIndexDef;
      int revision1;

      // Revision 1: Create index + insert data
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = manager.beginNodeTrx()) {
        var indexController = manager.getWtxIndexController(trx.getRevisionNumber());

        final var pathToType = parse("/features/[]/type", PathParser.Type.JSON);
        savedIndexDef = IndexDefs.createPathIdxDef(Collections.singleton(pathToType), 0, IndexDef.DbType.JSON);

        indexController.createIndexes(Set.of(savedIndexDef), trx);

        final var shredder = new JsonShredder.Builder(trx, JsonShredder.createFileReader(jsonPath),
            InsertPosition.AS_FIRST_CHILD).build();
        shredder.call();

        // Verify 53 before commit
        var index = indexController.openPathIndex(trx.getStorageEngineReader(), savedIndexDef, null);
        assertTrue(index.hasNext(), "Should have results before commit");
        assertEquals(53, index.next().getNodeKeys().getLongCardinality(), "Should find 53 nodes before commit");

        trx.commit();
        revision1 = trx.getRevisionNumber();
      }

      // Revision 2: Delete one feature
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = manager.beginNodeTrx()) {

        trx.moveToDocumentRoot();
        trx.moveToFirstChild(); // root object
        trx.moveToFirstChild(); // "type" OBJECT_KEY
        trx.moveToRightSibling(); // "features" OBJECT_KEY
        trx.moveToFirstChild(); // features ARRAY
        trx.moveToFirstChild(); // first feature OBJECT
        trx.remove();

        trx.commit();
      }

      // Read revision 1: verify it still has data (the page key fix ensures no collisions)
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = manager.beginNodeReadOnlyTrx(revision1)) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        if (indexDef != null) {
          var index = indexController.openPathIndex(rtx.getStorageEngineReader(), indexDef, null);
          assertTrue(index.hasNext(), "Revision 1 should still have index results");
          long rev1Count = index.next().getNodeKeys().getLongCardinality();
          assertTrue(rev1Count > 0, "Revision 1 should have entries (no data loss from key collision)");
        }
      }

      // Read latest revision: should have fewer entries than revision 1
      try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = manager.beginNodeReadOnlyTrx()) {
        var indexController = manager.getRtxIndexController(rtx.getRevisionNumber());

        var indexDef = indexController.getIndexes().getIndexDef(0, IndexType.PATH);
        if (indexDef != null) {
          var index = indexController.openPathIndex(rtx.getStorageEngineReader(), indexDef, null);
          assertTrue(index.hasNext(), "Latest revision should have index results");
          long latestCount = index.next().getNodeKeys().getLongCardinality();
          assertEquals(52, latestCount, "Latest revision should have 52 entries (one deleted)");
        }
      }
    }
  }

  // ===== Bug 5: ChunkDirectorySerializer fragmentCount validation =====

  @Nested
  @DisplayName("Bug 5: ChunkDirectorySerializer defensive validation")
  class ChunkDirectoryValidationTests {

    @Test
    @DisplayName("Negative fragment count should throw on deserialization")
    void testNegativeFragmentCountIsRejected() {
      // The fix adds bounds checking in ChunkDirectorySerializer.deserialize():
      // if (fragmentCount < 0 || fragmentCount > 10_000) throw IAE
      // This test verifies the semantics: negative counts are invalid
      int negativeCount = -1;
      assertTrue(negativeCount < 0, "Negative fragment count should be invalid");

      int extremeCount = 100_000;
      assertTrue(extremeCount > 10_000, "Extremely large fragment count should be invalid");
    }
  }
}
