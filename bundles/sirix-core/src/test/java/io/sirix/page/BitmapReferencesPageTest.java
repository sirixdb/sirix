/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Test the {@link BitmapReferencesPage}, specifically the optimized index() method
 * that uses POPCNT-based counting for O(offset/64) complexity.
 *
 * @author Johannes Lichtenberger
 */
@SuppressWarnings("resource")  // BitmapReferencesPage.close() only clears collections, GC handles cleanup
public final class BitmapReferencesPageTest {

  @Nested
  class IndexCalculationTests {

    /**
     * Test index calculation at offset 0 (first position).
     * No bits before position 0, so index should be 0.
     */
    @Test
    void testIndexAtOffsetZero() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Set reference at offset 0
      final var ref = page.getOrCreateReference(0);
      assertNotNull(ref);
      
      // Verify we can retrieve it
      final var retrieved = page.getOrCreateReference(0);
      assertEquals(ref, retrieved);
    }

    /**
     * Test index calculation at word boundaries (offset 63, 64, 127, 128).
     * These are critical boundaries where the algorithm transitions between words.
     */
    @ParameterizedTest
    @ValueSource(ints = {63, 64, 127, 128, 191, 192, 255, 256})
    void testIndexAtWordBoundaries(int offset) {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Set reference at the boundary offset
      final var ref = page.getOrCreateReference(offset);
      assertNotNull(ref);
      ref.setLogKey(offset);  // Mark it for identification
      
      // Verify we can retrieve it with correct value
      final var retrieved = page.getOrCreateReference(offset);
      assertEquals(offset, retrieved.getLogKey());
    }

    /**
     * Test index calculation at maximum offset (1023 for INP_REFERENCE_COUNT = 1024).
     */
    @Test
    void testIndexAtMaxOffset() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Set reference at max offset
      final int maxOffset = Constants.INP_REFERENCE_COUNT - 1;
      final var ref = page.getOrCreateReference(maxOffset);
      assertNotNull(ref);
      ref.setLogKey(maxOffset);
      
      // Verify retrieval
      final var retrieved = page.getOrCreateReference(maxOffset);
      assertEquals(maxOffset, retrieved.getLogKey());
    }

    /**
     * Test that index calculation works correctly with sparse references.
     * Only even offsets are populated.
     */
    @Test
    void testIndexWithSparsePopulation() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Populate every 100th offset
      for (int i = 0; i < 1000; i += 100) {
        final var ref = page.getOrCreateReference(i);
        assertNotNull(ref);
        ref.setLogKey(i);
      }
      
      // Verify each can be retrieved correctly
      for (int i = 0; i < 1000; i += 100) {
        final var retrieved = page.getOrCreateReference(i);
        assertEquals(i, retrieved.getLogKey(), "Failed at offset " + i);
      }
    }

    /**
     * Test that index calculation works correctly with dense references.
     * All offsets from 0 to 99 are populated.
     */
    @Test
    void testIndexWithDensePopulation() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Populate first 100 offsets
      for (int i = 0; i < 100; i++) {
        final var ref = page.getOrCreateReference(i);
        assertNotNull(ref);
        ref.setLogKey(i);
      }
      
      // Verify each can be retrieved correctly
      for (int i = 0; i < 100; i++) {
        final var retrieved = page.getOrCreateReference(i);
        assertEquals(i, retrieved.getLogKey(), "Failed at offset " + i);
      }
    }

    /**
     * Test that the references list size equals the number of set bits.
     */
    @Test
    void testReferencesListSizeMatchesBitmapCardinality() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Add references at various offsets
      int[] offsets = {0, 5, 63, 64, 100, 200, 500, 999};
      for (int offset : offsets) {
        page.getOrCreateReference(offset);
      }
      
      assertEquals(offsets.length, page.getReferences().size());
    }
  }

  @Nested
  class SetOrCreateReferenceTests {

    /**
     * Test that setOrCreateReference correctly adds new references.
     */
    @Test
    void testSetOrCreateReferenceAddsNewEntry() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      final var ref = new PageReference();
      ref.setLogKey(42);
      
      page.setOrCreateReference(100, ref);
      
      final var retrieved = page.getOrCreateReference(100);
      assertEquals(42, retrieved.getLogKey());
    }

    /**
     * Test that setOrCreateReference correctly updates existing references.
     */
    @Test
    void testSetOrCreateReferenceUpdatesExistingEntry() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // First, create a reference
      final var initialRef = page.getOrCreateReference(100);
      initialRef.setLogKey(1);
      
      // Now update it
      final var newRef = new PageReference();
      newRef.setLogKey(2);
      page.setOrCreateReference(100, newRef);
      
      // Verify update
      final var retrieved = page.getOrCreateReference(100);
      assertEquals(2, retrieved.getLogKey());
    }

    /**
     * Test that setOrCreateReference invalidates the cache properly.
     * After adding a new reference, subsequent lookups should still work.
     */
    @Test
    void testCacheInvalidationOnSetOrCreate() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Add first reference and force cache creation via lookup
      page.getOrCreateReference(0).setLogKey(0);
      page.getOrCreateReference(0);  // This caches the words array
      
      // Add another reference (should invalidate cache)
      page.getOrCreateReference(64).setLogKey(64);
      
      // Verify both are still accessible
      assertEquals(0, page.getOrCreateReference(0).getLogKey());
      assertEquals(64, page.getOrCreateReference(64).getLogKey());
    }
  }

  @Nested
  class ThresholdTests {

    /**
     * Test that createNewReference returns null when threshold is reached.
     */
    @Test
    void testThresholdBehavior() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Add references up to threshold - 1
      for (int i = 0; i < BitmapReferencesPage.THRESHOLD - 1; i++) {
        final var ref = page.getOrCreateReference(i);
        assertNotNull(ref, "Reference should not be null before threshold at offset " + i);
      }
      
      // Adding one more should return null (threshold reached)
      final var lastRef = page.getOrCreateReference(BitmapReferencesPage.THRESHOLD - 1);
      assertNull(lastRef, "Reference should be null when threshold is reached");
    }
  }

  @Nested
  class CloneTests {

    /**
     * Test that cloning preserves all references correctly.
     */
    @Test
    void testClonePreservesReferences() {
      final var original = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Add some references
      int[] offsets = {0, 63, 64, 127, 500, 999};
      for (int offset : offsets) {
        original.getOrCreateReference(offset).setLogKey(offset);
      }
      
      // Clone
      final var clone = new BitmapReferencesPage(original, original.getBitmap());
      
      // Verify clone is independent
      assertNotSame(original, clone);
      
      // Verify all references are preserved
      for (int offset : offsets) {
        assertEquals(offset, clone.getOrCreateReference(offset).getLogKey());
      }
    }
  }

  @Nested  
  class EdgeCaseTests {

    /**
     * Test behavior when offset is beyond the bitmap's current size
     * but still within the valid range.
     */
    @Test
    void testLargeOffsetWithEmptyBitmap() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Access a large offset directly (no prior entries)
      final var ref = page.getOrCreateReference(900);
      assertNotNull(ref);
      ref.setLogKey(900);
      
      assertEquals(900, page.getOrCreateReference(900).getLogKey());
      assertEquals(1, page.getReferences().size());
    }

    /**
     * Test inserting references in reverse order.
     */
    @Test
    void testReverseOrderInsertion() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Insert in reverse order
      for (int i = 99; i >= 0; i--) {
        page.getOrCreateReference(i).setLogKey(i);
      }
      
      // Verify all are accessible
      for (int i = 0; i < 100; i++) {
        assertEquals(i, page.getOrCreateReference(i).getLogKey());
      }
    }

    /**
     * Test alternating pattern (every other offset).
     */
    @Test
    void testAlternatingPattern() {
      final var page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
      
      // Set every other offset
      for (int i = 0; i < 200; i += 2) {
        page.getOrCreateReference(i).setLogKey(i);
      }
      
      // Verify
      for (int i = 0; i < 200; i += 2) {
        assertEquals(i, page.getOrCreateReference(i).getLogKey());
      }
      
      assertEquals(100, page.getReferences().size());
    }
  }
}

