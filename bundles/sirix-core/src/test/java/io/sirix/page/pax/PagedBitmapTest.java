package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PagedBitmap}. Covers rank correctness at word boundaries,
 * edge cases, and a randomized cross-check against {@link BitSet}.
 */
@DisplayName("PagedBitmap")
final class PagedBitmapTest {

  @Nested
  @DisplayName("construction")
  final class Construction {

    @Test
    @DisplayName("rejects non-multiple-of-64 slot counts")
    void rejectsMisalignedSlotCounts() {
      assertThrows(IllegalArgumentException.class, () -> new PagedBitmap(63));
      assertThrows(IllegalArgumentException.class, () -> new PagedBitmap(65));
      assertThrows(IllegalArgumentException.class, () -> new PagedBitmap(0));
      assertThrows(IllegalArgumentException.class, () -> new PagedBitmap(-64));
    }

    @Test
    @DisplayName("accepts common slot widths")
    void acceptsCommonSlotWidths() {
      for (final int n : new int[] { 64, 128, 256, 512, 1024, 4096 }) {
        final PagedBitmap b = new PagedBitmap(n);
        assertEquals(n, b.slotCount());
        assertEquals(n >>> 6, b.wordCount());
      }
    }
  }

  @Nested
  @DisplayName("basic set/get/clear")
  final class BasicOps {

    @Test
    @DisplayName("round-trips single-bit writes")
    void roundTripsSingleBit() {
      final PagedBitmap b = new PagedBitmap(1024);
      for (final int slot : new int[] { 0, 1, 63, 64, 65, 127, 128, 1023 }) {
        assertFalse(b.get(slot));
        b.set(slot);
        assertTrue(b.get(slot));
        b.clear(slot);
        assertFalse(b.get(slot));
      }
    }

    @Test
    @DisplayName("reset clears all bits")
    void resetClears() {
      final PagedBitmap b = new PagedBitmap(128);
      for (int i = 0; i < 128; i += 3) {
        b.set(i);
      }
      b.reset();
      for (int i = 0; i < 128; i++) {
        assertFalse(b.get(i));
      }
    }
  }

  @Nested
  @DisplayName("rank")
  final class Rank {

    @Test
    @DisplayName("empty bitmap has zero population")
    void emptyBitmap() {
      final PagedBitmap b = new PagedBitmap(1024);
      b.seal();
      assertEquals(0, b.popcount());
      assertEquals(0, b.rank(0));
      assertEquals(0, b.rank(1024));
    }

    @Test
    @DisplayName("full bitmap ranks are the identity")
    void fullBitmap() {
      final PagedBitmap b = new PagedBitmap(256);
      for (int i = 0; i < 256; i++) {
        b.set(i);
      }
      b.seal();
      assertEquals(256, b.popcount());
      for (int i = 0; i <= 256; i++) {
        assertEquals(i, b.rank(i), "rank(" + i + ")");
      }
    }

    @Test
    @DisplayName("rank is correct at every word boundary")
    void boundaries() {
      final PagedBitmap b = new PagedBitmap(512);
      // one bit per word, set at word boundary
      for (int w = 0; w < 8; w++) {
        b.set(w * 64);
      }
      b.seal();
      assertEquals(8, b.popcount());
      // rank at the boundary itself is strict-less-than, so the bit *at* that
      // slot is not counted
      assertEquals(0, b.rank(0));
      assertEquals(1, b.rank(64));
      assertEquals(2, b.rank(128));
      assertEquals(3, b.rank(192));
      assertEquals(8, b.rank(512));
      // rank-1 is strict-less-than, so once we cross past the bit at slot 64,
      // it's included in the count. Between bits 64 and 128 the rank stays 2.
      assertEquals(2, b.rank(65));
      assertEquals(2, b.rank(127));
      assertEquals(3, b.rank(129));
    }

    @Test
    @DisplayName("rank at slotCount equals total popcount")
    void rankAtUpperBound() {
      final PagedBitmap b = new PagedBitmap(1024);
      for (int i = 0; i < 1024; i += 7) {
        b.set(i);
      }
      b.seal();
      assertEquals(b.popcount(), b.rank(1024));
    }

    @Test
    @DisplayName("matches BitSet across random populations")
    void randomizedAgainstBitSet() {
      final SplittableRandom rng = new SplittableRandom(0xBE1ABE11L);
      for (int trial = 0; trial < 50; trial++) {
        final int slotCount = 64 * (1 + rng.nextInt(32)); // up to 2048
        final PagedBitmap b = new PagedBitmap(slotCount);
        final BitSet ref = new BitSet(slotCount);
        // randomized population
        final double density = rng.nextDouble();
        for (int i = 0; i < slotCount; i++) {
          if (rng.nextDouble() < density) {
            b.set(i);
            ref.set(i);
          }
        }
        b.seal();
        // check rank at every slot plus the upper bound
        for (int i = 0; i <= slotCount; i++) {
          final int expected = ref.get(0, i).cardinality();
          assertEquals(expected, b.rank(i), "trial " + trial + " rank(" + i + ")");
        }
        assertEquals(ref.cardinality(), b.popcount(), "trial " + trial + " popcount");
      }
    }
  }

  @Nested
  @DisplayName("select")
  final class Select {

    @Test
    @DisplayName("returns the slot of the k-th set bit")
    void basicSelect() {
      final PagedBitmap b = new PagedBitmap(128);
      b.set(3);
      b.set(17);
      b.set(64);
      b.set(127);
      b.seal();
      assertEquals(3, b.select(0));
      assertEquals(17, b.select(1));
      assertEquals(64, b.select(2));
      assertEquals(127, b.select(3));
      assertEquals(-1, b.select(4));
      assertEquals(-1, b.select(-1));
    }

    @Test
    @DisplayName("select is the inverse of rank")
    void selectInvertsRank() {
      final PagedBitmap b = new PagedBitmap(256);
      final SplittableRandom rng = new SplittableRandom(0xC0FFEEL);
      for (int i = 0; i < 256; i++) {
        if (rng.nextBoolean()) {
          b.set(i);
        }
      }
      b.seal();
      for (int k = 0; k < b.popcount(); k++) {
        final int slot = b.select(k);
        assertTrue(b.get(slot));
        assertEquals(k, b.rank(slot));
      }
    }
  }

  @Nested
  @DisplayName("nextSetBit")
  final class NextSetBit {

    @Test
    @DisplayName("returns -1 on empty bitmap")
    void emptyBitmap() {
      final PagedBitmap b = new PagedBitmap(128);
      assertEquals(-1, b.nextSetBit(0));
      assertEquals(-1, b.nextSetBit(127));
    }

    @Test
    @DisplayName("returns current slot when already set")
    void inclusiveOfStart() {
      final PagedBitmap b = new PagedBitmap(128);
      b.set(42);
      assertEquals(42, b.nextSetBit(42));
      assertEquals(42, b.nextSetBit(0));
      assertEquals(-1, b.nextSetBit(43));
    }

    @Test
    @DisplayName("walks through all set bits in order")
    void walksInOrder() {
      final PagedBitmap b = new PagedBitmap(1024);
      final int[] populated = { 5, 63, 64, 100, 511, 512, 1000, 1023 };
      for (final int s : populated) {
        b.set(s);
      }
      int cursor = 0;
      for (final int expected : populated) {
        final int found = b.nextSetBit(cursor);
        assertEquals(expected, found);
        cursor = found + 1;
      }
      assertEquals(-1, b.nextSetBit(cursor));
    }
  }
}
