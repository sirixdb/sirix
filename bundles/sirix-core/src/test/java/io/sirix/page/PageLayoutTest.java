package io.sirix.page;

import io.sirix.node.LE;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Salt initialization and speculative selection share the same header contract. */
final class PageLayoutTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void neighboringSaltsCannotReconstructTheRemovedSalt(final boolean disposable) {
    final long[] predictable = new long[4];
    for (int i = 0; i < predictable.length; i++) {
      predictable[i] = ThreadLocalRandom.current().nextLong();
    }
    assertEquals(unmixMurmur(predictable[1]), recoveredState(predictable, false));
    final SplittableRandom split = new SplittableRandom(17);
    for (int i = 0; i < predictable.length; i++) {
      predictable[i] = split.nextLong();
    }
    assertEquals(unmixStafford(predictable[1]), recoveredState(predictable, true));
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment header = arena.allocate(32);
      final long[] salts = new long[4];
      for (int i = 0; i < salts.length; i++) {
        header.set(LE.LONG, 24, 0L);
        if (disposable) {
          salts[i] = PageLayout.saltForWrite(header, true);
          assertEquals(0L, header.get(LE.LONG, 24));
        } else {
          PageLayout.initializeSalt(header, true);
          salts[i] = header.get(LE.LONG, 24);
        }
      }
      assertNotEquals(unmixMurmur(salts[1]), recoveredState(salts, false));
      assertNotEquals(unmixStafford(salts[1]), recoveredState(salts, true));
    }
  }

  private static long recoveredState(final long[] salts, final boolean stafford) {
    final long first = stafford
        ? unmixStafford(salts[0])
        : unmixMurmur(salts[0]);
    final long third = stafford
        ? unmixStafford(salts[2])
        : unmixMurmur(salts[2]);
    final long fourth = stafford
        ? unmixStafford(salts[3])
        : unmixMurmur(salts[3]);
    return first + (fourth - third);
  }

  private static long unmixMurmur(long value) {
    value = (value ^ (value >>> 33)) * 0x9cb4b2f8129337dbL;
    value = (value ^ (value >>> 33)) * 0x4f74430c22a54005L;
    return value ^ (value >>> 33);
  }

  private static long unmixStafford(long value) {
    value = (value ^ (value >>> 31) ^ (value >>> 62)) * 0x319642b2d24d8ec3L;
    value = (value ^ (value >>> 27) ^ (value >>> 54)) * 0x96de1b173f119089L;
    return value ^ (value >>> 30) ^ (value >>> 60);
  }

  @Test
  void zeroDrawUsesOneWithoutDiscardingOtherSaltBits() {
    assertEquals(1L, PageLayout.nonzeroSalt(0L));
    for (final long value : new long[] {1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
      assertEquals(value, PageLayout.nonzeroSalt(value));
    }
  }

  @Test
  void initializationPreservesEveryExistingNonzeroSaltAndDeterministicModeClearsIt() {
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment header = arena.allocate(32);
      for (final long value : new long[] {1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
        header.set(LE.LONG, 24, value);
        PageLayout.initializeSalt(header, true);
        assertEquals(value, header.get(LE.LONG, 24));
        PageLayout.initializeSalt(header, false);
        assertEquals(0L, header.get(LE.LONG, 24));
      }
      PageLayout.initializeSalt(header, true);
      assertNotEquals(0L, header.get(LE.LONG, 24));
    }
  }

}
