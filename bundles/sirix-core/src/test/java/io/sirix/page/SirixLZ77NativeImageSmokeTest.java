/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Native-image entry point for the optional decoder's resource, critical stub and tail contract.
 * Run the native executable with {@code -Dsirix.lz77Codec.diag.counters=true}. Unlike a JVM-only
 * round trip, this fails when missing reachability metadata silently selects the Java fallback.
 */
public final class SirixLZ77NativeImageSmokeTest {

  /** Native smoke checks require the library; unsupported JVM test platforms may skip it. */
  public static void main(final String[] args) throws Exception {
    assertTrue(SirixLZ77NativeDecoder.isAvailable(), "native decoder must be available in this image");
    assertTrue(Boolean.getBoolean("sirix.lz77Codec.diag.counters"), "native dispatch must be observable");
    new SirixLZ77NativeImageSmokeTest().heapAndNativeDestinationsUseTheCriticalStub();
    new SirixLZ77NativeImageSmokeTest().inaccessibleOutputsFailBeforeEnteringNativeCode();
    final SirixLZ77NativeContractTest contract = new SirixLZ77NativeContractTest();
    contract.decodesCorrectlyAndWritesNothingPastTheOutputAtEverySlack();
    contract.honoursOutputOffsetWithoutDisturbingWhatPrecedesIt();
    System.out.println("Native LZ77 image: resource, critical heap/native calls, slack and canaries PASS");
  }

  @Test
  void inaccessibleOutputsFailBeforeEnteringNativeCode() throws Exception {
    Assumptions.assumeTrue(SirixLZ77NativeDecoder.isAvailable(), "optional decoder unavailable on this platform");
    final byte[] expected = new byte[256];
    Arrays.fill(expected, (byte) 7);
    final byte[] encoded =
        new byte[SirixLZ77Codec.maxEncodedSize(expected.length) + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
    final int length = SirixLZ77Codec.encode(MemorySegment.ofArray(expected), 0L, expected.length, encoded, 0);
    final int capacity = expected.length + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
    final MemorySegment closed;
    try (final Arena arena = Arena.ofConfined(); final var worker = Executors.newSingleThreadExecutor()) {
      closed = arena.allocate(capacity);
      closed.fill((byte) 0xA5);
      final RuntimeException failure = worker
                                             .submit(() -> assertThrows(RuntimeException.class,
                                                 () -> SirixLZ77NativeDecoder.decode(encoded, 0, length, closed, 0)))
                                             .get(10, TimeUnit.SECONDS);
      assertInstanceOf(WrongThreadException.class, failure.getCause());
      for (int offset = 0; offset < capacity; offset++) {
        assertEquals((byte) 0xA5, closed.get(ValueLayout.JAVA_BYTE, offset));
      }
    }
    final RuntimeException failure =
        assertThrows(RuntimeException.class, () -> SirixLZ77NativeDecoder.decode(encoded, 0, length, closed, 0));
    assertInstanceOf(IllegalStateException.class, failure.getCause());
  }

  @Test
  void heapAndNativeDestinationsUseTheCriticalStub() {
    Assumptions.assumeTrue(SirixLZ77NativeDecoder.isAvailable(), "optional decoder unavailable on this platform");
    Assumptions.assumeTrue(Boolean.getBoolean("sirix.lz77Codec.diag.counters"), "dispatch counters disabled");
    final byte[] expected = new byte[32 * 1024];
    for (int index = 0; index < expected.length; index++) {
      expected[index] = (byte) ((index / 31) & 7);
    }
    final byte[] encoded =
        new byte[SirixLZ77Codec.maxEncodedSize(expected.length) + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
    final int length = SirixLZ77Codec.encode(MemorySegment.ofArray(expected), 0L, expected.length, encoded, 0);
    final int offset = 7;
    final int visible = offset + expected.length + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK;
    final byte canary = (byte) 0xA5;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment[] backings = {MemorySegment.ofArray(new byte[visible + 128]), arena.allocate(visible + 128)};
      for (final MemorySegment backing : backings) {
        backing.fill(canary);
        final long nativeBefore = SirixLZ77Codec.getNativeCallCount();
        final long javaBefore = SirixLZ77Codec.getJavaCallCount();
        assertEquals(expected.length, SirixLZ77Codec.decode(encoded, 0, length, backing.asSlice(0, visible), offset));
        assertEquals(1, SirixLZ77Codec.getNativeCallCount() - nativeBefore, "must use the native critical stub");
        assertEquals(0, SirixLZ77Codec.getJavaCallCount() - javaBefore, "must not silently fall back");
        assertArrayEquals(expected, backing.asSlice(offset, expected.length).toArray(ValueLayout.JAVA_BYTE));
        final byte[] before = new byte[offset];
        final byte[] after = new byte[128];
        Arrays.fill(before, canary);
        Arrays.fill(after, canary);
        assertArrayEquals(before, backing.asSlice(0, offset).toArray(ValueLayout.JAVA_BYTE));
        assertArrayEquals(after, backing.asSlice(visible, 128).toArray(ValueLayout.JAVA_BYTE));
      }
    }
  }
}
