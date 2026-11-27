/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.cache.RecordingVirtualMemory.Failure;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MemTestOracleRegressionTest {

  @ParameterizedTest
  @ValueSource(longs = {0x5A5A_C3C3_1234_5678L, 0x5A5A_C3C3_1234_5679L, 0x5A5A_C3C3_1234_567AL})
  @DisplayName("MEM-106 oracle accepts the last pattern and zeros, and rejects foreign bytes")
  void releasedContentsAreComparedToTheLastWrittenPattern(final long lastWritten) {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment words = arena.allocate(4L * Long.BYTES);
      words.set(ValueLayout.JAVA_LONG, 0L, 0L);
      words.set(ValueLayout.JAVA_LONG, Long.BYTES, lastWritten);
      words.set(ValueLayout.JAVA_LONG, 2L * Long.BYTES, lastWritten);
      words.set(ValueLayout.JAVA_LONG, 3L * Long.BYTES, -1L);
      assertArrayEquals(new long[] {1L, 2L, 1L}, PlatformReleaseSemanticsTest.classify(words, lastWritten));
    }
  }

  @Test
  @DisplayName("MEM-107 oracle rejects calls to a released region while another region stays live")
  void releasedTargetsAreRejectedBeforeAllRegionsAreReleased() {
    final RecordingVirtualMemory recording = RecordingVirtualMemory.withOsPageBytes(4096L);
    recording.withoutMapping();
    final VirtualMemory vm = recording.virtualMemory();
    final MemorySegment first = vm.reserve(65536L);
    final MemorySegment second = vm.reserve(65536L);
    vm.release(first);
    final MemorySegment stale = first.asSlice(0L, 4096L);
    assertThrows(AssertionError.class, () -> vm.commitFresh(stale));
    assertThrows(AssertionError.class, () -> vm.populate(stale));
    assertThrows(AssertionError.class, () -> vm.release(first));
    assertEquals(3, recording.callsAfterRegionsReleased());
    assertDoesNotThrow(() -> vm.commitFresh(second.asSlice(0L, 4096L)));
    assertDoesNotThrow(() -> vm.populate(second.asSlice(0L, 4096L)));
    vm.release(second);
    assertEquals(3, recording.callsAfterRegionsReleased());
  }

  @ParameterizedTest
  @ValueSource(longs = {16384L, 65536L})
  @DisplayName("MEM-104 recording boundary simulates release and recharge for its reported page geometry")
  void simulatedPrimitivesPreserveBytesAndHonorInjectedFailures(final long pageBytes) throws Throwable {
    final RecordingVirtualMemory recording = RecordingVirtualMemory.withOsPageBytes(pageBytes);
    final VirtualMemory vm = recording.virtualMemory();
    final MemorySegment region = vm.reserve(3L * pageBytes);
    final MemorySegment run = region.asSlice(pageBytes, pageBytes);
    final byte[] expected = new byte[(int) pageBytes];
    Arrays.fill(expected, (byte) 0x5A);
    try {
      vm.commitFresh(region);
      run.copyFrom(MemorySegment.ofArray(expected));
      assertTrue(invokePrimitive(recording, MemPlatformSeam.RELEASE_RUN, run));
      assertTrue(invokePrimitive(recording, MemPlatformSeam.RECHARGE, run));
      assertArrayEquals(expected, run.toArray(ValueLayout.JAVA_BYTE), "simulated OS calls touch no bytes");
      for (final Failure mode : Failure.values()) {
        recording.failNextReleaseRuns(1, mode);
        recording.failNextRecharges(1, mode);
        if (mode == Failure.RETURN_FAILURE) {
          assertFalse(invokePrimitive(recording, MemPlatformSeam.RELEASE_RUN, run));
          assertFalse(invokePrimitive(recording, MemPlatformSeam.RECHARGE, run));
        } else {
          assertThrows(IllegalStateException.class, () -> invokePrimitive(recording, MemPlatformSeam.RELEASE_RUN, run));
          assertThrows(IllegalStateException.class, () -> invokePrimitive(recording, MemPlatformSeam.RECHARGE, run));
        }
        assertArrayEquals(expected, run.toArray(ValueLayout.JAVA_BYTE), "injected failures touch no bytes");
      }
    } finally {
      vm.release(region);
    }
    assertThrows(AssertionError.class, () -> invokePrimitive(recording, MemPlatformSeam.RELEASE_RUN, run));
    assertThrows(AssertionError.class, () -> invokePrimitive(recording, MemPlatformSeam.RECHARGE, run));
    assertEquals(2, recording.callsAfterRegionsReleased());
  }

  @Test
  @DisplayName("MEM-301 oracle cannot pay a pool account with another owner's charge")
  void unrelatedChargesCannotCoverThePoolAccount() {
    assertThrows(AssertionError.class,
        () -> CompactImageAccountTest.assertPoolChargeCoverage(65536L, 65536L, 4096L, "owned frames"));
    assertThrows(AssertionError.class,
        () -> CompactImageAccountTest.assertPoolChargeCoverage(73728L, 73728L, 4096L, "frames and scratch"));
    assertDoesNotThrow(
        () -> CompactImageAccountTest.assertPoolChargeCoverage(73728L, 65536L, 4096L, "separate pool reservation"));
    assertDoesNotThrow(
        () -> CompactImageAccountTest.assertPoolChargeCoverage(73728L, 65536L, 8192L, "fully used pool reservation"));
  }

  private static boolean invokePrimitive(final RecordingVirtualMemory recording, final String operation,
      final MemorySegment run) throws Throwable {
    final Method method = ReleasePrimitives.class.getMethod(operation, MemorySegment.class);
    return (boolean) Proxy.getInvocationHandler(recording.virtualMemory())
                          .invoke(recording.virtualMemory(), method, new Object[] {run});
  }

  private interface ReleasePrimitives {
    boolean releaseRun(MemorySegment run);

    boolean recharge(MemorySegment run);
  }
}
