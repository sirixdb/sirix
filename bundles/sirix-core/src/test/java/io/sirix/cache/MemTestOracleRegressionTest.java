/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
