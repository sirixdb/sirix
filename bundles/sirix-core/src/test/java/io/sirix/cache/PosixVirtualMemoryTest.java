/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.sirix.utils.OS;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.junit.jupiter.api.Test;

/** HOT native-refusal contract, observed through real mmap/madvise calls on the current kernel. */
final class PosixVirtualMemoryTest {
  @Test
  void reservationsAreWritableAndImpossibleReservationsAreRefused() {
    assumeTrue(!OS.isWindows());
    final VirtualMemory memory = PosixVirtualMemory.INSTANCE;
    final MemorySegment segment = memory.tryReserve(4096);
    assertNotNull(segment);
    try {
      assertTrue(memory.tryCommitFresh(segment));
      segment.set(ValueLayout.JAVA_LONG, 0, 42L);
      assertEquals(42L, segment.get(ValueLayout.JAVA_LONG, 0));
      assertNull(memory.tryReserve(Long.MAX_VALUE));
      assertThrows(OutOfMemoryError.class, () -> memory.reserve(Long.MAX_VALUE));
    } finally {
      assertTrue(memory.release(segment));
    }
  }

  @Test
  void populationDistinguishesResidentPagesFromAnUnmappedRange() throws Throwable {
    assumeTrue(OS.isLinux());
    final VirtualMemory memory = PosixVirtualMemory.INSTANCE;
    final MemorySegment segment = memory.reserve(4096);
    try {
      // Capability is an independent syscall observation, never the backend's answer: returning
      // UNAVAILABLE incorrectly must fail the assertion below, rather than skip the test.
      final Linker linker = Linker.nativeLinker();
      final MethodHandle madvise =
          linker.downcallHandle(linker.defaultLookup().find("madvise").orElseThrow(), FunctionDescriptor.of(
              ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
      assumeTrue((int) madvise.invokeExact(segment, 4096L, 23) == 0,
          "kernel supports MADV_POPULATE_WRITE (Linux ABI advice 23)");
      assertEquals(VirtualMemory.POPULATED, memory.tryPopulate(segment));
      // Linux madvise reports ENOMEM for an unmapped range. Page zero is never mapped by Sirix;
      // the kernel inspects the mapping, and this test never dereferences the null address.
      final MemorySegment unmapped = MemorySegment.NULL.reinterpret(4096);
      assertEquals(VirtualMemory.POPULATE_REFUSED, memory.tryPopulate(unmapped));
      assertThrows(OutOfMemoryError.class, () -> memory.populate(unmapped));
    } finally {
      assertTrue(memory.release(segment));
    }
  }
}
