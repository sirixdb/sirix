/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.junit.jupiter.api.Test;

/**
 * Native boundary oracle: VirtualAlloc returns NULL on refusal, the reserved base on success, and
 * receives MEM_RESERVE (0x2000) or MEM_COMMIT (0x1000) with PAGE_READWRITE (0x04). HOT
 * specification section 3 requires only kernel refusal to become a nullable admission. These tests
 * execute the backend, and reject swapped flags, lost addresses and hidden heap errors.
 */
final class WindowsVirtualMemoryTest {
  @Test
  void reservationPreservesTheAddressAndTranslatesOnlyNativeRefusal() throws ReflectiveOperationException {
    final Calls calls = new Calls();
    final WindowsVirtualMemory memory = new WindowsVirtualMemory(calls.handle(), null);
    calls.result = MemorySegment.ofAddress(0x10000);
    final MemorySegment reserved = memory.tryReserve(8192);
    assertEquals(0x10000L, reserved.address());
    assertEquals(8192L, reserved.byteSize());
    calls.assertRequest(0L, 8192L, 0x2000L);
    assertEquals(reserved.address(), memory.reserve(8192).address());
    calls.result = MemorySegment.NULL;
    assertNull(memory.tryReserve(8192));
    assertThrows(OutOfMemoryError.class, () -> memory.reserve(8192));
    calls.failure = new OutOfMemoryError("Java heap");
    assertSame(calls.failure, assertThrows(OutOfMemoryError.class, () -> memory.tryReserve(8192)));
    assertSame(calls.failure, assertThrows(OutOfMemoryError.class, () -> memory.reserve(8192)));
  }

  @Test
  void commitmentPreservesTheSlotAndTranslatesOnlyNativeRefusal() throws ReflectiveOperationException {
    final Calls calls = new Calls();
    final WindowsVirtualMemory memory = new WindowsVirtualMemory(calls.handle(), null);
    final MemorySegment slot = MemorySegment.ofAddress(0x10000).reinterpret(4096);
    calls.result = slot;
    assertTrue(memory.tryCommitFresh(slot));
    calls.assertRequest(slot.address(), 4096L, 0x1000L);
    assertTrue(memory.commitFresh(slot));
    calls.result = MemorySegment.NULL;
    assertFalse(memory.tryCommitFresh(slot));
    assertThrows(OutOfMemoryError.class, () -> memory.commitFresh(slot));
    calls.failure = new OutOfMemoryError("Java heap");
    assertSame(calls.failure, assertThrows(OutOfMemoryError.class, () -> memory.tryCommitFresh(slot)));
    assertSame(calls.failure, assertThrows(OutOfMemoryError.class, () -> memory.commitFresh(slot)));
  }

  private static final class Calls {
    private MemorySegment result = MemorySegment.NULL;
    private OutOfMemoryError failure;
    private long address;
    private long bytes;
    private long flags;
    private long protection;

    MethodHandle handle() throws ReflectiveOperationException {
      return MethodHandles.lookup()
                          .findVirtual(Calls.class, "allocate",
                              MethodType.methodType(MemorySegment.class, MemorySegment.class, long.class, long.class,
                                  long.class))
                          .bindTo(this);
    }

    MemorySegment allocate(final MemorySegment base, final long size, final long allocationType, final long protect) {
      address = base.address();
      bytes = size;
      flags = allocationType;
      protection = protect;
      if (failure != null) {
        throw failure;
      }
      return result;
    }

    void assertRequest(final long expectedAddress, final long expectedBytes, final long expectedFlags) {
      assertEquals(expectedAddress, address);
      assertEquals(expectedBytes, bytes);
      assertEquals(expectedFlags, flags);
      assertEquals(0x04L, protection);
    }
  }
}
