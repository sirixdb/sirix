/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.MemorySegment;
import org.junit.jupiter.api.Test;

/** HOT packed-image native refusal contract: unavailable is lazy, refusal is fatal to populate. */
final class VirtualMemoryTest {
  @Test
  void populationStatusHasThreeDistinctMeanings() {
    final StatusMemory memory = new StatusMemory();
    assertFalse(memory.populate(MemorySegment.NULL));
    memory.status = VirtualMemory.POPULATED;
    assertTrue(memory.populate(MemorySegment.NULL));
    memory.status = VirtualMemory.POPULATE_REFUSED;
    assertThrows(OutOfMemoryError.class, () -> memory.populate(MemorySegment.NULL));
  }

  private static final class StatusMemory implements VirtualMemory {
    private int status = POPULATE_UNAVAILABLE;

    @Override
    public int tryPopulate(final MemorySegment segment) {
      return status;
    }

    @Override
    public MemorySegment reserve(final long bytes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MemorySegment tryReserve(final long bytes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean commitFresh(final MemorySegment slot) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryCommitFresh(final MemorySegment slot) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void discardToZeros(final MemorySegment segment) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean release(final MemorySegment region) {
      throw new UnsupportedOperationException();
    }
  }
}
