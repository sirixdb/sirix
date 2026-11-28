/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.List;

/**
 * MEM-101 — Accounting identity, as a jqwik property: the same oracle as
 * {@code FrameBudgetAccountingTest} ({@link FrameBudgetSequence}) over generated operation
 * sequences, class choices and OS page sizes. Shrinking reduces a failing sequence to the shortest
 * operation list that breaks an identity or the limit.
 */
final class FrameBudgetAccountingProperty {

  @MemPlannedProperty(step = 1, value = {"MEM-101"})
  @Property(tries = 40)
  void identitiesHoldForEveryGeneratedSequence(@ForAll("operations") final List<Integer> operations,
      @ForAll("pageSizes") final long osPageBytes, @ForAll final long seed) {
    final int[] codes = new int[operations.size()];
    for (int i = 0; i < codes.length; i++) {
      codes[i] = operations.get(i);
    }
    FrameBudgetSequence.run(seed, codes, osPageBytes);
  }

  @Provide
  Arbitrary<List<Integer>> operations() {
    return Arbitraries.integers().between(0, FrameBudgetSequence.OPERATIONS - 1).list().ofMinSize(1).ofMaxSize(150);
  }

  @Provide
  Arbitrary<Long> pageSizes() {
    return Arbitraries.of(4096L, 16384L, 65536L);
  }
}
