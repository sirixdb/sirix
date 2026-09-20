/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The intent-log probe borrows the async-flush fault hook, which fault-injection tests own. What it
 * counts is proven against a real load by {@code ProjectionLoadPinnedPageBudgetTest}; what is
 * proven here is that borrowing the hook never breaks its owner.
 */
@Isolated
final class IntentLogEpochProbeTest {

  /** A site the probe does not sample, so firing it needs no writer. */
  private static final String UNSAMPLED_SITE = "write";

  @BeforeEach
  @AfterEach
  void clearHook() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
  }

  @Test
  void aDisplacedFaultHookKeepsFiringAndIsHandedBack() {
    final List<String> seenByOwner = new ArrayList<>();
    final BiConsumer<NodeStorageEngineWriter, String> owner = (writer, site) -> seenByOwner.add(site);
    NodeStorageEngineWriter.asyncFlushFaultHook = owner;

    final IntentLogEpochProbe probe = new IntentLogEpochProbe();
    probe.open();
    final BiConsumer<NodeStorageEngineWriter, String> installed = NodeStorageEngineWriter.asyncFlushFaultHook;
    assertNotNull(installed);
    installed.accept(null, UNSAMPLED_SITE);
    probe.close();

    assertEquals(List.of(UNSAMPLED_SITE), seenByOwner, "a fault hook the probe displaced must still be reached");
    assertSame(owner, NodeStorageEngineWriter.asyncFlushFaultHook,
        "closing must restore the displaced hook, or the owner's fault injection is silently disarmed");
  }

  @Test
  void aFaultTheDisplacedHookInjectsStillPropagates() {
    NodeStorageEngineWriter.asyncFlushFaultHook = (writer, site) -> {
      throw new IllegalStateException("injected at " + site);
    };

    final IntentLogEpochProbe probe = new IntentLogEpochProbe();
    probe.open();
    try {
      final BiConsumer<NodeStorageEngineWriter, String> installed = NodeStorageEngineWriter.asyncFlushFaultHook;
      final IllegalStateException injected =
          assertThrows(IllegalStateException.class, () -> installed.accept(null, UNSAMPLED_SITE));
      assertEquals("injected at " + UNSAMPLED_SITE, injected.getMessage());
    } finally {
      probe.close();
    }
  }

  @Test
  void itCannotBeOpenedTwiceAndClosesIdempotently() {
    final IntentLogEpochProbe probe = new IntentLogEpochProbe();
    probe.close();
    probe.open();
    assertThrows(IllegalStateException.class, probe::open);
    probe.close();
    probe.close();

    assertNull(NodeStorageEngineWriter.asyncFlushFaultHook, "a hook that was empty must be empty again");
  }

  @Test
  void itStartsEveryCaptureFromZero() {
    final IntentLogEpochProbe probe = new IntentLogEpochProbe();
    probe.open();
    probe.close();

    assertEquals(4, probe.counters().size());
    assertEquals(0, probe.epochs().read());
    assertEquals(0, probe.spillBatches().read());
    assertEquals(0, probe.spilledPages().read());
    assertEquals(0, probe.pinnedPagesPeak().read());
  }
}
