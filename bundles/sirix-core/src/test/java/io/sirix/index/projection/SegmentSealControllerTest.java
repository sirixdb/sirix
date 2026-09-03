/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When a segment's dictionary may be sealed. The rule has to be conservative in one direction only:
 * sealing a segment that can still receive a value loses that value from the dictionary its own page
 * points at, while sealing late costs residency and nothing else.
 */
final class SegmentSealControllerTest {

  @Test
  @DisplayName("a segment with a page still encoding is never sealable, however far the writer has moved on")
  void anOutstandingPageHoldsItsSegmentOpen() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0);
    controller.adopted(0);
    controller.adopted(1);
    controller.adopted(2); // the writer is well past segment 0

    assertEquals(List.of(), controller.takeSealable(), "segment 0 still has two pages encoding");
    controller.encoded(0);
    assertEquals(1, controller.outstandingIn(0));
    assertEquals(List.of(), controller.takeSealable(), "one page is still out");
    controller.encoded(0);
    assertEquals(List.of(0L), controller.takeSealable(), "now, and only now");
    assertTrue(controller.isSealed(0));
  }

  @Test
  @DisplayName("THE SEALING HAZARD: a page stranded in the flush queue keeps its segment open")
  void aStrandedPageKeepsItsSegmentOpen() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0); // this one will sit in the queue for a long time
    for (long segment = 1; segment <= 5; segment++) {
      controller.adopted(segment);
      controller.encoded(segment);
    }
    // Five later segments have come and gone; the stranded page has still not been encoded.
    assertEquals(List.of(), controller.takeSealable().stream().filter(s -> s == 0L).toList(),
        "segment 0 must NOT be sealed while a page of it is still to mint values");
    assertTrue(!controller.isSealed(0));
    controller.encoded(0);
    assertTrue(controller.takeSealable().contains(0L), "sealable once its last page is encoded");
  }

  @Test
  @DisplayName("the newest segment is never sealed: the writer is still filling it")
  void theHighWaterSegmentIsHeldBack() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0);
    controller.encoded(0);
    // Nothing later has been adopted, so segment 0 may still grow — between two adoptions its
    // outstanding count is legitimately zero, and sealing on that alone would be the bug.
    assertEquals(List.of(), controller.takeSealable(), "no later segment exists yet");
    controller.adopted(1);
    assertEquals(List.of(0L), controller.takeSealable(), "a later segment proves the writer moved on");
    controller.encoded(1);
    assertEquals(List.of(), controller.takeSealable(), "segment 1 is still the newest");
  }

  @Test
  @DisplayName("a segment is offered for sealing exactly once")
  void sealingIsOfferedOnce() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0);
    controller.encoded(0);
    controller.adopted(1);
    assertEquals(List.of(0L), controller.takeSealable());
    assertEquals(List.of(), controller.takeSealable(), "a second call offers nothing");
    assertEquals(1, controller.sealedCount());
    controller.encoded(1);
    controller.adopted(2);
    assertEquals(List.of(1L), controller.takeSealable());
    assertEquals(2, controller.sealedCount());
  }

  @Test
  @DisplayName("drain sweeps the tail at commit, and refuses while anything is still encoding")
  void drainSweepsTheTail() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0);
    controller.encoded(0);
    controller.adopted(1);
    controller.adopted(2);
    controller.encoded(2);
    assertEquals(List.of(0L), controller.takeSealable(), "1 is still encoding, 2 is the newest");

    assertThrows(IllegalStateException.class, controller::drain,
        "draining while a page is still encoding would drop the values it is about to mint");
    controller.encoded(1);
    final List<Long> drained = controller.drain();
    assertTrue(drained.contains(1L) && drained.contains(2L), "the tail is swept: " + drained);
    assertTrue(!drained.contains(0L), "already sealed by the load");
    assertEquals(3, controller.sealedCount());
    assertEquals(List.of(), controller.drain(), "a second drain has nothing left");
  }

  @Test
  @DisplayName("a page adopted into a sealed segment is a defect, not a late arrival")
  void adoptingIntoASealedSegmentThrows() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0);
    controller.encoded(0);
    controller.adopted(1);
    assertEquals(List.of(0L), controller.takeSealable());
    assertThrows(IllegalStateException.class, () -> controller.adopted(0),
        "its dictionary is written; a value minted now could never be resolved");
  }

  @Test
  @DisplayName("completing a page nothing adopted is refused, and negative segments are rejected")
  void contractViolations() {
    final SegmentSealController controller = new SegmentSealController();
    assertThrows(IllegalStateException.class, () -> controller.encoded(0), "nothing was adopted");
    controller.adopted(0);
    controller.encoded(0);
    assertThrows(IllegalStateException.class, () -> controller.encoded(0), "the count is already zero");
    assertThrows(IllegalArgumentException.class, () -> controller.adopted(-1));
    assertThrows(IllegalArgumentException.class, () -> controller.encoded(-1));
  }

  @Test
  @DisplayName("out-of-order adoption seals less, never wrongly: everything unsealed is caught at commit")
  void outOfOrderAdoptionIsSafe() {
    final SegmentSealController controller = new SegmentSealController();
    // Not a bulk load: pages arrive for a high segment first, then a low one.
    controller.adopted(9);
    controller.encoded(9);
    controller.adopted(2);
    controller.encoded(2);
    // Segment 9 is the high-water mark, so 2 is sealable; 9 is held back although it is complete.
    assertEquals(List.of(2L), controller.takeSealable());
    assertTrue(!controller.isSealed(9), "the newest segment is always held for the load");
    assertEquals(List.of(9L), controller.drain(), "and commit catches it");
  }
}
