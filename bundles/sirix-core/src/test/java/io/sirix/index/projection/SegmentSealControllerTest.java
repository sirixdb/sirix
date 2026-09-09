/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * When a segment's dictionary may be sealed. The rule has to be conservative in one direction only:
 * sealing a segment that can still receive a value loses that value from the dictionary its own
 * page points at, while sealing late costs residency and nothing else.
 *
 * <p>
 * The notifications are per PAGE KEY and neither is exactly-once — a page re-encoded in a later
 * flush epoch speaks twice, a copy-on-write copy is adopted twice — so the tests below drive the
 * duplicates the writer really produces and check the controller reads them as one page.
 * </p>
 */
final class SegmentSealControllerTest {

  @Test
  @DisplayName("a segment with a page still encoding is never sealable, however far the writer has moved on")
  void anOutstandingPageHoldsItsSegmentOpen() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 10L);
    controller.adopted(0, 11L);
    controller.adopted(1, 20L);
    controller.adopted(2, 30L); // the writer is well past segment 0
    assertEquals(2, controller.outstandingIn(0));

    assertTrue(controller.encoded(0, 10L));
    assertEquals(1, controller.outstandingIn(0));
    assertTrue(controller.takeSealable().isEmpty(), "page 11 of segment 0 is still encoding");

    assertTrue(controller.encoded(0, 11L));
    assertEquals(0, controller.outstandingIn(0));
    assertEquals(IntList.of(0), controller.takeSealable());
  }

  @Test
  @DisplayName("a page encoded twice — a later flush epoch — is one page, not a negative count")
  void aSecondEncodeOfAPageChangesNothing() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 10L);
    controller.adopted(0, 11L);
    controller.adopted(1, 20L);
    controller.adopted(2, 30L);

    assertTrue(controller.encoded(0, 10L));
    assertFalse(controller.encoded(0, 10L), "the second encode of page 10 finds nothing outstanding");
    assertFalse(controller.encoded(0, 10L));
    // A counter would have gone to -1 here and offered the segment with page 11 still encoding.
    assertEquals(1, controller.outstandingIn(0));
    assertTrue(controller.takeSealable().isEmpty(), "page 11 is still outstanding whatever page 10 said twice");

    assertTrue(controller.encoded(0, 11L));
    assertEquals(IntList.of(0), controller.takeSealable());
  }

  @Test
  @DisplayName("a page adopted twice — a copy-on-write copy through the factory — is one page, not two")
  void aSecondAdoptionOfAPageChangesNothing() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 10L);
    controller.adopted(0, 10L); // the copy carries the same page key
    controller.adopted(0, 10L);
    controller.adopted(1, 20L);
    controller.adopted(2, 30L);
    assertEquals(1, controller.outstandingIn(0));

    assertTrue(controller.encoded(0, 10L));
    // A counter would sit at 2 here and never offer the segment.
    assertEquals(0, controller.outstandingIn(0));
    assertEquals(IntList.of(0), controller.takeSealable());
  }

  @Test
  @DisplayName("a page re-adopted after its encode is outstanding again until its next encode")
  void aReAdoptedPageIsOutstandingAgain() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 10L);
    controller.adopted(2, 30L);
    assertTrue(controller.encoded(0, 10L));
    assertEquals(0, controller.outstandingIn(0));

    controller.adopted(0, 10L); // written, then modified and adopted again before the seal
    assertEquals(1, controller.outstandingIn(0));
    assertTrue(controller.takeSealable().isEmpty(), "the re-adopted page has not been re-encoded");

    assertTrue(controller.encoded(0, 10L));
    assertEquals(IntList.of(0), controller.takeSealable());
  }

  @Test
  @DisplayName("an encode for a page nobody adopted is ignored, not counted")
  void anUnknownPageEncodeIsIgnored() {
    final SegmentSealController controller = new SegmentSealController();
    assertFalse(controller.encoded(0, 99L), "nothing adopted at all");
    controller.adopted(0, 10L);
    assertFalse(controller.encoded(0, 99L), "another page of the segment");
    assertFalse(controller.encoded(5, 10L), "a segment nobody adopted into");
    assertEquals(1, controller.outstandingIn(0));
    assertEquals(0, controller.outstandingIn(5));
  }

  @Test
  @DisplayName("a page stranded in the queue holds its segment open while later segments are sealed")
  void aStrandedPageKeepsItsSegmentOpen() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L); // this one will sit in the queue for a long time
    for (int segment = 1; segment <= 4; segment++) {
      controller.adopted(segment, segment * 100L);
      controller.encoded(segment, segment * 100L);
    }
    // Segments 1..3 are done and below the high-water mark (4); 0 is held by its stranded page.
    assertEquals(IntList.of(1, 2, 3), controller.takeSealable());

    controller.encoded(0, 1L);
    assertEquals(IntList.of(0), controller.takeSealable());
    assertTrue(controller.takeSealable().isEmpty());
  }

  @Test
  @DisplayName("the high-water segment is held back even with nothing outstanding — it is still adopting")
  void theHighWaterSegmentIsHeldBack() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    assertTrue(controller.takeSealable().isEmpty(), "segment 0 is where the next page lands");

    controller.adopted(1, 2L);
    assertEquals(IntList.of(0), controller.takeSealable(), "a higher segment adopted: 0 can never grow again");
    assertTrue(controller.takeSealable().isEmpty(), "1 is now the high-water segment");
  }

  @Test
  @DisplayName("each segment is offered exactly once")
  void sealingIsOfferedOnce() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    controller.adopted(1, 2L);
    assertEquals(IntList.of(0), controller.takeSealable());
    assertTrue(controller.isSealed(0));
    assertFalse(controller.isSealed(1));
    assertTrue(controller.takeSealable().isEmpty());
    assertTrue(controller.drain().isEmpty(), "1 is still outstanding");
    controller.encoded(1, 2L);
    assertEquals(IntList.of(1), controller.drain());
    assertTrue(controller.drain().isEmpty());
    assertEquals(2, controller.sealedCount());
  }

  @Test
  @DisplayName("drain sweeps the tail once no page can be adopted any more")
  void drainSweepsTheTail() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    controller.adopted(1, 2L);
    controller.encoded(1, 2L);
    assertEquals(IntList.of(0, 1), controller.drain(), "the high-water segment is fair game at the end");
    assertEquals(2, controller.sealedCount());
  }

  @Test
  @DisplayName("the fenced drain refuses a page the pool never encoded, and offers everything once it has")
  void theFencedDrainVerifiesTheFence() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    controller.adopted(1, 2L);
    controller.adopted(1, 3L);
    controller.encoded(1, 2L);

    final IllegalStateException failure = assertThrows(IllegalStateException.class, controller::drainAfterFence);
    assertTrue(failure.getMessage().contains("segment 1"), failure.getMessage());
    assertTrue(failure.getMessage().contains("1 page(s)"), failure.getMessage());
    assertFalse(controller.isSealed(0), "a refused drain seals nothing, not even the segments that were ready");

    controller.encoded(1, 3L);
    assertEquals(IntList.of(0, 1), controller.drainAfterFence());
    assertTrue(controller.drainAfterFence().isEmpty());
  }

  @Test
  @DisplayName("adopting into a segment that was already offered for sealing is refused")
  void adoptingIntoASealedSegmentThrows() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    controller.adopted(1, 2L);
    assertEquals(IntList.of(0), controller.takeSealable());
    final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> controller.adopted(0, 5L));
    assertTrue(failure.getMessage().contains("segment 0"), failure.getMessage());
    assertEquals(0, controller.outstandingIn(0), "the refused page was not recorded");
  }

  @Test
  @DisplayName("negative segments and page keys are refused")
  void contractViolations() {
    final SegmentSealController controller = new SegmentSealController();
    assertThrows(IllegalArgumentException.class, () -> controller.adopted(-1, 0L));
    assertThrows(IllegalArgumentException.class, () -> controller.adopted(0, -1L));
    assertThrows(IllegalArgumentException.class, () -> controller.encoded(-1, 0L));
    assertThrows(IllegalArgumentException.class, () -> controller.encoded(0, -1L));
    assertThrows(IllegalArgumentException.class, () -> controller.outstandingIn(-1));
    assertThrows(IllegalArgumentException.class, () -> controller.isSealed(-1));
    assertTrue(controller.takeSealable().isEmpty(), "nothing was recorded by the refused calls");
    assertTrue(controller.drainAfterFence().isEmpty());
  }

  @Test
  @DisplayName("out-of-order adoption is safe: a lower segment adopted after a higher one is just outstanding")
  void outOfOrderAdoptionIsSafe() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(2, 30L);
    controller.adopted(0, 1L); // segment 0 adopted after 2 — legal, and it is now outstanding
    controller.adopted(1, 20L);
    controller.encoded(2, 30L);
    controller.encoded(1, 20L);
    assertEquals(IntList.of(1), controller.takeSealable(), "0 is outstanding, 2 is the high-water segment");
    controller.encoded(0, 1L);
    assertEquals(IntList.of(0), controller.takeSealable());
    assertEquals(IntList.of(2), controller.drain());
  }

  @Test
  @DisplayName("a segment never adopted into is never offered, even below the high-water mark")
  void aSkippedSegmentIsNeverOffered() {
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 1L);
    controller.encoded(0, 1L);
    controller.adopted(3, 40L); // segments 1 and 2 saw no page
    controller.encoded(3, 40L);
    assertEquals(IntList.of(0), controller.takeSealable());
    assertEquals(IntList.of(3), controller.drain());
    assertFalse(controller.isSealed(1));
    assertFalse(controller.isSealed(2));
    assertEquals(2, controller.sealedCount());
  }

  @Test
  @DisplayName("slack keeps one more segment live: the consumer that mints a moment later still can")
  void slackKeepsASegmentLive() {
    // The race the slack exists for: the writer declares segment 0 finished by adopting into 1, but
    // the projection's leaf of segment-0 rows flushes just afterwards and mints into 0. Sealing at
    // the high-water mark alone would refuse a mint that was always going to arrive.
    final SegmentSealController controller = new SegmentSealController();
    controller.adopted(0, 10L);
    controller.encoded(0, 10L);
    controller.adopted(1, 20L);
    controller.encoded(1, 20L);
    controller.adopted(2, 30L);

    assertEquals(IntList.of(0, 1), controller.takeSealable(0), "no slack seals everything below the mark");

    final SegmentSealController withSlack = new SegmentSealController();
    withSlack.adopted(0, 10L);
    withSlack.encoded(0, 10L);
    withSlack.adopted(1, 20L);
    withSlack.encoded(1, 20L);
    withSlack.adopted(2, 30L);
    assertEquals(IntList.of(0), withSlack.takeSealable(1), "one segment of slack holds segment 1 back");
    assertFalse(withSlack.isSealed(1), "and it stays mintable");
  }

  @Test
  @DisplayName("an incremental seal leaves the drain a consistent tail, not a contradiction")
  void incrementalSealThenDrain() {
    // What a 100M load does: seal what is finished at each commit, then drain the rest at the end.
    // A segment taken by the incremental pass must not be offered again, and must not make the
    // final fence complain about pages it already accounted for.
    final SegmentSealController controller = new SegmentSealController();
    for (int segment = 0; segment < 4; segment++) {
      controller.adopted(segment, 100L + segment);
      controller.encoded(segment, 100L + segment);
    }
    assertEquals(IntList.of(0, 1), controller.takeSealable(1), "segments 2 and 3 stay live");
    final IntList tail = controller.drainAfterFence();
    assertEquals(IntList.of(2, 3), tail, "the drain takes exactly what the incremental pass left");
    assertEquals(4, controller.sealedCount(), "and every segment is sealed exactly once");
  }
}
