/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reader side of the optimistic-read protocol on {@link KeyValueLeafPage}.
 *
 * <p>
 * This is the machinery that replaces reference-counted pinning on the record path: a reader
 * snapshots {@link KeyValueLeafPage#readStampBinding()} and {@link KeyValueLeafPage#readStamp()},
 * reads page bytes without acquiring anything, and asks
 * {@link KeyValueLeafPage#validateStamp(long, long)} whether those bytes were stable. The protocol
 * is only worth anything if a stamp REFUSES to validate whenever the bytes could have moved, so
 * that is what these pin down — the ways they can move, and the one case where nothing moved at
 * all.
 * </p>
 *
 * <p>
 * The Arena segment the constructor takes is the LEGACY slot memory, which the page releases; the
 * slotted page these stamps actually cover is allocated by {@code ensureSlottedPage} from
 * {@code Allocators.getInstance()}, so on a normal run these exercise the frame-slot branch, where
 * a stamp is a real allocator slot version. The sibling page type runs the identical protocol and
 * is pinned by {@code HOTLeafPageStampTest}; the ABA ordering hazard in {@code readStamp} was found
 * on that side, in {@code HOTLeafUseAfterCloseTest}, and does not have to be re-learned here.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class KeyValueLeafPageStampTest {

  private KeyValueLeafPage page;

  private Arena arena;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    page = new KeyValueLeafPage(1L, IndexType.DOCUMENT, new ResourceConfiguration.Builder("testResource").build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);
  }

  @AfterEach
  void tearDown() {
    if (page != null) {
      page.close();
      page = null;
    }
    if (arena != null) {
      arena.close();
      arena = null;
    }
  }

  @Test
  @DisplayName("a stamp taken over an untouched page validates")
  void anUndisturbedStampValidates() {
    page.setSlot(new byte[] {1, 2, 3, 4}, 0);

    final long binding = page.readStampBinding();
    final long stamp = page.readStamp();

    assertTrue(page.validateStamp(binding, stamp), "nothing moved, so the read was stable");
  }

  @Test
  @DisplayName("a stamp is never the never-validating sentinel while the page is live")
  void aLivePageIssuesAUsableStamp() {
    // The control that keeps the other two honest. A readStamp that simply always answered
    // STAMP_INVALID would satisfy every rejection test in this class while making the protocol
    // useless — every read would retry forever.
    assertNotEquals(KeyValueLeafPage.STAMP_INVALID, page.readStamp(), "a live page must issue a usable stamp");
  }

  @Test
  @DisplayName("a stamp taken before close does not validate after it")
  void closingInvalidatesAnOutstandingStamp() {
    page.setSlot(new byte[] {1, 2, 3, 4}, 0);
    final long binding = page.readStampBinding();
    final long stamp = page.readStamp();
    assertTrue(page.validateStamp(binding, stamp), "precondition: the stamp is good while the page is live");

    page.close();

    // This is the whole point of the protocol. A reader that had already snapshotted this stamp and
    // was about to read the segment must be told to re-resolve rather than read freed memory — which
    // is what the page guard used to prevent by keeping the page alive instead.
    assertFalse(page.validateStamp(binding, stamp), "the page was closed under the reader");
    page = null; // tearDown must not close it twice
  }

  @Test
  @DisplayName("a stamp does not survive the page being re-bound to another segment")
  void rebindingInvalidatesAnOutstandingStamp() {
    // The hole a bare `validateStamp(stamp)` cannot close. A stamp is a per-SLOT sequence number, so
    // once the page is bound elsewhere the stamp names a counter this page no longer reads, and
    // validating it against the NEW slot compares two unrelated sequences.
    //
    // WHAT THIS PROVES, precisely: the GUARANTEE, not the mechanism. Deleting the generation check
    // does not reliably fail THIS case — without it the answer depends on whether the two slots'
    // versions happen to coincide, which is exactly the coincidence the check exists to rule out and
    // exactly why it cannot be pinned here. The case that DOES pin it deterministically is the
    // unbacked/backed swap below; read the two together.
    page.setSlot(new byte[] {1, 2, 3, 4}, 0);
    final long binding = page.readStampBinding();
    final long stamp = page.readStamp();
    assertTrue(page.validateStamp(binding, stamp), "precondition: good before the rebind");

    // Force a segment swap: fill past the initial capacity so the page must grow.
    final byte[] filler = new byte[512];
    for (int slot = 1; slot < 200; slot++) {
      page.setSlot(filler, slot);
    }
    assertNotEquals(binding, page.readStampBinding(), "growing the page must re-bind it");

    // Re-bind the coordinates onto the new slot, as any concurrent reader's first stamp would.
    final long afterBinding = page.readStampBinding();
    final long afterStamp = page.readStamp();
    assertTrue(page.validateStamp(afterBinding, afterStamp), "the new binding is usable");

    assertFalse(page.validateStamp(binding, stamp), "a stamp from the previous binding must not validate");
  }

  @Test
  @DisplayName("a stamp does not survive the page swapping between unbacked and frame-backed")
  void swappingBetweenUnbackedAndBackedInvalidatesAnOutstandingStamp() {
    // The case that pins the generation check DETERMINISTICALLY, which the sibling rebind test
    // above cannot. Point the binding at a segment the allocator never handed out — a mid-buffer
    // slice, which is exactly what a zero-copy deserializer produces — and the page has no slot
    // version to consult, so readStamp answers with the UNBACKED sentinel. Correct the base
    // afterwards and the page IS frame-backed again.
    //
    // Without the generation check, that outstanding UNBACKED stamp validates on the strength of
    // the page merely still being open, certifying reads taken while the page could not detect a
    // torn one at all. This is the false positive the field javadoc claims is covered, and it needs
    // no coincidence to reproduce.
    page.setSlot(new byte[] {1, 2, 3, 4}, 0);
    page.setStampBaseSegment(arena.allocate(SIXTYFOUR_KB).asSlice(64, 1024));
    final long unbackedBinding = page.readStampBinding();
    final long unbackedStamp = page.readStamp();
    assertTrue(page.validateStamp(unbackedBinding, unbackedStamp), "precondition: usable while unbacked");

    page.setStampBaseSegment(null); // back to the page's own, allocator-issued slotted page

    assertNotEquals(unbackedBinding, page.readStampBinding(), "clearing the base must re-bind the page");
    assertFalse(page.validateStamp(unbackedBinding, unbackedStamp),
        "an unbacked stamp must not validate once the page is frame-backed again");
  }

  @Test
  @DisplayName("a closed page issues only the never-validating stamp")
  void aClosedPageIssuesNoUsableStamp() {
    page.close();

    final long binding = page.readStampBinding();
    final long stamp = page.readStamp();

    assertFalse(page.validateStamp(binding, stamp), "a stamp taken from a closed page can never be trusted");
    page = null;
  }
}
