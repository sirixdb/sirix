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
 * snapshots {@link KeyValueLeafPage#readStamp()}, reads page bytes without acquiring anything, and
 * asks {@link KeyValueLeafPage#validateStamp(long)} whether those bytes were stable. The protocol
 * is only worth anything if a stamp REFUSES to validate whenever the bytes could have moved, so
 * that is what these pin down — the two ways they can move, and the one case where nothing moved at
 * all.
 * </p>
 *
 * <p>
 * The page here is Arena-backed rather than frame-slot-backed, so it takes the
 * {@code STAMP_UNBACKED} branch: its memory cannot be torn by slot reuse, and closing is the only
 * event that can invalidate it. That is the branch these tests exercise. The frame-slot branch —
 * where a stamp is a real allocator slot version — is covered for the sibling page type by
 * {@code HOTLeafUseAfterCloseTest}, which is where the ABA ordering hazard in {@code readStamp} was
 * found; both pages run the identical protocol so that lesson does not have to be re-learned here.
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
    // WHAT THIS PROVES, precisely, because the distinction cost a wrong claim once already: the
    // re-bind is what matters, not the swap. Immediately after a swap `stampCoordinates` is UNBOUND
    // and validateStamp already rejects on that alone — deleting the generation check does NOT fail
    // that case. It is the readStamp() below, which re-binds the coordinates to the NEW slot, that
    // leaves the old stamp facing a live and unrelated counter. Deleting the generation check does
    // not reliably fail THIS case either: without it the answer depends on whether the two slots'
    // versions happen to coincide, which is exactly the coincidence the check exists to rule out and
    // exactly why it cannot be pinned by a deterministic assertion. This test pins the GUARANTEE;
    // the mutation argument for it is that a passing run must not depend on two counters differing.
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
  @DisplayName("a closed page issues only the never-validating stamp")
  void aClosedPageIssuesNoUsableStamp() {
    page.close();

    final long binding = page.readStampBinding();
    final long stamp = page.readStamp();

    assertFalse(page.validateStamp(binding, stamp), "a stamp taken from a closed page can never be trusted");
    page = null;
  }
}
