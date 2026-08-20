/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameReusedException;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reader side of the optimistic-read protocol on {@link HOTLeafPage}.
 *
 * <p>
 * This is the protocol {@code HOTTrieReader} already runs in production: it resolves a leaf without
 * pinning it, snapshots {@link HOTLeafPage#readStampBinding()} and {@link HOTLeafPage#readStamp()},
 * reads leaf bytes, and asks {@link HOTLeafPage#validateStamp(long, long)} whether those bytes were
 * stable. It is only worth anything if a stamp REFUSES to validate whenever the bytes could have
 * moved, so that is what these pin down — the two ways they can move, and the one case where
 * nothing moved at all.
 * </p>
 *
 * <p>
 * The binding half is the part these were written for. A stamp is a per-SLOT sequence number, so it
 * means nothing without the slot it was issued against; validating one against whatever slot the
 * leaf happens to be bound to NOW compares two unrelated counters and can answer {@code true} by
 * coincidence. {@code KeyValueLeafPageStampTest} states the same contract for the sibling page
 * type, and both pages run the identical protocol so the lesson only has to be learned once.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class HOTLeafPageStampTest {

  /** Smaller than {@link HOTLeafPage#DEFAULT_SIZE}, so the first mutation must grow and re-bind. */
  private static final int UNDERSIZED = 4 * 1024;

  private HOTLeafPage leaf;

  @AfterEach
  void tearDown() {
    if (leaf != null) {
      leaf.close();
      leaf = null;
    }
  }

  /**
   * A leaf whose slot memory is deliberately too small to mutate, so that the first {@code put}
   * forces {@code ensureMutableSlotMemory} to swap the segment — the one re-binding event a HOT leaf
   * has, and the event the binding generation exists to make visible.
   */
  private static HOTLeafPage undersizedLeaf() {
    final MemorySegment memory = Allocators.getInstance().allocate(UNDERSIZED);
    return new HOTLeafPage(1L, 1, IndexType.CAS, memory, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
  }

  private static byte[] key(final int i) {
    return ("key-" + i).getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a stamp taken over an untouched leaf validates")
  void anUndisturbedStampValidates() {
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    assertTrue(leaf.put(key(0), new byte[] {1, 2, 3, 4}), "precondition: the entry fits");

    final long binding = leaf.readStampBinding();
    final long stamp = leaf.readStamp();

    assertTrue(leaf.validateStamp(binding, stamp), "nothing moved, so the read was stable");
  }

  @Test
  @DisplayName("a live leaf issues a usable stamp, and one bound to a real allocator slot")
  void aLiveLeafIssuesAUsableStamp() {
    // The control that keeps the rejection tests honest: a readStamp that simply always answered
    // STAMP_INVALID would satisfy every one of them while making the protocol useless, since every
    // read would retry forever.
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    final long stamp = leaf.readStamp();

    assertNotEquals(HOTLeafPage.STAMP_INVALID, stamp, "a live leaf must issue a usable stamp");
    // And the stronger control: the stamp must be a real frame-slot version, not the UNBACKED
    // sentinel. A leaf whose coordinates fail to resolve degrades validateStamp to a bare
    // closed-flag test — it still passes every assertion here while detecting no torn read at all,
    // which is exactly the silent failure setStampBaseSegment exists to prevent.
    assertNotEquals(0L, stamp, "the leaf's memory came from the frame-slot allocator, so its stamp must be a version");
  }

  @Test
  @DisplayName("a stamp taken before close does not validate after it")
  void closingInvalidatesAnOutstandingStamp() {
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    assertTrue(leaf.put(key(0), new byte[] {1, 2, 3, 4}), "precondition: the entry fits");
    final long binding = leaf.readStampBinding();
    final long stamp = leaf.readStamp();
    assertTrue(leaf.validateStamp(binding, stamp), "precondition: the stamp is good while the leaf is live");

    leaf.close();

    // The whole point of the protocol. A reader that had already snapshotted this stamp and was
    // about to read the segment must be told to re-resolve rather than read a recycled slot.
    assertFalse(leaf.validateStamp(binding, stamp), "the leaf was closed under the reader");
    leaf = null; // tearDown must not close it twice
  }

  @Test
  @DisplayName("a throwing frame releaser still invalidates stamps and retires side references")
  void throwingFrameReleaserStillInvalidatesOutstandingStamp() {
    final MemorySegment memory = Allocators.getInstance().allocate(HOTLeafPage.DEFAULT_SIZE);
    final IllegalStateException expectedFailure = new IllegalStateException("injected frame release failure");
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS, memory, () -> {
      throw expectedFailure;
    }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    leaf.setPageReference(7L, new PageReference().setKey(42L));
    leaf.setCompletePageRef(leaf);
    final long binding = leaf.readStampBinding();
    final long stamp = leaf.readStamp();
    assertTrue(leaf.validateStamp(binding, stamp), "precondition: the frame-backed stamp starts valid");

    try {
      assertSame(expectedFailure, assertThrows(IllegalStateException.class, leaf::close));

      assertEquals(1L, leaf.readStampBinding() & 1L, "teardown must leave a permanently invalid binding");
      assertFalse(leaf.validateStamp(binding, stamp), "a failed external release must not preserve a valid stamp");
      assertThrows(FrameReusedException.class, leaf::segmentRefCount);
      assertNull(leaf.getCompletePageRef());
      assertEquals(0L, leaf.estimatedRetainedHeapBytes());
    } finally {
      // The injected releaser deliberately did not return the slot; the test still owns that cleanup.
      Allocators.getInstance().release(memory);
      leaf = null;
    }
  }

  @Test
  @DisplayName("teardown suppression preserves a reused primary failure")
  void teardownSuppressionPreservesReusedPrimaryFailure() {
    final AssertionError sharedFailure = new AssertionError("shared teardown failure");
    final IllegalStateException secondaryFailure = new IllegalStateException("secondary teardown failure");

    HOTLeafPage.addSuppressedSafely(sharedFailure, sharedFailure);
    HOTLeafPage.addSuppressedSafely(sharedFailure, secondaryFailure);

    assertEquals(1, sharedFailure.getSuppressed().length);
    assertSame(secondaryFailure, sharedFailure.getSuppressed()[0]);
  }

  @Test
  @DisplayName("a closed leaf issues only the never-validating stamp")
  void aClosedLeafIssuesNoUsableStamp() {
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    leaf.close();

    final long binding = leaf.readStampBinding();
    final long stamp = leaf.readStamp();

    assertFalse(leaf.validateStamp(binding, stamp), "a stamp taken from a closed leaf can never be trusted");
    leaf = null;
  }

  @Test
  @DisplayName("a stamp does not survive the leaf being re-bound to another segment")
  void rebindingInvalidatesAnOutstandingStamp() {
    // The hole a bare validateStamp(stamp) cannot close, and the reason this test class exists. Once
    // the leaf is bound elsewhere the old stamp names a counter it no longer reads, so validating it
    // against the NEW slot compares two unrelated sequences.
    //
    // WHAT THIS PROVES, precisely: the GUARANTEE, not the mechanism. Deleting the generation check
    // would not reliably fail this assertion — the answer would then depend on whether the two
    // slots' versions happen to differ, which is exactly the coincidence the check rules out and
    // exactly why no deterministic assertion can pin it. Do not read a green run as proof of the
    // check itself.
    leaf = undersizedLeaf();
    final long binding = leaf.readStampBinding();
    final long stamp = leaf.readStamp();
    assertTrue(leaf.validateStamp(binding, stamp), "precondition: good before the rebind");

    // Undersized slot memory cannot be mutated in place, so the first put must grow and re-bind.
    assertTrue(leaf.put(key(0), new byte[] {1, 2, 3, 4}), "precondition: the entry fits after the grow");
    assertNotEquals(binding, leaf.readStampBinding(), "growing the leaf must re-bind it");

    final long afterBinding = leaf.readStampBinding();
    final long afterStamp = leaf.readStamp();
    assertTrue(leaf.validateStamp(afterBinding, afterStamp), "the new binding is usable");

    assertFalse(leaf.validateStamp(binding, stamp), "a stamp from the previous binding must not validate");
  }

  @Test
  @DisplayName("a published binding is always even, so no reader can snapshot one mid-swap")
  void aPublishedBindingIsEven() {
    // The parity is the load-bearing half of the publication order: the generation goes ODD before
    // the segment moves and EVEN once it and the coordinates agree again, so the window in which
    // they disagree is exactly the window in which validateStamp refuses. A single-threaded test
    // cannot schedule itself inside that window, so what it CAN pin is the invariant that makes the
    // window meaningful — every binding a reader can observe outside it is even, before and after a
    // re-bind alike. A publication that bumped by one instead of two would fail here.
    leaf = undersizedLeaf();
    assertEquals(0L, leaf.readStampBinding() & 1L, "a quiescent leaf must publish an even binding");

    assertTrue(leaf.put(key(0), new byte[] {1, 2, 3, 4}), "precondition: the entry fits after the grow");

    assertEquals(0L, leaf.readStampBinding() & 1L, "a re-bound leaf must publish an even binding again");
  }

  @Test
  @DisplayName("correcting the zero-copy stamp base is itself a re-bind")
  void settingTheStampBaseInvalidatesAnOutstandingStamp() {
    // A zero-copy deserialized leaf takes its slot memory as a mid-buffer SLICE, whose address the
    // allocator never handed out; the deserializer corrects that through setStampBaseSegment before
    // publishing the leaf. That correction changes which slot the coordinates name, so it is a
    // re-bind like any other and must invalidate anything outstanding — otherwise a stamp taken
    // against the (unresolvable) slice would survive into a binding that resolves.
    final MemorySegment base = Allocators.getInstance().allocate(HOTLeafPage.DEFAULT_SIZE);
    final MemorySegment slice = base.asSlice(64, UNDERSIZED);
    leaf = new HOTLeafPage(1L, 1, IndexType.CAS, slice, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    try {
      final long binding = leaf.readStampBinding();
      final long stamp = leaf.readStamp();

      leaf.setStampBaseSegment(base);

      assertNotEquals(binding, leaf.readStampBinding(), "correcting the base must re-bind the leaf");
      assertFalse(leaf.validateStamp(binding, stamp), "a stamp from before the correction must not validate");
      assertNotEquals(0L, leaf.readStamp(),
          "with the base corrected the leaf must stamp against a real slot version, not the UNBACKED sentinel");
    } finally {
      // The leaf holds a slice and no releaser, so the base is this test's to give back.
      leaf.close();
      leaf = null;
      Allocators.getInstance().release(base);
    }
  }

  @Test
  @DisplayName("an outstanding stamp does not survive the slot being recycled under it")
  void recyclingTheSlotInvalidatesAnOutstandingStamp() {
    // The event the whole protocol is aimed at, spelled out end to end: a reader snapshots a stamp,
    // the page it was reading is closed, its frame slot goes back to the allocator and is handed to
    // somebody else — and the reader must be told, because the bytes at that address are now another
    // page's. Poison-on-release makes the recycling unambiguous rather than merely likely.
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
    try {
      leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      assertTrue(leaf.put(key(0), new byte[] {1, 2, 3, 4}), "precondition: the entry fits");
      final long binding = leaf.readStampBinding();
      final long stamp = leaf.readStamp();
      assertTrue(leaf.validateStamp(binding, stamp), "precondition: good while the slot is the leaf's");

      leaf.close();
      // Re-issue the slot. FrameSlotAllocator hands the freed slot back out for a same-class
      // request, so this is the reader's worst case: the same address, another page's bytes.
      final MemorySegment reissued = Allocators.getInstance().allocate(HOTLeafPage.DEFAULT_SIZE);
      try {
        assertFalse(leaf.validateStamp(binding, stamp), "the slot was recycled under the reader");
      } finally {
        Allocators.getInstance().release(reissued);
      }
      leaf = null;
    } finally {
      FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
    }
  }
}
