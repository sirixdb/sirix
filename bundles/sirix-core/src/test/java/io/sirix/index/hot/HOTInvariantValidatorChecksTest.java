/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage C unit tests for the three new {@link HOTInvariantValidator} checks: I4 (first-partial-zero),
 * I11 (trie-condition), and I-leaf-insert-precondition. Each check has a positive test (well-formed
 * structure → no violation) and a negative test (deliberately malformed structure → violation
 * reported with the expected tag).
 *
 * <p>Tests construct synthetic {@link HOTIndirectPage} / {@link HOTLeafPage} objects directly via
 * the public factory methods, set them on freshly-allocated {@link PageReference}s, and pass a
 * Mockito stub {@link StorageEngineReader} (every page is in-memory via
 * {@link PageReference#setPage(Page)}, so the reader's load methods never fire).
 */
final class HOTInvariantValidatorChecksTest {

  // ===== I4 — first-partial-zero =====

  @Test
  void i4_positive_biNodeHasZeroFirstPartial() {
    // BiNode at bit 0 (MSB of byte 0). Two leaves: left has key 0x00 (β=0), right has 0x80 (β=1).
    final HOTLeafPage leftLeaf = newLeaf(1, /*key*/ new byte[] {0x00});
    final HOTLeafPage rightLeaf = newLeaf(2, new byte[] {(byte) 0x80});
    final PageReference leftRef = newRef(leftLeaf);
    final PageReference rightRef = newRef(rightLeaf);
    final HOTIndirectPage biNode = HOTIndirectPage.createBiNode(
        /*pageKey*/ 100, /*revision*/ 0, /*discBit*/ 0, leftRef, rightRef, /*height*/ 1);
    final PageReference rootRef = newRef(biNode);

    final HOTInvariantValidator.Result result =
        HOTInvariantValidator.validate(rootRef, mockReader());

    assertNoViolation(result, "I4-first-partial-zero");
  }

  @Test
  void i4_negative_spanNodeWithNonZeroFirstPartial() {
    // SpanNode whose smallest stored partial is non-zero. Mask has a single bit (bit 0 of byte 0
    // → BE long-bit 63), so partial-key space is {0, 1}. Setting partials = [1, 0] places the
    // unsigned-min (= 0) at slot 1 and slot 0's stored partial = 1 — but I4 checks the smallest
    // partial across ALL slots, not slot 0 specifically. To force a violation, both partials must
    // be > 0. We use a 2-bit mask (bits 0 and 1 of byte 0 → long-bits 63 and 62) so partial space
    // is {0,1,2,3}, and pick partials [2, 3] — neither is zero.
    final long bitMask = (1L << 63) | (1L << 62); // bits 0 and 1 of byte 0
    final HOTLeafPage leftLeaf = newLeaf(1, new byte[] {(byte) 0x80}); // 1000_0000 → bits=10
    final HOTLeafPage rightLeaf = newLeaf(2, new byte[] {(byte) 0xC0}); // 1100_0000 → bits=11
    final PageReference leftRef = newRef(leftLeaf);
    final PageReference rightRef = newRef(rightLeaf);
    final HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
        /*pageKey*/ 100, /*revision*/ 0, /*initialBytePos*/ 0, bitMask,
        new int[] {2, 3}, new PageReference[] {leftRef, rightRef});
    final PageReference rootRef = newRef(spanNode);

    final HOTInvariantValidator.Result result =
        HOTInvariantValidator.validate(rootRef, mockReader());

    assertHasViolation(result, "I4-first-partial-zero");
  }

  // ===== I11 — trie-condition =====

  @Test
  void i11_positive_childMSBIsLessSignificantThanParent() {
    // Parent BiNode at bit 0 (MSB of byte 0, position 0 = most significant).
    // Both children are themselves BiNodes at bit 8 (MSB of byte 1, position 8 = less significant).
    // Trie condition: parent.MSB=0 < child.MSB=8 ✓
    //
    // Leaf keys: 4 distinct keys whose first byte distinguishes them at bit 0, second byte at bit 8.
    final HOTLeafPage llLeaf = newLeaf(1, new byte[] {0x00, 0x00});
    final HOTLeafPage lrLeaf = newLeaf(2, new byte[] {0x00, (byte) 0x80});
    final HOTLeafPage rlLeaf = newLeaf(3, new byte[] {(byte) 0x80, 0x00});
    final HOTLeafPage rrLeaf = newLeaf(4, new byte[] {(byte) 0x80, (byte) 0x80});

    final HOTIndirectPage leftChild = HOTIndirectPage.createBiNode(
        /*pageKey*/ 10, /*revision*/ 0, /*discBit*/ 8, newRef(llLeaf), newRef(lrLeaf), /*height*/ 1);
    final HOTIndirectPage rightChild = HOTIndirectPage.createBiNode(
        /*pageKey*/ 20, /*revision*/ 0, /*discBit*/ 8, newRef(rlLeaf), newRef(rrLeaf), /*height*/ 1);
    final HOTIndirectPage root = HOTIndirectPage.createBiNode(
        /*pageKey*/ 100, /*revision*/ 0, /*discBit*/ 0,
        newRef(leftChild), newRef(rightChild), /*height*/ 2);
    final PageReference rootRef = newRef(root);

    final HOTInvariantValidator.Result result =
        HOTInvariantValidator.validate(rootRef, mockReader());

    assertNoViolation(result, "I11-trie-condition");
  }

  @Test
  void i11_negative_childMSBIsMoreSignificantThanParent() {
    // Parent BiNode at bit 8 (less significant), children at bit 0 (more significant).
    // Trie condition: parent.MSB=8 < child.MSB=0 → FALSE (8 > 0). Violation expected.
    //
    // We can't build this through normal construction (writer enforces it). Construct directly
    // by inverting the BiNode build order: parent at bit 8, child indirect at bit 0. The child
    // pages don't need to be semantically routable; the I11 check only inspects the mask MSB.
    final HOTLeafPage la = newLeaf(1, new byte[] {0x00, 0x00});
    final HOTLeafPage lb = newLeaf(2, new byte[] {(byte) 0x80, 0x00});
    final HOTLeafPage ra = newLeaf(3, new byte[] {0x00, (byte) 0x80});
    final HOTLeafPage rb = newLeaf(4, new byte[] {(byte) 0x80, (byte) 0x80});

    // children at bit 0 (more significant)
    final HOTIndirectPage leftChild = HOTIndirectPage.createBiNode(
        /*pageKey*/ 10, /*revision*/ 0, /*discBit*/ 0, newRef(la), newRef(lb), /*height*/ 1);
    final HOTIndirectPage rightChild = HOTIndirectPage.createBiNode(
        /*pageKey*/ 20, /*revision*/ 0, /*discBit*/ 0, newRef(ra), newRef(rb), /*height*/ 1);
    // parent at bit 8 (less significant) — INVERTED; trie condition broken.
    final HOTIndirectPage root = HOTIndirectPage.createBiNode(
        /*pageKey*/ 100, /*revision*/ 0, /*discBit*/ 8,
        newRef(leftChild), newRef(rightChild), /*height*/ 2);
    final PageReference rootRef = newRef(root);

    final HOTInvariantValidator.Result result =
        HOTInvariantValidator.validate(rootRef, mockReader());

    assertHasViolation(result, "I11-trie-condition");
  }

  // ===== I-leaf-insert-precondition =====

  @Test
  void leafInsertPrecondition_positive_newKeyAgreesWithSubtreeBeta() {
    // BiNode at bit 0. Left subtree contains key 0x00 (β=0). Right subtree contains 0x80 (β=1).
    // Insert newKey=0x00 → routes to left → left subtree is β=0-constant → newKey β=0 ✓.
    final HOTLeafPage leftLeaf = newLeaf(1, new byte[] {0x00});
    final HOTLeafPage rightLeaf = newLeaf(2, new byte[] {(byte) 0x80});
    final HOTIndirectPage biNode = HOTIndirectPage.createBiNode(
        /*pageKey*/ 100, /*revision*/ 0, /*discBit*/ 0,
        newRef(leftLeaf), newRef(rightLeaf), /*height*/ 1);
    final PageReference rootRef = newRef(biNode);

    final List<HOTInvariantValidator.Violation> violations =
        HOTInvariantValidator.checkLeafInsertPreservesI5(
            rootRef, /*newKey*/ new byte[] {0x10}, mockReader()); // β=0 (top bit clear)

    assertTrue(violations.stream().noneMatch(v -> v.invariant().equals("I-leaf-insert-precondition")),
        "expected no I-leaf-insert-precondition violations but got: " + violations);
  }

  @Test
  void leafInsertPrecondition_negative_newKeyDisagreesWithSubtreeBeta() {
    // Same tree as above. Construct a hypothetical newKey=0x40 with β-bit-0=0 but a partial
    // (under bit 0 mask) that subset-matches BOTH children's stored partials [0, 1] equally
    // poorly... actually findChildIndex picks the most-specific match, and for densePK=0
    // (bit 0 of 0x40 = 0), only stored partial 0 satisfies (dense & p) == p, so routing
    // picks slot 0 (the LEFT subtree). Left subtree is β=0-constant. newKey is also β=0.
    // → No violation. NOT what we want.
    //
    // For a strict negative we need a 2-bit mask where the chosen child's subtree has a
    // β-constant value but newKey disagrees. Build:
    //   - 4-child SpanNode at bits 0+1 (mask = 0xC000_0000_0000_0000), 8 byte initial pos.
    //   - children = 4 leaves with keys 0x00, 0x40, 0x80, 0xC0. Partials [0,1,2,3].
    //   - Each leaf has ONE key, so each subtree is β-constant on bits 0 and 1.
    //   - Insert newKey=0x80 → bit-0=1, bit-1=0, dense=2. Routing subset-match (dense=2, partial=2)
    //     picks slot 2 (=leaf with 0x80). Subtree-bit-0 of slot 2 is 1, newKey-bit-0 is 1 ✓.
    //     Subtree-bit-1 of slot 2 is 0, newKey-bit-1 is 0 ✓. NO VIOLATION.
    //
    // For a violation, the chosen subtree must include a key whose β-constant disagrees with
    // newKey's β. That requires a MULTI-ENTRY leaf where one ancestor disc bit captures the
    // leaf but the leaf's keys all share one β value, and newKey routes to that leaf with a
    // different β value.
    //
    // Construct:
    //   - Root BiNode at bit 0. Left subtree = leaf with TWO keys 0x10, 0x20 (both β-bit-0=0).
    //   - Right subtree = leaf with 0x80 (β-bit-0=1).
    //   - Insert newKey=0x80 → routes to RIGHT (subset-match). β-bit-0=1 == subtree's β-bit-0=1.
    //     No violation. NOT what we want.
    //
    // The leaf-insert-precondition check fires when ROUTING picks a slot but newKey disagrees
    // with the subtree's β-CONSTANT value. Routing subset-match `(dense & p) == p` requires
    // dense's bits to be a SUPERSET of p's bits. So if p has β-bit=1, dense MUST have β-bit=1.
    // Routing already forces agreement on bits where p=1. The check fires when subtree is
    // β-constant=0 (= p has β=0) but routing picks this slot AND newKey has β=1.
    //
    // To engineer this we need a slot whose stored partial p has β-bit=0 but the chosen routing
    // picks it (= dense & p == p, trivially satisfied since p bit 0 = 0 implies any dense bit
    // works at that position). Multiple slots can match; findChildIndex picks the MOST-SPECIFIC
    // (= largest matching p). So for newKey to land in a β-constant=0 slot with newKey β=1,
    // there must be NO MORE-SPECIFIC slot to outbid it.
    //
    // Construct a 1-mask, 1-child indirect... wait, 2-child minimum. Use a SpanNode with 2 slots:
    //   - mask = 0x8000_0000_0000_0000 (bit 0 of byte 0).
    //   - partials = [0, 1].
    //   - Left slot 0 = leaf with TWO keys 0x10, 0x20 (both β=0). Left subtree β-constant=0.
    //   - Right slot 1 = leaf with 0x80 (β=1). Right subtree β-constant=1.
    //   - newKey = 0x10 → dense=0. Subset-match: (0 & 0) == 0 ✓ for slot 0. (0 & 1) != 1 ✗
    //     for slot 1. Routes to slot 0. β=0 = subtree=0 ✓. No violation. (newKey is β=0 like
    //     slot 0's keys.)
    //
    // To force the violation: make newKey's route to slot 0 (β-constant=0) BUT newKey's β=1.
    // findChildIndex picks slot with maximal-specificity stored partial subset of dense. If
    // dense=1 (bit 0 set), both p=0 (subset of 1) and p=1 (subset of 1) match; the more specific
    // is p=1, so slot 1 wins. Routing CAN'T put a β=1 newKey into a β=0 slot. So in a SINGLE-bit
    // mask scenario, the check can't fire.
    //
    // The check fires when the indirect captures MULTIPLE bits and a slot's stored partial has
    // some bits=1 (which routing enforces) and some bits=0 (where newKey may disagree). Use:
    //   - 2-bit mask: bits 0 (byte 0 MSB) AND 1 (next bit). long-bits 63 + 62.
    //   - 2 slots with partials [1, 2] (binary 01, 10).
    //   - slot 0 = leaf with key 0x40 (binary 0100_0000 → bit 0=0, bit 1=1; dense=binary 01=1).
    //   - slot 1 = leaf with key 0x80 (binary 1000_0000 → bit 0=1, bit 1=0; dense=binary 10=2).
    //   - newKey = 0xC0 (binary 1100_0000 → bit 0=1, bit 1=1; dense=3=11). subset-match with p=1
    //     (3&1==1 ✓), with p=2 (3&2==2 ✓). Both match; most-specific = p=2 (= slot 1, 0x80).
    //     newKey β-bit-0=1 == slot 1's subtree β-bit-0=1 ✓. β-bit-1=1 vs slot 1's β-bit-1=0
    //     → DISAGREEMENT at bit 1. Slot 1's subtree is β-bit-1=0-constant; newKey would change
    //     it to β-mixed.
    //   → I-leaf-insert-precondition VIOLATION at bit 1 of slot 1.
    final long bitMask = (1L << 63) | (1L << 62); // bits 0, 1 of byte 0
    final HOTLeafPage slot0Leaf = newLeaf(1, new byte[] {0x40}); // bit-0=0, bit-1=1
    final HOTLeafPage slot1Leaf = newLeaf(2, new byte[] {(byte) 0x80}); // bit-0=1, bit-1=0
    final PageReference slot0Ref = newRef(slot0Leaf);
    final PageReference slot1Ref = newRef(slot1Leaf);
    final HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(
        /*pageKey*/ 100, /*revision*/ 0, /*initialBytePos*/ 0, bitMask,
        new int[] {1, 2}, new PageReference[] {slot0Ref, slot1Ref});
    final PageReference rootRef = newRef(spanNode);

    final List<HOTInvariantValidator.Violation> violations =
        HOTInvariantValidator.checkLeafInsertPreservesI5(
            rootRef, /*newKey*/ new byte[] {(byte) 0xC0}, mockReader());

    assertTrue(
        violations.stream().anyMatch(v -> v.invariant().equals("I-leaf-insert-precondition")),
        "expected I-leaf-insert-precondition violation but got: " + violations);
  }

  // ===== Test helpers =====

  /** Creates a minimal HOTLeafPage and inserts the given keys (each with a 1-byte value = 0xAA). */
  private static HOTLeafPage newLeaf(long pageKey, byte[]... keys) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, /*revision*/ 0, IndexType.NAME);
    for (final byte[] k : keys) {
      leaf.put(k, new byte[] {(byte) 0xAA});
    }
    return leaf;
  }

  /** Build a PageReference pre-populated with the in-memory page (no disk fetch needed). */
  private static PageReference newRef(Page page) {
    final PageReference ref = new PageReference();
    ref.setPage(page);
    return ref;
  }

  /** Lenient Mockito stub — never asked anything because all pages are in-memory. */
  private static StorageEngineReader mockReader() {
    return Mockito.mock(StorageEngineReader.class);
  }

  private static void assertNoViolation(HOTInvariantValidator.Result result, String invariantTag) {
    final long count = result.violations().stream()
        .filter(v -> v.invariant().equals(invariantTag))
        .count();
    assertEquals(0L, count, "expected no " + invariantTag + " violations but got: "
        + result.violations());
  }

  private static void assertHasViolation(HOTInvariantValidator.Result result, String invariantTag) {
    assertTrue(
        result.violations().stream().anyMatch(v -> v.invariant().equals(invariantTag)),
        "expected " + invariantTag + " violation but got: " + result.violations());
  }
}
