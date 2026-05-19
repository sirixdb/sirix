/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

/**
 * Thrown by {@link HOTIncrementalInsert#addEntry} when a discriminative bit cannot be folded
 * into a compound node because a multi-value sibling subtree <em>straddles</em> it — some of
 * the sibling's keys have the bit set, some clear. Folding the bit in would re-encode that
 * sibling with a single bit value and so violate invariant I5 (every child subtree is constant
 * on every one of the node's discriminative bits).
 *
 * <p>The incremental fold is impossible; the driver catches this and recanonicalizes the
 * affected subtree ({@code HOTBulkBuilder} is canonical by construction). {@link #nodeDepthHint}
 * carries the spine depth of the node {@code addEntry} targeted, stamped by
 * {@link HOTIncrementalInsert#integrate}, so the driver scopes the recanonicalization
 * minimally; {@code -1} means untagged — a defensive caller then rebuilds from the index root.
 *
 * <p>See {@code docs/HOT_ADDENTRY_STRADDLE_FIX.md}.
 *
 * @author Johannes Lichtenberger
 */
public final class HOTStraddleException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Spine depth of the node whose {@code addEntry} straddled; {@code -1} when untagged. */
  public final int nodeDepthHint;

  /**
   * @param message       a diagnostic description of the straddle
   * @param nodeDepthHint the spine depth of the straddling node, or {@code -1} if untagged
   */
  public HOTStraddleException(final String message, final int nodeDepthHint) {
    super(message);
    this.nodeDepthHint = nodeDepthHint;
  }
}
