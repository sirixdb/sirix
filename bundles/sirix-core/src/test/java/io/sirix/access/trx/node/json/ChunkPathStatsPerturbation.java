/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

/**
 * Same-package handle on {@link ChunkPathStatsBatch}'s package-private perturbation seam, so a
 * differential test in another package can install the inversion witness without the production
 * class exposing a mutable global.
 */
public final class ChunkPathStatsPerturbation {

  private ChunkPathStatsPerturbation() {
    throw new AssertionError("no instances");
  }

  /** Corrupt every drained chunk partial by folding one extra long observation into it. */
  public static void addLongToEveryPartial(final long delta) {
    ChunkPathStatsBatch.partialPerturbation = (pathNodeKey, partial) -> partial.addLong(delta);
  }

  /** Remove any installed perturbation — production behaviour. */
  public static void clear() {
    ChunkPathStatsBatch.partialPerturbation = null;
  }
}
