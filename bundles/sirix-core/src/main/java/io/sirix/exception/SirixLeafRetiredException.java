package io.sirix.exception;

import java.util.concurrent.TimeUnit;

/**
 * A HOT leaf a mutation had to read was retired by page-cache eviction before the mutation could
 * make it safe to read, and bounded re-resolution did not recover it.
 *
 * <p>
 * This names one cause and no other, so a caller can tell it apart from corruption
 * ({@link SirixCorruptionException}), an I/O failure ({@link SirixIOException}), exhausted frame
 * memory, and a key that simply is not there. It is an interim failure of FMT-304: the target
 * guarantee is that eviction never reclaims a frame a reader or writer may still read, and until
 * epoch-deferred reuse lands, a required re-resolution is bounded and raises this at the bound.
 * </p>
 *
 * <p>
 * Published revisions are unaffected: the mutation that failed published nothing a reader can see,
 * and a fresh reader observes the contents it observed before. If this failure occurs inside a
 * mutation, the transaction may already hold a partially built private index state, so it is marked
 * rollback-only and commit is refused. A read that fails before entering the mutation does not mark
 * the transaction rollback-only. Explicit {@code rollback} remains available in either case.
 * </p>
 */
public final class SirixLeafRetiredException extends SirixRuntimeException {
  private static final long serialVersionUID = 1L;

  /** The page key of the leaf that could not be made safe to read. */
  private final long pageKey;

  /** How long re-resolution tried before it gave up, in nanoseconds. */
  private final long waitedNanos;

  /**
   * @param pageKey the page key of the leaf that could not be made safe to read
   * @param waitedNanos how long re-resolution tried before giving up, never negative
   */
  public SirixLeafRetiredException(final long pageKey, final long waitedNanos) {
    super("HOT leaf " + pageKey + " was retired by page-cache eviction before this mutation could make it safe to "
        + "read, and re-resolution did not recover it within " + TimeUnit.NANOSECONDS.toMillis(waitedNanos)
        + " ms. No published revision changed; this transaction is rollback-only.");
    if (waitedNanos < 0L) {
      throw new IllegalArgumentException("Waited nanoseconds must not be negative: " + waitedNanos);
    }
    this.pageKey = pageKey;
    this.waitedNanos = waitedNanos;
  }

  /** @return the page key of the leaf that could not be made safe to read */
  public long getPageKey() {
    return pageKey;
  }

  /** @return how long re-resolution tried before it gave up, in nanoseconds */
  public long getWaitedNanos() {
    return waitedNanos;
  }
}
