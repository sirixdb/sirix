package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.temporal.RevisionPrefetcher.RtxResult;

import static java.util.Objects.requireNonNull;

/**
 * Look-ahead-prefetch variant of {@link AllTimeAxis}. Yields the same sequence of
 * read-only transactions but overlaps the per-revision
 * {@link ResourceSession#beginNodeReadOnlyTrx} cost with consumer-side work via a
 * fixed-depth virtual-thread pipeline. See {@link RevisionPrefetcher}.
 *
 * <p>Iteration semantics match {@link AllTimeAxis} byte-for-byte:
 * <ul>
 *   <li>Walks revisions 1 through the most-recent revision in ascending order.</li>
 *   <li>Yields the rtx for each revision in which {@code moveTo(nodeKey)} succeeds.</li>
 *   <li>Terminates once a {@code moveTo} fails after at least one prior success
 *       (the node was deleted) — the consumer keeps the previously-yielded rtxs.</li>
 *   <li>Closes any rtx it opened that is not yielded to the consumer (no leak).</li>
 * </ul>
 *
 * <p>This class additionally avoids a pre-existing leak in the original
 * {@link AllTimeAxis}: the prefix of revisions before the node is created used to
 * leak the opened-but-not-yielded rtx — here, every rtx whose {@code moveTo} fails
 * is closed.
 */
public final class PrefetchedAllTimeAxis<R extends NodeReadOnlyTrx & NodeCursor,
    W extends NodeTrx & NodeCursor> extends AbstractTemporalAxis<R, W> {

  private final ResourceSession<R, W> resourceSession;
  private final RevisionPrefetcher<R, W> prefetcher;
  private boolean hasMoved;
  private boolean drained;

  public PrefetchedAllTimeAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this.resourceSession = requireNonNull(resourceSession);
    final long nodeKey = rtx.getNodeKey();
    final int maxRevision = resourceSession.getMostRecentRevisionNumber();
    final int[] cursor = new int[] {1};
    this.prefetcher = new RevisionPrefetcher<>(resourceSession, nodeKey, () -> {
      final int next = cursor[0];
      if (next > maxRevision) {
        return -1;
      }
      cursor[0] = next + 1;
      return next;
    }, RevisionPrefetcher.DEFAULT_DEPTH);
  }

  @Override
  protected R computeNext() {
    while (!drained) {
      final RtxResult<R> result = prefetcher.poll();
      if (result == null) {
        drained = true;
        prefetcher.close();
        return endOfData();
      }
      if (result.nodeFound) {
        hasMoved = true;
        return result.rtx;
      }
      result.rtx.close();
      if (hasMoved) {
        // Node existed and is now gone — terminate, releasing any further prefetched rtx.
        prefetcher.close();
        drained = true;
        return endOfData();
      }
    }
    return endOfData();
  }

  @Override
  public ResourceSession<R, W> getResourceSession() {
    return resourceSession;
  }

  @Override
  public void close() {
    drained = true;
    prefetcher.close();
  }
}
