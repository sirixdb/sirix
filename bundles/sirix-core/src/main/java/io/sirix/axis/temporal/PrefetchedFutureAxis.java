package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.RevisionPrefetcher.RtxResult;

import static java.util.Objects.requireNonNull;

/**
 * Look-ahead-prefetch variant of {@link FutureAxis}. Walks revisions ascending from
 * (currentRevision + 1) (or currentRevision when {@code includeSelf == YES}) up to
 * the most-recent revision, yielding the rtx in each revision in which
 * {@code moveTo(nodeKey)} succeeds. Terminates on the first failure (deletion).
 *
 * <p>The look-ahead overlaps the per-revision trx-open cost with consumer-side work
 * via {@link RevisionPrefetcher}.
 */
public final class PrefetchedFutureAxis<R extends NodeReadOnlyTrx & NodeCursor,
    W extends NodeTrx & NodeCursor> extends AbstractTemporalAxis<R, W> {

  private final ResourceSession<R, W> resourceSession;
  private final RevisionPrefetcher<R, W> prefetcher;
  private boolean drained;

  public PrefetchedFutureAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    this(resourceSession, rtx, IncludeSelf.NO);
  }

  public PrefetchedFutureAxis(final ResourceSession<R, W> resourceSession, final R rtx,
      final IncludeSelf includeSelf) {
    this.resourceSession = requireNonNull(resourceSession);
    final long nodeKey = rtx.getNodeKey();
    final int startRevision = requireNonNull(includeSelf) == IncludeSelf.YES
        ? rtx.getRevisionNumber()
        : rtx.getRevisionNumber() + 1;
    final int maxRevision = resourceSession.getMostRecentRevisionNumber();
    final int[] cursor = new int[] {startRevision};
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
    if (drained) {
      return endOfData();
    }
    final RtxResult<R> result = prefetcher.poll();
    if (result == null) {
      drained = true;
      prefetcher.close();
      return endOfData();
    }
    if (result.nodeFound) {
      return result.rtx;
    }
    result.rtx.close();
    prefetcher.close();
    drained = true;
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
