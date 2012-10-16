package org.sirix.gui.view.sunburst.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RecursiveTask;

import org.slf4j.LoggerFactory;
import org.sirix.diff.DiffTuple;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;
import org.sirix.gui.view.model.interfaces.TraverseModel;
import org.sirix.utils.LogWrapper;

/** Counts modifications. */
public final class Modifications extends RecursiveTask<Modification> implements Callable<Modification> {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = 1L;

  /** {@link LogWrapper}. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(Modifications.class));

  /** Index of current diff. */
  private int mIndex;

  /** {@link List} of {@link DiffTuple}s which is needed to count deleted nodes. */
  private Map<Integer, DiffTuple> mDiffs;

  /** Static factory method to get an empty modification instance. */
  public static Modification emptyModification() {
    return Modification.EMPTY_MODIFICATION;
  }

  /**
   * Static factory method to get a {@link Modifications} instance.
   * 
   * @param paramIndex
   *          current index in diff list
   * @param paramDiffs
   *          {@link List} of {@link DiffTuple}s
   * @throws IllegalArgumentException
   *           if {@code paramIndex <= -1}
   * @throws NullPointerException
   *           if {@code paramDiffs} is {@code null}
   * @return {@link Modifications} reference
   * @throws SirixException
   *           if anything inside sirix fails
   */
  public static Modifications getInstance(final int paramIndex, final Map<Integer, DiffTuple> paramDiffs)
    throws SirixException {
    checkArgument(paramIndex > -1, "paramIndex must be > -1!");
    checkNotNull(paramDiffs);
    return new Modifications(paramIndex, paramDiffs);
  }

  /**
   * Private constructor.
   * 
   * @param paramIndex
   *          current index in diff list
   * @param paramDiffs
   *          {@link List} of {@link DiffTuple}s
   * @param param
   */
  private Modifications(final int paramIndex, final Map<Integer, DiffTuple> paramDiffs) {
    mIndex = paramIndex;
    mDiffs = paramDiffs;
  }

  /**
   * Increment diff counter if it's a modified diff.
   * 
   * @param paramIndex
   *          index of diff to get
   * @param paramDiffCounts
   *          diff counter
   * @return modified diff counter
   */
  private int incrDiffCounter(final int paramIndex, final int paramDiffCounts) {
    assert paramIndex >= 0;
    assert paramDiffCounts >= 0;
    int diffCounts = paramDiffCounts;
    if (paramIndex < mDiffs.size() && mDiffs.get(paramIndex).getDiff() != DiffType.SAME
      && mDiffs.get(paramIndex).getDiff() != DiffType.SAMEHASH) {
      diffCounts++;
    }
    return diffCounts;
  }

  /**
   * Count how many differences in the subtree exists and add descendant-or-self count.
   * 
   * @return number of differences and descendants
   * @throws SirixException
   *           if sirix can't close the transaction
   */
  public Modification countDiffs() throws SirixException {
    int index = mIndex;
    final DiffTuple diffCont = mDiffs.get(index);
    final DiffType diff = diffCont.getDiff();
    final int rootDepth =
      (diff == DiffType.DELETED || diff == DiffType.MOVEDFROM || diff == DiffType.REPLACEDOLD) ? diffCont.getDepth()
        .getOldDepth() : diffCont.getDepth().getNewDepth();

    int diffCounts = 0;
    int descendantCounts = 1;
    boolean subtract = false;

    diffCounts = incrDiffCounter(index, diffCounts);
    index++;

    if (diffCounts == 1 && index < mDiffs.size()) {
      final DiffTuple cont = mDiffs.get(index);
      final int depth =
        (cont.getDiff() == DiffType.DELETED || cont.getDiff() == DiffType.MOVEDFROM || cont.getDiff() == DiffType.REPLACEDOLD)
          ? cont.getDepth().getOldDepth() : cont.getDepth().getNewDepth();
      if (depth == rootDepth + 1) {
        // Current node is modified and has at least one child.
        subtract = true;
      }
    }

    boolean done = false;
    while (!done && index < mDiffs.size()) {
      final DiffTuple currDiffCont = mDiffs.get(index);
      final DiffType currDiff = currDiffCont.getDiff();
      final DiffDepth currDepth = currDiffCont.getDepth();
      final int depth =
        (currDiff == DiffType.DELETED || currDiff == DiffType.MOVEDFROM || currDiff == DiffType.REPLACEDOLD)
          ? currDepth.getOldDepth() : currDepth.getNewDepth();
      if (depth <= rootDepth) {
        done = true;
      }
      if (!done) {
        descendantCounts++;
        if (currDiff != DiffType.SAME && currDiff != DiffType.SAMEHASH) {
          diffCounts++;
        }
        index++;
      }
    }

    // Add a factor to add some weighting to the diffCounts.
    return new Modification(TraverseModel.FACTOR * diffCounts, descendantCounts, subtract);
  }

  @Override
  public Modification compute() {
    Modification result = Modifications.emptyModification();
    try {
      result = countDiffs();
    } catch (SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return result;
  }

  @Override
  public Modification call() throws Exception {
    return countDiffs();
  }
}
