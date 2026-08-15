package io.sirix.index;

/**
 * An index builder that has work left to do once the document traversal is over.
 *
 * <p>
 * Most builders write straight through: a visit is a write. A builder that <em>bulk-loads</em>
 * instead — collecting the whole entry set and materialising the index structure in one pass, the
 * only way to build a HOT trie in {@code Θ(n)} — has nothing on disk until it is told the traversal
 * has ended. {@link IndexBuilder#build} calls {@link #finishIndexBuild()} on every builder that
 * implements this, exactly once, after the last visit.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public interface IndexBuildFinalizer {

  /** Flush whatever the traversal accumulated. Called once, after the traversal. */
  void finishIndexBuild();
}
