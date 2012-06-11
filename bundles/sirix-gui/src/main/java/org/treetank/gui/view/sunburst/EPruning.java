/**
 * 
 */
package org.treetank.gui.view.sunburst;

/**
 * Determines if tree should be pruned or not.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EPruning {
  /** Tree should be pruned by depth. */
  DEPTH,

  /** Tree should be pruned by item size. */
  ITEMSIZE,

  /**
   * Tree should be pruned by taking the diffs into account. Note that this only can be used in case of
   * diff-views.
   */
  DIFF,

  /**
   * Tree should be pruned by taking the diffs into account. Nodes which have SAMEHASHES are pruned. Note
   * that this only can be used in case of diff-views.
   */
  DIFF_WITHOUT_SAMEHASHES,

  /** No pruning of the tree. */
  NO
}
