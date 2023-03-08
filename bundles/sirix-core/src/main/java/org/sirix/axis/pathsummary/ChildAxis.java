package org.sirix.axis.pathsummary;

import org.sirix.index.path.summary.PathNode;

/**
 * Iterate over all children starting at a given node. Self is not included.
 *
 * @author Johannes Lichtenberger
 */
public final class ChildAxis extends AbstractAxis {

  /**
   * Has another child node.
   */
  private boolean first;

  /**
   * Constructor initializing internal state.
   *
   * @param pathNode context node
   */
  public ChildAxis(final PathNode pathNode) {
    super(pathNode);
    first = true;
  }

  @Override
  public void reset(PathNode pathNode) {
    first = true;
    super.reset(pathNode);
  }

  @Override
  protected PathNode nextNode() {
    if (!first && nextNode.hasRightSibling()) {
      return nextNode.getRightSibling();
    } else if (first && nextNode.hasFirstChild()) {
      first = false;
      return nextNode.getFirstChild();
    }

    return done();
  }
}
