package org.sirix.axis.pathsummary;

import org.sirix.index.path.summary.PathNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Perform a test on a given axis.
 *
 * @author Johannes Lichtenberger
 */
public final class FilterAxis extends AbstractAxis {

  /**
   * Axis to test.
   */
  private final ChildAxis axis;

  /**
   * Test to apply to axis.
   */
  private final List<Predicate<PathNode>> axisFilter;

  /**
   * Constructor initializing internal state.
   *
   * @param axis          axis to iterate over
   * @param firstAxisTest test to perform for each node found with axis
   * @param axisTest      tests to perform for each node found with axis
   */
  @SuppressWarnings("unlikely-arg-type")
  @SafeVarargs
  public FilterAxis(final ChildAxis axis, final Predicate<PathNode> firstAxisTest,
      final Predicate<PathNode>... axisTest) {
    super(axis.startPathNode);
    this.axis = axis;
    axisFilter = new ArrayList<>();
    axisFilter.add(firstAxisTest);
  }

  @Override
  protected PathNode nextNode() {
    while (axis.hasNext()) {
      final var node = axis.next();
      boolean filterResult = true;
      for (final Predicate<PathNode> filter : axisFilter) {
        filterResult = filterResult && filter.test(node);
        if (!filterResult) {
          break;
        }
      }
      if (filterResult) {
        return node;
      }
    }
    return done();
  }
}
