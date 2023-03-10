package org.sirix.axis.filter.json;

import java.util.ArrayList;
import java.util.List;
import org.sirix.api.Filter;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;

import static java.util.Objects.requireNonNull;

/**
 * Filter for temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalJsonNodeReadFilterAxis<F extends Filter<JsonNodeReadOnlyTrx>>
    extends AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /** Axis to test. */
  private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> mAxis;

  /** Test to apply to axis. */
  private final List<F> mAxisFilter;

  /**
   * Constructor initializing internal state.
   *
   * @param axis axis to iterate over
   * @param firstAxisTest test to perform for each node found with axis
   * @param axisTest tests to perform for each node found with axis
   */
  @SafeVarargs
  public TemporalJsonNodeReadFilterAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
      final F firstAxisTest, final F... axisTest) {
    requireNonNull(firstAxisTest);
    mAxis = axis;
    mAxisFilter = new ArrayList<F>();
    mAxisFilter.add(firstAxisTest);

    if (axisTest != null) {
      for (int i = 0, length = axisTest.length; i < length; i++) {
        mAxisFilter.add(axisTest[i]);
      }
    }
  }

  @Override
  protected JsonNodeReadOnlyTrx computeNext() {
    while (mAxis.hasNext()) {
      final JsonNodeReadOnlyTrx rtx = mAxis.next();
      final boolean filterResult = doFilter(rtx);
      if (filterResult) {
        return rtx;
      }

      rtx.close();
    }
    return endOfData();
  }

  private boolean doFilter(final JsonNodeReadOnlyTrx rtx) {
    boolean filterResult = true;
    for (final F filter : mAxisFilter) {
      filter.setTrx(rtx);
      filterResult = filterResult && filter.filter();
      if (!filterResult) {
        break;
      }
    }
    return filterResult;
  }

  /**
   * Returns the inner axis.
   *
   * @return the axis
   */
  public AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> getAxis() {
    return mAxis;
  }

  @Override
  public ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> getResourceManager() {
    return mAxis.getResourceManager();
  }
}
