package org.sirix.axis.filter.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sirix.api.Filter;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;

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

  private final Map<Integer, JsonNodeReadOnlyTrx> mCache;

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
    checkNotNull(firstAxisTest);
    mCache = new HashMap<>();
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
  protected Pair<Integer, Long> computeNext() {
    while (mAxis.hasNext()) {
      final ResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager = mAxis.getResourceManager();
      final Pair<Integer, Long> pair = mAxis.next();
      final int revision = pair.getFirst();
      final long nodeKey = pair.getSecond();

      final JsonNodeReadOnlyTrx rtx =
          mCache.computeIfAbsent(revision, revisionNumber -> resourceManager.beginNodeReadOnlyTrx(revisionNumber));
      rtx.moveTo(nodeKey);
      final boolean filterResult = doFilter(rtx);
      if (filterResult) {
        return pair;
      }
    }

    mCache.forEach((revision, rtx) -> rtx.close());
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
  public ResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> getResourceManager() {
    return mAxis.getResourceManager();
  }
}
