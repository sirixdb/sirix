package org.sirix.axis.filter.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.List;
import org.sirix.api.Filter;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;

/**
 * Filter for temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalXmlNodeReadFilterAxis<F extends Filter<XmlNodeReadOnlyTrx>>
    extends AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /** Axis to test. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> mAxis;

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
  public TemporalXmlNodeReadFilterAxis(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final F firstAxisTest, final F... axisTest) {
    checkNotNull(firstAxisTest);
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
      final ResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager = mAxis.getResourceManager();
      final Pair<Integer, Long> pair = mAxis.next();
      try (final XmlNodeReadOnlyTrx rtx = resourceManager.beginNodeReadOnlyTrx(pair.getFirst())) {
        rtx.moveTo(pair.getSecond());
        boolean filterResult = true;
        for (final F filter : mAxisFilter) {
          filter.setTrx(rtx);
          filterResult = filterResult && filter.filter();
          if (!filterResult) {
            break;
          }
        }
        if (filterResult) {
          return pair;
        }
      }
    }
    return endOfData();
  }

  /**
   * Returns the inner axis.
   *
   * @return the axis
   */
  public AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> getAxis() {
    return mAxis;
  }

  @Override
  public ResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> getResourceManager() {
    return mAxis.getResourceManager();
  }
}
