package org.sirix.axis.filter.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.List;
import org.sirix.api.Filter;
import org.sirix.api.ResourceSession;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;

/**
 * Filter for temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalXmlNodeReadFilterAxis<F extends Filter<XmlNodeReadOnlyTrx>>
    extends AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /** Axis to test. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis;

  /** Test to apply to axis. */
  private final List<F> axisFilter;

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
    this.axis = axis;
    axisFilter = new ArrayList<>();
    axisFilter.add(firstAxisTest);

    if (axisTest != null) {
      for (int i = 0, length = axisTest.length; i < length; i++) {
        axisFilter.add(axisTest[i]);
      }
    }
  }

  @Override
  protected XmlNodeReadOnlyTrx computeNext() {
    while (axis.hasNext()) {
      final XmlNodeReadOnlyTrx rtx = axis.next();
      final boolean filterResult = doFilter(rtx);
      if (filterResult) {
        return rtx;
      }
      rtx.close();
    }

    return endOfData();
  }

  private boolean doFilter(final XmlNodeReadOnlyTrx rtx) {
    boolean filterResult = true;
    for (final F filter : axisFilter) {
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
  public AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> getAxis() {
    return axis;
  }

  @Override
  public ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> getResourceManager() {
    return axis.getResourceManager();
  }
}
