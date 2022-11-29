package org.sirix.axis.filter.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;

/**
 * Match name of json object records.
 *
 * @author Johannes Lichtenberger
 */
public final class JsonNameFilter extends AbstractFilter<JsonNodeReadOnlyTrx> {

  /** Key of local name to test. */
  private final QNm name;

  /**
   * Default constructor.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} this filter is bound to
   * @param name name to check
   */
  public JsonNameFilter(final JsonNodeReadOnlyTrx rtx, final QNm name) {
    super(rtx);
    this.name = name;
  }

  /**
   * Default constructor.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} this filter is bound to
   * @param name name to check
   */
  public JsonNameFilter(final JsonNodeReadOnlyTrx rtx, final String name) {
    super(rtx);
    this.name = new QNm(name);
  }

  @Override
  public boolean filter() {
    final JsonNodeReadOnlyTrx rtx = getTrx();
    return rtx.isObjectKey() && name.equals(rtx.getName());
  }
}
