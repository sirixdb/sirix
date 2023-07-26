package io.sirix.axis.filter.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.brackit.xquery.atomic.QNm;
import io.sirix.axis.filter.AbstractFilter;

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
