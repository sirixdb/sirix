package org.sirix.xquery.stream.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonObjectKeyDBArray;
import org.sirix.xquery.node.XmlDBCollection;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalSirixJsonObjectKeyArrayStream implements Stream<JsonObjectKeyDBArray> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis;

  /** The {@link JsonDBCollection} reference. */
  private final JsonDBCollection collection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public TemporalSirixJsonObjectKeyArrayStream(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
      final JsonDBCollection collection) {
    this.axis = checkNotNull(axis);
    this.collection = checkNotNull(collection);
  }

  @Override
  public JsonObjectKeyDBArray next() {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new JsonObjectKeyDBArray(rtx, collection);
    }
    return null;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", axis).toString();
  }
}
