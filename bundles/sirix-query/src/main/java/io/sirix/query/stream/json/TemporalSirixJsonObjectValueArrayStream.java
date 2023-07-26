package io.sirix.query.stream.json;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.jdm.Stream;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonObjectValueDBArray;
import io.sirix.query.node.XmlDBCollection;

import static java.util.Objects.requireNonNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalSirixJsonObjectValueArrayStream implements Stream<JsonObjectValueDBArray> {

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
  public TemporalSirixJsonObjectValueArrayStream(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
      final JsonDBCollection collection) {
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
  }

  @Override
  public JsonObjectValueDBArray next() {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new JsonObjectValueDBArray(rtx, collection);
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
