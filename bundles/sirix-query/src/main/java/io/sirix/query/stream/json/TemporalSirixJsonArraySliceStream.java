package io.sirix.query.stream.json;

import com.google.common.base.MoreObjects;
import io.brackit.query.jdm.Stream;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.json.JsonDBArraySlice;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBCollection;

import static java.util.Objects.requireNonNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalSirixJsonArraySliceStream implements Stream<JsonDBArraySlice> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis;

  /** The {@link JsonDBCollection} reference. */
  private final JsonDBCollection collection;

  private final int fromIndex;

  private final int toIndex;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   * @param fromIndex the from index
   * @param toIndex the to index
   */
  public TemporalSirixJsonArraySliceStream(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
      final JsonDBCollection collection, final int fromIndex, final int toIndex) {
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
    this.fromIndex = fromIndex;
    this.toIndex = toIndex;
  }

  @Override
  public JsonDBArraySlice next() {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new JsonDBArraySlice(rtx, collection, fromIndex, toIndex);
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
