package io.sirix.query.stream.json;

import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.JsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;

import static java.util.Objects.requireNonNull;

/**
 * {@link Stream} over a temporal axis that yields, per revision, the stored item for the axis' node:
 * a number, string, boolean or {@code null} value, an object field name, or an object/array, as
 * that revision holds it.
 */
public final class TemporalSirixJsonItemStream implements Stream<JsonItem> {

  /** Temporal axis positioning one read-only transaction per revision on the node. */
  private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis;

  /** The collection the node belongs to. */
  private final JsonDBCollection collection;

  /** Whether the navigated item is an object field NAME rather than a value. */
  private final boolean fieldName;

  /**
   * Constructor.
   *
   * @param axis the temporal axis
   * @param collection the collection the node belongs to
   * @param fieldName {@code true} when the navigated item is an object field name
   */
  public TemporalSirixJsonItemStream(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
      final JsonDBCollection collection, final boolean fieldName) {
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
    this.fieldName = fieldName;
  }

  @Override
  public JsonItem next() {
    if (axis.hasNext()) {
      return JsonItemFactory.INSTANCE.getSequence(axis.next(), collection, fieldName);
    }
    return null;
  }

  @Override
  public void close() {
    axis.close();
  }
}
