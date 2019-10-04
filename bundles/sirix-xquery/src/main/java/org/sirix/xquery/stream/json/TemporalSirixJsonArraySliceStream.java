package org.sirix.xquery.stream.json;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.xquery.json.JsonDBArraySlice;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.node.XmlDBCollection;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalSirixJsonArraySliceStream implements Stream<JsonDBArraySlice> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> mAxis;

  /** The {@link JsonDBCollection} reference. */
  private final JsonDBCollection mCollection;

  private final int mFromIndex;

  private final int mToIndex;

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
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
    mFromIndex = fromIndex;
    mToIndex = toIndex;
  }

  @Override
  public JsonDBArraySlice next() {
    if (mAxis.hasNext()) {
      final var rtx = mAxis.next();
      return new JsonDBArraySlice(rtx, mCollection, mFromIndex, mToIndex);
    }

    return null;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", mAxis).toString();
  }
}
