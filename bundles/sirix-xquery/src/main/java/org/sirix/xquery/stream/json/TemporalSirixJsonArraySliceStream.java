package org.sirix.xquery.stream.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.HashMap;
import java.util.Map;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBArraySlice;
import org.sirix.xquery.node.XmlDBCollection;
import com.google.common.base.MoreObjects;

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

  private final Map<Integer, JsonNodeReadOnlyTrx> mCache;

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
    mCache = new HashMap<>();
    mFromIndex = fromIndex;
    mToIndex = toIndex;
  }

  @Override
  public JsonDBArraySlice next() {
    if (mAxis.hasNext()) {
      final ResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager = mAxis.getResourceManager();
      final Pair<Integer, Long> pair = mAxis.next();

      final int revision = pair.getFirst();
      final long nodeKey = pair.getSecond();

      final JsonNodeReadOnlyTrx rtx =
          mCache.computeIfAbsent(revision, revisionNumber -> resourceManager.beginNodeReadOnlyTrx(revisionNumber));
      rtx.moveTo(nodeKey);

      return new JsonDBArraySlice(rtx, mCollection, mFromIndex, mToIndex);
    }

    mCache.forEach((revision, rtx) -> rtx.close());
    return null;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", mAxis).toString();
  }
}
