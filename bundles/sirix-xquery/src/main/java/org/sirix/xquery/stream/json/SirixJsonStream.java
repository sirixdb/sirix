package org.sirix.xquery.stream.json;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.SirixAxis;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBObject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Stream}, wrapping a Sirix {@link Axis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixJsonStream implements Stream<JsonDBObject> {
  /** Sirix {@link Axis}. */
  private final Axis mAxis;

  /** {@link JsonDBCollection} the nodes belong to. */
  private final JsonDBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link SirixAxis}
   * @param collection {@link JsonDBCollection} the nodes belong to
   */
  public SirixJsonStream(final Axis axis, final JsonDBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public JsonDBObject next() {
    if (mAxis.hasNext()) {
      mAxis.next();
      return new JsonDBObject(mAxis.asJsonNodeReadTrx(), mCollection);
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
