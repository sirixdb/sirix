package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.SirixAxis;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a Sirix {@link Axis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixStream implements Stream<DBNode> {
  /** Sirix {@link Axis}. */
  private final Axis mAxis;

  /** {@link DBCollection} the nodes belong to. */
  private final DBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link SirixAxis}
   * @param collection {@link DBCollection} the nodes belong to
   */
  public SirixStream(final Axis axis, final DBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public DBNode next() throws DocumentException {
    if (mAxis.hasNext()) {
      mAxis.next();
      return new DBNode(mAxis.getTrx(), mCollection);
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
