package org.sirix.xquery.stream.json;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.SirixAxis;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonItemFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Stream}, wrapping a Sirix {@link Axis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixJsonStream implements Stream<Item> {

  private static final JsonItemFactory itemFactory = new JsonItemFactory();

  /** Sirix {@link Axis}. */
  private final Axis axis;

  /** {@link JsonDBCollection} the nodes belong to. */
  private final JsonDBCollection collection;


  /**
   * Constructor.
   *
   * @param axis Sirix {@link SirixAxis}
   * @param collection {@link JsonDBCollection} the nodes belong to
   */
  public SirixJsonStream(final Axis axis, final JsonDBCollection collection) {
    this.axis = checkNotNull(axis);
    this.collection = checkNotNull(collection);
  }

  @Override
  public Item next() {
    if (axis.hasNext()) {
      axis.next();
      return itemFactory.getSequence(axis.asJsonNodeReadTrx(), collection);
    }
    return null;
  }

  public Axis getAxis() {
    return axis;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", axis).toString();
  }
}
