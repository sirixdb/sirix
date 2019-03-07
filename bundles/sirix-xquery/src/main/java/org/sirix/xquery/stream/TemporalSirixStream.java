package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.utils.Pair;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public class TemporalSirixStream implements Stream<AbstractTemporalNode<XmlDBNode>> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> mAxis;

  /** The {@link XmlDBCollection} reference. */
  private XmlDBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public TemporalSirixStream(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final XmlDBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public AbstractTemporalNode<XmlDBNode> next() throws DocumentException {
    if (mAxis.hasNext()) {
      final Pair<Integer, Long> pair = mAxis.next();
      final ResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager = mAxis.getResourceManager();
      final Optional<XmlNodeReadOnlyTrx> optionalRtx = resourceManager.getNodeReadTrxByRevisionNumber(pair.getFirst());

      final XmlNodeReadOnlyTrx rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
        rtx.moveTo(pair.getSecond());
      } else {
        rtx = resourceManager.beginNodeReadOnlyTrx(pair.getFirst());
        rtx.moveTo(pair.getSecond());
      }
      return new XmlDBNode(rtx, mCollection);
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
