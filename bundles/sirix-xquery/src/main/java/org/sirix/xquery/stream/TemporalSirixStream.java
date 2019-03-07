package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.HashMap;
import java.util.Map;
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
  private final XmlDBCollection mCollection;

  private final Map<Integer, XmlNodeReadOnlyTrx> mCache;

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
    mCache = new HashMap<>();
  }

  @Override
  public AbstractTemporalNode<XmlDBNode> next() throws DocumentException {
    if (mAxis.hasNext()) {
      final ResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager = mAxis.getResourceManager();
      final Pair<Integer, Long> pair = mAxis.next();

      final int revision = pair.getFirst();
      final long nodeKey = pair.getSecond();

      final XmlNodeReadOnlyTrx rtx =
          mCache.computeIfAbsent(revision, revisionNumber -> resourceManager.beginNodeReadOnlyTrx(revisionNumber));
      rtx.moveTo(nodeKey);

      return new XmlDBNode(rtx, mCollection);
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
