package org.sirix.xquery.stream.node;

import org.brackit.xquery.xdm.Stream;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SirixNodeKeyStream implements Stream<XmlDBNode> {

  private final Iterator<NodeReferences> mIter;

  private final XmlDBCollection mCollection;

  private final XmlNodeReadOnlyTrx mRtx;

  public SirixNodeKeyStream(final Iterator<NodeReferences> iter, final XmlDBCollection collection,
      final XmlNodeReadOnlyTrx rtx) {
    mIter = checkNotNull(iter);
    mCollection = checkNotNull(collection);
    mRtx = checkNotNull(rtx);
  }

  @Override
  public XmlDBNode next() {
    while (mIter.hasNext()) {
      final NodeReferences nodeReferences = mIter.next();
      final Set<Long> nodeKeys = nodeReferences.getNodeKeys();
      for (final long nodeKey : nodeKeys) {
        mRtx.moveTo(nodeKey);
        return new XmlDBNode(mRtx, mCollection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
