package org.sirix.xquery.stream.node;

import org.brackit.xquery.jdm.Stream;
import org.roaringbitmap.longlong.PeekableLongIterator;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SirixNodeKeyStream implements Stream<XmlDBNode> {

  private final Iterator<NodeReferences> iter;

  private final XmlDBCollection collection;

  private final XmlNodeReadOnlyTrx rtx;

  private PeekableLongIterator nodeKeyIterator;

  public SirixNodeKeyStream(final Iterator<NodeReferences> iter, final XmlDBCollection collection,
      final XmlNodeReadOnlyTrx rtx) {
    this.iter = checkNotNull(iter);
    this.collection = checkNotNull(collection);
    this.rtx = checkNotNull(rtx);
  }

  @Override
  public XmlDBNode next() {
    if (nodeKeyIterator != null && nodeKeyIterator.hasNext()) {
      var nodeKey = nodeKeyIterator.next();
      rtx.moveTo(nodeKey);
      return new XmlDBNode(rtx, collection);
    }
    while (iter.hasNext()) {
      final NodeReferences nodeReferences = iter.next();
      nodeKeyIterator = nodeReferences.getNodeKeys().getLongIterator();
      if (nodeKeyIterator.hasNext()) {
        var nodeKey = nodeKeyIterator.next();
        rtx.moveTo(nodeKey);
        return new XmlDBNode(rtx, collection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
