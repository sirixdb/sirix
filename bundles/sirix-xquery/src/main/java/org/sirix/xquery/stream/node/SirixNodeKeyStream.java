package org.sirix.xquery.stream.node;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

public final class SirixNodeKeyStream implements Stream<XmlDBNode> {

  private final Iterator<NodeReferences> iter;

  private final XmlDBCollection collection;

  private final XmlNodeReadOnlyTrx rtx;

  public SirixNodeKeyStream(final Iterator<NodeReferences> iter, final XmlDBCollection collection,
      final XmlNodeReadOnlyTrx rtx) {
    this.iter = checkNotNull(iter);
    this.collection = checkNotNull(collection);
    this.rtx = checkNotNull(rtx);
  }

  @Override
  public XmlDBNode next() {
    while (iter.hasNext()) {
      final NodeReferences nodeReferences = iter.next();
      final Set<Long> nodeKeys = nodeReferences.getNodeKeys();
      for (final long nodeKey : nodeKeys) {
        rtx.moveTo(nodeKey);
        return new XmlDBNode(rtx, collection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
