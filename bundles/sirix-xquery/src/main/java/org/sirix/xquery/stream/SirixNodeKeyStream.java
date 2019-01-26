package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;

public final class SirixNodeKeyStream implements Stream<DBNode> {

  private final Iterator<NodeReferences> mIter;
  private final DBCollection mCollection;
  private final XdmNodeReadOnlyTrx mRtx;

  public SirixNodeKeyStream(final Iterator<NodeReferences> iter, final DBCollection collection,
      final XdmNodeReadOnlyTrx rtx) {
    mIter = checkNotNull(iter);
    mCollection = checkNotNull(collection);
    mRtx = checkNotNull(rtx);
  }

  @Override
  public DBNode next() throws DocumentException {
    while (mIter.hasNext()) {
      final NodeReferences nodeReferences = mIter.next();
      final Set<Long> nodeKeys = nodeReferences.getNodeKeys();
      for (final long nodeKey : nodeKeys) {
        mRtx.moveTo(nodeKey);
        return new DBNode(mRtx, mCollection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
