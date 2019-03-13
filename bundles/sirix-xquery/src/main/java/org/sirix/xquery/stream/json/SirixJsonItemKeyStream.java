package org.sirix.xquery.stream.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBNode;

public final class SirixJsonItemKeyStream implements Stream<JsonDBNode> {

  private final Iterator<NodeReferences> mIter;

  private final JsonDBCollection mCollection;

  private final JsonNodeReadOnlyTrx mRtx;

  public SirixJsonItemKeyStream(final Iterator<NodeReferences> iter, final JsonDBCollection collection,
      final JsonNodeReadOnlyTrx rtx) {
    mIter = checkNotNull(iter);
    mCollection = checkNotNull(collection);
    mRtx = checkNotNull(rtx);
  }

  @Override
  public JsonDBNode next() {
    while (mIter.hasNext()) {
      final NodeReferences nodeReferences = mIter.next();
      final Set<Long> nodeKeys = nodeReferences.getNodeKeys();
      for (final long nodeKey : nodeKeys) {
        mRtx.moveTo(nodeKey);
        return new JsonDBNode(mRtx, mCollection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
