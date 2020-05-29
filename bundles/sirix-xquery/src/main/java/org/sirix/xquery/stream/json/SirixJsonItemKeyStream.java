package org.sirix.xquery.stream.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBObject;

public final class SirixJsonItemKeyStream implements Stream<JsonDBObject> {

  private final Iterator<NodeReferences> iter;

  private final JsonDBCollection collection;

  private final JsonNodeReadOnlyTrx rtx;

  public SirixJsonItemKeyStream(final Iterator<NodeReferences> iter, final JsonDBCollection collection,
      final JsonNodeReadOnlyTrx rtx) {
    this.iter = checkNotNull(iter);
    this.collection = checkNotNull(collection);
    this.rtx = checkNotNull(rtx);
  }

  @Override
  public JsonDBObject next() {
    while (iter.hasNext()) {
      final NodeReferences nodeReferences = iter.next();
      final Set<Long> nodeKeys = nodeReferences.getNodeKeys();
      for (final long nodeKey : nodeKeys) {
        rtx.moveTo(nodeKey);
        return new JsonDBObject(rtx, collection);
      }
    }
    return null;
  }

  @Override
  public void close() {}

}
