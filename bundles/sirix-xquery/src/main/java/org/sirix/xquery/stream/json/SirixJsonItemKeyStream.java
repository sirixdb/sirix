package org.sirix.xquery.stream.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;

import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Stream;
import org.jetbrains.annotations.Nullable;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.node.NodeKind;
import org.sirix.xquery.json.JsonDBArray;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBObject;
import org.sirix.xquery.json.JsonItemFactory;

public final class SirixJsonItemKeyStream implements Stream<Item> {

  private final Iterator<NodeReferences> iter;

  private final JsonDBCollection collection;

  private final JsonNodeReadOnlyTrx rtx;

  private final JsonItemFactory itemFactory;

  private Iterator<Long> nodeKeys;


  public SirixJsonItemKeyStream(final Iterator<NodeReferences> iter, final JsonDBCollection collection,
      final JsonNodeReadOnlyTrx rtx) {
    this.iter = checkNotNull(iter);
    this.collection = checkNotNull(collection);
    this.rtx = checkNotNull(rtx);
    itemFactory = new JsonItemFactory();
  }

  @Override
  public Item next() {
    if (nodeKeys == null || !nodeKeys.hasNext()) {
      while (iter.hasNext()) {
        final NodeReferences nodeReferences = iter.next();
        nodeKeys = nodeReferences.getNodeKeys().iterator();
        return getItem();
      }
    } else {
      return getItem();
    }
    return null;
  }

  @Nullable
  private Item getItem() {
    while (nodeKeys.hasNext()) {
      final long nodeKey = nodeKeys.next();
      rtx.moveTo(nodeKey);
      return itemFactory.getSequence(rtx, collection);
    }
    return null;
  }

  @Override
  public void close() {}

}
