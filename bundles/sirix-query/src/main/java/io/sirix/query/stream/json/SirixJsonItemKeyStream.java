package io.sirix.query.stream.json;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Stream;
import org.jspecify.annotations.Nullable;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public final class SirixJsonItemKeyStream implements Stream<Item> {

  private static final JsonItemFactory itemFactory = new JsonItemFactory();

  private final Iterator<NodeReferences> iter;

  private final JsonDBCollection collection;

  private final JsonNodeReadOnlyTrx rtx;

  private Iterator<Long> nodeKeys;

  public SirixJsonItemKeyStream(final Iterator<NodeReferences> iter, final JsonDBCollection collection,
      final JsonNodeReadOnlyTrx rtx) {
    this.iter = requireNonNull(iter);
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
  }

  @Override
  public Item next() {
    // Skip over EMPTY NodeReferences and STALE keys ITERATIVELY (no recursion — a long run of
    // stale entries must not grow the stack). Previously the first empty entry terminated the
    // whole scan (RBTreeWriter.remove leaves emptied tree nodes in place → truncated results),
    // and an unchecked moveTo materialized the PREVIOUS node for stale keys (ghost results).
    // Mirrors the XML SirixNodeKeyStream skip loop.
    while (true) {
      if (nodeKeys != null && nodeKeys.hasNext()) {
        final long nodeKey = nodeKeys.next();
        if (rtx.moveTo(nodeKey)) {
          return itemFactory.getSequence(rtx, collection);
        }
        continue; // stale key (node deleted) — skip
      }
      if (!iter.hasNext()) {
        return null;
      }
      nodeKeys = iter.next().getNodeKeys().iterator();
    }
  }

  @Override
  public void close() {}

}
