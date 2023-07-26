package org.sirix.index;

import java.util.Iterator;
import java.util.Set;
import org.sirix.index.redblacktree.RBNodeKey;
import org.sirix.index.redblacktree.RBTreeReader;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import com.google.common.collect.AbstractIterator;

import static java.util.Objects.requireNonNull;

public final class IndexFilterAxis<K extends Comparable<? super K>>
    extends AbstractIterator<NodeReferences> {

  private final RBTreeReader<K, NodeReferences> treeReader;

  private final Iterator<RBNodeKey<K>> iter;

  private final Set<? extends Filter> filter;


  public IndexFilterAxis(final RBTreeReader<K, NodeReferences> treeReader, final Iterator<RBNodeKey<K>> iter,
      final Set<? extends Filter> filter) {
    this.treeReader = requireNonNull(treeReader);
    this.iter = requireNonNull(iter);
    this.filter = requireNonNull(filter);
  }

  @Override
  protected NodeReferences computeNext() {
    while (iter.hasNext()) {
      final RBNodeKey<K> node = iter.next();
      boolean filterResult = true;
      for (final Filter filter : filter) {
        filterResult = filter.filter(node);
        if (!filterResult) {
          break;
        }
      }
      if (filterResult) {
        treeReader.moveTo(node.getValueNodeKey());
        assert treeReader.getCurrentNodeAsRBNodeValue() != null;
        return treeReader.getCurrentNodeAsRBNodeValue().getValue();
      }
    }
    return endOfData();
  }
}
