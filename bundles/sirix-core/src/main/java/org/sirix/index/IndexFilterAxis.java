package org.sirix.index;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import com.google.common.collect.AbstractIterator;

public final class IndexFilterAxis<K extends Comparable<? super K>>
    extends AbstractIterator<NodeReferences> {

  private final Iterator<RBNode<K, NodeReferences>> mIter;

  private final Set<? extends Filter> mFilter;

  public IndexFilterAxis(final Iterator<RBNode<K, NodeReferences>> iter,
      final Set<? extends Filter> filter) {
    mIter = checkNotNull(iter);
    mFilter = checkNotNull(filter);
  }

  @Override
  protected NodeReferences computeNext() {
    while (mIter.hasNext()) {
      final RBNode<K, NodeReferences> node = mIter.next();
      boolean filterResult = true;
      for (final Filter filter : mFilter) {
        filterResult = filterResult && filter.filter(node);
        if (!filterResult) {
          break;
        }
      }
      if (filterResult) {
        return node.getValue();
      }
    }
    return endOfData();
  }
}
