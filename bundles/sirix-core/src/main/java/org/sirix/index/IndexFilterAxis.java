package org.sirix.index;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.Set;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import com.google.common.collect.AbstractIterator;

public final class IndexFilterAxis<K extends Comparable<? super K>>
    extends AbstractIterator<NodeReferences> {

  private final Iterator<AVLNode<K, NodeReferences>> mIter;

  private final Set<? extends Filter> mFilter;

  public IndexFilterAxis(final Iterator<AVLNode<K, NodeReferences>> iter,
      final Set<? extends Filter> filter) {
    mIter = checkNotNull(iter);
    mFilter = checkNotNull(filter);
  }

  @Override
  protected NodeReferences computeNext() {
    while (mIter.hasNext()) {
      final AVLNode<K, NodeReferences> node = mIter.next();
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
