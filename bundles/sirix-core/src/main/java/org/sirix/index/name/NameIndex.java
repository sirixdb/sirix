package org.sirix.index.name;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.ChangeListener;
import org.sirix.index.Filter;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.RBTreeReader;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.settings.Fixed;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

public interface NameIndex<B, L extends ChangeListener> {
  B createBuilder(PageTrx pageTrx, IndexDef indexDef);

  L createListener(PageTrx pageTrx, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, NameFilter filter) {
    final RBTreeReader<QNm, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceManager().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getIncludes().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNode<QNm, NodeReferences>> iter =
          reader.new AVLNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = filter == null ? ImmutableSet.of() : ImmutableSet.of(filter);

      return new IndexFilterAxis<>(iter, setFilter);
    }
  }
}
