package org.sirix.index.path;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.*;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBNodeKey;
import org.sirix.index.redblacktree.RBTreeReader;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.settings.Fixed;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public interface PathIndex<B, L extends ChangeListener> {
  B createBuilder(PageTrx pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(PageTrx pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    final RBTreeReader<Long, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceSession().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    if (filter != null && filter.getPCRs().size() == 1) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getPCRs().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNodeKey<Long>> iter =
          reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = filter == null ? ImmutableSet.of() : ImmutableSet.of(filter);

      return new IndexFilterAxis<>(reader, iter, setFilter);
    }
  }
}
