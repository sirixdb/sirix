package io.sirix.index.path;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.sirix.index.*;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public interface PathIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(StorageEngineWriter pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    // Note: HOT index reading is handled by the listeners during indexing.
    // The index query still uses RBTreeReader for now because RBTreeReader
    // is the standard query interface. Full HOT query support requires
    // implementing HOTLongIndexReader integration.
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
