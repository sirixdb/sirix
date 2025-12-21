package io.sirix.index.name;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.*;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public interface NameIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter pageTrx, IndexDef indexDef);

  L createListener(StorageEngineWriter pageTrx, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader pageRtx, IndexDef indexDef, NameFilter filter) {
    final RBTreeReader<QNm, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceSession().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getIncludes().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNodeKey<QNm>> iter =
          reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = ImmutableSet.of(filter);

      return new IndexFilterAxis<>(reader, iter, setFilter);
    }
  }
}
