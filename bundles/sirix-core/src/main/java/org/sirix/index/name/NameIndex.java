package org.sirix.index.name;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.*;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public interface NameIndex<B, L extends ChangeListener> {
  B createBuilder(PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, IndexDef indexDef);

  L createListener(PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, NameFilter filter) {
    final AVLTreeReader<QNm, NodeReferences> reader =
        AVLTreeReader.getInstance(pageRtx, indexDef.getType(), indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getIncludes().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<AVLNode<QNm, NodeReferences>> iter =
          reader.new AVLNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = filter == null
          ? ImmutableSet.of()
          : ImmutableSet.of(filter);

      return new IndexFilterAxis<>(iter, setFilter);
    }
  }
}
