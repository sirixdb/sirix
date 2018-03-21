package org.sirix.index.cas;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.atomic.Atomic;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

public final class CASIndexImpl implements CASIndex<CASValue, NodeReferences> {

  @Override
  public CASIndexBuilder createBuilder(XdmNodeReadTrx rtx,
      PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    return new CASIndexBuilder(rtx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  @Override
  public CASIndexListener createListener(
      PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      PathSummaryReader pathSummaryReader, IndexDef indexDef) {
    return new CASIndexListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  @Override
  public Iterator<NodeReferences> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef,
      CASFilterRange filter) {
    final AVLTreeReader<CASValue, NodeReferences> reader =
        AVLTreeReader.getInstance(pageReadTrx, indexDef.getType(), indexDef.getID());

    final Iterator<AVLNode<CASValue, NodeReferences>> iter =
        reader.new AVLNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

    return new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter));
  }

  @Override
  public Iterator<NodeReferences> openIndex(PageReadTrx pageReadTrx, IndexDef indexDef,
      CASFilter filter) {
    final AVLTreeReader<CASValue, NodeReferences> reader =
        AVLTreeReader.getInstance(pageReadTrx, indexDef.getType(), indexDef.getID());

    // PCRs requested.
    final Set<Long> pcrsRequested = filter.getPCRs();

    // PCRs available in index.
    final Set<Long> pcrsAvailable =
        filter.getPCRCollector().getPCRsForPaths(indexDef.getPaths()).getPCRs();

    // Only one path indexed and requested. All PCRs are the same in each
    // CASValue.
    if (pcrsAvailable.size() <= 1 && pcrsRequested.size() == 1) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic.type(), pcr);

      if (mode == SearchMode.EQUAL) {
        // Compare for equality by PCR and atomic value.
        final Optional<AVLNode<CASValue, NodeReferences>> node = reader.getAVLNode(value, mode);

        if (node.isPresent()) {
          return Iterators.forArray(node.get().getValue());
        }

        return Collections.emptyIterator();
      } else {
        // Compare for search criteria by PCR and atomic value.
        final Optional<AVLNode<CASValue, NodeReferences>> node = reader.getAVLNode(value, mode);

        if (node.isPresent()) {
          // Iterate over subtree.
          final Iterator<AVLNode<CASValue, NodeReferences>> iter =
              reader.new AVLNodeIterator(node.get().getNodeKey());

          return Iterators.concat(Iterators.forArray(node.get().getValue()),
              new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter)));
        }

        return Collections.emptyIterator();
      }
    } else if (pcrsRequested.size() == 1) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic.type(), pcr);

      if (mode == SearchMode.EQUAL) {
        // Compare for equality by PCR and atomic value.
        final Optional<AVLNode<CASValue, NodeReferences>> node = reader.getAVLNode(value, mode);

        if (node.isPresent()) {
          // Iterate over subtree.
          final Iterator<AVLNode<CASValue, NodeReferences>> iter =
              reader.new AVLNodeIterator(node.get().getNodeKey());

          return Iterators.concat(Iterators.forArray(node.get().getValue()),
              new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter)));
        }

        return Collections.emptyIterator();
      } else {
        // Compare for equality only by PCR.
        final Optional<AVLNode<CASValue, NodeReferences>> node =
            reader.getAVLNode(value, SearchMode.EQUAL, (CASValue v1,
                CASValue v2) -> ((Long) v1.getPathNodeKey()).compareTo(v2.getPathNodeKey()));

        if (node.isPresent()) {
          // Now compare for equality by PCR and atomic value and find first
          // node which satisfies criteria.
          final Optional<AVLNode<CASValue, NodeReferences>> firstFoundNode =
              reader.getAVLNode(node.get().getNodeKey(), value, mode);

          if (firstFoundNode.isPresent()) {
            // Iterate over subtree.
            final Iterator<AVLNode<CASValue, NodeReferences>> iter =
                reader.new AVLNodeIterator(firstFoundNode.get().getNodeKey());

            return Iterators.concat(Iterators.forArray(firstFoundNode.get().getValue()),
                new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter)));
          } else {
            return Iterators.forArray(firstFoundNode.get().getValue());
          }
        }

        return Collections.emptyIterator();
      }
    } else {
      final Iterator<AVLNode<CASValue, NodeReferences>> iter =
          reader.new AVLNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

      return new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter));
    }
  }
}
