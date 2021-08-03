package org.sirix.index.cas;

import com.google.common.collect.Iterators;
import org.brackit.xquery.atomic.Atomic;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.RBTreeReader;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.settings.Fixed;

import java.util.*;
import java.util.function.Function;

public interface CASIndex<B, L extends ChangeListener, R extends NodeReadOnlyTrx & NodeCursor> {
  B createBuilder(R rtx, PageTrx pageWriteTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(PageTrx pageWriteTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, CASFilterRange filter) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceManager().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    final Iterator<RBNode<CASValue, NodeReferences>> iter =
        reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

    return new IndexFilterAxis<>(iter, Set.of(filter));
  }

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, CASFilter filter) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceManager().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    // PCRs requested.
    final Set<Long> pcrsRequested = filter == null ? Collections.emptySet() : filter.getPCRs();

    // PCRs available in index.
    final Set<Long> pcrsAvailable = filter == null
        ? Collections.emptySet()
        : filter.getPCRCollector().getPCRsForPaths(indexDef.getPaths()).getPCRs();

    // Only one path indexed and requested. All PCRs are the same in each CASValue.
    if (pcrsAvailable.size() <= 1 && pcrsRequested.size() == 1) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic != null ? atomic.type() : null, pcr);

      if (mode == SearchMode.EQUAL) {
        // Compare for equality by PCR and atomic value.
        final Optional<RBNode<CASValue, NodeReferences>> optionalNode = reader.getCurrentNode(value, mode);

        return optionalNode.map(node -> Iterators.forArray(node.getValue()))
                           .orElse(Iterators.unmodifiableIterator(Collections.emptyIterator()));
      } else {
        // Compare for search criteria by PCR and atomic value.
        final Optional<RBNode<CASValue, NodeReferences>> optionalNode = reader.getCurrentNode(value, mode);

        return optionalNode.map(concatWithFilterAxis(filter, reader)).orElse(Collections.emptyIterator());
      }
    } else if (pcrsRequested.size() == 1) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic.type(), pcr);

      if (mode == SearchMode.EQUAL) {
        // Compare for equality by PCR and atomic value.
        final Optional<RBNode<CASValue, NodeReferences>> optionalNode = reader.getCurrentNode(value, mode);

        return optionalNode.map(concatWithFilterAxis(filter, reader)).orElse(Collections.emptyIterator());
      } else {
        // Compare for equality only by PCR.
        final Optional<RBNode<CASValue, NodeReferences>> optionalNode =
            reader.getCurrentNode(value, SearchMode.EQUAL, Comparator.comparingLong(CASValue::getPathNodeKey));

        return optionalNode.map(findFirstNodeWithMatchingPCRAndAtomicValue(filter, reader, mode, value))
                           .orElse(Collections.emptyIterator());
      }
    } else {
      final Iterator<RBNode<CASValue, NodeReferences>> iter =
          reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

      return new IndexFilterAxis<>(iter, Set.of(filter));
    }
  }

  private Function<RBNode<CASValue, NodeReferences>, Iterator<NodeReferences>> findFirstNodeWithMatchingPCRAndAtomicValue(
      CASFilter filter, RBTreeReader<CASValue, NodeReferences> reader, SearchMode mode, CASValue value) {
    return node -> {
      // Now compare for equality by PCR and atomic value and find first
      // node which satisfies criteria.
      final Optional<RBNode<CASValue, NodeReferences>> firstFoundNode =
          reader.getCurrentNode(node.getNodeKey(), value, mode);

      return firstFoundNode.map(theNode -> {
        // Iterate over subtree.
        final Iterator<RBNode<CASValue, NodeReferences>> iter = reader.new RBNodeIterator(theNode.getNodeKey());

        return (Iterator<NodeReferences>) new IndexFilterAxis<>(iter, Set.of(filter));
      }).orElse(Collections.emptyIterator());
    };
  }

  private Function<RBNode<CASValue, NodeReferences>, Iterator<NodeReferences>> concatWithFilterAxis(CASFilter filter,
      RBTreeReader<CASValue, NodeReferences> reader) {
    return node -> {
      // Iterate over subtree.
      final Iterator<RBNode<CASValue, NodeReferences>> iter = reader.new RBNodeIterator(node.getNodeKey());

      return new IndexFilterAxis<>(iter, Set.of(filter));
    };
  }
}
