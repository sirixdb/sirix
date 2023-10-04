package io.sirix.index.cas;

import com.google.common.collect.Iterators;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.PageTrx;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexFilterAxis;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.RBNodeValue;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.Atomic;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.*;
import java.util.function.Function;

public interface CASIndex<B, L extends ChangeListener, R extends NodeReadOnlyTrx & NodeCursor> {
  B createBuilder(R rtx, PageTrx pageWriteTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(PageTrx pageWriteTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, CASFilterRange filter) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceSession().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    final Iterator<RBNodeKey<CASValue>> iter = reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

    return new IndexFilterAxis<>(reader, iter, Set.of(filter));
  }

  default Iterator<NodeReferences> openIndex(PageReadOnlyTrx pageRtx, IndexDef indexDef, CASFilter filter) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(pageRtx.getResourceSession().getIndexCache(),
                                 pageRtx,
                                 indexDef.getType(),
                                 indexDef.getID());

    // PCRs requested.
    final Set<Long> pcrsRequested = filter == null ? Set.of() : filter.getPCRs();

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
        final Optional<RBNodeKey<CASValue>> optionalNode = reader.getCurrentNodeAsRBNodeKey(value, mode);

        return optionalNode.map(node -> {
          reader.moveTo(node.getValueNodeKey());
          final RBNodeValue<NodeReferences> currentNodeAsRBNodeValue = reader.getCurrentNodeAsRBNodeValue();
          assert currentNodeAsRBNodeValue != null;
          return Iterators.forArray(currentNodeAsRBNodeValue.getValue());
        }).orElse(Iterators.unmodifiableIterator(Collections.emptyIterator()));
      } else {
        // Compare for search criteria by PCR and atomic value.
        final Optional<RBNodeKey<CASValue>> optionalNode = reader.getCurrentNodeAsRBNodeKey(value, mode);

        return optionalNode.map(concatWithFilterAxis(filter, reader)).orElse(Collections.emptyIterator());
      }
    } else if (pcrsRequested.size() == 1) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic.type(), pcr);

      if (mode == SearchMode.EQUAL) {
        // Compare for equality by PCR and atomic value.
        final Optional<RBNodeKey<CASValue>> optionalNode = reader.getCurrentNodeAsRBNodeKey(value, mode);

        return optionalNode.map(concatWithFilterAxis(filter, reader)).orElse(Collections.emptyIterator());
      } else {
        // Compare for equality only by PCR.
        final Optional<RBNodeKey<CASValue>> optionalNode = reader.getCurrentNodeAsRBNodeKey(value,
                                                                                            SearchMode.EQUAL,
                                                                                            Comparator.comparingLong(
                                                                                                CASValue::getPathNodeKey));

        return optionalNode.map(findFirstNodeWithMatchingPCRAndAtomicValue(filter, reader, mode, value))
                           .orElse(Collections.emptyIterator());
      }
    } else {
      final Iterator<RBNodeKey<CASValue>> iter =
          reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

      return new IndexFilterAxis<>(reader, iter, filter == null ? Set.of() : Set.of(filter));
    }
  }

  private Function<RBNodeKey<CASValue>, Iterator<NodeReferences>> findFirstNodeWithMatchingPCRAndAtomicValue(
      CASFilter filter, RBTreeReader<CASValue, NodeReferences> reader, SearchMode mode, CASValue value) {
    return node -> {
      // Now compare for equality by PCR and atomic value and find first
      // node which satisfies criteria.
      final Optional<RBNodeKey<CASValue>> firstFoundNode =
          reader.getCurrentNodeAsRBNodeKey(node.getNodeKey(), value, mode);

      return firstFoundNode.map(theNode -> {
        // Iterate over subtree.
        final Iterator<RBNodeKey<CASValue>> iter = reader.new RBNodeIterator(theNode.getNodeKey());

        return (Iterator<NodeReferences>) new IndexFilterAxis<>(reader, iter, Set.of(filter));
      }).orElse(Collections.emptyIterator());
    };
  }

  private Function<RBNodeKey<CASValue>, Iterator<NodeReferences>> concatWithFilterAxis(CASFilter filter,
      RBTreeReader<CASValue, NodeReferences> reader) {
    return node -> {
      // Iterate over subtree.
      final Iterator<RBNodeKey<CASValue>> iter = reader.new RBNodeIterator(node.getNodeKey());

      return new IndexFilterAxis<>(reader, iter, Set.of(filter));
    };
  }
}
