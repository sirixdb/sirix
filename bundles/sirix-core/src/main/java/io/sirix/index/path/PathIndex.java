package io.sirix.index.path;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.sirix.access.IndexBackendType;
import io.sirix.index.Filter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexFilterAxis;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTLongIndexReader;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.ChangeListener;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public interface PathIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(StorageEngineWriter pageTrx, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final PathFilter filter) {

    // Check if HOT is enabled (system property takes precedence, then resource config)
    if (isHOTEnabled(pageRtx)) {
      return openHOTIndex(pageRtx, indexDef, filter);
    }

    // Use RBTree (default)
    return openRBTreeIndex(pageRtx, indexDef, filter);
  }

  /**
   * Checks if HOT indexes should be used for reading.
   */
  private static boolean isHOTEnabled(final StorageEngineReader pageRtx) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(PathIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = pageRtx.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }

  /**
   * Open HOT-based path index.
   */
  private Iterator<NodeReferences> openHOTIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    final HOTLongIndexReader reader = HOTLongIndexReader.create(pageRtx, indexDef.getType(), indexDef.getID());

    if (filter != null && filter.getPCRs().size() == 1) {
      // Single PCR lookup
      long pcr = filter.getPCRs().iterator().next();
      NodeReferences refs = reader.get(pcr, SearchMode.EQUAL);
      if (refs != null) {
        return Iterators.forArray(refs);
      }
      return Collections.emptyIterator();
    } else {
      // Iterate over all entries and apply filter
      final Set<Long> pcrsRequested = filter != null
          ? filter.getPCRs()
          : Set.of();
      final Iterator<Map.Entry<Long, NodeReferences>> entryIterator = reader.iterator();

      return new Iterator<>() {
        private NodeReferences next = null;

        @Override
        public boolean hasNext() {
          if (next != null) {
            return true;
          }
          while (entryIterator.hasNext()) {
            Map.Entry<Long, NodeReferences> entry = entryIterator.next();
            if (pcrsRequested.isEmpty() || pcrsRequested.contains(entry.getKey())) {
              next = entry.getValue();
              return true;
            }
          }
          return false;
        }

        @Override
        public NodeReferences next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          NodeReferences result = next;
          next = null;
          return result;
        }
      };
    }
  }

  /**
   * Open RBTree-based path index (default).
   */
  private Iterator<NodeReferences> openRBTreeIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    final RBTreeReader<Long, NodeReferences> reader = RBTreeReader.getInstance(
        pageRtx.getResourceSession().getIndexCache(), pageRtx, indexDef.getType(), indexDef.getID());

    if (filter != null && filter.getPCRs().size() == 1) {
      final var optionalNodeReferences = reader.get(filter.getPCRs().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNodeKey<Long>> iter = reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = filter == null
          ? ImmutableSet.of()
          : ImmutableSet.of(filter);

      return new IndexFilterAxis<>(reader, iter, setFilter);
    }
  }
}
