package io.sirix.index.name;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.sirix.access.IndexBackendType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.ChangeListener;
import io.sirix.index.Filter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexFilterAxis;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface NameIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter pageTrx, IndexDef indexDef);

  L createListener(StorageEngineWriter pageTrx, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader pageRtx, IndexDef indexDef, NameFilter filter) {
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
    final String sysProp = System.getProperty(NameIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = pageRtx.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }

  /**
   * Open HOT-based name index.
   */
  private Iterator<NodeReferences> openHOTIndex(StorageEngineReader pageRtx, IndexDef indexDef, NameFilter filter) {
    final HOTIndexReader<QNm> reader =
        HOTIndexReader.create(pageRtx, NameKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      // Single name lookup
      QNm name = filter.getIncludes().iterator().next();
      NodeReferences refs = reader.get(name, SearchMode.EQUAL);
      if (refs != null) {
        return Iterators.forArray(refs);
      }
      return Iterators.forArray(new NodeReferences());
    } else {
      // Iterate over all entries and apply filter
      final Set<QNm> includes = filter.getIncludes();
      final Set<QNm> excludes = filter.getExcludes();
      final Iterator<Map.Entry<QNm, NodeReferences>> entryIterator = reader.iterator();

      return new Iterator<>() {
        private NodeReferences next = null;

        @Override
        public boolean hasNext() {
          if (next != null) {
            return true;
          }
          while (entryIterator.hasNext()) {
            Map.Entry<QNm, NodeReferences> entry = entryIterator.next();
            QNm name = entry.getKey();

            // Check includes/excludes
            if ((includes.isEmpty() || includes.contains(name)) && !excludes.contains(name)) {
              next = entry.getValue();
              return true;
            }
          }
          return false;
        }

        @Override
        public NodeReferences next() {
          if (!hasNext()) {
            throw new java.util.NoSuchElementException();
          }
          NodeReferences result = next;
          next = null;
          return result;
        }
      };
    }
  }

  /**
   * Open RBTree-based name index (default).
   */
  private Iterator<NodeReferences> openRBTreeIndex(StorageEngineReader pageRtx, IndexDef indexDef, NameFilter filter) {
    final RBTreeReader<QNm, NodeReferences> reader = RBTreeReader.getInstance(
        pageRtx.getResourceSession().getIndexCache(), pageRtx, indexDef.getType(), indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getIncludes().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNodeKey<QNm>> iter = reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = ImmutableSet.of(filter);

      return new IndexFilterAxis<>(reader, iter, setFilter);
    }
  }
}
