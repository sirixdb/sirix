package io.sirix.index.name;

import io.sirix.utils.Iterators;
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
  B createBuilder(StorageEngineWriter storageEngineWriter, IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, NameFilter filter) {
    // Check if HOT is enabled (system property takes precedence, then resource config)
    if (isHOTEnabled(storageEngineReader)) {
      return openHOTIndex(storageEngineReader, indexDef, filter);
    }

    // Use RBTree (default)
    return openRBTreeIndex(storageEngineReader, indexDef, filter);
  }

  /**
   * Checks if HOT indexes should be used for reading.
   */
  private static boolean isHOTEnabled(final StorageEngineReader storageEngineReader) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(NameIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineReader.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }

  /**
   * Open HOT-based name index.
   */
  private Iterator<NodeReferences> openHOTIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, NameFilter filter) {
    final HOTIndexReader<QNm> reader =
        HOTIndexReader.create(storageEngineReader, NameKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

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
      // Values-only: the name is used solely by the predicate above and never escapes, so no
      // Map.Entry is allocated per emitted group and no unwrapping iterator is needed here.
      return reader.valueIterator(
          name -> (includes.isEmpty() || includes.contains(name)) && !excludes.contains(name));
    }
  }

  /**
   * Open RBTree-based name index (default).
   */
  private Iterator<NodeReferences> openRBTreeIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, NameFilter filter) {
    final RBTreeReader<QNm, NodeReferences> reader = RBTreeReader.getInstance(
        storageEngineReader.getResourceSession().getIndexCache(), storageEngineReader, indexDef.getType(), indexDef.getID());

    if (filter.getIncludes().size() == 1 && filter.getExcludes().isEmpty()) {
      final Optional<NodeReferences> optionalNodeReferences =
          reader.get(filter.getIncludes().iterator().next(), SearchMode.EQUAL);
      return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
    } else {
      final Iterator<RBNodeKey<QNm>> iter = reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
      final Set<Filter> setFilter = Set.of(filter);

      return new IndexFilterAxis<>(reader, iter, setFilter);
    }
  }
}
