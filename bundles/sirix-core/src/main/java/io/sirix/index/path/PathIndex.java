package io.sirix.index.path;

import io.sirix.index.IndexDef;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTLongIndexReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.ChangeListener;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.utils.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public interface PathIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final PathFilter filter) {
    final HOTLongIndexReader reader =
        HOTLongIndexReader.create(storageEngineReader, indexDef.getType(), indexDef.getID());

    if (filter != null && filter.getPCRs().size() == 1) {
      // Single PCR lookup
      final long pcr = filter.getPCRs().iterator().next();
      final NodeReferences refs = reader.get(pcr, SearchMode.EQUAL);
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
            final Map.Entry<Long, NodeReferences> entry = entryIterator.next();
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
          final NodeReferences result = next;
          next = null;
          return result;
        }
      };
    }
  }
}
