package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.AbstractHOTIndexReader.RawKeyBytes;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.utils.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public interface NameIndex<B, L extends ChangeListener> {
  B createBuilder(StorageEngineWriter storageEngineWriter, IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, IndexDef indexDef);

  /** Open the canonical HOT-backed NAME index. */
  default Iterator<NodeReferences> openIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final NameFilter filter) {
    requireNonNull(storageEngineReader);
    requireNonNull(indexDef);
    requireNonNull(filter);
    if (indexDef.getType() != IndexType.NAME) {
      throw new IllegalArgumentException("NAME reader requires an IndexType.NAME definition");
    }
    final HOTIndexReader<QNm> reader =
        HOTIndexReader.create(storageEngineReader, NameKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());
    final Set<QNm> includes = filter.getIncludes();
    final Set<QNm> excludes = filter.getExcludes();

    if (includes.size() == 1 && excludes.isEmpty()) {
      final QNm name = includes.iterator().next();
      final NodeReferences refs = reader.get(name, SearchMode.EQUAL);
      if (refs == null) {
        return Collections.emptyIterator();
      }
      // A name past the routable bound shares its stored key with every name of the same prefix, so
      // those postings are candidates (FMT-305). A name inside the bound returns here untouched.
      final NodeReferences exact = NameIndexRecheck.exactPostings(storageEngineReader, refs, name);
      return exact == null
          ? Collections.emptyIterator()
          : Iterators.forArray(exact);
    }

    final boolean filtering = !includes.isEmpty() || !excludes.isEmpty();
    final Iterator<Map.Entry<QNm, NodeReferences>> entries = reader.iterator();
    return new Iterator<>() {
      private NodeReferences next;
      private NameIndexRecheck.NamePredicate recheckFilter;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        while (entries.hasNext()) {
          final Map.Entry<QNm, NodeReferences> entry = entries.next();
          if (!filtering) {
            // Every posting is included, so no name decides anything — and the entry's key stays
            // undeserialized, which is one name fewer to build per entry of the whole index.
            next = entry.getValue();
            return true;
          }
          final RawKeyBytes raw = (RawKeyBytes) entry;
          if (NameKeySerializer.isTruncated(raw.rawKeyBytes(), 0, raw.rawKeyLength())) {
            if (recheckFilter == null) {
              recheckFilter = NameIndexRecheck.filter(includes, excludes);
            }
            final NodeReferences filtered =
                NameIndexRecheck.filteredPostings(storageEngineReader, entry.getValue(), recheckFilter);
            if (filtered != null) {
              next = filtered;
              return true;
            }
            continue;
          }
          final QNm name = entry.getKey();
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
          throw new NoSuchElementException();
        }
        final NodeReferences result = next;
        next = null;
        return result;
      }
    };
  }
}
