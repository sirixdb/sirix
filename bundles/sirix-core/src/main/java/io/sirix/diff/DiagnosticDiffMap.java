package io.sirix.diff;

import io.sirix.utils.ReplayWorkDiagnostics;
import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;

/**
 * Diagnostic-only map: counts keyed operations AND entry visits, including scans through values or
 * keys inherited from AbstractLong2ObjectMap. Production keeps the ordinary primitive hash map.
 */
public final class DiagnosticDiffMap extends AbstractLong2ObjectMap<DiffTuple> {
  private final Long2ObjectOpenHashMap<DiffTuple> delegate = new Long2ObjectOpenHashMap<>();

  @Override
  public DiffTuple get(final long key) {
    ReplayWorkDiagnostics.bookkeeping(1);
    return delegate.get(key);
  }

  @Override
  public DiffTuple put(final long key, final DiffTuple value) {
    ReplayWorkDiagnostics.bookkeeping(1);
    return delegate.put(key, value);
  }

  @Override
  public DiffTuple remove(final long key) {
    ReplayWorkDiagnostics.bookkeeping(1);
    return delegate.remove(key);
  }

  @Override
  public boolean containsKey(final long key) {
    ReplayWorkDiagnostics.bookkeeping(1);
    return delegate.containsKey(key);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void clear() {
    ReplayWorkDiagnostics.bookkeeping(delegate.size());
    delegate.clear();
  }

  @Override
  public ObjectSet<Entry<DiffTuple>> long2ObjectEntrySet() {
    return new AbstractObjectSet<>() {
      @Override
      public int size() {
        return delegate.size();
      }

      @Override
      public ObjectIterator<Entry<DiffTuple>> iterator() {
        final var iterator = delegate.long2ObjectEntrySet().iterator();
        return new ObjectIterator<>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Entry<DiffTuple> next() {
            ReplayWorkDiagnostics.bookkeeping(1);
            return iterator.next();
          }

          @Override
          public void remove() {
            ReplayWorkDiagnostics.bookkeeping(1);
            iterator.remove();
          }
        };
      }
    };
  }
}
