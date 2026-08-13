package io.sirix.index.cas;

import io.sirix.utils.Iterators;
import io.sirix.access.IndexBackendType;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.AtomicUtil;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexFilterAxis;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.AbstractHOTIndexReader;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.RBNodeKey;
import io.sirix.index.redblacktree.RBNodeValue;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.Atomic;
import io.sirix.index.path.summary.PathSummaryReader;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public interface CASIndex<B, L extends ChangeListener, R extends NodeReadOnlyTrx & NodeCursor> {
  B createBuilder(R rtx, StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader,
      IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilterRange filter) {
    // Check if HOT is enabled (system property takes precedence, then resource config)
    if (isHOTEnabled(storageEngineReader)) {
      return openHOTIndexWithRangeFilter(storageEngineReader, indexDef, filter);
    }

    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(storageEngineReader.getResourceSession().getIndexCache(), storageEngineReader,
            indexDef.getType(), indexDef.getID());

    final Iterator<RBNodeKey<CASValue>> iter = reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

    return new IndexFilterAxis<>(reader, iter, Set.of(filter));
  }

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilter filter) {
    // Check if HOT is enabled (system property takes precedence, then resource config)
    if (isHOTEnabled(storageEngineReader)) {
      return openHOTIndexWithFilter(storageEngineReader, indexDef, filter);
    }

    // Use RBTree (default)
    return openRBTreeIndex(storageEngineReader, indexDef, filter);
  }

  /**
   * Checks if HOT indexes should be used for reading.
   */
  private static boolean isHOTEnabled(final StorageEngineReader storageEngineReader) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(CASIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineReader.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }

  /**
   * Open HOT-based CAS index.
   */
  private Iterator<NodeReferences> openHOTIndex(StorageEngineReader storageEngineReader, IndexDef indexDef) {
    final HOTIndexReader<CASValue> reader =
        HOTIndexReader.create(storageEngineReader, CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

    // Iterate over all entries
    final Iterator<Map.Entry<CASValue, NodeReferences>> entryIterator = reader.iterator();

    return new Iterator<>() {
      private NodeReferences next = null;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        if (entryIterator.hasNext()) {
          next = entryIterator.next().getValue();
          return true;
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

  /**
   * Open HOT-based CAS index with range filter. Applies min/max bounds and inclusivity to filter
   * results.
   */
  private Iterator<NodeReferences> openHOTIndexWithRangeFilter(StorageEngineReader storageEngineReader,
      IndexDef indexDef, CASFilterRange filter) {
    final HOTIndexReader<CASValue> reader =
        HOTIndexReader.create(storageEngineReader, CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

    // Bounded-cursor fast path. Bound inclusivity is enforced INSIDE the cursor, on each group's
    // logical key bytes: index keys are not prefix-free (a string value is raw UTF-8 with no
    // terminator), so the composite byte window is wider than the logical range — "carpet" sorts
    // below the ceiling built for "car" — and trimming positionally here would both admit those
    // extensions and miss an equal group that is not first or last.
    // Gated on the content type, not just on the bounds: the cursor decides a range by unsigned BYTE
    // order over the serialized key, which is the value order only for the families
    // CASKeySerializer encodes deliberately. The instant family (xs:dateTime/xs:date/xs:time) is
    // stored as its raw lexical form, whose text order is NOT chronological order, so
    // isByteOrderPreserving reports false for it and those queries fall through to the full scan
    // below, which compares typed atomics via CASFilterRange#inRange.
    if (filter != null && filter.getPCRs().size() == 1 && (filter.getMin() != null || filter.getMax() != null)
        && CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
      final Set<Long> pcrsRequested = filter.getPCRs();
      final long pcr = pcrsRequested.iterator().next();
      final Atomic min = filter.getMin();
      final Atomic max = filter.getMax();

      // Two-sided: the logical range is ONE contiguous composite range. CAS keys serialize
      // PCR-major (the sign-flipped pathNodeKey is the first 8 bytes), so BOTH bounds pin the same
      // PCR prefix and every key between them carries it — no per-entry PCR check is needed, and
      // nothing in the scan deserializes a key at all.
      if (min != null && max != null) {
        return valuesOf(reader.range(new CASValue(min, indexDef.getContentType(), pcr),
            new CASValue(max, indexDef.getContentType(), pcr), filter.isMinInclusive(), filter.isMaxInclusive()));
      }

      // One-sided: only ONE end pins the PCR, so the open end runs straight off this PCR's key range
      // into its neighbours' — a `>= min` cursor keeps going into every higher PCR, and a `<= max`
      // cursor starts at the first key of the index, below every lower PCR. The check is therefore
      // UNCONDITIONAL here. It is tempting to skip it when the path summary reports a single PCR,
      // but the summary describes the paths at the QUERY revision while the index holds whatever
      // every revision put there: a path node dropped and re-created takes a new pathNodeKey, and
      // the stale postings under the old one live exactly at the open end. The check is cheap
      // because it reads the PCR in place — it never materializes a key.
      final Iterator<Map.Entry<CASValue, NodeReferences>> boundedIterator = min != null
          ? reader.iteratorFrom(new CASValue(min, indexDef.getContentType(), pcr), filter.isMinInclusive())
          : reader.iteratorTo(new CASValue(max, indexDef.getContentType(), pcr), filter.isMaxInclusive());
      return valuesOfMatchingPCR(boundedIterator, pcrsRequested);
    }

    // Full scan with range filter applied
    final Iterator<Map.Entry<CASValue, NodeReferences>> entryIterator = reader.iterator();
    final CASFilterRange rangeFilter = filter;

    return new Iterator<>() {
      private NodeReferences next = null;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        while (entryIterator.hasNext()) {
          Map.Entry<CASValue, NodeReferences> entry = entryIterator.next();
          CASValue key = entry.getKey();

          // Apply range filter
          if (rangeFilter == null || matchesRangeFilter(key, rangeFilter)) {
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

      private boolean matchesRangeFilter(CASValue key, CASFilterRange f) {
        // Check PCRs
        Set<Long> filterPCRs = f.getPCRs();
        if (filterPCRs != null && !filterPCRs.isEmpty() && !filterPCRs.contains(key.getPathNodeKey())) {
          return false;
        }

        // Check range bounds
        return f.inRange(AtomicUtil.toType(key.getAtomicValue(), key.getType()));
      }
    };
  }

  /**
   * Project a bounded index cursor to its posting lists. The cursor already enforced every bound, so
   * this never touches a key — {@code LazyKeyEntry} keys stay undeserialized.
   */
  private static Iterator<NodeReferences> valuesOf(final Iterator<Map.Entry<CASValue, NodeReferences>> entries) {
    // CloseForwardingIterator, not a bare Iterator: the underlying ChunkAggregatingIterator is
    // AutoCloseable and documents that an abandoned scan MUST route through close(), because
    // nothing else will. Erasing that here is what made the contract unreachable for every
    // short-circuiting consumer (a positional predicate, fn:head, an early-exit filter).
    return new CloseForwardingIterator(entries) {
      @Override
      public NodeReferences next() {
        return entries.next().getValue();
      }
    };
  }

  /**
   * Projects an entry iterator to its values while forwarding {@link AutoCloseable#close()} to the
   * source when the source has one. Subclasses supply {@link #next()}.
   */
  abstract class CloseForwardingIterator implements Iterator<NodeReferences>, AutoCloseable {
    private final Iterator<Map.Entry<CASValue, NodeReferences>> source;

    CloseForwardingIterator(final Iterator<Map.Entry<CASValue, NodeReferences>> source) {
      this.source = requireNonNull(source);
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public void close() throws Exception {
      if (source instanceof AutoCloseable closeable) {
        closeable.close();
      }
    }
  }

  /**
   * {@link #valuesOf} plus a per-entry path-class-record check, for the case where the requested PCRs
   * could not be resolved up front and the index may hold several.
   */
  private static Iterator<NodeReferences> valuesOfMatchingPCR(
      final Iterator<Map.Entry<CASValue, NodeReferences>> entries, final Set<Long> pcrs) {
    // Unbox once, up front: the membership test runs per entry and Set<Long>#contains(long) boxes
    // on every call. A PCR set holds one entry per indexed path, so a linear scan over a
    // cache-resident long[] beats hashing it.
    final long[] acceptedPCRs = new long[pcrs.size()];
    int i = 0;
    for (final Long pcr : pcrs) {
      acceptedPCRs[i++] = pcr;
    }
    return new Iterator<>() {
      private @Nullable NodeReferences next;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        while (entries.hasNext()) {
          final Map.Entry<CASValue, NodeReferences> entry = entries.next();
          if (containsPCR(acceptedPCRs, pathNodeKeyOf(entry))) {
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

  /**
   * The entry's path class record, without materializing its key where possible. The HOT readers emit
   * {@link AbstractHOTIndexReader.RawKeyBytes} entries, whose serialized key already begins with the
   * sign-flipped pathNodeKey — so this is an eight-byte read plus an XOR, against a full UTF-8 decode
   * and four allocations per entry for {@code getKey()}. Any other entry shape falls back to the key
   * itself.
   */
  private static long pathNodeKeyOf(final Map.Entry<CASValue, NodeReferences> entry) {
    if (entry instanceof AbstractHOTIndexReader.RawKeyBytes raw && raw.rawKeyLength() >= Long.BYTES) {
      return CASKeySerializer.pathNodeKeyAt(raw.rawKeyBytes(), 0);
    }
    final CASValue key = entry.getKey();
    if (key == null) {
      throw new IllegalStateException("index entry carries neither raw key bytes nor a decodable key");
    }
    return key.getPathNodeKey();
  }

  /** Primitive membership test over a tiny, unsorted PCR array — no boxing, no hashing. */
  private static boolean containsPCR(final long[] acceptedPCRs, final long pcr) {
    for (int i = 0, n = acceptedPCRs.length; i < n; i++) {
      if (acceptedPCRs[i] == pcr) {
        return true;
      }
    }
    return false;
  }

  /**
   * Open HOT-based CAS index with filter.
   */
  private Iterator<NodeReferences> openHOTIndexWithFilter(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilter filter) {
    final HOTIndexReader<CASValue> reader =
        HOTIndexReader.create(storageEngineReader, CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

    // PCRs requested.
    final Set<Long> pcrsRequested = filter == null
        ? Set.of()
        : filter.getPCRs();

    // PCRs available in index.
    final Set<Long> pcrsAvailable = filter == null
        ? Collections.emptySet()
        : filter.getPCRCollector().getPCRsForPaths(indexDef.getPaths()).getPCRs();

    // Only one path indexed and requested. All PCRs are the same in each CASValue.
    // A null probe key has nothing to seek to, so it falls through to the scan below.
    if (pcrsAvailable.size() <= 1 && pcrsRequested.size() == 1 && filter != null && filter.getKey() != null) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      // The probe key must be typed like the entries the index stores, NOT like the atomic the
      // caller happened to pass: a HOT key is a byte string that carries the type id, so probing
      // an xs:decimal index with, say, an xs:double of the same numeric value produced a
      // different key and the lookup silently found nothing. (The RBTree backend never had this
      // problem — CASValue#compareTo coerces both sides with asType before comparing.)
      final CASValue value = new CASValue(atomic, indexDef.getContentType(), pcr);

      if (mode == SearchMode.EQUAL) {
        // Direct lookup
        NodeReferences refs = reader.get(value, mode);
        if (refs != null) {
          return Iterators.forArray(refs);
        }
        return Collections.emptyIterator();
      }

      // If the single PCR the summary can resolve is NOT the requested one, no entry can match at
      // all. Note this only ever SHORT-CIRCUITS a scan; it is never grounds for skipping the
      // per-entry PCR check on a cursor that survives it, because the summary describes the query
      // revision while the index holds every revision's postings (see the one-sided branch below).
      if (pcrsAvailable.size() == 1 && !pcrsRequested.containsAll(pcrsAvailable)) {
        return Collections.emptyIterator();
      }

      // Range queries: a bounded cursor seeks straight to the bound and stops at it. Inclusivity is
      // the cursor's job (see the range-filter path above for why positional trimming is wrong), and
      // with the PCR check hoisted out nothing in these scans deserializes a key at all. Same
      // content-type gate as the range-filter path: byte order decides these bounds, so a type whose
      // key bytes are its raw lexical form must use the typed comparison in the full scan instead.
      if (CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
        final Iterator<Map.Entry<CASValue, NodeReferences>> rangeIter = switch (mode) {
          case GREATER, GREATER_OR_EQUAL -> reader.iteratorFrom(value, mode == SearchMode.GREATER_OR_EQUAL);
          case LOWER, LOWER_OR_EQUAL -> reader.iteratorTo(value, mode == SearchMode.LOWER_OR_EQUAL);
          default -> null;
        };
        if (rangeIter != null) {
          // The PCR check is UNCONDITIONAL on these cursors, exactly as on the one-sided branch of
          // openHOTIndexWithRangeFilter — see the reasoning there. Both of these are one-sided by
          // construction, so only ONE end pins the PCR prefix and the open end runs straight into a
          // neighbouring pathNodeKey's key range: `iteratorFrom` keeps going into every higher PCR,
          // and `iteratorTo` starts at the first key of the index, below every lower one. Skipping
          // the check when the path summary reports a single PCR (the old `pcrCheckPerEntry`
          // shortcut) is unsound for the same reason it is unsound there: the summary describes the
          // paths at the QUERY revision, while the index holds whatever every revision put there,
          // so a path node dropped and re-created leaves stale postings under its old pathNodeKey —
          // living precisely at the open end. The check reads the PCR in place and never
          // materializes a key, so it is cheap.
          return valuesOfMatchingPCR(rangeIter, pcrsRequested);
        }
      }
      // Not byte-order-preserving (or an EQUAL probe): fall through to the full scan below, which
      // compares typed atomics via CASFilterRange#inRange.
    }

    // Fall back to full scan with filter (when no specific PCR or no atomic key)
    final Iterator<Map.Entry<CASValue, NodeReferences>> entryIterator = reader.iterator();
    final CASFilter effectiveFilter = filter;

    return new Iterator<>() {
      private NodeReferences next = null;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        while (entryIterator.hasNext()) {
          Map.Entry<CASValue, NodeReferences> entry = entryIterator.next();
          CASValue key = entry.getKey();

          // Apply filter
          if (effectiveFilter == null || matchesFilter(key, effectiveFilter)) {
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

      private boolean matchesFilter(CASValue key, CASFilter f) {
        // Check PCR
        if (!f.getPCRs().isEmpty() && !f.getPCRs().contains(key.getPathNodeKey())) {
          return false;
        }

        // Check atomic value
        Atomic filterKey = f.getKey();
        if (filterKey == null) {
          return true; // No atomic filter
        }

        Atomic entryValue = key.getAtomicValue();
        return switch (f.getMode()) {
          case EQUAL -> entryValue.compareTo(filterKey) == 0;
          case GREATER -> entryValue.compareTo(filterKey) > 0;
          case GREATER_OR_EQUAL -> entryValue.compareTo(filterKey) >= 0;
          case LOWER -> entryValue.compareTo(filterKey) < 0;
          case LOWER_OR_EQUAL -> entryValue.compareTo(filterKey) <= 0;
        };
      }
    };
  }

  /**
   * Open RBTree-based CAS index (default).
   */
  private Iterator<NodeReferences> openRBTreeIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilter filter) {
    final RBTreeReader<CASValue, NodeReferences> reader =
        RBTreeReader.getInstance(storageEngineReader.getResourceSession().getIndexCache(), storageEngineReader,
            indexDef.getType(), indexDef.getID());

    // PCRs requested.
    final Set<Long> pcrsRequested = filter == null
        ? Set.of()
        : filter.getPCRs();

    // PCRs available in index.
    final Set<Long> pcrsAvailable = filter == null
        ? Collections.emptySet()
        : filter.getPCRCollector().getPCRsForPaths(indexDef.getPaths()).getPCRs();

    // Only one path indexed and requested. All PCRs are the same in each CASValue.
    if (pcrsAvailable.size() <= 1 && pcrsRequested.size() == 1 && filter != null) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      final CASValue value = new CASValue(atomic, atomic != null
          ? atomic.type()
          : null, pcr);

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
    } else if (pcrsRequested.size() == 1 && filter != null) {
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
        final Optional<RBNodeKey<CASValue>> optionalNode = reader.getCurrentNodeAsRBNodeKey(value, SearchMode.EQUAL,
            Comparator.comparingLong(CASValue::getPathNodeKey));

        return optionalNode.map(findFirstNodeWithMatchingPCRAndAtomicValue(filter, reader, mode, value))
                           .orElse(Collections.emptyIterator());
      }
    } else {
      final Iterator<RBNodeKey<CASValue>> iter =
          reader.new RBNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

      return new IndexFilterAxis<>(reader, iter, filter == null
          ? Set.of()
          : Set.of(filter));
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
