package io.sirix.index.cas;

import io.sirix.utils.Iterators;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineReader.RecordPageGuard;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.AtomicUtil;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.node.interfaces.BooleanValueNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.index.hot.AbstractHOTIndexReader;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.index.path.summary.PathSummaryReader;
import org.jspecify.annotations.Nullable;

import org.roaringbitmap.longlong.LongIterator;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static io.sirix.utils.StringComparisons.compareCodePoints;

public interface CASIndex<B, L extends ChangeListener, R extends NodeReadOnlyTrx & NodeCursor> {
  B createBuilder(R rtx, StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader,
      IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilterRange filter) {
    if (filter != null && filter.hasPathConstraint() && filter.getPCRs().isEmpty()) {
      return Collections.emptyIterator();
    }
    return openHOTIndexWithRangeFilter(transactionView(storageEngineReader), indexDef, filter);
  }

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilter filter) {
    if (filter != null && filter.hasPathConstraint() && filter.getPCRs().isEmpty()) {
      return Collections.emptyIterator();
    }
    return openHOTIndexWithFilter(transactionView(storageEngineReader), indexDef, filter);
  }

  private static StorageEngineReader transactionView(final StorageEngineReader reader) {
    return reader.hasTrxIntentLog()
        ? reader.getTransactionView()
        : reader;
  }

  /**
   * Open HOT-based CAS index with range filter. Applies min/max bounds and inclusivity to filter
   * results.
   */
  private Iterator<NodeReferences> openHOTIndexWithRangeFilter(StorageEngineReader storageEngineReader,
      IndexDef indexDef, CASFilterRange filter) {
    final HOTIndexReader<CASValue> reader =
        HOTIndexReader.create(storageEngineReader, CASKeySerializer.INSTANCE, indexDef.getType(), indexDef.getID());

    final Type contentType = indexDef.getContentType();
    if (filter != null && (requiresValueResidual(filter.getMin(), contentType)
        || requiresValueResidual(filter.getMax(), contentType) || !CASKeySerializer.isByteOrderPreserving(contentType)
        || (isDecimalType(contentType) && filter.getPCRs().size() != 1))) {
      return openRangeWithResidual(storageEngineReader, reader, indexDef, filter.getPCRs(), filter.getMin(),
          filter.getMax(), filter.isMinInclusive(), filter.isMaxInclusive());
    }

    // Bounded-cursor fast path. Bound inclusivity is enforced inside the cursor, on each group's
    // logical key bytes. Capped decimal or lexical bounds take the residual path above: their
    // shared posting groups need the original document values to enforce the requested bounds.
    // Gated on the content type, not just on the bounds: the cursor decides a range by unsigned BYTE
    // order over the serialized key, which is the value order only for the families
    // CASKeySerializer encodes deliberately. Types for which isByteOrderPreserving reports false
    // fall through to the full scan below, which compares typed atomics via CASFilterRange#inRange.
    //
    // Numeric narrowing is separate from capped decimal suffixes and lexical values. Preserve the
    // ordinary bounded numeric path rather than gating it on every kind of information loss.
    if (filter != null && filter.getPCRs().size() == 1 && (filter.getMin() != null || filter.getMax() != null)
        && CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
      final Set<Long> pcrsRequested = filter.getPCRs();
      final long pcr = pcrsRequested.iterator().next();
      final Atomic min = filter.getMin();
      final Atomic max = filter.getMax();
      final boolean minInclusive = filter.isMinInclusive();
      final boolean maxInclusive = filter.isMaxInclusive();

      // Two-sided: the logical range is ONE contiguous composite range. CAS keys serialize
      // PCR-major (the sign-flipped pathNodeKey is the first 8 bytes), so BOTH bounds pin the same
      // PCR prefix and every key between them carries it — no per-entry PCR check is needed, and
      // nothing in the scan deserializes a key at all.
      if (min != null && max != null) {
        return valuesOf(reader.range(new CASValue(min, indexDef.getContentType(), pcr),
            new CASValue(max, indexDef.getContentType(), pcr), minInclusive, maxInclusive));
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
          ? reader.iteratorFrom(new CASValue(min, indexDef.getContentType(), pcr), minInclusive)
          : reader.iteratorTo(new CASValue(max, indexDef.getContentType(), pcr), maxInclusive);
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

  private static boolean requiresValueResidual(final @Nullable Atomic bound, final Type type) {
    return CASKeySerializer.truncates(bound, type) || hasLossyStringBound(bound, type);
  }

  private static boolean isDecimalType(final Type type) {
    return type.instanceOf(Type.DEC) && !type.instanceOf(Type.INR);
  }

  private static boolean hasCappedDecimalSuffix(final Map.Entry<CASValue, NodeReferences> entry) {
    return !(entry instanceof AbstractHOTIndexReader.RawKeyBytes raw)
        || CASKeySerializer.hasCappedDecimalSuffix(raw.rawKeyBytes(), 0, raw.rawKeyLength());
  }

  private static boolean hasLossyStringBound(final @Nullable Atomic bound, final Type type) {
    if (bound == null || !CASKeySerializer.isLexicalFamily(type)) {
      return false;
    }
    final String literal = bound.stringValue();
    for (int i = 0; i < literal.length(); i++) {
      final char ch = literal.charAt(i);
      if (Character.isHighSurrogate(ch)) {
        if (++i == literal.length() || !Character.isLowSurrogate(literal.charAt(i))) {
          return true;
        }
      } else if (Character.isLowSurrogate(ch)) {
        return true;
      }
    }
    return false;
  }

  // Open only an unencodable side; keep encodable bounds on the cursor, including capped bounds
  // relaxed to inclusive. Original bounds and inclusivity remain the document-value residual.
  private static Iterator<NodeReferences> openRangeWithResidual(final StorageEngineReader storageEngineReader,
      final HOTIndexReader<CASValue> reader, final IndexDef indexDef, final Set<Long> pcrs, final @Nullable Atomic min,
      final @Nullable Atomic max, final boolean minInclusive, final boolean maxInclusive) {
    final Type type = indexDef.getContentType();
    final Atomic minBound = min == null
        ? null
        : AtomicUtil.toType(min, type);
    final Atomic maxBound = max == null
        ? null
        : AtomicUtil.toType(max, type);
    final Iterator<Map.Entry<CASValue, NodeReferences>> entries =
        residualRangeEntries(reader, type, pcrs, minBound, maxBound, minInclusive, maxInclusive);
    final long[] acceptedPCRs = new long[pcrs.size()];
    int i = 0;
    for (final Long pcr : pcrs) {
      acceptedPCRs[i++] = pcr;
    }
    final boolean readValues = hasLossyStringBound(minBound, type) || hasLossyStringBound(maxBound, type);
    final boolean decimal = isDecimalType(type);
    return new CloseForwardingIterator(entries) {
      private @Nullable NodeReferences next;

      @Override
      public boolean hasNext() {
        if (next != null) {
          return true;
        }
        while (entries.hasNext()) {
          final Map.Entry<CASValue, NodeReferences> entry = entries.next();
          if (acceptedPCRs.length != 0 && !containsPCR(acceptedPCRs, pathNodeKeyOf(entry))) {
            continue;
          }
          final CASValue key = entry.getKey();
          if (readValues || (decimal
              ? hasCappedDecimalSuffix(entry)
              : CASKeySerializer.truncates(key.getAtomicValue(), type))) {
            next = exactRangeMatches(storageEngineReader, entry.getValue(), minBound, maxBound, minInclusive,
                maxInclusive, type);
          } else if (inRange(key.getAtomicValue(), minBound, maxBound, minInclusive, maxInclusive, type)) {
            next = entry.getValue();
          }
          if (next != null) {
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

  private static Iterator<Map.Entry<CASValue, NodeReferences>> residualRangeEntries(
      final HOTIndexReader<CASValue> reader, final Type type, final Set<Long> pcrs, final @Nullable Atomic min,
      final @Nullable Atomic max, final boolean minInclusive, final boolean maxInclusive) {
    final Atomic scanMin = hasLossyStringBound(min, type)
        ? null
        : min;
    final Atomic scanMax = hasLossyStringBound(max, type)
        ? null
        : max;
    if (pcrs.size() == 1 && CASKeySerializer.isByteOrderPreserving(type) && (scanMin != null || scanMax != null)) {
      final long pcr = pcrs.iterator().next();
      final boolean includeMin = minInclusive || CASKeySerializer.truncates(scanMin, type);
      final boolean includeMax = maxInclusive || CASKeySerializer.truncates(scanMax, type);
      if (scanMin != null && scanMax != null) {
        return reader.range(new CASValue(scanMin, type, pcr), new CASValue(scanMax, type, pcr), includeMin, includeMax);
      }
      return scanMin == null
          ? reader.iteratorTo(new CASValue(requireNonNull(scanMax), type, pcr), includeMax)
          : reader.iteratorFrom(new CASValue(scanMin, type, pcr), includeMin);
    }
    return reader.iterator();
  }

  private static boolean inRange(final Atomic value, final @Nullable Atomic min, final @Nullable Atomic max,
      final boolean minInclusive, final boolean maxInclusive, final Type type) {
    final boolean string = type.instanceOf(Type.STR);
    // Matching the exact type descriptor avoids a redundant cast; distinct descriptors still cast.
    @SuppressWarnings("ReferenceEquality")
    final Atomic typed = value.type() == type
        ? value
        : AtomicUtil.toType(value, type);
    final int lower = min == null
        ? 1
        : string
            ? compareCodePoints(typed.stringValue(), min.stringValue())
            : typed.compareTo(min);
    final int upper = max == null
        ? -1
        : string
            ? compareCodePoints(typed.stringValue(), max.stringValue())
            : typed.compareTo(max);
    return (lower > 0 || (lower == 0 && minInclusive)) && (upper < 0 || (upper == 0 && maxInclusive));
  }

  private static @Nullable NodeReferences exactRangeMatches(final StorageEngineReader storageEngineReader,
      final NodeReferences candidates, final @Nullable Atomic min, final @Nullable Atomic max,
      final boolean minInclusive, final boolean maxInclusive, final Type type) {
    final long candidateCount = candidates.cardinality();
    long[] matching = new long[(int) Math.min(candidateCount, 16)];
    int kept = 0;
    try (final StorageEngineReader separateReader = storageEngineReader.hasTrxIntentLog()
        ? null
        : storageEngineReader.getResourceSession().createStorageEngineReader(storageEngineReader.getRevisionNumber())) {
      final StorageEngineReader records = separateReader == null
          ? storageEngineReader
          : separateReader;
      try (final RecordPageGuard guard = separateReader == null
          ? records.preserveRecordPageGuard()
          : null) {
        final LongIterator it = candidates.nodeKeyIterator();
        while (it.hasNext()) {
          final long nodeKey = it.next();
          final DataRecord record = records.getRecord(nodeKey, IndexType.DOCUMENT, -1);
          final String value;
          if (record instanceof ValueNode valueNode) {
            value = valueNode.getValue();
          } else if (record instanceof NumericValueNode numericNode) {
            value = String.valueOf(numericNode.getValue());
          } else if (record instanceof BooleanValueNode booleanNode) {
            value = Boolean.toString(booleanNode.getValue());
          } else {
            continue;
          }
          if (inRange(new Str(value), min, max, minInclusive, maxInclusive, type)) {
            if (kept == matching.length) {
              matching = Arrays.copyOf(matching, Math.max(matching.length << 1, 16));
            }
            matching[kept++] = nodeKey;
          }
        }
      }
    }
    if (kept == 0) {
      return null;
    }
    return kept == candidateCount
        ? candidates
        : NodeReferences.ofSortedArray(Arrays.copyOf(matching, kept));
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

  private static @Nullable NodeReferences exactNumericMatches(final StorageEngineReader storageEngineReader,
      final NodeReferences candidates, final Atomic wanted) {
    final BigDecimal wantedNumber = parseOrNull(wanted.stringValue());
    if (wantedNumber == null) {
      return null;
    }
    final byte[] wantedBytes = wanted.stringValue().getBytes(StandardCharsets.UTF_8);
    final long candidateCount = candidates.cardinality();
    long[] matching = new long[(int) Math.min(candidateCount, 16)];
    int kept = 0;
    try (final StorageEngineReader separateReader = storageEngineReader.hasTrxIntentLog()
        ? null
        : storageEngineReader.getResourceSession().createStorageEngineReader(storageEngineReader.getRevisionNumber())) {
      final StorageEngineReader records = separateReader == null
          ? storageEngineReader
          : separateReader;
      try (final RecordPageGuard guard = separateReader == null
          ? records.preserveRecordPageGuard()
          : null) {
        final LongIterator it = candidates.nodeKeyIterator();
        while (it.hasNext()) {
          final long nodeKey = it.next();
          final DataRecord record = records.getRecord(nodeKey, IndexType.DOCUMENT, -1);
          if (record == null) {
            continue; // does not resolve at this revision, so it cannot be a match
          }
          if (matchesNumericValue(record, wantedBytes, wantedNumber)) {
            if (kept == matching.length) {
              matching = Arrays.copyOf(matching, Math.max(matching.length << 1, 16));
            }
            matching[kept++] = nodeKey;
          }
        }
      }
    }
    if (kept == 0) {
      return null;
    }
    return kept == candidateCount
        ? candidates
        : NodeReferences.ofSortedArray(Arrays.copyOf(matching, kept));
  }

  private static boolean matchesNumericValue(final DataRecord record, final byte[] wantedBytes,
      final BigDecimal wantedNumber) {
    if (record instanceof final ValueNode valueNode && Arrays.equals(valueNode.getRawValue(), wantedBytes)) {
      return true;
    }
    final BigDecimal stored = storedNumber(record);
    return stored != null && stored.compareTo(wantedNumber) == 0;
  }

  /**
   * The candidate's value as a number, or {@code null} when it cannot be read as one.
   *
   * @param record the candidate's record
   * @return its numeric value, or {@code null} if unreadable
   */
  private static @Nullable BigDecimal storedNumber(final DataRecord record) {
    if (record instanceof final NumericValueNode numericNode) {
      final Number stored = numericNode.getValue();
      return stored == null
          ? null
          : parseOrNull(stored.toString());
    }
    if (record instanceof final ValueNode valueNode) {
      return parseOrNull(new String(valueNode.getRawValue(), StandardCharsets.UTF_8));
    }
    return null;
  }

  /**
   * {@code text} as a decimal, or {@code null} when it is not one — NaN and the infinities included,
   * which {@link BigDecimal} cannot represent.
   *
   * @param text the lexical form
   * @return the parsed value, or {@code null}
   */
  private static @Nullable BigDecimal parseOrNull(final String text) {
    try {
      return new BigDecimal(text);
    } catch (final NumberFormatException e) {
      return null;
    }
  }

  /**
   * Whether the summary resolves this index's paths to a SINGLE path class that the query did not ask
   * for — in which case nothing the index holds under the requested class can match at the query
   * revision, and the caller may answer empty without reading anything.
   *
   * <p>
   * Deliberately narrow. It fires only when the summary resolves exactly one path class, because that
   * is the only case where "not the requested one" settles the whole question; a multi-path index
   * needs the per-entry PCR check instead. Callers must therefore treat a {@code false} answer as "no
   * short cut available", never as "the requested class is current".
   * </p>
   *
   * <p>
   * Not cheap: {@code getPCRsForPaths} walks the path summary and allocates a fresh set per call.
   * Call it where a walk is affordable — after a seek has already found something, or ahead of a scan
   * — never on the path of a query that is about to answer empty anyway.
   * </p>
   *
   * @param filter the query's filter, carrying the PCR collector
   * @param indexDef the index definition, carrying the indexed paths
   * @param pcrsRequested the path classes the query pinned
   * @return {@code true} when the query can answer empty without reading the index
   */
  private static boolean resolvesToADifferentPathClass(final CASFilter filter, final IndexDef indexDef,
      final Set<Long> pcrsRequested) {
    final Set<Long> pcrsAvailable = filter.getPCRCollector().getPCRsForPaths(indexDef.getPaths()).getPCRs();
    return pcrsAvailable.size() == 1 && !pcrsRequested.containsAll(pcrsAvailable);
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

  private static Iterator<NodeReferences> openComparisonWithResidual(final StorageEngineReader storageEngineReader,
      final HOTIndexReader<CASValue> reader, final IndexDef indexDef, final CASFilter filter,
      final Set<Long> pcrsRequested) {
    if (pcrsRequested.size() == 1 && resolvesToADifferentPathClass(filter, indexDef, pcrsRequested)) {
      return Collections.emptyIterator();
    }
    final SearchMode mode = filter.getMode();
    if (!CASKeySerializer.isOfType(filter.getKey(), indexDef.getContentType())) {
      return Collections.emptyIterator();
    }
    if (mode == SearchMode.EQUAL) {
      return openRangeWithResidual(storageEngineReader, reader, indexDef, pcrsRequested, filter.getKey(),
          filter.getKey(), true, true);
    }
    final boolean lower = mode == SearchMode.GREATER || mode == SearchMode.GREATER_OR_EQUAL;
    return openRangeWithResidual(storageEngineReader, reader, indexDef, pcrsRequested, lower
        ? filter.getKey()
        : null,
        lower
            ? null
            : filter.getKey(),
        mode == SearchMode.GREATER_OR_EQUAL, mode == SearchMode.LOWER_OR_EQUAL);
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

    if (filter != null && filter.getKey() != null
        && (requiresValueResidual(filter.getKey(), indexDef.getContentType())
            || (isDecimalType(indexDef.getContentType()) && pcrsRequested.size() != 1)
            || (!CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())
                && (filter.getMode() != SearchMode.EQUAL || pcrsRequested.size() != 1)))) {
      return openComparisonWithResidual(storageEngineReader, reader, indexDef, filter, pcrsRequested);
    }

    // Gated on what the QUERY pins, not on what the INDEX spans. A seek needs one exact key, so it
    // needs exactly one requested path class; how many path classes the index happens to hold is
    // irrelevant, because CAS keys are PCR-major (the sign-flipped pathNodeKey is the first 8 bytes)
    // and entries under a different path class therefore carry different key bytes and are simply
    // not reached. This used to also demand pcrsAvailable.size() <= 1, which sent EVERY equality
    // query over a multi-path index to the full scan at the bottom of this method — O(index) with a
    // materialized key per entry, in place of one descent. It was not buying correctness: the scan
    // it fell back to filters on `pcrsRequested` too (see matchesFilter), so both paths return the
    // postings of the one requested path class and nothing else. The sibling range method never had
    // the restriction either.
    // A null probe key has nothing to seek to, so it falls through to the scan below.
    if (pcrsRequested.size() == 1 && filter != null && filter.getKey() != null) {
      final Atomic atomic = filter.getKey();
      final long pcr = pcrsRequested.iterator().next();
      final SearchMode mode = filter.getMode();

      // The probe key must be typed like the entries the index stores, NOT like the atomic the
      // caller happened to pass: a HOT key is a byte string that carries the type id, so probing
      // an xs:decimal index with, say, an xs:double of the same numeric value produced a
      // different key and the lookup silently found nothing.
      // A probe that is not of the index's declared type matches NOTHING, and saying so here is the
      // difference between an empty answer and a wrong one. CASIndexBuilder skips any node whose
      // value is not of the declared type, so nothing of that shape was ever indexed — but the
      // encoder cannot express "no match" and falls back to 0, false or the empty string instead.
      // `eq "abc"` on an xs:decimal index therefore parsed to 0.0, landed on ZERO's key, and
      // returned every zero-valued node.
      //
      // ScanCASIndex casts its probe and so raises a type error before reaching here; this covers
      // the programmatic callers that build a CASFilter directly, which is the path that had no
      // guard at all.
      if (!CASKeySerializer.isOfType(atomic, indexDef.getContentType())) {
        return Collections.emptyIterator();
      }
      final CASValue value = new CASValue(atomic, indexDef.getContentType(), pcr);

      // Reaching this branch SKIPS the `pcrsAvailable` short-circuit further down, and that is sound
      // rather than an oversight — worth stating, because the omission looks like one. Both PCR sets
      // come from the same PCRCollector at the same revision (PathFilter resolves its own in its
      // constructor); they differ only in WHICH paths they resolve, the query's against the index's.
      // So the check asks "is the query's path not the index's path", and a CAS key is PCR-major: a
      // seek under a pathNodeKey this index never populated matches no key bytes at all, so
      // reader.get returns null and this branch answers empty — the very thing the check would have
      // returned, without the path-summary walk it costs. HOTIndexMemoizationJsoniqTest's crossed
      // probe pins exactly that (an index on one path, probed with another, must answer 0).
      if (mode == SearchMode.EQUAL) {
        final NodeReferences refs = reader.get(value, mode);
        if (refs == null) {
          return Collections.emptyIterator();
        }
        // The requested path class must be one the summary can still resolve, or these postings are
        // stale. HOT posting lists span EVERY revision while the summary describes the QUERY revision,
        // so a path node dropped and re-created takes a new pathNodeKey and leaves its old postings
        // filed under a PCR the summary no longer resolves — which a byte-exact seek under that stale
        // PCR happily returns. Every mode was checked against this before the EQUAL branch was allowed
        // to return ahead of it.
        //
        // Deferred to HERE, after a hit, rather than hoisted back above the seek: getPCRsForPaths
        // walks the path summary and allocates a fresh set per call, so hoisting taxes every equality
        // query — including the ones that MISS, which is exactly when the walk buys nothing, since an
        // empty seek result cannot be stale. On a hit it is amortized against materializing the rows.
        if (resolvesToADifferentPathClass(filter, indexDef, pcrsRequested)) {
          return Collections.emptyIterator();
        }
        if (!CASKeySerializer.losesInformation(atomic, indexDef.getContentType())) {
          return Iterators.forArray(refs);
        }
        final NodeReferences exact = exactNumericMatches(storageEngineReader, refs, atomic);
        return exact == null
            ? Collections.emptyIterator()
            : Iterators.forArray(exact);
      }

      // If the single PCR the summary can resolve is NOT the requested one, no entry can match at
      // all. Note this only ever SHORT-CIRCUITS a scan; it is never grounds for skipping the
      // per-entry PCR check on a cursor that survives it, because the summary describes the query
      // revision while the index holds every revision's postings (see the one-sided branch below).
      if (resolvesToADifferentPathClass(filter, indexDef, pcrsRequested)) {
        return Collections.emptyIterator();
      }

      // Range queries: a bounded cursor seeks straight to the bound and stops at it. Inclusivity is
      // the cursor's job (see the range-filter path above for why positional trimming is wrong), and
      // with the PCR check hoisted out nothing in these scans deserializes a key at all. Same
      // content-type gate as the range-filter path: byte order decides these bounds, so a type whose
      // key bytes are its raw lexical form must use the typed comparison in the full scan instead.
      //
      // Capped decimal or lexical bounds already took the residual path above. Relaxing a cursor alone
      // would
      // keep the shared posting group but also return values outside the requested comparison.
      if (CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
        final boolean inclusive = mode == SearchMode.GREATER_OR_EQUAL || mode == SearchMode.LOWER_OR_EQUAL;
        final Iterator<Map.Entry<CASValue, NodeReferences>> rangeIter = switch (mode) {
          case GREATER, GREATER_OR_EQUAL -> reader.iteratorFrom(value, inclusive);
          case LOWER, LOWER_OR_EQUAL -> reader.iteratorTo(value, inclusive);
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

}
