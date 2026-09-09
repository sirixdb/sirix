package io.sirix.index.cas;

import io.sirix.utils.Iterators;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.AtomicUtil;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.index.hot.AbstractHOTIndexReader;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.Atomic;
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

public interface CASIndex<B, L extends ChangeListener, R extends NodeReadOnlyTrx & NodeCursor> {
  B createBuilder(R rtx, StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader,
      IndexDef indexDef);

  L createListener(StorageEngineWriter storageEngineWriter, PathSummaryReader pathSummaryReader, IndexDef indexDef);

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilterRange filter) {
    return openHOTIndexWithRangeFilter(storageEngineReader, indexDef, filter);
  }

  default Iterator<NodeReferences> openIndex(StorageEngineReader storageEngineReader, IndexDef indexDef,
      CASFilter filter) {
    return openHOTIndexWithFilter(storageEngineReader, indexDef, filter);
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
    //
    // NOT gated on losesInformation, and that is a deliberate reversal. The full scan below is not a
    // more accurate answer for a lossy bound — it is the SAME answer at O(index) cost, because
    // CASKeySerializer#decodeAtomic rebuilds its comparison value out of the very key the cursor
    // already compared. A decimal key decodes to Dec(BigDecimal.valueOf(d)) — the same double; a
    // truncated string key decodes to the truncated string. So the scan compares narrowed values
    // exactly as the cursor does, only after materializing a CASValue for every entry in the index.
    //
    // Worse, for a truncating bound the scan is strictly WORSE than the cursor. With a 247-byte bound
    // V, the stored key for V and the bound's key are byte-identical, so an inclusive cursor returns
    // V; the scan deserializes the stored key to its 246-byte prefix, finds that prefix < V, and
    // DROPS V — a missing row where the cursor merely over-matched.
    //
    // Truncation still has to be handled, because it does break the cursor in one direction: a bound
    // at or past the cap collapses onto the same key as every stored value sharing its 246-byte
    // prefix, so an EXCLUSIVE bound drops that whole group, matching records included. The remedy is
    // to relax that bound to inclusive rather than to abandon the cursor — it keeps the scan O(result)
    // and turns the drop into an over-match, which is the direction this index errs in everywhere
    // else. Numeric narrowing needs no such relaxation: its encoder is monotone and the collapsed
    // group is one ULP wide, so relaxing would pull every `= bound` row into a `> bound` query.
    if (filter != null && filter.getPCRs().size() == 1 && (filter.getMin() != null || filter.getMax() != null)
        && CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
      final Set<Long> pcrsRequested = filter.getPCRs();
      final long pcr = pcrsRequested.iterator().next();
      final Atomic min = filter.getMin();
      final Atomic max = filter.getMax();
      final boolean minInclusive =
          filter.isMinInclusive() || CASKeySerializer.truncates(min, indexDef.getContentType());
      final boolean maxInclusive =
          filter.isMaxInclusive() || CASKeySerializer.truncates(max, indexDef.getContentType());

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

  /**
   * Keep only the candidates whose stored value really equals {@code wanted}.
   *
   * <p>
   * Runs ONLY when {@link CASKeySerializer#losesInformation} reports that the probe cannot be
   * represented exactly, which is the rare case — a value past the serializer's string cap.
   * Everything inside that cap is decided by the seek alone and never reaches here, so the ordinary
   * equality query pays nothing for this.
   * </p>
   *
   * <p>
   * Reads through its OWN {@link StorageEngineReader}, never the query's. {@code getRecordPage} calls
   * {@code closeCurrentPageGuard()}, which releases the guard keeping alive the page the caller's
   * flyweight current node is bound to — so fetching candidates on the query's reader would leave
   * that node a dangling view into a frame the sweeper may free, and the caller's next field read
   * would be a use-after-free. {@code AbstractResourceSession#getRecordChangeRevisions} opens its own
   * reader for exactly this reason.
   * </p>
   *
   * <p>
   * A WRITER-backed reader cannot be shadowed that way: it represents {@code lastCommitted + 1}, a
   * revision the session will not open a second reader on, and its uncommitted changes live in a trx
   * intent log no other reader can see. Re-checking on the caller's own reader instead is not an
   * option — an index scan inside an updating transaction is precisely a query holding a live cursor,
   * so that is the hazard above rather than an exception to it. This therefore declines to filter
   * there and returns the candidates unchanged, which is the superset the branch answered with before
   * the re-check existed. De-pinning the record-page path would remove the hazard, and with it this
   * carve-out.
   * </p>
   *
   * <p>
   * Two rejection rules, and the distinction matters. A candidate whose record this cannot judge is
   * KEPT: erring towards the superset preserves the behaviour that shipped before, where dropping
   * would turn an over-match into silently missing rows — the worse of the two failures. A candidate
   * whose record is NULL is DROPPED: the posting lists span every revision, so a node key that does
   * not resolve at the query revision provably is not a match, and keeping it hands the caller a key
   * that {@code moveTo} cannot resolve.
   * </p>
   *
   * <p>
   * The survivors stay in the COMPACT representation: {@code nodeKeyIterator} yields ascending and
   * filtering preserves that, so an exactly-sized {@code long[]} is ascending by construction and
   * {@link NodeReferences#ofSortedArray} adopts it directly. Rebuilding a {@code Roaring64Bitmap}
   * here would reintroduce the container tree the compact form exists to avoid, and would hand every
   * downstream {@code cardinality}/{@code contains}/{@code nodeKeyIterator} call the slower branch —
   * making a FILTERED result more expensive than the unfiltered one it replaces. When nothing is
   * filtered out, which is the common case, the seek's own instance is handed straight back and
   * nothing is allocated at all.
   * </p>
   *
   * @param storageEngineReader reader for fetching the candidate records
   * @param candidates the posting list the seek returned
   * @param wanted the atomic the query asked for
   * @param contentType the index's content type, which decides numeric versus lexical comparison
   * @return the matching references, or {@code null} when none match
   */
  private static @Nullable NodeReferences exactMatches(final StorageEngineReader storageEngineReader,
      final NodeReferences candidates, final Atomic wanted, final Type contentType) {
    if (storageEngineReader.hasTrxIntentLog()) {
      return candidates; // writer-backed: cannot be shadowed, and must not be borrowed — see above
    }
    // ONE type dispatch, not two. isNumericFamily walks Type#instanceOf's parent chain, which
    // getTypeId's own comment calls a pointer chase worth ordering by frequency, and the answer is
    // loop-invariant. Two calls also coupled the branches by convention alone: editing one to a
    // different type would have silently changed what the null check below meant.
    final boolean numeric = CASKeySerializer.isNumericFamily(contentType);
    final BigDecimal wantedNumber = numeric
        ? parseOrNull(wanted.stringValue())
        : null;
    if (numeric && wantedNumber == null) {
      // A numeric probe that is not a finite number — NaN or an infinity. The encoder canonicalizes
      // NaN onto a real value's key, so the seek's posting list is other values entirely; and XQuery
      // gives NaN no equals, so the answer is empty rather than that posting list.
      return null;
    }
    // Built only where it is read. matchesExactly consults it on the lexical path, and on the
    // numeric path only to short-circuit an exact byte hit, so a numeric index still needs it — but
    // a writer-backed reader, which returned above, now allocates neither this nor the BigDecimal.
    final byte[] wantedBytes = wanted.stringValue().getBytes(StandardCharsets.UTF_8);
    final long candidateCount = candidates.cardinality();
    // GROWN, not sized from the cardinality. A posting list is unbounded — a value shared by a
    // 246-byte prefix across millions of documents is exactly what drives a probe into this method —
    // and sizing eagerly turned a query that answers from a handful of survivors into a
    // hundreds-of-megabytes allocation, or an ArithmeticException out of Math.toIntExact past 2^31
    // postings. Doubling from a small floor costs log(kept) copies and never over-allocates by more
    // than a factor of two on what actually SURVIVES.
    long[] matching = new long[(int) Math.min(candidateCount, 16)];
    int kept = 0;
    final var session = storageEngineReader.getResourceSession();
    try (final StorageEngineReader records =
        session.createStorageEngineReader(storageEngineReader.getRevisionNumber())) {
      final LongIterator it = candidates.nodeKeyIterator();
      while (it.hasNext()) {
        final long nodeKey = it.next();
        final DataRecord record = records.getRecord(nodeKey, IndexType.DOCUMENT, -1);
        if (record == null) {
          continue; // does not resolve at this revision, so it cannot be a match
        }
        if (matchesExactly(record, wantedBytes, wantedNumber)) {
          if (kept == matching.length) {
            matching = Arrays.copyOf(matching, Math.max(matching.length << 1, 16));
          }
          matching[kept++] = nodeKey;
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
   * Whether {@code record}'s stored value really is {@code wanted}.
   *
   * <p>
   * Which comparison applies is decided by the caller from the INDEX's content type, not from the
   * candidate's node kind, and arrives as {@code wantedNumber} being non-{@code null}. A numeric
   * index compares NUMBERS — {@code 1.50} and {@code 1.5} are one value and both must match a probe
   * of {@code 1.5}, and the same logical number reaches here as a {@link NumericValueNode} in JSON
   * but as a lexical {@link ValueNode} in XML, so a byte comparison would drop the XML row. Every
   * other index compares BYTES against the probe's UTF-8, which is what the truncating string encoder
   * lost.
   * </p>
   *
   * <p>
   * A candidate this cannot read — an unrecognized node kind, or a numeric index over a node whose
   * value will not parse — is KEPT, preserving the superset rather than risking dropped rows on
   * something the re-check cannot judge.
   * </p>
   *
   * @param record the candidate's record, never {@code null}
   * @param wantedBytes the probe's lexical form in UTF-8, computed once by the caller
   * @param wantedNumber the probe as a number on a numeric index, {@code null} on every other index
   * @return {@code true} when the candidate matches, or cannot be judged
   */
  private static boolean matchesExactly(final DataRecord record, final byte[] wantedBytes,
      final @Nullable BigDecimal wantedNumber) {
    // BYTES FIRST, on both paths, because byte-identical settles a match either way and this runs
    // once per candidate. The numeric branch below allocates a String and a BigDecimal per candidate
    // and then parses a decimal, while the overwhelmingly common case is that the stored lexical
    // bytes are exactly the probe's — so the vectorized Arrays.equals answers nearly every candidate
    // and the decimal machinery is left for the case it exists to handle, where 1.50 must still match
    // a probe of 1.5.
    if (record instanceof final ValueNode valueNode && Arrays.equals(valueNode.getRawValue(), wantedBytes)) {
      return true;
    }
    if (wantedNumber != null) {
      // Numeric index: compare NUMBERS, whatever shape the node stores them in. The same logical
      // number reaches here as a NumericValueNode in JSON and as raw lexical bytes in XML, and 1.50
      // and 1.5 are one value — a byte comparison alone would drop rows for both reasons.
      final BigDecimal stored = storedNumber(record);
      return stored == null || stored.compareTo(wantedNumber) == 0;
    }
    // A lexical index whose bytes did not match is a non-match; anything this cannot read is KEPT.
    return !(record instanceof ValueNode);
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
        // Byte-exact seek, and the CAS key encoding is LOSSY, so for some probes the seek alone
        // answers with a superset rather than an answer. CASKeySerializer truncates a string value at
        // MAX_STRING_VALUE_BYTES, so two values sharing that prefix share one key and one posting
        // list; the integer/decimal encoders narrow similarly.
        //
        // That is why the seek is not the whole answer here: losesInformation decides whether the
        // probe survives the encoding, and only when it does NOT is the posting list re-checked
        // against the real document values by exactMatches below. The disambiguating bytes were never
        // stored, so nothing in the INDEX can separate the collided values — reaching the document
        // nodes is the only thing that can, which is exactly what the re-check does. Do not drop it on
        // the reasoning that a byte-exact seek must already be exact; CASKeyTruncationTest fails if it
        // goes (it asserts 1, not 2, for two titles sharing a 246-byte prefix).
        //
        // The re-check is TYPED, not lexical: it picks its comparison from the index's content type,
        // so it closes the numeric-narrowing case as well as the truncation one. That matters because
        // the same logical number arrives as a NumericValueNode in JSON and as raw lexical bytes in
        // XML, and because 1.50 and 1.5 are one value — a byte comparison would drop rows for both
        // reasons. See exactMatches.
        //
        // Relaxing the pcrsAvailable gate above did not introduce the truncation defect, which is
        // worth stating because it is easy to misread. A SINGLE-path index has always taken this
        // branch and returned 2; a multi-path index returned 0 before (the scan compares the truncated
        // STORED value against the full probe and matches nothing). The relaxation made multi-path
        // consistent with single-path, and the re-check then made BOTH exact.
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
        // The probe does not fit the key encoding, so the seek answered with a superset: every value
        // sharing the truncated prefix collapses onto this one key. The index cannot separate them —
        // the distinguishing bytes were never stored — so the only thing that can is the document.
        // Re-check the candidates against their real values.
        final NodeReferences exact = exactMatches(storageEngineReader, refs, atomic, indexDef.getContentType());
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
      // A TRUNCATING bound is handled by relaxing the bound, not by abandoning the cursor — the same
      // reversal as in openHOTIndexWithRangeFilter, for the same reason. The full scan below is not
      // the more accurate answer for such a bound: CASFilterRange#inRange judges the atomic that
      // CASKeySerializer#decodeAtomic rebuilt out of the stored key, and for a truncated value that
      // is its 246-byte prefix. So the scan compares the bound against the prefix, finds the prefix
      // below it, and DROPS the record the cursor would have returned — a missing row bought with an
      // O(index) scan. Once the bound reaches the cap it collapses onto the same key as every stored
      // value sharing that prefix, and only the EXCLUSIVE cursors mishandle the collapsed group (they
      // drop it whole, matching records included); making the bound inclusive keeps the group and
      // errs towards the superset, which is the direction this index errs in everywhere else.
      //
      // Note this is NOT a consequence of relaxing the pcrsAvailable gate above; a SINGLE-path index
      // has always reached this cursor. The relaxation extended the exposure to multi-path indexes,
      // which is what made the pre-existing hole worth closing at the same time.
      if (CASKeySerializer.isByteOrderPreserving(indexDef.getContentType())) {
        final boolean inclusive = mode == SearchMode.GREATER_OR_EQUAL || mode == SearchMode.LOWER_OR_EQUAL
            || CASKeySerializer.truncates(atomic, indexDef.getContentType());
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
