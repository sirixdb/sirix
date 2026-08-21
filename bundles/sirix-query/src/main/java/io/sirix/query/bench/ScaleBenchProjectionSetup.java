/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexFences;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bench helper for ScaleBenchMain: builds an (age, active, dept) projection
 * index on the current revision and registers it wildcard in
 * {@link ProjectionIndexRegistry}. Lives in its own file so
 * {@link io.brackit.query.util.path.Path} can be imported without colliding
 * with {@link java.nio.file.Path} that ScaleBenchMain uses extensively.
 */
final class ScaleBenchProjectionSetup {

  private static final String[] FIELD_NAMES = {"age", "active", "dept", "city", "amount", "score"};

  private ScaleBenchProjectionSetup() {
  }

  /** IndexDef.getID() for the bench projection — one sub-tree per resource. */
  private static final int INDEX_NUMBER = 0;

  static int installWildcard(final JsonResourceSession session) {
    // Fast path: projection has been persisted previously — hydrate the
    // in-memory registry from the HOT sub-tree. Avoids the 24-minute rebuild
    // the non-persistent path pays at 100 M (shred → close → reopen).
    //
    // iter#13: -Dsirix.projection.inMemoryReencode=true re-serialises each
    // persisted leaf through the current serializer. Intended for perf
    // A/B of a new leaf wire format (e.g. NUMERIC_LONG_FOR_BP upgrade)
    // without paying a 24-min disk reshred + HOT-put loop — the re-encoded
    // leaves are installed in-memory only, persisted bytes stay untouched.
    // Adds ~{millis-per-leaf × leaf-count} to the cold start, in return
    // for query-phase format benefits.
    //
    // iter#13: -Dsirix.projection.forceRebuild=true bypasses the fast path
    // even when a persisted projection exists, re-walks the current
    // revision and streams new leaves through HOT storage. Use this to
    // permanently migrate an existing DB to a new wire format.
    final boolean forceRebuild = Boolean.getBoolean("sirix.projection.forceRebuild");
    final boolean inMemoryReencode = Boolean.getBoolean("sirix.projection.inMemoryReencode");
    // iter#13: -Dsirix.projection.repersistReencoded=true upgrades the
    // persisted leaves in-place by re-serialising them through the current
    // serializer and writing them back to HOT storage. Executes ONCE per
    // DB; next cold run reads the new wire format directly (no reencode
    // overhead). Intended for migrating an existing DB to FOR-BP without
    // paying a full resource reshred.
    final boolean repersistReencoded = Boolean.getBoolean("sirix.projection.repersistReencoded");
    final String resourceKey = session.getResourceConfig().getResource().toString();
    final int revision = session.getMostRecentRevisionNumber();
    boolean probeUnreadable = false;
    if (!forceRebuild) {
      try (JsonNodeReadOnlyTrx probeRtx = session.beginNodeReadOnlyTrx(revision)) {
        // Descriptor layout: metadata is the slot-0 blob — read FIRST and unconditionally (a
        // catalogued EMPTY projection has metadata with zero leaves and must hydrate, not
        // rebuild-and-clobber the catalogued definition's metadata); leaves assemble directly
        // to the raw scan form (no decode step). Legacy/corrupt payloads degrade to a rebuild.
        // A STALE tombstone (update-transaction invalidation) falls through to the rebuild
        // path below.
        // A slot 0 that cannot be READ AT ALL (no PIXB marker — a pre-descriptor chunked store, or
        // one written before the segment-slot layout) degrades to a rebuild, which is the migration
        // path: the rebuild resets the sub-tree and repopulates it. Only that case; a metadata blob
        // that reads but declares something inconsistent still aborts below, because rebuilding over
        // a store this harness cannot describe would be writing blind.
        ProjectionIndexMetadata probedMetadata = null;
        try {
          probedMetadata = ProjectionIndexMetadata.parse(
              ProjectionIndexHOTStorage.readBlob(probeRtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
        } catch (final IllegalStateException unreadable) {
          System.out.println("# Persisted projection metadata unreadable (" + unreadable.getMessage()
              + ") — rebuilding");
          // Break out rather than rebuilding HERE: the rebuild re-walks the whole resource and opens
          // its own transactions, and doing that inside this try-with-resources would hold the probe
          // read transaction — and the revision it pins — open for the entire rebuild.
          probeUnreadable = true;
        }
        if (!probeUnreadable) {
          ProjectionIndexMetadata parsedMetadata = probedMetadata;
          List<byte[]> compact = new ArrayList<>();
          int[] compactPhysicalOrder = null;
          if (probedMetadata != null) {
            try {
              compactPhysicalOrder = ProjectionIndexFences.readPhysicalOrder(
                  probeRtx.getStorageEngineReader(), INDEX_NUMBER, probedMetadata.rowGroupCount());
              compact = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
                  probeRtx.getStorageEngineReader(), INDEX_NUMBER, probedMetadata.rowGroupCount(),
                  compactPhysicalOrder);
            } catch (final IllegalStateException corrupt) {
              System.out.println("# Persisted projection unreadable (" + corrupt.getMessage()
                  + ") — rebuilding");
              parsedMetadata = null;
              compact = new ArrayList<>();
            }
          }
          final ProjectionIndexMetadata metadata = parsedMetadata;
          final boolean stale = metadata != null && metadata.isStale();
          if (stale) {
            System.out.println("# Persisted projection is stale (invalidated by updates) — rebuilding");
          }
          if ((parsedMetadata != null || !compact.isEmpty()) && !stale) {
            if (metadata != null && compact.size() < metadata.rowGroupCount()) {
              // Same contract as ProjectionIndexCatalog: a truncated store is
              // corrupt — refuse loudly instead of benchmarking partial data.
              throw new IllegalStateException("Persisted projection declares " + metadata.rowGroupCount()
                  + " leaves but only " + compact.size()
                  + " are stored — rebuild with -Dsirix.projection.forceRebuild=true.");
            }
            final int leafEnd = metadata == null ? compact.size() : metadata.rowGroupCount();
            // Leaves are already in the flat scan form (assembled from segments).
            final List<byte[]> persisted = new ArrayList<>(leafEnd);
            for (int i = 0; i < leafEnd; i++) {
              persisted.add(compact.get(i));
            }
            // Guard the shape: hydrating leaves with a different column count
            // under the bench's static field list would mislabel every column.
            if (!persisted.isEmpty()) {
              final byte[] first = persisted.get(0);
              final int persistedColumns =
                  first == null || first.length < 8 ? -1 : ProjectionIndexRowGroupPage.columnCountOf(first);
              if (persistedColumns != FIELD_NAMES.length) {
                throw new IllegalStateException("Persisted projection has " + persistedColumns
                    + " columns but the bench expects " + FIELD_NAMES.length + " "
                    + Arrays.toString(FIELD_NAMES)
                    + " — rebuild it with -Dsirix.projection.forceRebuild=true.");
              }
            }
            final List<byte[]> reencoded;
            if (inMemoryReencode || repersistReencoded) {
              if (metadata == null) {
                throw new IllegalStateException("Cannot re-encode a projection without live metadata");
              }
              reencoded = reencodeLeaves(persisted, metadata.columnKinds());
            } else {
              reencoded = persisted;
            }
            if (repersistReencoded) {
              // Repersist the re-encoded leaves back to HOT storage under a
              // single write trx. Next cold run will skip the reencode step
              // because the on-disk bytes will already be in the new format.
              if (metadata == null || compactPhysicalOrder == null
                  || compactPhysicalOrder.length != reencoded.size()) {
                throw new IllegalStateException(
                    "Cannot repersist a projection without its exact persisted physical row-group order");
              }
              final long t0 = System.nanoTime();
              try (JsonNodeTrx wtx = session.beginNodeTrx()) {
                // The exclusive writer is opened only after the read snapshot and the re-encode pass.
                // Its revision is the revision it will publish, hence `- 1` is the base snapshot it
                // actually inherited while holding the resource-wide writer lock. Never overlay old
                // row-group bytes/order onto a projection maintained by an intervening commit.
                validateWriterBaseRevision(revision, wtx.getRevisionNumber());
                final ProjectionIndexHOTStorage storage =
                    new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
                for (int i = 0; i < reencoded.size(); i++) {
                  // `persisted` is in DOCUMENT order; physical ids can contain gaps and reuse after
                  // incremental split/delete. Rewriting i+1 would silently permute row groups under
                  // unchanged fences, locators and Bloom summaries.
                  storage.putRowGroupAsColumnSegmentSlots(compactPhysicalOrder[i],
                      ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(reencoded.get(i)));
                }
                wtx.commit();
              }
              System.out.printf("# Projection repersisted: %,d leaves in %,d ms%n",
                  reencoded.size(), (System.nanoTime() - t0) / 1_000_000L);
            }
            // No builder flags on this path — the Handle lazily re-derives the
            // NUMERIC_LONG integrality evidence from the leaves' persisted
            // presence tails, so aggregate fast paths survive close/re-open.
            ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, reencoded);
            return reencoded.size();
          }
        }
      }
    }

    return rebuildAndPersist(session, resourceKey, revision);
  }

  /**
   * Build the projection from {@code revision} and persist it — the path taken when nothing usable
   * is stored yet, when {@code -Dsirix.projection.forceRebuild=true}, and when slot 0 cannot be read
   * at all (a store predating the current layout, whose migration IS this rebuild).
   */
  private static int rebuildAndPersist(final JsonResourceSession session, final String resourceKey,
      final int revision) {
    // Slow path: index is not yet persisted. Build it from the current
    // revision and stream each leaf into HOT storage in one write trx.
    // Requires the resource to have been shredded with
    // {@code buildPathSummary=true} — without it, {@code openPathSummary}
    // returns an empty / unusable summary and {@link ProjectionIndexBuilder}
    // either silently builds an empty index or NPEs deep in path resolution.
    // Surface that mis-config up front with a clear message instead of a
    // cryptic exit 1 several seconds later.
    if (!session.getResourceConfig().withPathSummary) {
      throw new IllegalStateException(
          "ProjectionIndexSetup: no persisted projection index was found and the "
              + "resource was shredded without a PathSummary — building it now is impossible. "
              + "Re-shred with -DbuildPathSummary=true -Dprojection=true, or reuse an "
              + "existing DB that already has a persisted projection (see -Dsirix.db=<path>).");
    }

    final Path<QNm> rootPath = Path.parse("/[]", PathParser.Type.JSON);
    final Path<QNm> agePath = Path.parse("/[]/age", PathParser.Type.JSON);
    final Path<QNm> activePath = Path.parse("/[]/active", PathParser.Type.JSON);
    final Path<QNm> deptPath = Path.parse("/[]/dept", PathParser.Type.JSON);
    final Path<QNm> cityPath = Path.parse("/[]/city", PathParser.Type.JSON);
    final Path<QNm> amountPath = Path.parse("/[]/amount", PathParser.Type.JSON);
    // score is typically non-integral — its column exists to exercise the
    // builder's integrality flags (value-exact consumers must decline it).
    final Path<QNm> scorePath = Path.parse("/[]/score", PathParser.Type.JSON);
    final List<Path<QNm>> projectedFieldPaths =
        List.of(agePath, activePath, deptPath, cityPath, amountPath, scorePath);
    final IndexDef def = IndexDefs.createProjectionIdxDef(
        rootPath,
        projectedFieldPaths,
        List.of(Type.LON, Type.BOOL, Type.STR, Type.STR, Type.LON, Type.LON),
        INDEX_NUMBER,
        IndexDef.DbType.JSON);

    // Persist under a single write trx. Putting leaves outside the trx would
    // require setting up a StorageEngineWriter by hand — the node trx gives
    // us one for free plus handles commit.
    // -Dsirix.projection.persist=false skips persistence (in-memory registry
    // only). Historically this was REQUIRED when force-rebuilding a wider
    // column set over an already-persisted projection — the in-place HOT
    // overwrite of larger leaves tripped a chunk-split use-after-close bug.
    // That bug is fixed (grown leaves split + replace correctly; guarded by
    // ProjectionPersistForceRebuildTest and the sirix-core
    // ProjectionIndexHOTStorageGrowingPayloadTest), so the flag is now just
    // an optional fast-iteration knob.
    if (!Boolean.parseBoolean(System.getProperty("sirix.projection.persist", "true"))) {
      final List<byte[]> leaves = new ArrayList<>();
      final ProjectionIndexBuilder builder;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
          var pathSummary = session.openPathSummary(revision)) {
        builder = new ProjectionIndexBuilder(def, pathSummary, leaves::add);
        builder.build(rtx);
      }
      ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves,
          builder.numericColumnNonIntegralFlags());
      return leaves.size();
    }
    // Use the production persistence boundary rather than duplicating it here. This is
    // load-bearing for non-monotone record locators, resource-wide dictionary headers, Bloom
    // manifests and metadata-last publication. The former hand-written loop published metadata
    // before its leaves and persisted neither exception locators nor global dictionary anchors.
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      ProjectionIndexBuilder.buildAndPersist(def, wtx.getPathSummary(), wtx,
          wtx.getStorageEngineWriter(), false);
      wtx.commit();
    }

    final List<byte[]> leaves;
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L));
      if (metadata == null || metadata.isStale()) {
        throw new IllegalStateException("Production projection build did not publish live metadata");
      }
      final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(
          rtx.getStorageEngineReader(), INDEX_NUMBER, metadata.rowGroupCount());
      leaves = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
          rtx.getStorageEngineReader(), INDEX_NUMBER, metadata.rowGroupCount(), physicalOrder);
    }
    long rawBytes = 0L;
    long compactBytes = 0L;
    for (final byte[] raw : leaves) {
      final var encoded = ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(raw);
      rawBytes += raw.length;
      compactBytes += encoded.descriptor().length;
      for (final byte[] segment : encoded.segments()) {
        compactBytes += segment.length;
      }
    }
    System.out.printf("# Projection persisted: %,d leaves, raw %,d bytes -> compact %,d bytes (%.1f%%)%n",
        leaves.size(), rawBytes, compactBytes, rawBytes == 0 ? 0.0 : 100.0 * compactBytes / rawBytes);

    ProjectionIndexRegistry.installWildcard(resourceKey, FIELD_NAMES, leaves);
    return leaves.size();
  }

  /**
   * iter#13 helper — re-serialise every persisted leaf through the
   * current serializer so any wire-format upgrades (e.g. the
   * NUMERIC_LONG_FOR_BP column-kind introduced in iter#13) take effect
   * in-memory without touching the on-disk HOT sub-tree. Each leaf is
   * deserialised, its in-memory columns normalised, then re-serialised.
   *
   * <p>Cost: one pass over all leaves (~100K at 100M scale). Uses a
   * right-sized {@code ArrayList} to avoid growth copies — matches the
   * HFT discipline of the rest of this class.
   */
  static List<byte[]> reencodeLeaves(final List<byte[]> persisted, final byte[] metadataKinds) {
    final List<byte[]> out = new ArrayList<>(persisted.size());
    for (int documentPosition = 0; documentPosition < persisted.size(); documentPosition++) {
      final byte[] payload = persisted.get(documentPosition);
      if (payload == null) {
        throw new IllegalStateException("Persisted projection row group is null at document position "
            + documentPosition);
      }
      final ProjectionIndexRowGroupPage before = ProjectionIndexRowGroupPage.deserialize(payload);
      final byte[] reencoded = before.serialize();
      // Decode the candidate once because the persisted meaning, not merely serializer scratch, is
      // the contract. Reuse `before` from the re-encode above: two decodes per leaf, not the former
      // three-decode reencode + validation sequence.
      final ProjectionIndexRowGroupPage after = ProjectionIndexRowGroupPage.deserialize(reencoded);
      validateWireRewrite(before, after, metadataKinds, documentPosition);
      out.add(reencoded);
    }
    return out;
  }

  static void validateWriterBaseRevision(final int probedRevision, final int writerRevision) {
    final int writerBaseRevision = writerRevision - 1;
    if (writerBaseRevision != probedRevision) {
      throw new IllegalStateException("Projection changed while its wire rewrite was prepared: read revision "
          + probedRevision + " but the writer is based on revision " + writerBaseRevision);
    }
  }

  /**
   * A wire-only rewrite may change bytes, but it may not change any logical fact consumed by the
   * unchanged fences, sparse locators, Bloom chunks, summaries, dictionaries, or slot-0 metadata.
   * Comparisons stay on borrowed primitive arrays and dictionary byte ranges: no bitmap copies or
   * per-cell strings are allocated during the 100M-row migration pass.
   */
  static void validateWireRewrite(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after,
      final byte[] metadataKinds, final int documentPosition) {
    if (before.getRowCount() != after.getRowCount()
        || before.getColumnCount() != after.getColumnCount()
        || before.getColumnCount() != metadataKinds.length) {
      throw wireRewriteMismatch(documentPosition, "row-group shape");
    }
    if (before.firstRecordKey() != after.firstRecordKey()
        || before.lastRecordKey() != after.lastRecordKey()
        || before.hasOrderExceptions() != after.hasOrderExceptions()) {
      throw wireRewriteMismatch(documentPosition, "record-key fence/order metadata");
    }
    for (int column = 0; column < metadataKinds.length; column++) {
      if (before.columnKind(column) != metadataKinds[column]
          || after.columnKind(column) != metadataKinds[column]) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " kind");
      }
    }
    final long[] beforeKeys = before.recordKeys();
    final long[] afterKeys = after.recordKeys();
    for (int row = 0; row < before.getRowCount(); row++) {
      if (beforeKeys[row] != afterKeys[row]
          || before.orderExceptionAt(row) != after.orderExceptionAt(row)) {
        throw wireRewriteMismatch(documentPosition, "record identity/order at row " + row);
      }
    }

    final int rowCount = before.getRowCount();
    for (int column = 0; column < metadataKinds.length; column++) {
      if (before.columnMin(column) != after.columnMin(column)
          || before.columnMax(column) != after.columnMax(column)) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " zone map");
      }
      if (before.columnUnrepresentable(column) != after.columnUnrepresentable(column)
          || before.columnNumericNonIntegral(column) != after.columnNumericNonIntegral(column)
          || before.columnPureDoubleSource(column) != after.columnPureDoubleSource(column)) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " provenance");
      }
      if (!liveBitsEqual(before.presenceColumnBits(column), after.presenceColumnBits(column), rowCount)) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " presence");
      }

      switch (metadataKinds[column]) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
             ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
             ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL ->
            validateLongValues(before, after, column, rowCount, documentPosition);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> {
          if (!liveBitsEqual(before.booleanColumnBits(column), after.booleanColumnBits(column), rowCount)) {
            throw wireRewriteMismatch(documentPosition, "column " + column + " boolean values");
          }
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
            validateScalarStrings(before, after, column, rowCount, documentPosition);
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET ->
            validateStringSets(before, after, column, rowCount, documentPosition);
        default -> throw wireRewriteMismatch(documentPosition,
            "column " + column + " unknown kind " + metadataKinds[column]);
      }
    }
  }

  private static void validateLongValues(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after, final int column, final int rowCount,
      final int documentPosition) {
    final long[] beforeValues = before.numericColumn(column);
    final long[] afterValues = after.numericColumn(column);
    for (int row = 0; row < rowCount; row++) {
      if (beforeValues[row] != afterValues[row]) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " value at row " + row);
      }
    }
  }

  private static void validateScalarStrings(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after, final int column, final int rowCount,
      final int documentPosition) {
    validateLocalDictionary(before, after, column, documentPosition, "string");
    final int[] beforeIds = before.stringDictIdColumn(column);
    final int[] afterIds = after.stringDictIdColumn(column);
    for (int row = 0; row < rowCount; row++) {
      if (beforeIds[row] != afterIds[row]) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " string id at row " + row);
      }
    }
  }

  private static void validateStringSets(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after, final int column, final int rowCount,
      final int documentPosition) {
    validateLocalDictionary(before, after, column, documentPosition, "set");
    if (before.stringSetLength(column) != after.stringSetLength(column)) {
      throw wireRewriteMismatch(documentPosition, "column " + column + " set cardinality");
    }
    final int[] beforeCounts = before.stringSetCountColumn(column);
    final int[] afterCounts = after.stringSetCountColumn(column);
    for (int row = 0; row < rowCount; row++) {
      if (beforeCounts[row] != afterCounts[row]) {
        throw wireRewriteMismatch(documentPosition, "column " + column + " set count at row " + row);
      }
    }
    final int[] beforeIds = before.stringSetIdColumn(column);
    final int[] afterIds = after.stringSetIdColumn(column);
    for (int element = 0; element < before.stringSetLength(column); element++) {
      if (beforeIds[element] != afterIds[element]) {
        throw wireRewriteMismatch(documentPosition,
            "column " + column + " set id at element " + element);
      }
    }
  }

  private static void validateLocalDictionary(final ProjectionIndexRowGroupPage before,
      final ProjectionIndexRowGroupPage after, final int column, final int documentPosition,
      final String description) {
    final int beforeSize = before.stringDictionarySize(column);
    if (beforeSize != after.stringDictionarySize(column)) {
      throw wireRewriteMismatch(documentPosition, "column " + column + ' ' + description + " dictionary size");
    }
    for (int id = 0; id < beforeSize; id++) {
      if (!dictionaryEntryEquals(before, after, column, id)) {
        throw wireRewriteMismatch(documentPosition,
            "column " + column + ' ' + description + " dictionary entry " + id);
      }
    }
  }

  private static boolean dictionaryEntryEquals(final ProjectionIndexRowGroupPage left,
      final ProjectionIndexRowGroupPage right, final int column, final int id) {
    final int leftLength = left.stringDictionaryEntryLength(column, id);
    if (leftLength != right.stringDictionaryEntryLength(column, id)) {
      return false;
    }
    final byte[] leftBytes = left.stringDictionaryEntryBacking(column, id);
    final byte[] rightBytes = right.stringDictionaryEntryBacking(column, id);
    int leftOffset = left.stringDictionaryEntryOffset(column, id);
    int rightOffset = right.stringDictionaryEntryOffset(column, id);
    final int leftEnd = leftOffset + leftLength;
    while (leftOffset < leftEnd) {
      if (leftBytes[leftOffset++] != rightBytes[rightOffset++]) {
        return false;
      }
    }
    return true;
  }

  private static boolean liveBitsEqual(final long[] left, final long[] right, final int bitCount) {
    final int fullWords = bitCount >>> 6;
    for (int word = 0; word < fullWords; word++) {
      if (left[word] != right[word]) {
        return false;
      }
    }
    final int remaining = bitCount & 63;
    if (remaining == 0) {
      return true;
    }
    final long mask = (1L << remaining) - 1L;
    return (left[fullWords] & mask) == (right[fullWords] & mask);
  }

  private static IllegalStateException wireRewriteMismatch(final int documentPosition, final String detail) {
    return new IllegalStateException("Wire rewrite changed projection " + detail
        + " at document position " + documentPosition);
  }
}
