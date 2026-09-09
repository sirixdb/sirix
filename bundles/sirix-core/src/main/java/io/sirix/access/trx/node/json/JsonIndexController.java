package io.sirix.access.trx.node.json;

import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.AbstractIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexBuilder;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.cas.json.JsonCASIndexImpl;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter;
import io.sirix.index.interval.json.JsonValidTimeIndexBuilder;
import io.sirix.index.interval.json.JsonValidTimeIndexListener;
import io.sirix.index.name.json.JsonNameIndexImpl;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.json.JsonPathIndexImpl;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.vector.json.JsonVectorIndexImpl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.path.PathParser;

import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 */
public final class JsonIndexController extends AbstractIndexController<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /**
   * Constructor.
   */
  public JsonIndexController() {
    super(new Indexes(), new HashSet<>(), new JsonPathIndexImpl(), new JsonCASIndexImpl(), new JsonNameIndexImpl(),
        new JsonVectorIndexImpl());
  }

  @Override
  protected Path<QNm> parsePath(String path) {
    return Path.parse(path, PathParser.Type.JSON);
  }

  @Override
  public JsonIndexController createIndexes(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    // Validate before createIndexBuilders catalogues any definition or the shared traversal writes a
    // single index page. Unsupported lifecycle mixes therefore fail atomically at the API boundary.
    validateNewIndexDefinitions(indexDefs, nodeWriteTrx);
    validateProjectionDefinitions(indexDefs, nodeWriteTrx);

    // Build the visitor-driven indexes (PATH/CAS/NAME/VALIDTIME) in one
    // shared document traversal.
    IndexBuilder.build(nodeWriteTrx, createIndexBuilders(indexDefs, nodeWriteTrx));

    // Projection indexes are cursor-driven (record-at-a-time columnar
    // extraction), so they build outside the shared visitor traversal —
    // leaves and metadata stream straight into the definition's HOT
    // sub-tree and the in-memory registry.
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isProjectionIndex()) {
        createProjectionIndex(indexDef, nodeWriteTrx);
      }
    }

    // Create index listeners for upcoming changes.
    createIndexListeners(indexDefs, nodeWriteTrx);

    return this;
  }

  /**
   * Declare projection indexes on a resource that has no records yet, and arm the LOAD-TIME build
   * that maintains them as the data is shredded.
   *
   * <p>
   * The ordinary creation path ({@link #createIndexes}) derives a projection from a finished resource
   * by walking it — a second full pass whose cost is proportional to the corpus, and which at real
   * scale takes as long again as the load. This path instead catalogues the definition first and lets
   * the shred itself produce the rows, through the same change-notification lifecycle that maintains
   * the PATH/CAS/NAME indexes, so a load is one pass.
   *
   * <p>
   * Nothing readable is written until the load's final commit: {@link ProjectionBulkLoad} tombstones
   * slot 0 for the duration, so a load that never finishes leaves an index every reader skips rather
   * than an empty one every reader trusts.
   *
   * @throws IllegalStateException if the resource already holds records under a definition's root
   *         path — the load-time build appends, and would append after rows nobody extracted
   */
  public JsonIndexController createProjectionIndexesAtLoadStart(final Set<IndexDef> indexDefs,
      final JsonNodeTrx nodeWriteTrx) {
    return createProjectionIndexesAtLoadStart(indexDefs, nodeWriteTrx, -1L);
  }

  /**
   * As above, carrying an expected record count so each build's global-dictionary election can
   * decline a column that would not fit its budget. {@code -1} means unknown, and leaves the writer's
   * runtime cap as the only protection.
   */
  public JsonIndexController createProjectionIndexesAtLoadStart(final Set<IndexDef> indexDefs,
      final JsonNodeTrx nodeWriteTrx, final long expectedRows) {
    armProjectionIndexesAtLoadStart(indexDefs, nodeWriteTrx, expectedRows);
    return this;
  }

  /**
   * Arm one load-time projection and return its exact lifecycle owner. Unlike an ACTIVE-map lookup,
   * the returned handle can never name a pre-existing build that won a concurrent/duplicate arm.
   */
  public ProjectionBulkLoad createProjectionIndexAtLoadStart(final IndexDef indexDef, final JsonNodeTrx nodeWriteTrx,
      final long expectedRows) {
    final ProjectionBulkLoad[] ownedLoads =
        armProjectionIndexesAtLoadStart(Set.of(indexDef), nodeWriteTrx, expectedRows);
    return ownedLoads[0];
  }

  /**
   * Arm all requested definitions as one ownership transaction. Every successful {@code begin} result
   * is captured before listener setup; a later begin/listener failure aborts only those exact owners.
   * In particular, a failed {@code putIfAbsent} never resolves and aborts the winning owner.
   */
  private ProjectionBulkLoad[] armProjectionIndexesAtLoadStart(final Set<IndexDef> indexDefs,
      final JsonNodeTrx nodeWriteTrx, final long expectedRows) {
    requireNonNull(indexDefs);
    requireNonNull(nodeWriteTrx);
    for (final IndexDef indexDef : indexDefs) {
      requireNonNull(indexDef);
      if (!indexDef.isProjectionIndex()) {
        throw new IllegalArgumentException(
            "createProjectionIndexesAtLoadStart accepts PROJECTION definitions only; got " + indexDef.getType());
      }
    }

    // Reject a stale/reused physical slot before resolving resource identity, walking the path
    // summary, publishing an owner, or cataloguing anything. This keeps the load-time arm as atomic
    // as the ordinary createIndexes path and prevents precondition work from obscuring the actual
    // lifecycle violation.
    validateNewIndexDefinitions(indexDefs, nodeWriteTrx);

    final String resourceKey = nodeWriteTrx.getResourceSession().getResourceConfig().getResource().toString();
    for (final IndexDef indexDef : indexDefs) {
      final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
      if (!pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath())).isEmpty()) {
        throw new IllegalStateException("Projection root path '" + indexDef.getProjectionRootPath()
            + "' already has records — a load-time build can only start on an empty record set. Use "
            + "jn:create-projection-index to build over existing data.");
      }
    }
    nodeWriteTrx.awaitPendingAsyncCommit();

    // Allocate all ownership bookkeeping before publishing the first ACTIVE entry. Once begin()
    // returns, storing its reference in the preallocated array cannot fail and strand the owner.
    final ProjectionBulkLoad[] ownedLoads = new ProjectionBulkLoad[indexDefs.size()];
    final IndexDef[] publishedDefs = new IndexDef[indexDefs.size()];
    int ownedCount = 0;
    int publishedCount = 0;
    try {
      for (final IndexDef indexDef : indexDefs) {
        // Catalogues the def so it serializes on commit and is discoverable after re-open, exactly as
        // createIndexBuilders does for the walking path. Track only definitions this call actually
        // publishes: duplicate-arm cleanup must never remove a pre-existing catalogue entry.
        if (indexes.getIndexDef(indexDef.getID(), indexDef.getType()) == null) {
          indexes.add(indexDef);
          publishedDefs[publishedCount] = indexDef;
          publishedCount++;
        }
        final ProjectionBulkLoad ownedLoad = ProjectionBulkLoad.begin(indexDef, resourceKey, nodeWriteTrx,
            nodeWriteTrx.getPathSummary(), nodeWriteTrx.getStorageEngineWriter(), expectedRows);
        ownedLoads[ownedCount] = ownedLoad;
        ownedCount++;
      }
      // Binds the listeners that feed the armed builds, and sets the projection capability flag without
      // which the write hot paths drop every notification.
      createIndexListeners(indexDefs, nodeWriteTrx);
      return ownedLoads;
    } catch (final Throwable armFailure) {
      for (int i = ownedCount - 1; i >= 0; i--) {
        try {
          ownedLoads[i].abort();
        } catch (final Throwable cleanupFailure) {
          if (cleanupFailure != armFailure) {
            armFailure.addSuppressed(cleanupFailure);
          }
        }
      }
      // Unpublish every def catalogued above. A def left behind with no bound listener would
      // serialize on commit and answer discovery while no maintenance ever feeds it — an index
      // that exists in name only.
      for (int i = publishedCount - 1; i >= 0; i--) {
        try {
          indexes.removeIndex(publishedDefs[i]);
        } catch (final Throwable cleanupFailure) {
          if (cleanupFailure != armFailure) {
            armFailure.addSuppressed(cleanupFailure);
          }
        }
      }
      throw JsonIndexController.<RuntimeException>rethrowUnchecked(armFailure);
    }
  }

  /** Preserve the original arm/listener failure, including any suppressed owner cleanup failures. */
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T rethrowUnchecked(final Throwable failure) throws T {
    throw (T) failure;
  }

  /**
   * Bulk-build a projection index over the transaction's revision: one columnar row per record under
   * the definition's root path, streamed as compact leaves into the projection's HOT sub-tree
   * (metadata at slot 0, leaves at 1..N — see {@link ProjectionIndexMetadata}). The writes ride the
   * given transaction — the caller's commit persists them. Query-side consumption happens through the
   * revision-scoped catalog + pages ({@link io.sirix.index.projection.ProjectionIndexCatalog}),
   * exactly like the other index families — no process-global publication, so uncommitted or
   * rolled-back builds are never visible to other sessions.
   */
  private void createProjectionIndex(final IndexDef indexDef, final JsonNodeTrx nodeWriteTrx) {
    final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
    nodeWriteTrx.awaitPendingAsyncCommit();
    // Creation fails loudly on a root path with no instances (caller error).
    ProjectionIndexBuilder.buildAndPersist(indexDef, pathSummary, nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(),
        false);
  }

  @Override
  protected ChangeListener createProjectionIndexListener(final JsonNodeTrx nodeWriteTrx, final IndexDef indexDef) {
    final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
    // The write transaction doubles as the maintenance navigation handle:
    // at pre-commit the listener re-extracts dirty records from its current
    // state to patch the persisted leaves incrementally.
    return new ProjectionIndexChangeListener(nodeWriteTrx.getStorageEngineWriter(), pathSummary, indexDef,
        nodeWriteTrx);
  }

  @Override
  protected void validateSupportedIndexLifecycles(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isValidTimeIndex()
          && nodeWriteTrx.getResourceSession().getResourceConfig().getValidTimeConfig() == null) {
        throw new IllegalStateException(
            "Cannot create VALIDTIME index " + indexDef.getID() + ": the JSON resource has no ValidTimeConfig");
      }
    }
  }

  /**
   * Validate projection prerequisites before createIndexBuilders can publish a definition or page.
   */
  private static void validateProjectionDefinitions(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isProjectionIndex()) {
        requireProjectionPathSummary(nodeWriteTrx, indexDef);
      }
    }
  }

  private static PathSummaryReader requireProjectionPathSummary(final JsonNodeTrx nodeWriteTrx,
      final IndexDef indexDef) {
    if (!nodeWriteTrx.getResourceSession().getResourceConfig().withPathSummary) {
      // A persisted definition is query-visible after reopen. Silently omitting its listener would
      // let the document change while the projection kept serving the previous rows. Refuse the
      // write transaction instead: projection creation already requires a path summary, and a
      // catalogued definition on a summary-less resource is therefore an inconsistent persisted
      // lifecycle which must fail closed before any mutation can occur.
      throw new IllegalStateException("Cannot bind incremental maintenance for projection index " + indexDef.getID()
          + ": the resource has no path summary. Projection indexes require buildPathSummary=true.");
    }
    final PathSummaryReader pathSummary = nodeWriteTrx.getPathSummary();
    if (pathSummary == null) {
      throw new IllegalStateException("Cannot bind incremental maintenance for projection index " + indexDef.getID()
          + ": the resource's path summary is unavailable.");
    }
    return pathSummary;
  }

  /**
   * Create index builders.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link JsonNodeTrx}
   * @return the created index builder instances
   */
  Set<JsonNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<JsonNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      // Store the index definition so it can be serialized during commit
      indexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH -> indexBuilders.add(
            createPathIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
        case CAS -> indexBuilders.add(createCASIndexBuilder(nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(),
            nodeWriteTrx.getPathSummary(), indexDef));
        case NAME -> indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), indexDef));
        case VECTOR -> {
          // Vector indexes are populated explicitly, not by document traversal.
          // No builder needed.
        }
        case PROJECTION -> {
          // No visitor builder — projection indexes build cursor-driven in
          // createProjectionIndex (invoked by createIndexes after the shared
          // traversal). The indexes.add above catalogues the def so it
          // serializes on commit and is discoverable after re-open.
        }
        case VALIDTIME -> {
          final JsonNodeVisitor vtBuilder = createValidTimeIndexBuilder(nodeWriteTrx, indexDef);
          if (vtBuilder != null) {
            indexBuilders.add(vtBuilder);
          }
        }
      }
    }
    return indexBuilders;
  }

  @Override
  public PathFilter createPathFilter(final Set<String> queryString, final JsonNodeReadOnlyTrx rtx)
      throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(queryString.size());
    for (final String path : queryString) {
      paths.add(Path.parse(path, PathParser.Type.JSON));
    }
    return new PathFilter(paths, new JsonPCRCollector(rtx));
  }

  private JsonNodeVisitor createPathIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (JsonNodeVisitor) pathIndex.createBuilder(storageEngineWriter, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createCASIndexBuilder(final JsonNodeReadOnlyTrx nodeReadTrx,
      final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (JsonNodeVisitor) casIndex.createBuilder(nodeReadTrx, storageEngineWriter, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createNameIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final IndexDef indexDef) {
    return (JsonNodeVisitor) nameIndex.createBuilder(storageEngineWriter, indexDef);
  }

  /**
   * Create the full-scan builder for a valid-time interval index. The builder writes into a
   * writer-backed Relational-Interval-Tree over the index's HOT sub-tree.
   *
   * @return the builder visitor, or {@code null} if the resource has no valid-time configuration
   */
  private JsonNodeVisitor createValidTimeIndexBuilder(final JsonNodeTrx nodeWriteTrx, final IndexDef indexDef) {
    final var storageEngineWriter = nodeWriteTrx.getStorageEngineWriter();
    final ValidTimeConfig validTimeConfig =
        storageEngineWriter.getResourceSession().getResourceConfig().getValidTimeConfig();
    if (validTimeConfig == null) {
      return null;
    }
    final IntervalDomain domain = new IntervalDomain();
    final RelationalIntervalTree tree =
        ValidTimeIntervalIndexFactory.createWriterTree(storageEngineWriter, indexDef.getID(), domain);
    final ValidTimeIntervalIndexWriter indexWriter = new ValidTimeIntervalIndexWriter(tree, domain,
        validTimeConfig.getNormalizedValidFromPath(), validTimeConfig.getNormalizedValidToPath());
    return new JsonValidTimeIndexBuilder(indexWriter, nodeWriteTrx);
  }

  @Override
  protected ChangeListener createValidTimeIndexListener(final JsonNodeTrx nodeWriteTrx, final IndexDef indexDef) {
    final var storageEngineWriter = nodeWriteTrx.getStorageEngineWriter();
    final ValidTimeConfig validTimeConfig =
        storageEngineWriter.getResourceSession().getResourceConfig().getValidTimeConfig();
    if (validTimeConfig == null) {
      return null;
    }
    final IntervalDomain domain = new IntervalDomain();
    final RelationalIntervalTree tree =
        ValidTimeIntervalIndexFactory.createWriterTree(storageEngineWriter, indexDef.getID(), domain);
    final ValidTimeIntervalIndexWriter indexWriter = new ValidTimeIntervalIndexWriter(tree, domain,
        validTimeConfig.getNormalizedValidFromPath(), validTimeConfig.getNormalizedValidToPath());
    return new JsonValidTimeIndexListener(storageEngineWriter, indexWriter,
        validTimeConfig.getNormalizedValidFromPath(), validTimeConfig.getNormalizedValidToPath());
  }
}
