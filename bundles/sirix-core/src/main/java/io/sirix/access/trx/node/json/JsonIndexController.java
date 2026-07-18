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
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexLeafCodec;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.vector.json.JsonVectorIndexImpl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.path.PathParser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
   * Bulk-build a projection index over the transaction's revision: one
   * columnar row per record under the definition's root path, streamed as
   * compact leaves into the projection's HOT sub-tree (metadata at slot 0,
   * leaves at 1..N — see {@link ProjectionIndexMetadata}) and installed in
   * the {@link ProjectionIndexRegistry} for the analytical executor. The
   * writes ride the given transaction — the caller's commit persists them.
   */
  private void createProjectionIndex(final IndexDef indexDef, final JsonNodeTrx nodeWriteTrx) {
    final StorageEngineWriter storageEngineWriter = nodeWriteTrx.getStorageEngineWriter();
    final List<byte[]> leaves = new ArrayList<>();
    final ProjectionIndexBuilder builder =
        new ProjectionIndexBuilder(indexDef, nodeWriteTrx.getPathSummary(), leaves::add);
    builder.build(nodeWriteTrx);

    final List<Path<QNm>> fieldPaths = indexDef.getProjectionFields();
    final String[] paths = new String[fieldPaths.size()];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = fieldPaths.get(i).toString();
    }
    final String[] names = ProjectionIndexChangeListener.trailingFieldNames(indexDef);
    final ProjectionIndexHOTStorage storage =
        new ProjectionIndexHOTStorage(storageEngineWriter, indexDef.getID());
    final ProjectionIndexMetadata metadata = new ProjectionIndexMetadata(
        indexDef.getProjectionRootPath().toString(), paths, names, builder.columnKinds(),
        leaves.size());
    storage.put(0, metadata.serialize());
    for (int i = 0; i < leaves.size(); i++) {
      storage.put(i + 1, ProjectionIndexLeafCodec.encode(leaves.get(i)));
    }
    final String resourceKey =
        storageEngineWriter.getResourceSession().getResourceConfig().getResource().toString();
    ProjectionIndexRegistry.installWildcard(resourceKey, names, leaves,
        builder.numericColumnNonIntegralFlags());
  }

  @Override
  protected ChangeListener createProjectionIndexListener(final JsonNodeTrx nodeWriteTrx,
      final IndexDef indexDef) {
    return new ProjectionIndexChangeListener(nodeWriteTrx.getStorageEngineWriter(),
        nodeWriteTrx.getPathSummary(), indexDef);
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
        case PATH ->
          indexBuilders.add(createPathIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
        case CAS -> indexBuilders.add(
            createCASIndexBuilder(nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
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
      final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (JsonNodeVisitor) casIndex.createBuilder(nodeReadTrx, storageEngineWriter, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createNameIndexBuilder(final StorageEngineWriter storageEngineWriter, final IndexDef indexDef) {
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
