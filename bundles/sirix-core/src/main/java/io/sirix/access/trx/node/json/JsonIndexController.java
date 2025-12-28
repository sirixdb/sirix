package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.AbstractIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.index.IndexBuilder;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.cas.json.JsonCASIndexImpl;
import io.sirix.index.name.json.JsonNameIndexImpl;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.path.json.JsonPathIndexImpl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.path.PathParser;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.HashSet;
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
    super(new Indexes(), new HashSet<>(), new JsonPathIndexImpl(), new JsonCASIndexImpl(), new JsonNameIndexImpl());
  }

  @Override
  protected Path<QNm> parsePath(String path) {
    return Path.parse(path, PathParser.Type.JSON);
  }

  @Override
  public JsonIndexController createIndexes(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    // Build the indexes.
    IndexBuilder.build(nodeWriteTrx, createIndexBuilders(indexDefs, nodeWriteTrx));

    // Create index listeners for upcoming changes.
    createIndexListeners(indexDefs, nodeWriteTrx);

    return this;
  }

  /**
   * Create index builders.
   *
   * @param indexDefs    the {@link IndexDef}s
   * @param nodeWriteTrx the {@link JsonNodeTrx}
   * @return the created index builder instances
   */
  Set<JsonNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final JsonNodeTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<JsonNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      switch (indexDef.getType()) {
        case PATH -> indexBuilders.add(createPathIndexBuilder(nodeWriteTrx.getPageWtx(),
                                                              nodeWriteTrx.getPathSummary(),
                                                              indexDef));
        case CAS -> indexBuilders.add(createCASIndexBuilder(nodeWriteTrx,
                                                            nodeWriteTrx.getPageWtx(),
                                                            nodeWriteTrx.getPathSummary(),
                                                            indexDef));
        case NAME -> indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getPageWtx(), indexDef));
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

  private JsonNodeVisitor createPathIndexBuilder(final StorageEngineWriter pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (JsonNodeVisitor) pathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createCASIndexBuilder(final JsonNodeReadOnlyTrx nodeReadTrx, final StorageEngineWriter pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (JsonNodeVisitor) casIndex.createBuilder(nodeReadTrx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createNameIndexBuilder(final StorageEngineWriter pageWriteTrx, final IndexDef indexDef) {
    return (JsonNodeVisitor) nameIndex.createBuilder(pageWriteTrx, indexDef);
  }
}
