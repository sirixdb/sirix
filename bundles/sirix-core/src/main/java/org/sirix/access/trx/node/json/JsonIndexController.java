package org.sirix.access.trx.node.json;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.util.path.PathParser;
import org.sirix.access.trx.node.AbstractIndexController;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.index.IndexBuilder;
import org.sirix.index.IndexDef;
import org.sirix.index.Indexes;
import org.sirix.index.cas.json.JsonCASIndexImpl;
import org.sirix.index.name.json.JsonNameIndexImpl;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.index.path.json.JsonPathIndexImpl;
import org.sirix.index.path.summary.PathSummaryReader;

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

  private JsonNodeVisitor createPathIndexBuilder(final PageTrx pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (JsonNodeVisitor) pathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createCASIndexBuilder(final JsonNodeReadOnlyTrx nodeReadTrx, final PageTrx pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (JsonNodeVisitor) casIndex.createBuilder(nodeReadTrx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  private JsonNodeVisitor createNameIndexBuilder(final PageTrx pageWriteTrx, final IndexDef indexDef) {
    return (JsonNodeVisitor) nameIndex.createBuilder(pageWriteTrx, indexDef);
  }
}
