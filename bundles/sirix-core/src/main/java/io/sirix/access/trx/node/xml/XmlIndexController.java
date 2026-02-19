package io.sirix.access.trx.node.xml;

import io.sirix.access.trx.node.AbstractIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.IndexBuilder;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.cas.xml.XmlCASIndexImpl;
import io.sirix.index.name.xml.XmlNameIndexImpl;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.index.path.xml.XmlPathIndexImpl;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;

import java.util.HashSet;
import java.util.Set;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlIndexController extends AbstractIndexController<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /**
   * Constructor.
   */
  public XmlIndexController() {
    super(new Indexes(), new HashSet<>(), new XmlPathIndexImpl(), new XmlCASIndexImpl(), new XmlNameIndexImpl());
  }

  @Override
  protected Path<QNm> parsePath(String path) {
    return Path.parse(path, PathParser.Type.XML);
  }

  @Override
  public XmlIndexController createIndexes(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    // Build the indexes.
    IndexBuilder.build(nodeWriteTrx, createIndexBuilders(indexDefs, nodeWriteTrx));

    // Create index listeners for upcoming changes.
    createIndexListeners(indexDefs, nodeWriteTrx);

    return this;
  }

  /**
   * Create index builders.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XmlNodeTrx}
   * @return the created index builder instances
   */
  Set<XmlNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<XmlNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      indexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH:
          indexBuilders.add(createPathIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          indexBuilders.add(
              createCASIndexBuilder(nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), indexDef));
          break;
        default:
          break;
      }
    }
    return indexBuilders;
  }

  @Override
  public PathFilter createPathFilter(final Set<String> stringPaths, final XmlNodeReadOnlyTrx rtx) {
    final Set<Path<QNm>> paths = new HashSet<>(stringPaths.size());
    for (final String path : stringPaths) {
      paths.add(Path.parse(path, PathParser.Type.XML));
    }
    return new PathFilter(paths, new XmlPCRCollector(rtx));
  }

  private XmlNodeVisitor createPathIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (XmlNodeVisitor) pathIndex.createBuilder(storageEngineWriter, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createCASIndexBuilder(final XmlNodeReadOnlyTrx nodeReadTrx, final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (XmlNodeVisitor) casIndex.createBuilder(nodeReadTrx, storageEngineWriter, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createNameIndexBuilder(final StorageEngineWriter pageWriteTrx, final IndexDef indexDef) {
    return (XmlNodeVisitor) nameIndex.createBuilder(pageWriteTrx, indexDef);
  }
}
