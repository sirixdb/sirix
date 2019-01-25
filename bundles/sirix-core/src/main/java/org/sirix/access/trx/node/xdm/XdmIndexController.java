package org.sirix.access.trx.node.xdm;

import java.util.HashSet;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.access.trx.node.AbstractIndexController;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.XdmNodeVisitor;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.index.IndexBuilder;
import org.sirix.index.IndexDef;
import org.sirix.index.Indexes;
import org.sirix.index.cas.xdm.XdmCASIndexImpl;
import org.sirix.index.name.xdm.XdmNameIndexImpl;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.xdm.XdmPCRCollector;
import org.sirix.index.path.xdm.XdmPathIndexImpl;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class XdmIndexController extends AbstractIndexController<XdmNodeReadTrx, XdmNodeWriteTrx> {

  /** Type of change. */
  public enum ChangeType {
    /** Insertion. */
    INSERT,

    /** Deletion. */
    DELETE
  }

  /**
   * Constructor.
   */
  public XdmIndexController() {
    super(new Indexes(), new HashSet<>(), new XdmPathIndexImpl(), new XdmCASIndexImpl(), new XdmNameIndexImpl());
  }

  @Override
  public XdmIndexController createIndexes(final Set<IndexDef> indexDefs, final XdmNodeWriteTrx nodeWriteTrx) {
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
   * @param nodeWriteTrx the {@link XdmNodeWriteTrx}
   *
   * @return the created index builder instances
   */
  Set<XdmNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final XdmNodeWriteTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<XdmNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      switch (indexDef.getType()) {
        case PATH:
          indexBuilders.add(createPathIndexBuilder(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          indexBuilders.add(
              createCASIndexBuilder(nodeWriteTrx, nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getPageWtx(), indexDef));
          break;
        default:
          break;
      }
    }
    return indexBuilders;
  }

  @Override
  public PathFilter createPathFilter(final String[] queryString, final XdmNodeReadTrx rtx) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(queryString.length);
    for (final String path : queryString)
      paths.add(Path.parse(path));
    return new PathFilter(paths, new XdmPCRCollector(rtx));
  }

  private XdmNodeVisitor createPathIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (XdmNodeVisitor) mPathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private XdmNodeVisitor createCASIndexBuilder(final XdmNodeReadTrx nodeReadTrx,
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (XdmNodeVisitor) mCASIndex.createBuilder(nodeReadTrx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  private XdmNodeVisitor createNameIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    return (XdmNodeVisitor) mNameIndex.createBuilder(pageWriteTrx, indexDef);
  }
}
