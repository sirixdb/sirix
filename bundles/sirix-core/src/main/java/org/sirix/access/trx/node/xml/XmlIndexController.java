package org.sirix.access.trx.node.xml;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.access.trx.node.AbstractIndexController;
import org.sirix.api.PageTrx;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.index.IndexBuilder;
import org.sirix.index.IndexDef;
import org.sirix.index.Indexes;
import org.sirix.index.cas.xml.XmlCASIndexImpl;
import org.sirix.index.name.xdm.XmlNameIndexImpl;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.xml.XmlPCRCollector;
import org.sirix.index.path.xml.XmlPathIndexImpl;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

import java.util.HashSet;
import java.util.Set;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class XmlIndexController extends AbstractIndexController<XmlNodeReadOnlyTrx, XmlNodeTrx> {

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
  public XmlIndexController() {
    super(new Indexes(), new HashSet<>(), new XmlPathIndexImpl(), new XmlCASIndexImpl(), new XmlNameIndexImpl());
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
   *
   * @return the created index builder instances
   */
  Set<XmlNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<XmlNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      mIndexes.add(indexDef);
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
  public PathFilter createPathFilter(final Set<String> stringPaths, final XmlNodeReadOnlyTrx rtx) {
    final Set<Path<QNm>> paths = new HashSet<>(stringPaths.size());
    for (final String path : stringPaths) {
      paths.add(Path.parse(path));
    }
    return new PathFilter(paths, new XmlPCRCollector(rtx));
  }

  private XmlNodeVisitor createPathIndexBuilder(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (XmlNodeVisitor) mPathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createCASIndexBuilder(final XmlNodeReadOnlyTrx nodeReadTrx,
      final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (XmlNodeVisitor) mCASIndex.createBuilder(nodeReadTrx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createNameIndexBuilder(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    return (XmlNodeVisitor) mNameIndex.createBuilder(pageWriteTrx, indexDef);
  }
}
