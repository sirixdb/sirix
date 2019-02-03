package org.sirix.access.trx.node;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.brackit.xquery.xdm.DocumentException;
import org.sirix.access.trx.node.xdm.XdmIndexController.ChangeType;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.Indexes;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.cas.CASIndex;
import org.sirix.index.name.NameFilter;
import org.sirix.index.name.NameIndex;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.PathIndex;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

public abstract class AbstractIndexController<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements IndexController<R, W> {
  /** The index types. */
  private final Indexes mIndexes;

  /** Set of {@link ChangeListener}. */
  private final Set<ChangeListener> mListeners;

  /** Used to provide path indexes. */
  protected final PathIndex<?, ?> mPathIndex;

  /** Used to provide CAS indexes. */
  protected final CASIndex<?, ?, R> mCASIndex;

  /** Used to provide name indexes. */
  protected final NameIndex<?, ?> mNameIndex;

  /**
   * Constructor.
   *
   * @param indexes the index definitions
   * @param listener the index listeners
   * @param pathIndex the path index manager
   * @param casIndex the CAS index manager
   * @param nameIndex the name index manager
   */
  public AbstractIndexController(final Indexes indexes, final Set<ChangeListener> listeners,
      final PathIndex<?, ?> pathIndex, final CASIndex<?, ?, R> casIndex, final NameIndex<?, ?> nameIndex) {
    mIndexes = indexes;
    mListeners = listeners;
    mPathIndex = pathIndex;
    mCASIndex = casIndex;
    mNameIndex = nameIndex;
  }

  @Override
  public boolean containsIndex(final IndexType type) {
    for (final IndexDef indexDef : mIndexes.getIndexDefs()) {
      if (indexDef.getType() == type)
        return true;
    }
    return false;
  }

  @Override
  public Indexes getIndexes() {
    return mIndexes;
  }

  @Override
  public void serialize(final OutputStream out) {
    try {
      final SubtreePrinter serializer = new SubtreePrinter(new PrintStream(checkNotNull(out)));
      serializer.print(mIndexes.materialize());
      serializer.end();
    } catch (final DocumentException e) {
      throw new SirixRuntimeException(e);
    }
  }

  @Override
  public void notifyChange(final ChangeType type, @Nonnull final ImmutableNode node, final long pathNodeKey) {
    for (final ChangeListener listener : mListeners) {
      listener.listen(type, node, pathNodeKey);
    }
  }

  @Override
  public IndexController<R, W> createIndexListeners(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    checkNotNull(nodeWriteTrx);
    // Save for upcoming modifications.
    for (final IndexDef indexDef : indexDefs) {
      mIndexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH:
          mListeners.add(createPathIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          mListeners.add(createCASIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          mListeners.add(createNameIndexListener(nodeWriteTrx.getPageWtx(), indexDef));
          break;
        default:
          break;
      }
    }

    return this;
  }

  private ChangeListener createPathIndexListener(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mPathIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createCASIndexListener(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mCASIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createNameIndexListener(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    return mNameIndex.createListener(pageWriteTrx, indexDef);
  }

  @Override
  public NameFilter createNameFilter(final String[] queryString) {
    final Set<QNm> includes = new HashSet<>(queryString.length);
    for (final String name : queryString) {
      // TODO: Prefix/NspURI
      includes.add(new QNm(name));
    }
    return new NameFilter(includes, Collections.emptySet());
  }

  @Override
  public CASFilter createCASFilter(final String[] pathArray, final Atomic key, final SearchMode mode,
      final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(pathArray.length);
    if (pathArray.length > 0) {
      for (final String path : pathArray)
        paths.add(Path.parse(path));
    }
    return new CASFilter(paths, key, mode, pcrCollector);
  }

  @Override
  public CASFilterRange createCASFilterRange(final String[] pathArray, final Atomic min, final Atomic max,
      final boolean incMin, final boolean incMax, final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(pathArray.length);
    if (pathArray.length > 0) {
      for (final String path : pathArray)
        paths.add(Path.parse(path));
    }
    return new CASFilterRange(paths, min, max, incMin, incMax, pcrCollector);
  }

  @Override
  public Iterator<NodeReferences> openPathIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    if (mPathIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mPathIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openNameIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final NameFilter filter) {
    if (mNameIndex == null) {
      throw new IllegalStateException("This document does not support name indexes.");
    }

    return mNameIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final CASFilter filter) {
    if (mCASIndex == null) {
      throw new IllegalStateException("This document does not support CAS indexes.");
    }

    return mCASIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final CASFilterRange filter) {
    if (mCASIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mCASIndex.openIndex(pageRtx, indexDef, filter);
  }
}
