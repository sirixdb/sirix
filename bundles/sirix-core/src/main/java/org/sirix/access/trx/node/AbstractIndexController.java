package org.sirix.access.trx.node;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.*;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.*;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.cas.CASIndex;
import org.sirix.index.name.NameFilter;
import org.sirix.index.name.NameIndex;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.PathIndex;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractIndexController<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements IndexController<R, W> {

  /**
   * The index types.
   */
  protected final Indexes indexes;

  /**
   * Set of {@link ChangeListener}.
   */
  private final Set<ChangeListener> listeners;

  /**
   * Used to provide path indexes.
   */
  protected final PathIndex<?, ?> pathIndex;

  /**
   * Used to provide CAS indexes.
   */
  protected final CASIndex<?, ?, R> casIndex;

  /**
   * Used to provide name indexes.
   */
  protected final NameIndex<?, ?> nameIndex;

  /**
   * Constructor.
   *
   * @param indexes   the index definitions
   * @param listeners the set of listeners
   * @param pathIndex the path index manager
   * @param casIndex  the CAS index manager
   * @param nameIndex the name index manager
   */
  public AbstractIndexController(final Indexes indexes, final Set<ChangeListener> listeners,
      final PathIndex<?, ?> pathIndex, final CASIndex<?, ?, R> casIndex, final NameIndex<?, ?> nameIndex) {
    this.indexes = indexes;
    this.listeners = listeners;
    this.pathIndex = pathIndex;
    this.casIndex = casIndex;
    this.nameIndex = nameIndex;
  }

  @Override
  public boolean containsIndex(final IndexType type) {
    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      if (indexDef.getType() == type)
        return true;
    }
    return false;
  }

  @Override
  public Indexes getIndexes() {
    return indexes;
  }

  @Override
  public void serialize(final OutputStream out) {
    try {
      final SubtreePrinter serializer = new SubtreePrinter(new PrintStream(checkNotNull(out)));
      serializer.print(indexes.materialize());
      serializer.end();
    } catch (final DocumentException e) {
      throw new SirixRuntimeException(e);
    }
  }

  @Override
  public void notifyChange(final ChangeType type, @NonNull final ImmutableNode node, final long pathNodeKey) {
    if (listeners.isEmpty()) {
      return;
    }
    for (final ChangeListener listener : listeners) {
      listener.listen(type, node, pathNodeKey);
    }
  }

  @Override
  public IndexController<R, W> createIndexListeners(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    checkNotNull(nodeWriteTrx);
    // Save for upcoming modifications.
    for (final IndexDef indexDef : indexDefs) {
      indexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH:
          listeners.add(createPathIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          listeners.add(createCASIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          listeners.add(createNameIndexListener(nodeWriteTrx.getPageWtx(), indexDef));
          break;
        default:
          break;
      }
    }

    return this;
  }

  private ChangeListener createPathIndexListener(final PageTrx pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return pathIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createCASIndexListener(final PageTrx pageWriteTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return casIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createNameIndexListener(final PageTrx pageWriteTrx, final IndexDef indexDef) {
    return nameIndex.createListener(pageWriteTrx, indexDef);
  }

  @Override
  public NameFilter createNameFilter(final Set<String> names) {
    final Set<QNm> includes = new HashSet<>(names.size());
    for (final String name : names) {
      // TODO: Prefix/NspURI
      includes.add(new QNm(name));
    }
    return new NameFilter(includes, Collections.emptySet());
  }

  @Override
  public CASFilter createCASFilter(final Set<String> stringPaths, final Atomic key, final SearchMode mode,
      final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(stringPaths.size());
    if (!stringPaths.isEmpty()) {
      for (final String path : stringPaths) {
        paths.add(parsePath(path));
      }
    }
    return new CASFilter(paths, key, mode, pcrCollector);
  }

  protected abstract Path<QNm> parsePath(String path);

  @Override
  public CASFilterRange createCASFilterRange(final Set<String> thePaths, final Atomic min, final Atomic max,
      final boolean incMin, final boolean incMax, final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(thePaths.size());
    if (!thePaths.isEmpty()) {
      for (final String path : thePaths) {
        paths.add(parsePath(path));
      }
    }
    return new CASFilterRange(paths, min, max, incMin, incMax, pcrCollector);
  }

  @Override
  public Iterator<NodeReferences> openPathIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    if (pathIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return pathIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openNameIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final NameFilter filter) {
    if (nameIndex == null) {
      throw new IllegalStateException("This document does not support name indexes.");
    }

    return nameIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final CASFilter filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support CAS indexes.");
    }

    return casIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final PageReadOnlyTrx pageRtx, final IndexDef indexDef,
      final CASFilterRange filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return casIndex.openIndex(pageRtx, indexDef, filter);
  }
}
