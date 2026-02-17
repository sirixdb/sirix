package io.sirix.access.trx.node;

import io.sirix.api.*;
import io.sirix.index.*;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.serialize.SubtreePrinter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.cas.CASIndex;
import io.sirix.index.name.NameFilter;
import io.sirix.index.name.NameIndex;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.PathIndex;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static java.util.Objects.requireNonNull;

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
   * Set of primitive listeners for allocation-conscious hot-path notifications.
   */
  private final Set<PathNodeKeyChangeListener> primitiveListeners;

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
   * Cached capabilities for hot-path checks.
   */
  private boolean hasPathIndex;
  private boolean hasCASIndex;
  private boolean hasNameIndex;

  /**
   * Constructor.
   *
   * @param indexes the index definitions
   * @param listeners the set of listeners
   * @param pathIndex the path index manager
   * @param casIndex the CAS index manager
   * @param nameIndex the name index manager
   */
  public AbstractIndexController(final Indexes indexes, final Set<ChangeListener> listeners,
      final PathIndex<?, ?> pathIndex, final CASIndex<?, ?, R> casIndex, final NameIndex<?, ?> nameIndex) {
    this.indexes = indexes;
    this.listeners = listeners;
    this.primitiveListeners = new HashSet<>(listeners.size());
    for (final ChangeListener listener : listeners) {
      if (listener instanceof final PathNodeKeyChangeListener primitiveListener) {
        primitiveListeners.add(primitiveListener);
      }
    }
    this.pathIndex = pathIndex;
    this.casIndex = casIndex;
    this.nameIndex = nameIndex;
    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      updateIndexCapability(indexDef.getType());
    }
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
  public boolean hasPathIndex() {
    return hasPathIndex;
  }

  @Override
  public boolean hasCASIndex() {
    return hasCASIndex;
  }

  @Override
  public boolean hasNameIndex() {
    return hasNameIndex;
  }

  @Override
  public Indexes getIndexes() {
    return indexes;
  }

  @Override
  public void serialize(final OutputStream out) {
    try {
      final SubtreePrinter serializer = new SubtreePrinter(new PrintStream(requireNonNull(out)));
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
  public void notifyChange(final ChangeType type, final long nodeKey, final NodeKind nodeKind, final long pathNodeKey,
      final @Nullable QNm name, final @Nullable Str value) {
    if (primitiveListeners.isEmpty()) {
      return;
    }
    for (final PathNodeKeyChangeListener primitiveListener : primitiveListeners) {
      primitiveListener.listen(type, nodeKey, nodeKind, pathNodeKey, name, value);
    }
  }

  @Override
  public IndexController<R, W> createIndexListeners(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    requireNonNull(nodeWriteTrx);
    // Save for upcoming modifications.
    for (final IndexDef indexDef : indexDefs) {
      indexes.add(indexDef);
      updateIndexCapability(indexDef.getType());
      switch (indexDef.getType()) {
        case PATH ->
          addListener(createPathIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
        case CAS ->
          addListener(createCASIndexListener(nodeWriteTrx.getPageWtx(), nodeWriteTrx.getPathSummary(), indexDef));
        case NAME -> addListener(createNameIndexListener(nodeWriteTrx.getPageWtx(), indexDef));
        default -> {
        }
      }
    }

    return this;
  }

  private void updateIndexCapability(final IndexType type) {
    switch (type) {
      case PATH -> hasPathIndex = true;
      case CAS -> hasCASIndex = true;
      case NAME -> hasNameIndex = true;
      default -> {
      }
    }
  }

  private void addListener(final ChangeListener listener) {
    listeners.add(listener);
    if (listener instanceof final PathNodeKeyChangeListener primitiveListener) {
      primitiveListeners.add(primitiveListener);
    } else {
      throw new IllegalStateException(
          "Listener does not support primitive change events: " + listener.getClass().getName());
    }
  }

  private ChangeListener createPathIndexListener(final StorageEngineWriter pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return pathIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createCASIndexListener(final StorageEngineWriter pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return casIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createNameIndexListener(final StorageEngineWriter pageWriteTrx, final IndexDef indexDef) {
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
  public Iterator<NodeReferences> openPathIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    if (pathIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return pathIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openNameIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final NameFilter filter) {
    if (nameIndex == null) {
      throw new IllegalStateException("This document does not support name indexes.");
    }

    return nameIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final CASFilter filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support CAS indexes.");
    }

    return casIndex.openIndex(pageRtx, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final StorageEngineReader pageRtx, final IndexDef indexDef,
      final CASFilterRange filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return casIndex.openIndex(pageRtx, indexDef, filter);
  }
}
