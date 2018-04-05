package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.node.d2linked.D2NodeBuilder;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Node;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexBuilder;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.Indexes;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.cas.CASIndex;
import org.sirix.index.cas.CASIndexImpl;
import org.sirix.index.name.NameFilter;
import org.sirix.index.name.NameIndex;
import org.sirix.index.name.NameIndexImpl;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PCRCollectorImpl;
import org.sirix.index.path.PathFilter;
import org.sirix.index.path.PathIndex;
import org.sirix.index.path.PathIndexImpl;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndexController {

  /** Type of change. */
  public enum ChangeType {
    /** Insertion. */
    INSERT,

    /** Deletion. */
    DELETE
  }

  /** The index types. */
  private final Indexes mIndexes;

  /** Set of {@link ChangeListener}. */
  private final Set<ChangeListener> mListeners;

  /** The {@link PathIndex} implementation used to provide path indexes. */
  private final PathIndex<Long, NodeReferences> mPathIndex;

  /** The {@link CASIndex} implementation used to provide CAS indexes. */
  private final CASIndex<CASValue, NodeReferences> mCASIndex;

  /** The {@link NameIndex} implementation used to provide Name indexes. */
  private final NameIndex<QNm, NodeReferences> mNameIndex;

  /**
   * Constructor.
   *
   * @param session the {@link ResourceManager} this {@link IndexController} is bound to
   */
  public IndexController() {
    mIndexes = new Indexes();
    mListeners = new HashSet<>();
    mPathIndex = new PathIndexImpl();
    mCASIndex = new CASIndexImpl();
    mNameIndex = new NameIndexImpl();
  }

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   */
  public boolean containsIndex(final IndexType type) {
    for (final IndexDef indexDef : mIndexes.getIndexDefs()) {
      if (indexDef.getType() == type)
        return true;
    }
    return false;
  }

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @param resourceManager the {@link ResourceManager} this index controller is bound to
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   * @throws SirixIOException if an I/O exception occurs while deserializing the index configuration
   *         for the specified {@code revision}
   */
  public static boolean containsIndex(final IndexType type, final ResourceManager resourceManager,
      final int revision) throws SirixIOException {
    final Indexes indexes = new Indexes();

    final java.nio.file.Path indexesFile = resourceManager.getResourceConfig().mPath.resolve(
        ResourceConfiguration.ResourcePaths.INDEXES.getFile()).resolve(
            String.valueOf(revision) + ".xml");

    try {
      if (Files.exists(indexesFile) && Files.size(indexesFile) > 0) {
        try (final InputStream in = new FileInputStream(indexesFile.toFile())) {
          indexes.init(deserialize(in).getFirstChild());
        }
      }
    } catch (IOException | DocumentException | SirixException e) {
      throw new SirixIOException("Index definitions couldn't be deserialized!", e);
    }

    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      if (indexDef.getType() == type)
        return true;
    }

    return false;
  }

  /**
   * Get the indexes.
   *
   * @return the indexes
   */
  public Indexes getIndexes() {
    return mIndexes;
  }

  /**
   * Serialize to an {@link OutputStream}.
   *
   * @param out the {@link OutputStream} to serialize to
   * @throws SirixRuntimeException if an exception occurs during serialization
   */
  public void serialize(final OutputStream out) {
    try {
      final SubtreePrinter serializer = new SubtreePrinter(new PrintStream(checkNotNull(out)));
      serializer.print(mIndexes.materialize());
      serializer.end();
    } catch (final DocumentException e) {
      throw new SirixRuntimeException(e);
    }
  }

  /**
   * Deserialize from an {@link InputStream}.
   *
   * @param out the {@link InputStream} from which to deserialize the XML fragment
   * @throws SirixException if an exception occurs during serialization
   */
  public static Node<?> deserialize(final InputStream in) throws SirixException {
    try {
      final DocumentParser parser = new DocumentParser(in);
      final D2NodeBuilder builder = new D2NodeBuilder();
      parser.parse(builder);
      return builder.root();
    } catch (final DocumentException e) {
      throw new SirixException(e);
    }
  }

  /**
   * Notify the changes to all listening indexes.
   *
   * @param type type of change
   * @param node the node which has changed (either was inserted or deleted)
   * @param pathNodeKey the path node key of the node (might also be the path node key of the parent
   *        node)
   * @throws SirixIOException if an I/O error occurs
   */
  public void notifyChange(final ChangeType type, @Nonnull final ImmutableNode node,
      final long pathNodeKey) throws SirixIOException {
    for (final ChangeListener listener : mListeners) {
      listener.listen(type, node, pathNodeKey);
    }
  }

  /**
   * Create new indexes.
   *
   * @param indexDefs Set of {@link IndexDef}s
   * @param nodeWriteTrx the {@link XdmNodeWriteTrx} used
   * @return this {@link IndexController} instance
   * @throws SirixIOException if an I/O exception during index creation occured
   */
  public IndexController createIndexes(final Set<IndexDef> indexDefs,
      final XdmNodeWriteTrx nodeWriteTrx) throws SirixIOException {
    // Build the indexes.
    IndexBuilder.build(nodeWriteTrx, createIndexBuilders(indexDefs, nodeWriteTrx));

    // Create index listeners for upcoming changes.
    return createIndexListeners(indexDefs, nodeWriteTrx);
  }

  /**
   * Create index builders.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XdmNodeWriteTrx}
   *
   * @return the created index builder instances
   */
  Set<Visitor> createIndexBuilders(final Set<IndexDef> indexDefs,
      final XdmNodeWriteTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final Set<Visitor> indexBuilders = new HashSet<>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      switch (indexDef.getType()) {
        case PATH:
          indexBuilders.add(
              createPathIndexBuilder(
                  nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          indexBuilders.add(
              createCASIndexBuilder(
                  nodeWriteTrx, nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(),
                  indexDef));
          break;
        case NAME:
          indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getPageTransaction(), indexDef));
          break;
        default:
          break;
      }
    }
    return indexBuilders;
  }

  /**
   * Create index listeners.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XdmNodeWriteTrx}
   *
   * @return this {@link IndexController} instance
   */
  IndexController createIndexListeners(final Set<IndexDef> indexDefs,
      final XdmNodeWriteTrx nodeWriteTrx) {
    checkNotNull(nodeWriteTrx);
    // Save for upcoming modifications.
    for (final IndexDef indexDef : indexDefs) {
      mIndexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH:
          mListeners.add(
              createPathIndexListener(
                  nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          mListeners.add(
              createCASIndexListener(
                  nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          mListeners.add(createNameIndexListener(nodeWriteTrx.getPageTransaction(), indexDef));
          break;
        default:
          break;
      }
    }
    return this;
  }

  private ChangeListener createPathIndexListener(
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mPathIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createCASIndexListener(
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mCASIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private ChangeListener createNameIndexListener(
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    return mNameIndex.createListener(pageWriteTrx, indexDef);
  }

  private Visitor createPathIndexBuilder(
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mPathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
  }

  private Visitor createCASIndexBuilder(final XdmNodeReadTrx nodeReadTrx,
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return mCASIndex.createBuilder(nodeReadTrx, pageWriteTrx, pathSummaryReader, indexDef);
  }

  private Visitor createNameIndexBuilder(
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final IndexDef indexDef) {
    return mNameIndex.createBuilder(pageWriteTrx, indexDef);
  }

  @SuppressWarnings("static-method")
  public NameFilter createNameFilter(final String[] queryString) {
    final Set<QNm> includes = new HashSet<>(queryString.length);
    for (final String name : queryString) {
      // TODO: Prefix/NspURI
      includes.add(new QNm(name));
    }
    return new NameFilter(includes, Collections.emptySet());
  }

  @SuppressWarnings("static-method")
  public PathFilter createPathFilter(final String[] queryString, final XdmNodeReadTrx rtx)
      throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(queryString.length);
    for (final String path : queryString)
      paths.add(Path.parse(path));
    return new PathFilter(paths, new PCRCollectorImpl(rtx));
  }

  @SuppressWarnings("static-method")
  public CASFilter createCASFilter(final String[] pathArray, final Atomic key,
      final SearchMode mode, final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(pathArray.length);
    if (pathArray.length > 0) {
      for (final String path : pathArray)
        paths.add(Path.parse(path));
    }
    return new CASFilter(paths, key, mode, pcrCollector);
  }

  @SuppressWarnings("static-method")
  public CASFilterRange createCASFilterRange(final String[] pathArray, final Atomic min,
      final Atomic max, final boolean incMin, final boolean incMax, final PCRCollector pcrCollector)
      throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(pathArray.length);
    if (pathArray.length > 0) {
      for (final String path : pathArray)
        paths.add(Path.parse(path));
    }
    return new CASFilterRange(paths, min, max, incMin, incMax, pcrCollector);
  }

  public Iterator<NodeReferences> openPathIndex(final PageReadTrx pageRtx, final IndexDef indexDef,
      final PathFilter filter) {
    if (mPathIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mPathIndex.openIndex(pageRtx, indexDef, filter);
  }

  public Iterator<NodeReferences> openNameIndex(final PageReadTrx pageRtx, final IndexDef indexDef,
      final NameFilter filter) {
    if (mNameIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mNameIndex.openIndex(pageRtx, indexDef, filter);
  }

  public Iterator<NodeReferences> openCASIndex(final PageReadTrx pageRtx, final IndexDef indexDef,
      final CASFilter filter) {
    if (mCASIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mCASIndex.openIndex(pageRtx, indexDef, filter);
  }

  public Iterator<NodeReferences> openCASIndex(final PageReadTrx pageRtx, final IndexDef indexDef,
      final CASFilterRange filter) {
    if (mCASIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return mCASIndex.openIndex(pageRtx, indexDef, filter);
  }
}
