package io.sirix.access.trx.node;

import io.sirix.api.*;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.node.d2linked.D2NodeBuilder;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.util.path.PathException;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.SearchMode;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.name.NameFilter;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Set;


public interface IndexController<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> {

  /**
   * Type of change.
   */
  enum ChangeType {
    /**
     * Insertion.
     */
    INSERT,

    /**
     * Deletion.
     */
    DELETE
  }

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   */
  boolean containsIndex(IndexType type);

  /**
   * Fast-path check for path indexes.
   *
   * <p>
   * Implementations may override with cached constant-time checks.
   * </p>
   */
  default boolean hasPathIndex() {
    return containsIndex(IndexType.PATH);
  }

  /**
   * Fast-path check for CAS indexes.
   *
   * <p>
   * Implementations may override with cached constant-time checks.
   * </p>
   */
  default boolean hasCASIndex() {
    return containsIndex(IndexType.CAS);
  }

  /**
   * Fast-path check for name indexes.
   *
   * <p>
   * Implementations may override with cached constant-time checks.
   * </p>
   */
  default boolean hasNameIndex() {
    return containsIndex(IndexType.NAME);
  }

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @param resourceSession the {@link ResourceSession} this index controller is bound to
   * @param revision the revision for this index controller
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   * @throws SirixIOException if an I/O exception occurs while deserializing the index configuration
   *         for the specified {@code revision}
   */
  static boolean containsIndex(final IndexType type, final ResourceSession<?, ?> resourceSession, final int revision) {
    final Indexes indexes = new Indexes();

    final java.nio.file.Path indexesFile =
        resourceSession.getResourcePath()
                       .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                       .resolve(revision + ".xml");

    try {
      if (Files.exists(indexesFile) && Files.size(indexesFile) > 0) {
        try (final InputStream in = new FileInputStream(indexesFile.toFile())) {
          indexes.init(IndexController.deserialize(in).getFirstChild());
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
  Indexes getIndexes();

  /**
   * Serialize to an {@link OutputStream}.
   *
   * @param out the {@link OutputStream} to serialize to
   * @throws SirixRuntimeException if an exception occurs during serialization
   */
  void serialize(OutputStream out);

  /**
   * Notify the changes to all listening indexes.
   *
   * @param type type of change
   * @param node the node which has changed (either was inserted or deleted)
   * @param pathNodeKey the path node key of the node (might also be the path node key of the parent
   *        node)
   * @throws SirixIOException if an I/O error occurs
   */
  void notifyChange(ChangeType type, ImmutableNode node, long pathNodeKey);

  /**
   * Notify primitive change details to all listening indexes.
   *
   * <p>
   * Used on write hot paths to avoid immutable-node snapshot materialization.
   * </p>
   *
   * @param type type of change
   * @param nodeKey node key of the changed node
   * @param nodeKind node kind of the changed node
   * @param pathNodeKey path node key of the changed node (or parent path key for value nodes)
   * @param name optional name (only relevant for name-indexed kinds)
   * @param value optional value (only relevant for value-indexed kinds)
   */
  void notifyChange(ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey, @Nullable QNm name,
      @Nullable Str value);

  /**
   * Create new indexes.
   *
   * @param indexDefs Set of {@link IndexDef}s
   * @param nodeWriteTrx the {@link NodeTrx} used
   * @return this {@link IndexController} instance
   * @throws SirixIOException if an I/O exception during index creation occured
   */
  IndexController<R, W> createIndexes(Set<IndexDef> indexDefs, W nodeWriteTrx);

  /**
   * Create index listeners.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XmlNodeTrx}
   *
   * @return this {@link XmlIndexController} instance
   */
  IndexController<R, W> createIndexListeners(Set<IndexDef> indexDefs, W nodeWriteTrx);

  NameFilter createNameFilter(Set<String> names);

  PathFilter createPathFilter(Set<String> paths, R rtx) throws PathException;

  CASFilter createCASFilter(Set<String> paths, Atomic key, SearchMode mode, PCRCollector pcrCollector)
      throws PathException;

  CASFilterRange createCASFilterRange(Set<String> paths, Atomic min, Atomic max, boolean incMin, boolean incMax,
      PCRCollector pcrCollector) throws PathException;

  Iterator<NodeReferences> openPathIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, PathFilter filter);

  Iterator<NodeReferences> openNameIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, NameFilter filter);

  Iterator<NodeReferences> openCASIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, CASFilter filter);

  Iterator<NodeReferences> openCASIndex(StorageEngineReader storageEngineReader, IndexDef indexDef, CASFilterRange filter);

  /**
   * Deserialize from an {@link InputStream}.
   *
   * @param in the {@link InputStream} from which to deserialize the XML fragment
   * @return the deserialized XML fragment Node
   * @throws SirixException if an exception occurs during serialization
   */
  static Node<?> deserialize(final InputStream in) {
    try {
      final DocumentParser parser = new DocumentParser(in);
      final D2NodeBuilder builder = new D2NodeBuilder();
      parser.parse(builder);
      return builder.root();
    } catch (final DocumentException e) {
      throw new SirixException(e);
    }
  }
}
