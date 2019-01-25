package org.sirix.access.trx.node;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Set;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.node.d2linked.D2NodeBuilder;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Node;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.trx.node.xdm.XdmIndexController;
import org.sirix.access.trx.node.xdm.XdmIndexController.ChangeType;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.Indexes;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASFilter;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.name.NameFilter;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PathFilter;
import org.sirix.node.interfaces.immutable.ImmutableNode;


public interface IndexController<R extends NodeReadTrx & NodeCursor, W extends NodeWriteTrx & NodeCursor> {

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   */
  boolean containsIndex(IndexType type);

  /**
   * Determines if an index of the specified type is available.
   *
   * @param type type of index to lookup
   * @param resourceManager the {@link ResourceManager} this index controller is bound to
   * @return {@code true} if an index of the specified type exists, {@code false} otherwise
   * @throws SirixIOException if an I/O exception occurs while deserializing the index configuration
   *         for the specified {@code revision}
   */
  public static boolean containsIndex(final IndexType type, final ResourceManager<?, ?> resourceManager,
      final int revision) {
    final Indexes indexes = new Indexes();

    final java.nio.file.Path indexesFile =
        resourceManager.getResourcePath()
                       .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                       .resolve(String.valueOf(revision) + ".xml");

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
   * Create new indexes.
   *
   * @param indexDefs Set of {@link IndexDef}s
   * @param nodeWriteTrx the {@link NodeWriteTrx} used
   * @return this {@link IndexController} instance
   * @throws SirixIOException if an I/O exception during index creation occured
   */
  IndexController<R, W> createIndexes(Set<IndexDef> indexDefs, W nodeWriteTrx);

  /**
   * Create index listeners.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XdmNodeWriteTrx}
   *
   * @return this {@link XdmIndexController} instance
   */
  IndexController<R, W> createIndexListeners(Set<IndexDef> indexDefs, W nodeWriteTrx);

  NameFilter createNameFilter(String[] queryString);

  PathFilter createPathFilter(String[] queryString, R rtx) throws PathException;

  CASFilter createCASFilter(String[] pathArray, Atomic key, SearchMode mode, PCRCollector pcrCollector)
      throws PathException;

  CASFilterRange createCASFilterRange(String[] pathArray, Atomic min, Atomic max, boolean incMin, boolean incMax,
      PCRCollector pcrCollector) throws PathException;

  Iterator<NodeReferences> openPathIndex(PageReadTrx pageRtx, IndexDef indexDef, PathFilter filter);

  Iterator<NodeReferences> openNameIndex(PageReadTrx pageRtx, IndexDef indexDef, NameFilter filter);

  Iterator<NodeReferences> openCASIndex(PageReadTrx pageRtx, IndexDef indexDef, CASFilter filter);

  Iterator<NodeReferences> openCASIndex(PageReadTrx pageRtx, IndexDef indexDef, CASFilterRange filter);

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
}
