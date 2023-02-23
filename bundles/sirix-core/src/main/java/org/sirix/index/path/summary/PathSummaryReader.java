package org.sirix.index.path.summary;

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.*;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.PathNameFilter;
import org.sirix.cache.Cache;
import org.sirix.cache.PathSummaryData;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.NullNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import org.sirix.node.immutable.xml.ImmutableXmlDocumentRootNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.JsonDocumentRootNode;
import org.sirix.node.xml.XmlDocumentRootNode;
import org.sirix.page.PathSummaryPage;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import java.time.Instant;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Path summary reader organizing the path classes of a resource.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
@SuppressWarnings({ "unused", "UnusedReturnValue" })
public final class PathSummaryReader implements NodeReadOnlyTrx, NodeCursor {

  /**
   * Strong reference to currently selected node.
   */
  private StructNode currentNode;

  /**
   * Page reader.
   */
  private final PageReadOnlyTrx pageReadTrx;

  /**
   * {@link ResourceSession} reference.
   */
  private final ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceSession;

  /**
   * Determines if path summary is closed or not.
   */
  private boolean isClosed;

  /**
   * Mapping of a path node key to the path node/document root node.
   */
  private final Long2ObjectMap<StructNode> pathNodeMapping;

  /**
   * Mapping of a {@link QNm} to a set of path nodes.
   */
  private final Map<QNm, Set<PathNode>> qnmMapping;

  /**
   * The path cache.
   */
  private final Map<Path<QNm>, LongSet> pathCache;

  private boolean init = true;

  /**
   * Private constructor.
   *
   * @param pageReadTrx     page reader
   * @param resourceSession {@link ResourceSession} reference
   */
  private PathSummaryReader(final PageReadOnlyTrx pageReadTrx,
      final ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceSession) {
    pathCache = new HashMap<>();
    this.pageReadTrx = pageReadTrx;
    isClosed = false;
    this.resourceSession = resourceSession;

    final Cache<Integer, PathSummaryData> pathSummaryCache = pageReadTrx.getBufferManager().getPathSummaryCache();
    final PathSummaryData pathSummaryData = pathSummaryCache.get(pageReadTrx.getRevisionNumber());

    final int maxNrOfNodes =
        (int) this.pageReadTrx.getPathSummaryPage(this.pageReadTrx.getActualRevisionRootPage()).getMaxNodeKey(0);
    if (pathSummaryData == null || pageReadTrx.hasTrxIntentLog()) {
      currentNode =
          this.pageReadTrx.getRecord(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), IndexType.PATH_SUMMARY, 0);

      if (currentNode == null) {
        throw new IllegalStateException("Node couldn't be fetched from persistent storage!");
      }
      pathNodeMapping = new Long2ObjectOpenHashMap<>(maxNrOfNodes);
      qnmMapping = new HashMap<>(maxNrOfNodes);
      boolean first = true;
      PathNode previousPathNode = null;
      var axis = new DescendantAxis(this, IncludeSelf.YES);
      while (axis.hasNext()) {
        final var nodeKey = axis.nextLong();
        final var structuralNode = this.getStructuralNode();
        pathNodeMapping.put(nodeKey, structuralNode);

        if (first) {
          first = false;
        } else {
          final var pathNode = this.getPathNode();
          if (!(pageReadTrx instanceof PageTrx)) {
            updateInMemoryNodeRelations(previousPathNode, structuralNode, pathNode);
          }
          qnmMapping.computeIfAbsent(this.getName(), (unused) -> new HashSet<>()).add(pathNode);
          previousPathNode = pathNode;
        }
      }

      if (!pageReadTrx.hasTrxIntentLog()) {
        pathSummaryCache.put(pageReadTrx.getRevisionNumber(),
                             new PathSummaryData(currentNode, pathNodeMapping, qnmMapping));
      }
    } else {
      currentNode = pathSummaryData.currentNode();
      pathNodeMapping = pathSummaryData.pathNodeMapping();
      qnmMapping = pathSummaryData.qnmMapping();
    }

    init = false;
  }

  private void updateInMemoryNodeRelations(PathNode previousPathNode, StructNode structuralNode, PathNode pathNode) {
    if (previousPathNode != null) {
      if (!structuralNode.hasLeftSibling()) {
        previousPathNode.setFirstChild(pathNode);
        previousPathNode.setLastChild(pathNode);
        pathNode.setParent(previousPathNode);
      } else {
        if (previousPathNode.getNodeKey() == structuralNode.getLeftSiblingKey()) {
          previousPathNode.setRightSibling(pathNode);
          pathNode.setLeftSibling(previousPathNode);
        } else {
          final var leftSiblingPathNode = (PathNode) pathNodeMapping.get(structuralNode.getLeftSiblingKey());
          leftSiblingPathNode.setRightSibling(pathNode);
          pathNode.setLeftSibling(leftSiblingPathNode);
        }
      }

      if (structuralNode.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        if (pathNode.getParent() == null) {
          final var parentPathNode = (PathNode) pathNodeMapping.get(structuralNode.getParentKey());
          pathNode.setParent(parentPathNode);
        }
        if (!structuralNode.hasRightSibling()) {
          final var parentPathNode = (PathNode) pathNodeMapping.get(structuralNode.getParentKey());
          parentPathNode.setLastChild(pathNode);
        }
      }
    }
  }

  @Override
  public Optional<User> getUser() {
    return pageReadTrx.getActualRevisionRootPage().getUser();
  }

  @Override
  public boolean storeDeweyIDs() {
    return false;
  }

  @Override
  public PageReadOnlyTrx getPageTrx() {
    return pageReadTrx;
  }

  /**
   * Get a new path summary reader instance.
   *
   * @param pageReadTrx     the {@link PageReadOnlyTrx} instance
   * @param resourceSession the {@link ResourceSession} instance
   * @return new path summary reader instance
   */
  public static PathSummaryReader getInstance(final PageReadOnlyTrx pageReadTrx,
      final ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> resourceSession) {
    return new PathSummaryReader(checkNotNull(pageReadTrx), checkNotNull(resourceSession));
  }

  // package private, only used in writer to keep the mapping always up-to-date
  void putMapping(final @NonNegative long pathNodeKey, final StructNode node) {
    pathNodeMapping.put(pathNodeKey, node);
  }

  // package private, only used in writer to keep the mapping always up-to-date
  StructNode removeMapping(final @NonNegative long pathNodeKey) {
    return pathNodeMapping.remove(pathNodeKey);
  }

  // package private, only used in writer to keep the mapping always up-to-date
  void putQNameMapping(final PathNode node, final QNm name) {
    final Set<PathNode> pathNodes = qnmMapping.computeIfAbsent(this.getName(), (unused) -> new HashSet<>());
    pathNodes.add(node);
    qnmMapping.put(name, pathNodes);
  }

  // package private, only used in writer to keep the mapping always up-to-date
  void removeQNameMapping(final @NonNegative PathNode node, final QNm name) {
    final Set<PathNode> pathNodes = qnmMapping.computeIfAbsent(this.getName(), (unused) -> new HashSet<>());
    if (pathNodes.size() == 1) {
      qnmMapping.remove(name);
    } else {
      pathNodes.remove(node);
    }
  }

  /**
   * Match all descendants of the node denoted by its {@code pathNodeKey} with the given {@code name}.
   *
   * @param name        the QName
   * @param pathNodeKey the path node key to start the search from
   * @param includeSelf if current node should be included or not
   * @return a set with bits set for each matching path node (its {@code pathNodeKey})
   */
  public BitSet matchDescendants(final QNm name, final @NonNegative long pathNodeKey, final IncludeSelf includeSelf) {
    assertNotClosed();
    final Set<PathNode> set = qnmMapping.get(name);
    if (set == null) {
      return new BitSet(0);
    }
    moveTo(pathNodeKey);
    final BitSet matches = new BitSet();
    for (final long nodeKey : new FilterAxis<>(new DescendantAxis(this, includeSelf),
                                               new PathNameFilter(this, name.toString()))) {
      matches.set((int) nodeKey);
    }
    return matches;
  }

  /**
   * Match a {@link QNm} with a minimum level.
   *
   * @param name     the QName
   * @param minLevel minimum level
   * @return a set with bits set for each matching path node
   */
  public BitSet match(final QNm name, final @NonNegative int minLevel) {
    assertNotClosed();
    final Set<PathNode> set = qnmMapping.get(name);
    if (set == null) {
      return new BitSet(0);
    }
    final BitSet matches = new BitSet();
    for (final PathNode psn : set) {
      if (psn.getLevel() >= minLevel) {
        matches.set((int) psn.getNodeKey());
      }
    }
    return matches;
  }

  /**
   * Match a {@link QNm} with a minimum level.
   *
   * @param name     the QName
   * @param minLevel minimum level
   * @return a set with bits set for each matching path node
   */
  public BitSet match(final QNm name, final @NonNegative int minLevel, NodeKind nodeKind) {
    assertNotClosed();
    final Set<PathNode> set = qnmMapping.get(name);
    if (set == null) {
      return new BitSet(0);
    }
    final BitSet matches = new BitSet();
    for (final PathNode psn : set) {
      if (psn.getLevel() >= minLevel && psn.getPathKind() == nodeKind) {
        matches.set((int) psn.getNodeKey());
      }
    }
    return matches;
  }

  /**
   * Match a {@link QNm} with a specific level.
   *
   * @param name     the QName
   * @param level    minimum level
   * @param nodeKind the node type
   * @return a set with bits set for each matching path node
   */
  public Optional<PathNode> matchLevel(final QNm name, final @NonNegative int level, NodeKind nodeKind) {
    assertNotClosed();
    final Set<PathNode> set = qnmMapping.get(name);
    if (set == null) {
      return Optional.empty();
    }
    for (final PathNode pathNode : set) {
      if (pathNode.getLevel() == level && pathNode.getPathKind() == nodeKind) {
        return Optional.of(pathNode);
      }
    }

    return Optional.empty();
  }

  /**
   * Get a set of PCRs matching the specified collection of paths
   *
   * @param expressions the paths to lookup
   * @param useCache    determines if the cache can be used or not
   * @return a set of PCRs matching the specified collection of paths
   * @throws SirixException if parsing a path fails
   */
  public LongSet getPCRsForPaths(final Collection<Path<QNm>> expressions, final boolean useCache)
      throws PathException {
    assertNotClosed();
    final LongSet pcrs = new LongOpenHashSet();
    for (final Path<QNm> path : expressions) {
      pcrs.addAll(getPCRsForPath(path, useCache));
    }
    return pcrs;
  }

  /**
   * Get the path node corresponding to the key.
   *
   * @param pathNodeKey path node key
   * @return path node corresponding to the provided key
   */
  public PathNode getPathNodeForPathNodeKey(final @NonNegative long pathNodeKey) {
    assertNotClosed();

    return (PathNode) pathNodeMapping.get(pathNodeKey);
  }

  @Override
  public ImmutableNode getNode() {
    assertNotClosed();
    if (currentNode instanceof XmlDocumentRootNode) {
      return ImmutableXmlDocumentRootNode.of((XmlDocumentRootNode) currentNode);
    } else if (currentNode instanceof JsonDocumentRootNode) {
      return ImmutableJsonDocumentRootNode.of((JsonDocumentRootNode) currentNode);
    }
    return ImmutablePathNode.of((PathNode) currentNode);
  }

  /**
   * Get path class records (PCRs) for the specified path.
   *
   * @param path     the path for which to get a set of PCRs
   * @param useCache determines if the path cache can be used or not
   * @return set of PCRs belonging to the specified path
   * @throws SirixException if anything went wrong
   */
  public LongSet getPCRsForPath(final Path<QNm> path, final boolean useCache) throws PathException {
    final LongSet pcrSet;
    if (useCache) {
      pcrSet = pathCache.computeIfAbsent(path, (unused) -> new LongOpenHashSet());
    } else {
      pcrSet = new LongOpenHashSet();
    }

    final boolean isAttributePattern = path.isAttribute();
    final int pathLength = path.getLength();

    final long nodeKey = currentNode.getNodeKey();
    moveToDocumentRoot();
    for (final Axis axis = new DescendantAxis(this); axis.hasNext(); ) {
      axis.nextLong();
      final PathNode node = this.getPathNode();

      if (node == null) {
        continue;
      }

      if (node.getLevel() < pathLength) {
        continue;
      }

      if (isAttributePattern ^ (node.getPathKind() == NodeKind.ATTRIBUTE)) {
        continue;
      }

      if (path.matches(node.getPath(this))) {
        pcrSet.add(node.getNodeKey());
      }
    }
    moveTo(nodeKey);
    if (useCache) {
      pathCache.put(path, pcrSet);
    }
    return pcrSet;
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    return getStructuralNode().getChildCount() > 0;
  }

  /**
   * Get a path node.
   *
   * @return {@link PathNode} reference or null for the document root.
   */
  public PathNode getPathNode() {
    assertNotClosed();
    if (currentNode instanceof PathNode) {
      return (PathNode) currentNode;
    }
    return null;
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    assertNotClosed();

    if (!init && nodeKey != 0) {
      final PathNode node = getPathNodeForPathNodeKey(nodeKey);

      if (node != null) {
        currentNode = node;
        return true;
      } else {
        return false;
      }
    }

    // Remember old node and fetch new one.
    final StructNode oldNode = currentNode;
    StructNode newNode;
    try {
      newNode = pageReadTrx.getRecord(nodeKey, IndexType.PATH_SUMMARY, 0);
    } catch (final SirixIOException e) {
      newNode = null;
    }

    if (newNode == null) {
      currentNode = oldNode;
      return false;
    } else {
      currentNode = newNode;
      return true;
    }
  }

  @Override
  public boolean moveToParent() {
    assertNotClosed();
    return moveTo(getStructuralNode().getParentKey());
  }

  @Override
  public boolean moveToFirstChild() {
    assertNotClosed();
    if (!getStructuralNode().hasFirstChild()) {
      return false;
    }
    return moveTo(getStructuralNode().getFirstChildKey());
  }

  @Override
  public boolean moveToLeftSibling() {
    assertNotClosed();
    if (!getStructuralNode().hasLeftSibling()) {
      return false;
    }
    return moveTo(getStructuralNode().getLeftSiblingKey());
  }

  @Override
  public boolean moveToRightSibling() {
    assertNotClosed();
    if (!getStructuralNode().hasRightSibling()) {
      return false;
    }
    return moveTo(getStructuralNode().getRightSiblingKey());
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Immediately release all references.
      currentNode = null;
      isClosed = true;

      if (pageReadTrx != null && !pageReadTrx.isClosed()) {
        pageReadTrx.close();
      }
    }
  }

  /**
   * Make sure that the path summary is not yet closed when calling this method.
   */
  private void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Path summary is already closed.");
    }
  }

  @Override
  public boolean moveToDocumentRoot() {
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  /**
   * Get the current node as a structural node.
   *
   * @return structural node
   */
  private StructNode getStructuralNode() {
    if (currentNode != null) {
      return currentNode;
    }
    return new NullNode(null);
  }

  @Override
  public long getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    return pageReadTrx.getRevisionNumber();
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(pageReadTrx.getActualRevisionRootPage().getRevisionTimestamp());
  }

  @Override
  public long getMaxNodeKey() {
    assertNotClosed();
    final var pageReference = pageReadTrx.getActualRevisionRootPage().getPathSummaryPageReference();

    if (pageReference.getPage() == null) {
      pageReference.setPage(pageReadTrx.getReader().read(pageReference, pageReadTrx));
    }

    return ((PathSummaryPage) pageReference.getPage()).getMaxNodeKey(0);
  }

  @Override
  public boolean moveToNextFollowing() {
    assertNotClosed();
    while (!getStructuralNode().hasRightSibling() && currentNode.hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public QNm getName() {
    assertNotClosed();
    if (currentNode instanceof NameNode nameNode) {
      if (nameNode.getName() != null) {
        return nameNode.getName();
      }
      final int uriKey = nameNode.getURIKey();
      final String uri = uriKey == -1 || pageReadTrx.getResourceSession() instanceof JsonResourceSession
          ? ""
          : pageReadTrx.getName(nameNode.getURIKey(), NodeKind.NAMESPACE);
      final int prefixKey = nameNode.getPrefixKey();
      final String prefix =
          prefixKey == -1 ? "" : pageReadTrx.getName(prefixKey, ((PathNode) currentNode).getPathKind());
      final int localNameKey = nameNode.getLocalNameKey();
      final String localName =
          localNameKey == -1 ? "" : pageReadTrx.getName(localNameKey, ((PathNode) currentNode).getPathKind());
      final var qNm = new QNm(uri, prefix, localName);
      if (nameNode instanceof PathNode pathNode) {
        pathNode.setName(qNm);
      }
      return qNm;
    } else {
      return null;
    }
  }

  @Override
  public int keyForName(final String pName) {
    assertNotClosed();
    return NamePageHash.generateHashForString(pName);
  }

  @Override
  public String nameForKey(final int key) {
    assertNotClosed();
    if (currentNode instanceof PathNode node) {
      return pageReadTrx.getName(key, node.getPathKind());
    } else {
      return "";
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession() {
    assertNotClosed();
    return resourceSession;
  }

  @Override
  public boolean moveToLastChild() {
    assertNotClosed();
    if (getStructuralNode().hasFirstChild()) {
      moveToFirstChild();

      while (getStructuralNode().hasRightSibling()) {
        moveToRightSibling();
      }

      return true;
    }
    return false;
  }

  /**
   * Get the path up to the root path node.
   *
   * @return path up to the root
   */
  public Path<QNm> getPath() {
    PathNode node = getPathNode();
    if (node == null) {
      moveToFirstChild();
      node = getPathNode();

      if (node == null) {
        return null;
      }
    }
    final long nodeKey = getNodeKey();
    moveTo(node.getNodeKey());
    final PathNode[] paths = new PathNode[node.getLevel()];
    for (int i = node.getLevel() - 1; i >= 0; i--) {
      paths[i] = node;
      moveToParent();
      node = getPathNode();
    }

    final Path<QNm> path = new Path<>();
    for (final PathNode pathNode : paths) {
      moveTo(pathNode.getNodeKey());
      if (pathNode.getPathKind() == NodeKind.ATTRIBUTE) {
        path.attribute(getName());
      } else if (pathNode.getPathKind() == NodeKind.ARRAY) {
        path.childArray();
      } else if (pathNode.getPathKind() == NodeKind.OBJECT_KEY) {
        path.childObjectField(getName());
      } else {
        path.child(getName());
      }
    }
    moveTo(nodeKey);
    return path;
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);

    if (currentNode instanceof PathNode node) {
      helper.add("uri", pageReadTrx.getName(node.getURIKey(), node.getPathKind()));
      helper.add("prefix", pageReadTrx.getName(node.getPrefixKey(), node.getPathKind()));
      helper.add("localName", pageReadTrx.getName(node.getLocalNameKey(), node.getPathKind()));
    }

    helper.add("node", currentNode);
    return helper.toString();
  }

  /**
   * Get level of currently selected path node.
   *
   * @return level of currently selected path node
   */
  public int getLevel() {
    assertNotClosed();
    if (currentNode instanceof PathNode) {
      assert getPathNode() != null;
      return getPathNode().getLevel();
    }
    return 0;
  }

  @Override
  public boolean hasNode(final @NonNegative long key) {
    assertNotClosed();
    final long currNodeKey = currentNode.getNodeKey();
    final boolean retVal = moveTo(key);
    final boolean movedBack = moveTo(currNodeKey);
    assert movedBack : "moveTo(currNodeKey) must succeed!";
    return retVal;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    return currentNode.hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    // If it has a first child it also has a last child :)
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    return currentNode.getNodeKey();
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    return getStructuralNode().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    return getStructuralNode().getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    if (getStructuralNode().hasFirstChild()) {
      moveToFirstChild();
      while (getStructuralNode().hasRightSibling()) {
        moveToRightSibling();
      }
      return currentNode.getNodeKey();
    }
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    return currentNode.getParentKey();
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    return currentNode.getKind();
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    return -1;
  }

  @Override
  public NodeKind getPathKind() {
    assertNotClosed();
    if (currentNode instanceof PathNode) {
      return ((PathNode) currentNode).getPathKind();
    }
    return NodeKind.NULL;
  }

  @Override
  public long getChildCount() {
    assertNotClosed();
    return getStructuralNode().getChildCount();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
    return getStructuralNode().getDescendantCount();
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    return NodeKind.PATH;
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    return NodeKind.PATH;
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    assertNotClosed();
    return NodeKind.PATH;
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    if (currentNode.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      final var currentStructNode = getStructuralNode();
      moveToParent();
      final var parentKind = currentNode.getKind();
      currentNode = currentStructNode;
      return parentKind;
    }
    if (currentNode.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    return NodeKind.PATH;
  }

  @Override
  public NodeKind getRightSiblingKind() {
    assertNotClosed();
    return NodeKind.PATH;
  }

  /**
   * Get references.
   *
   * @return number of references of a node
   */
  public int getReferences() {
    assertNotClosed();
    if (currentNode.getKind() == NodeKind.XML_DOCUMENT) {
      return 1;
    } else {
      assert getPathNode() != null;
      return getPathNode().getReferences();
    }
  }

  @Override
  public boolean isDocumentRoot() {
    assertNotClosed();
    return currentNode.getKind() == NodeKind.XML_DOCUMENT || currentNode.getKind() == NodeKind.JSON_DOCUMENT;
  }

  @Override
  public boolean moveToPrevious() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasLeftSibling()) {
      // Left sibling node.
      boolean leftSiblMove = moveTo(node.getLeftSiblingKey());
      // Now move down to rightmost descendant node if it has one.
      while (hasFirstChild()) {
        moveToLastChild();
      }
      return leftSiblMove;
    }
    // Parent node.
    return moveTo(node.getParentKey());
  }

  @Override
  public boolean moveToNext() {
    assertNotClosed();
    final StructNode node = getStructuralNode();
    if (node.hasRightSibling()) {
      // Right sibling node.
      return moveTo(node.getRightSiblingKey());
    }
    // Next following node.
    return moveToNextFollowing();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    assertNotClosed();
    return pageReadTrx.getCommitCredentials();
  }

  public boolean isNameNode() {
    assertNotClosed();
    return currentNode instanceof NameNode;
  }

  public int getLocalNameKey() {
    assertNotClosed();
    if (currentNode instanceof NameNode) {
      return ((NameNode) currentNode).getLocalNameKey();
    }
    return -1;
  }

  public int getPrefixKey() {
    assertNotClosed();
    if (currentNode instanceof NameNode nameNode) {
      return nameNode.getPrefixKey();
    }
    return -1;
  }

  @Override
  public long getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }
}
