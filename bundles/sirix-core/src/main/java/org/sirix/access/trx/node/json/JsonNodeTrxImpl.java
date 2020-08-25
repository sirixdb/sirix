/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access.trx.node.json;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.User;
import org.sirix.access.trx.node.AfterCommitState;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.HashType;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.InternalResourceManager.Abort;
import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.api.PageTrx;
import org.sirix.api.PostCommitHook;
import org.sirix.api.PreCommitHook;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffTuple;
import org.sirix.diff.JsonDiffSerializer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.*;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * Single-threaded instance of only read/write-transaction per resource, thus it is not thread-safe.
 * </p>
 *
 * <p>
 * If auto-commit is enabled, that is a scheduled commit(), all access to public methods is
 * synchronized, such that a commit() and another method doesn't interfere, which could produce
 * severe inconsistencies.
 * </p>
 *
 * <p>
 * All methods throw {@link NullPointerException}s in case of null values for reference parameters.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
final class JsonNodeTrxImpl extends AbstractForwardingJsonNodeReadOnlyTrx implements InternalJsonNodeTrx {

  /**
   * Maximum number of node modifications before auto commit.
   */
  private final int maxNodeCount;

  /**
   * Json DeweyID manager.
   */
  private final JsonDeweyIDManager deweyIDManager;

  /**
   * After commit state: keep open or close.
   */
  private final AfterCommitState afterCommitState;

  /**
   * Hashes nodes.
   */
  private JsonNodeHashing nodeHashing;

  /**
   * Modification counter.
   */
  long modificationCount;

  /**
   * Hash kind of Structure.
   */
  private final HashType hashType;

  /**
   * Scheduled executor service.
   */
  private final ScheduledExecutorService threadPool =
      Executors.newScheduledThreadPool(1, new JsonNodeTrxThreadFactory());

  /**
   * {@link InternalJsonNodeReadOnlyTrx} reference.
   */
  final InternalJsonNodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * {@link PathSummaryWriter} instance.
   */
  private PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter;

  /**
   * Determines if a path summary should be built and kept up-to-date or not.
   */
  private final boolean buildPathSummary;

  /**
   * {@link JsonNodeFactory} to be able to create nodes.
   */
  private JsonNodeFactory nodeFactory;

  /**
   * An optional lock for all methods, if an automatic commit is issued.
   */
  private final Lock lock;

  /**
   * Determines if text values should be compressed or not.
   */
  private final boolean useTextCompression;

  /**
   * The {@link JsonIndexController} used within the session this {@link JsonNodeTrx} is bound to.
   */
  private final JsonIndexController indexController;

  /**
   * The resource manager.
   */
  private final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager;

  /**
   * The page write trx.
   */
  private PageTrx pageTrx;

  /**
   * Collection holding pre-commit hooks.
   */
  private final List<PreCommitHook> preCommitHooks = new ArrayList<>();

  /**
   * Collection holding post-commit hooks.
   */
  private final List<PostCommitHook> postCommitHooks = new ArrayList<>();

  /**
   * The hash function to use to hash node contents.
   */
  private final HashFunction hashFunction;

  /**
   * Collects update operations in pre-order, thus it must be an order-preserving sorted map.
   */
  private final SortedMap<SirixDeweyID, DiffTuple> updateOperationsOrdered;

  /**
   * Collects update operations in no particular order (if DeweyIDs used for sorting are not stored).
   */
  private final Map<Long, DiffTuple> updateOperationsUnordered;

  /**
   * Flag to decide whether to store child count.
   */
  private final boolean storeChildCount;

  /**
   * Determines if a value can be replaced, used for replacing an object record.
   */
  private boolean canRemoveValue;

  /**
   * The revision number before bulk-inserting nodes.
   */
  private int beforeBulkInsertionRevisionNumber;

  /**
   * {@code true}, if transaction is auto-committing, {@code false} if not.
   */
  private final boolean isAutoCommitting;

  /**
   * Transaction state.
   */
  private volatile State state;

  /**
   * The transaction states.
   */
  private enum State {
    Running,

    Committing,

    Committed,

    Closed
  }

  /**
   * Constructor.
   *
   * @param resourceManager  the resource manager instance this transaction is bound to
   * @param maxNodeCount     maximum number of node modifications before auto commit
   * @param timeUnit         unit of the number of the next param {@code pMaxTime}
   * @param maxTime          maximum number of seconds before auto commit
   * @param nodeHashing      hashes node contents
   * @param nodeFactory      to create nodes
   * @param afterCommitState state after committing, keep open or close
   * @throws SirixIOException    if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  JsonNodeTrxImpl(final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final InternalJsonNodeReadOnlyTrx nodeReadTrx, final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter,
      final @Nonnegative int maxNodeCount, final TimeUnit timeUnit, final @Nonnegative int maxTime,
      final @Nonnull JsonNodeHashing nodeHashing, final JsonNodeFactory nodeFactory,
      final @Nonnull AfterCommitState afterCommitState) {
    // Do not accept negative values.
    Preconditions.checkArgument(maxNodeCount >= 0 && maxTime >= 0,
                                "Negative arguments for maxNodeCount and maxTime are not accepted.");

    this.nodeHashing = Preconditions.checkNotNull(nodeHashing);
    this.hashFunction = resourceManager.getResourceConfig().nodeHashFunction;
    this.resourceManager = Preconditions.checkNotNull(resourceManager);
    this.nodeReadOnlyTrx = Preconditions.checkNotNull(nodeReadTrx);
    this.buildPathSummary = resourceManager.getResourceConfig().withPathSummary;
    this.pathSummaryWriter = pathSummaryWriter;

    indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    pageTrx = (PageTrx) nodeReadOnlyTrx.getPageTrx();
    storeChildCount = this.resourceManager.getResourceConfig().getStoreChildCount();

    this.nodeFactory = Preconditions.checkNotNull(nodeFactory);

    // Only auto commit by node modifications if it is more then 0.
    this.maxNodeCount = maxNodeCount;
    this.modificationCount = 0L;

    isAutoCommitting = maxNodeCount > 0 || maxTime > 0;

    if (maxTime > 0) {
      threadPool.scheduleWithFixedDelay(() -> commit("autoCommit"), maxTime, maxTime, timeUnit);
    }

    // Synchronize commit and other public methods if needed.
    lock = maxTime > 0 ? new ReentrantLock() : null;

    hashType = resourceManager.getResourceConfig().hashType;
    useTextCompression = resourceManager.getResourceConfig().useTextCompression;

    deweyIDManager = new JsonDeweyIDManager(this);

    updateOperationsOrdered = new TreeMap<>();
    updateOperationsUnordered = new HashMap<>();

    this.afterCommitState = Preconditions.checkNotNull(afterCommitState);
    state = State.Running;

    // // Redo last transaction if the system crashed.
    // if (!pPageWriteTrx.isCreated()) {
    // try {
    // commit();
    // } catch (final SirixException e) {
    // throw new IllegalStateException(e);
    // }
    // }
  }

  @Override
  public Optional<User> getUser() {
    return resourceManager.getUser();
  }

  @Override
  public Optional<User> getUserOfRevisionToRepresent() {
    return nodeReadOnlyTrx.getUser();
  }

  /**
   * Acquire a lock if necessary.
   */
  private void acquireLockIfNecessary() {
    if (lock != null) {
      lock.lock();
    }
  }

  /**
   * Release a lock if necessary.
   */
  private void unLockIfNecessary() {
    if (lock != null) {
      lock.unlock();
    }
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, Commit.Implicit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, Commit.Implicit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, checkParentNode);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, Commit.Implicit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, Commit.Implicit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, CheckParentNode.Yes);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode);
  }

  private JsonNodeTrx insertSubtree(final JsonReader reader, final InsertPosition insertionPosition, Commit commit,
      CheckParentNode checkParentNode) {
    nodeReadOnlyTrx.assertNotClosed();
    checkNotNull(reader);
    assert insertionPosition != null;

    acquireLockIfNecessary();

    try {
      checkState();
      final var peekedJsonToken = reader.peek();

      if (peekedJsonToken != JsonToken.BEGIN_OBJECT && peekedJsonToken != JsonToken.BEGIN_ARRAY)
        throw new SirixUsageException("JSON to insert must begin with an array or object.");

      final var nodeKind = getKind();
      var skipRootJsonToken = false;

      // $CASES-OMITTED$
      switch (insertionPosition) {
        case AS_FIRST_CHILD, AS_LAST_CHILD -> {
          if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT) {
            throw new IllegalStateException("Current node must either be the document root, an array or an object key.");
          }
          switch (peekedJsonToken) {
            case BEGIN_OBJECT:
              if (nodeKind == NodeKind.OBJECT)
                skipRootJsonToken = true;
              break;
            case BEGIN_ARRAY:
              if (nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.JSON_DOCUMENT) {
                throw new IllegalStateException("Current node in storage must be an array node.");
              }
              break;
            // $CASES-OMITTED$
            default:
          }
        }
        case AS_LEFT_SIBLING, AS_RIGHT_SIBLING -> {
          if (checkParentNode == CheckParentNode.Yes) {
            final NodeKind parentKind = getParentKind();
            if (parentKind != NodeKind.ARRAY) {
              throw new IllegalStateException("Current parent node must be an array node.");
            }
          }
        }
        default -> throw new UnsupportedOperationException();
      }

      checkAccessAndCommit();
      beforeBulkInsertionRevisionNumber = nodeReadOnlyTrx.getRevisionNumber();
      nodeHashing.setBulkInsert(true);
      if (isAutoCommitting) {
        nodeHashing.setAutoCommit(true);
      }
      var nodeKey = getCurrentNode().getNodeKey();
      final var shredderBuilder = new JsonShredder.Builder(this, reader, insertionPosition);

      if (skipRootJsonToken) {
        shredderBuilder.skipRootJsonToken();
      }

      final var shredder = shredderBuilder.build();
      shredder.call();
      moveTo(nodeKey);

      switch (insertionPosition) {
        case AS_FIRST_CHILD:
          moveToFirstChild();
          break;
        case AS_LAST_CHILD:
          moveToLastChild();
          break;
        case AS_LEFT_SIBLING:
          moveToLeftSibling();
          break;
        case AS_RIGHT_SIBLING:
          moveToRightSibling();
          break;
        default:
          // May not happen.
      }

      adaptUpdateOperationsForInsert(getDeweyID(), getNodeKey());

      // bulk inserts will be disabled for auto-commits after the first commit
      if (!isAutoCommitting) {
        adaptHashesInPostorderTraversal();
      }

      nodeHashing.setBulkInsert(false);

      if (commit == Commit.Implicit) {
        commit();
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      unLockIfNecessary();
    }
    return this;
  }

  private void checkState() {
    if (state != State.Running) {
      throw new IllegalStateException("Transaction state is not running: " + state);
    }
  }

  /**
   * Modifying hashes in a postorder-traversal.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void postOrderTraversalHashes() {
    new PostOrderAxis(this, IncludeSelf.YES).forEach((unused) -> nodeHashing.addHashAndDescendantCount());
  }

  @Override
  public JsonNodeTrx insertObjectAsFirstChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY)
        throw new SirixUsageException(
            "Insert is not allowed if current node is not the document-, an object key- or a json array node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey =
          kind == NodeKind.OBJECT_KEY ? Fixed.NULL_NODE_KEY.getStandardProperty() : structNode.getFirstChildKey();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsLastChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY)
        throw new SirixUsageException(
            "Insert is not allowed if current node is not the document-, an object key- or a json array node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey =
          kind == NodeKind.OBJECT_KEY ? Fixed.NULL_NODE_KEY.getStandardProperty() : structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : ((structNode.getChildCount() == 0)
              ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsLeftSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();

      if (!nodeHashing.isBulkInsert()) {
        if (getParentKind() != NodeKind.ARRAY) {
          throw new SirixUsageException("Insert is not allowed if parent node is not an array node!");
        }
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsRightSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();

      if (!nodeHashing.isBulkInsert()) {
        if (getParentKind() != NodeKind.ARRAY) {
          throw new SirixUsageException("Insert is not allowed if parent node is not an array node!");
        }
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsFirstChild(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(structNode.getNodeKey(), key, NodeKind.OBJECT_KEY);

      final SirixDeweyID id = deweyIDManager.newFirstChildID();

      final ObjectKeyNode node = nodeFactory.createJsonObjectKeyNode(parentKey,
                                                                     leftSibKey,
                                                                     rightSibKey,
                                                                     pathNodeKey,
                                                                     key,
                                                                     Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                     id);

      adaptNodesAndHashesForInsertAsChild(node);

      nodeReadOnlyTrx.setCurrentNode(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLastChild(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();

      final long pathNodeKey = getPathNodeKey(structNode.getNodeKey(), key, NodeKind.OBJECT_KEY);

      final SirixDeweyID id = (structNode.getChildCount() == 0)
                                ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID();

      final ObjectKeyNode node = nodeFactory.createJsonObjectKeyNode(parentKey,
                                                                     leftSibKey,
                                                                     rightSibKey,
                                                                     pathNodeKey,
                                                                     key,
                                                                     Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                     id);

      adaptNodesAndHashesForInsertAsChild(node);

      nodeReadOnlyTrx.setCurrentNode(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  public void adaptUpdateOperationsForInsert(SirixDeweyID id, long newNodeKey) {
    final var diffTuple = new DiffTuple(DiffFactory.DiffType.INSERTED,
                                        newNodeKey,
                                        0,
                                        id == null ? null : new DiffDepth(id.getLevel(), 0));
    if (id == null) {
      updateOperationsUnordered.put(newNodeKey, diffTuple);
    } else {
      updateOperationsOrdered.put(id, diffTuple);
    }
  }

  private void setFirstChildOfObjectKeyNode(final ObjectKeyNode node) {
    final ObjectKeyNode objectKeyNode = pageTrx.prepareRecordForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
    objectKeyNode.setFirstChildKey(getNodeKey());
  }

  private void insertValue(final ObjectRecordValue<?> value) throws AssertionError {
    final NodeKind valueKind = value.getKind();

    // $CASES-OMITTED$
    switch (valueKind) {
      case OBJECT -> insertObjectAsFirstChild();
      case ARRAY -> insertArrayAsFirstChild();
      case STRING_VALUE -> insertStringValueAsFirstChild((String) value.getValue());
      case BOOLEAN_VALUE -> insertBooleanValueAsFirstChild((Boolean) value.getValue());
      case NUMBER_VALUE -> insertNumberValueAsFirstChild((Number) value.getValue());
      case NULL_VALUE -> insertNullValueAsFirstChild();
      default -> throw new AssertionError("Type not known.");
    }
  }

  private long getPathNodeKey(final long nodeKey, final String name, final NodeKind kind) {
    moveToParentObjectKeyArrayOrDocumentRoot();

    final long pathNodeKey = buildPathSummary ? pathSummaryWriter.getPathNodeKey(new QNm(name), kind) : 0;

    assert nodeReadOnlyTrx.moveTo(nodeKey).hasMoved();

    return pathNodeKey;
  }

  private void moveToParentObjectKeyArrayOrDocumentRoot() {
    while (nodeReadOnlyTrx.getKind() != NodeKind.OBJECT_KEY && nodeReadOnlyTrx.getKind() != NodeKind.ARRAY
        && nodeReadOnlyTrx.getKind() != NodeKind.JSON_DOCUMENT) {
      nodeReadOnlyTrx.moveToParent();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLeftSibling(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long rightSibKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), key, NodeKind.OBJECT_KEY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ObjectKeyNode node =
          nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, key, -1, id);

      insertAsSibling(node);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsRightSibling(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), key, NodeKind.OBJECT_KEY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ObjectKeyNode node =
          nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, key, -1, id);

      insertAsSibling(node);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsFirstChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();
      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY)
        throw new SirixUsageException(
            "Insert is not allowed if current node is not the document node or an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = currentNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "__array__", NodeKind.ARRAY);

      final SirixDeweyID id = currentNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsLastChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();
      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY)
        throw new SirixUsageException(
            "Insert is not allowed if current node is not the document node or an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();

      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "__array__", NodeKind.ARRAY);

      final SirixDeweyID id = currentNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : ((currentNode.getChildCount() == 0)
            ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsLeftSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "array", NodeKind.ARRAY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsRightSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "array", NodeKind.ARRAY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx replaceObjectRecordValue(String key, ObjectRecordValue<?> value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Replacing is only permitted for record object key nodes.");

      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
      moveToFirstChild();

      canRemoveValue = true;
      remove();
      moveTo(nodeKey);

      insertValue(value);

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final String value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or an array node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long pathNodeKey = getPathNodeKey(structNode);
      final long parentKey = structNode.getNodeKey();

      final byte[] textValue = getBytes(value);

      final SirixDeweyID id;
      final AbstractStringNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectStringNode(parentKey, textValue, useTextCompression, id);
      } else {
        id = deweyIDManager.newFirstChildID();
        final long rightSibKey = structNode.getFirstChildKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsLastChild(final String value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or an array node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long pathNodeKey = getPathNodeKey(structNode);
      final long parentKey = structNode.getNodeKey();

      final byte[] textValue = getBytes(value);

      final SirixDeweyID id;
      final AbstractStringNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectStringNode(parentKey, textValue, useTextCompression, id);
      } else {
        id = ((structNode.getChildCount() == 0)
              ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());
        final long leftSibKey = structNode.getLastChildKey();
        final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  private long getPathNodeKey(StructNode structNode) {
    final long pathNodeKey;

    if (structNode.getKind() == NodeKind.ARRAY) {
      pathNodeKey = ((ArrayNode) structNode).getPathNodeKey();
    } else if (structNode.getKind() == NodeKind.OBJECT_KEY) {
      pathNodeKey = ((ObjectKeyNode) structNode).getPathNodeKey();
    } else {
      pathNodeKey = -1;
    }
    return pathNodeKey;
  }

  private void adaptNodesAndHashesForInsertAsChild(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    nodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node);
    nodeReadOnlyTrx.setCurrentNode(node);
    nodeHashing.adaptHashesWithAdd();
  }

  @Override
  public JsonNodeTrx insertStringValueAsLeftSibling(final String value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      final byte[] textValue = getBytes(value);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final StringNode node =
          nodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final String value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final byte[] textValue = getBytes(value);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final StringNode node =
          nodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsFirstChild(boolean value) {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or array node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = (StructNode) getCurrentNode();
      final long pathNodeKey = getPathNodeKey(structNode);
      final long parentKey = structNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractBooleanNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectBooleanNode(parentKey, value, id);
      } else {
        id = deweyIDManager.newFirstChildID();
        final long rightSibKey = structNode.getFirstChildKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLastChild(boolean value) {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or array node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = (StructNode) getCurrentNode();
      final long pathNodeKey = getPathNodeKey(structNode);
      final long parentKey = structNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractBooleanNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectBooleanNode(parentKey, value, id);
      } else {
        id = ((structNode.getChildCount() == 0)
              ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());
        final long leftSibKey = structNode.getLastChildKey();
        final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLeftSibling(boolean value) {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final BooleanNode node = nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsRightSibling(boolean value) {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final BooleanNode node = nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  private void checkPrecondition() {
    if (!nodeHashing.isBulkInsert() && getParentKind() != NodeKind.ARRAY) {
      throw new SirixUsageException("Insert is not allowed if parent node is not an array node!");
    }
  }

  private void insertAsSibling(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    nodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node);
    nodeReadOnlyTrx.setCurrentNode(node);
    nodeHashing.adaptHashesWithAdd();

    // Get the path node key.
    moveToParentObjectKeyArrayOrDocumentRoot();

    final long pathNodeKey;

    if (isObjectKey()) {
      pathNodeKey = ((ImmutableObjectKeyNode) getNode()).getPathNodeKey();
    } else if (isArray()) {
      pathNodeKey = ((ImmutableArrayNode) getNode()).getPathNodeKey();
    } else {
      pathNodeKey = -1;
    }

    nodeReadOnlyTrx.setCurrentNode(node);

    indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);
  }

  @Override
  public JsonNodeTrx insertNumberValueAsFirstChild(Number value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long pathNodeKey = getPathNodeKey(currentNode);
      final long parentKey = currentNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractNumberNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectNumberNode(parentKey, value, id);
      } else {
        id = deweyIDManager.newFirstChildID();
        final long rightSibKey = currentNode.getFirstChildKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLastChild(Number value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long pathNodeKey = getPathNodeKey(currentNode);
      final long parentKey = currentNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractNumberNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectNumberNode(parentKey, value, id);
      } else {
        id = ((currentNode.getChildCount() == 0)
              ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());
        final long leftSibKey = currentNode.getLastChildKey();
        final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLeftSibling(Number value) {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getParentKey();
      final long rightSibKey = structNode.getNodeKey();
      final long leftSibKey = structNode.getLeftSiblingKey();

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final NumberNode node = nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsRightSibling(Number value) {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getParentKey();
      final long leftSibKey = structNode.getNodeKey();
      final long rightSibKey = structNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final NumberNode node = nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsFirstChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractNullNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectNullNode(parentKey, id);
      } else {
        id = deweyIDManager.newFirstChildID();
        final long rightSibKey = structNode.getRightSiblingKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsLastChild() {
    acquireLockIfNecessary();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      if (kind != NodeKind.OBJECT_KEY) {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();

      final SirixDeweyID id;
      final AbstractNullNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        node = nodeFactory.createJsonObjectNullNode(parentKey, id);
      } else {
        id = ((structNode.getChildCount() == 0)
              ? deweyIDManager.newFirstChildID() : deweyIDManager.newLastChildID());
        final long leftSibKey = structNode.getLeftSiblingKey();
        final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsLeftSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long rightSibKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final NullNode node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsRightSibling() {
    acquireLockIfNecessary();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final NullNode node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  /**
   * Get a byte-array from a value.
   *
   * @param value the value
   * @return byte-array representation of {@code pValue}
   */
  private static byte[] getBytes(final String value) {
    return value.getBytes(Constants.DEFAULT_ENCODING);
  }

  @Override
  public JsonNodeTrx remove() {
    checkAccessAndCommit();
    acquireLockIfNecessary();
    try {
      final StructNode node = (StructNode) getCurrentNode();
      if (node.getKind() == NodeKind.JSON_DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      }

      adaptUpdateOperationsForRemove(node.getDeweyID(), node.getNodeKey());

      final var parentNodeKind = getParentKind();

      if ((parentNodeKind != NodeKind.JSON_DOCUMENT && parentNodeKind != NodeKind.OBJECT
          && parentNodeKind != NodeKind.ARRAY) && !canRemoveValue) {
        throw new SirixUsageException(
            "An object record value can not be removed, you have to remove the whole object record (parent of this value).");
      }

      canRemoveValue = false;

      // Remove subtree.
      for (final var axis = new PostOrderAxis(this); axis.hasNext(); ) {
        axis.next();

        final var currentNode = axis.getCursor().getNode();

        // Remove name.
        removeName();

        // Remove text value.
        removeValue();

        // Then remove node.
        pageTrx.removeRecord(currentNode.getNodeKey(), PageKind.RECORDPAGE, -1);
      }

      // Remove the name of subtree-root.
      if (node.getKind() == NodeKind.OBJECT_KEY) {
        removeName();
      } else {
        removeValue();
      }

      // Adapt hashes and neighbour nodes as well as the name from the NamePage mapping if it's not a text node.
      final ImmutableJsonNode jsonNode = (ImmutableJsonNode) node;
      nodeReadOnlyTrx.setCurrentNode(jsonNode);
      nodeHashing.adaptHashesWithRemove();
      adaptForRemove(node);
      nodeReadOnlyTrx.setCurrentNode(jsonNode);

      if (node.hasRightSibling()) {
        moveTo(node.getRightSiblingKey());
      } else if (node.hasLeftSibling()) {
        moveTo(node.getLeftSiblingKey());
      } else {
        moveTo(node.getParentKey());
      }

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  private void adaptUpdateOperationsForRemove(SirixDeweyID id, final long oldNodeKey) {
    moveToNext();
    final var diffTuple =
        new DiffTuple(DiffFactory.DiffType.DELETED, 0, oldNodeKey, id == null ? null : new DiffDepth(0, id.getLevel()));
    if (id == null) {
      updateOperationsUnordered.values().removeIf(currDiffTuple -> currDiffTuple.getNewNodeKey() == oldNodeKey);
      updateOperationsUnordered.put(oldNodeKey, diffTuple);
    } else {
      updateOperationsOrdered.values().removeIf(currDiffTuple -> currDiffTuple.getNewNodeKey() == oldNodeKey);
      updateOperationsOrdered.put(id, diffTuple);
    }
    moveTo(oldNodeKey);
  }

  private void removeValue() {
    final var currentNode = getCurrentNode();
    if (currentNode.getKind() == NodeKind.OBJECT_STRING_VALUE || currentNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE
        || currentNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE || currentNode.getKind() == NodeKind.STRING_VALUE
        || currentNode.getKind() == NodeKind.NUMBER_VALUE || currentNode.getKind() == NodeKind.BOOLEAN_VALUE) {
      final long nodeKey = getNodeKey();

      final long pathNodeKey;

      assert moveToParent().hasMoved();

      if (getNode().getKind() == NodeKind.ARRAY) {
        pathNodeKey = ((ImmutableArrayNode) getNode()).getPathNodeKey();
      } else if (getNode().getKind() == NodeKind.OBJECT_KEY) {
        pathNodeKey = ((ImmutableObjectKeyNode) getNode()).getPathNodeKey();
      } else {
        pathNodeKey = -1;
      }

      moveTo(nodeKey);
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);
    }
  }

  /**
   * Remove a name from the {@link NamePage} reference and the path summary if needed.
   *
   * @throws SirixException if Sirix fails
   */
  private void removeName() {
    if (getCurrentNode() instanceof ImmutableNameNode node) {
      indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
      final NodeKind nodeKind = node.getKind();
      final NamePage page = ((NamePage) pageTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(node.getLocalNameKey(), nodeKind, pageTrx);

      assert nodeKind != NodeKind.JSON_DOCUMENT;
      if (buildPathSummary) {
        pathSummaryWriter.remove(node, nodeKind, page);
      }
    }
  }

  @Override
  public JsonNodeTrx setObjectKeyName(final String key) {
    checkNotNull(key);
    acquireLockIfNecessary();
    try {
      if (getKind() != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Not allowed if current node is not an object key node!");
      checkAccessAndCommit();

      ObjectKeyNode node = (ObjectKeyNode) nodeReadOnlyTrx.getCurrentNode();
      final BigInteger oldHash = node.computeHash();

      // Remove old keys from mapping.
      final NodeKind nodeKind = node.getKind();
      final int oldNameKey = node.getNameKey();
      final NamePage page = ((NamePage) pageTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(oldNameKey, nodeKind, pageTrx);

      // Create new key for mapping.
      final int newNameKey = pageTrx.createNameKey(key, node.getKind());

      // Set new keys for current node.
      node = pageTrx.prepareRecordForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setNameKey(newNameKey);

      // Adapt path summary.
      if (buildPathSummary) {
        pathSummaryWriter.adaptPathForChangedNode(node, new QNm(key), -1, -1, newNameKey, OPType.SETNAME);
      }

      // Set path node key.
      node.setPathNodeKey(buildPathSummary ? pathSummaryWriter.getNodeKey() : 0);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  private void adaptUpdateOperationsForUpdate(SirixDeweyID id, long nodeKey) {
    final var diffTuple = new DiffTuple(DiffFactory.DiffType.UPDATED,
                                        nodeKey,
                                        nodeKey,
                                        id == null ? null : new DiffDepth(id.getLevel(), id.getLevel()));
    if (id == null && updateOperationsUnordered.get(nodeKey) == null) {
      updateOperationsUnordered.put(nodeKey, diffTuple);
    } else if (hasNoUpdatingNodeWithGivenNodeKey(nodeKey)) {
      updateOperationsOrdered.put(id, diffTuple);
    }
  }

  private boolean hasNoUpdatingNodeWithGivenNodeKey(long nodeKey) {
    return updateOperationsOrdered.values()
                                  .stream()
                                  .filter(filterInsertedOrDeletedTuplesWithNodeKey(nodeKey))
                                  .findAny()
                                  .isEmpty();
  }

  private Predicate<DiffTuple> filterInsertedOrDeletedTuplesWithNodeKey(long nodeKey) {
    return currDiffTuple ->
        (currDiffTuple.getNewNodeKey() == nodeKey && currDiffTuple.getDiff() == DiffFactory.DiffType.INSERTED) || (
            currDiffTuple.getOldNodeKey() == nodeKey && currDiffTuple.getDiff() == DiffFactory.DiffType.DELETED);
  }

  @Override
  public JsonNodeTrx setStringValue(final String value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      if (getKind() != NodeKind.STRING_VALUE && getKind() != NodeKind.OBJECT_STRING_VALUE) {
        throw new SirixUsageException(
            "Not allowed if current node is not a string value and not an object string value node!");
      }

      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      // Remove old value from indexes.
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();
      final byte[] byteVal = getBytes(value);

      final AbstractStringNode node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(byteVal);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx setBooleanValue(final boolean value) {
    acquireLockIfNecessary();
    try {
      if (getKind() != NodeKind.BOOLEAN_VALUE && getKind() != NodeKind.OBJECT_BOOLEAN_VALUE) {
        throw new SirixUsageException("Not allowed if current node is not a boolean value node!");
      }

      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      final long pathNodeKey = moveToParent().trx().getPathNodeKey();
      moveTo(nodeKey);

      // Remove old value from indexes.
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractBooleanNode node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(value);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx setNumberValue(final Number value) {
    checkNotNull(value);
    acquireLockIfNecessary();
    try {
      if (getKind() != NodeKind.NUMBER_VALUE && getKind() != NodeKind.OBJECT_NUMBER_VALUE) {
        throw new SirixUsageException(
            "Not allowed if current node is not a number value and not an object number value node!");
      }
      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      final long pathNodeKey = moveToParent().trx().getPathNodeKey();
      moveTo(nodeKey);

      // Remove old value from indexes.
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractNumberNode node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(value);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx revertTo(final @Nonnegative int revision) {
    acquireLockIfNecessary();
    try {
      nodeReadOnlyTrx.assertNotClosed();
      resourceManager.assertAccess(revision);

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      resourceManager.closeNodePageWriteTransaction(getId());
      final PageTrx pageTrx = resourceManager.createPageTransaction(trxID, revision, revNumber - 1, Abort.NO, true);
      nodeReadOnlyTrx.setPageReadTransaction(null);
      nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
      resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

      nodeHashing = new JsonNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);

      // Reset node factory.
      nodeFactory = null;
      nodeFactory = new JsonNodeFactoryImpl(hashFunction, pageTrx);

      // New index instances.
      reInstantiateIndexes();

      // Reset modification counter.
      modificationCount = 0L;

      // Move to document root.
      moveToDocumentRoot();

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public void close() {
    acquireLockIfNecessary();
    try {
      if (!isClosed()) {
        // Make sure to commit all dirty data.
        if (modificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Release all state immediately.
        final long trxId = getId();
        nodeReadOnlyTrx.close();
        resourceManager.closeWriteTransaction(trxId);
        removeCommitFile();

        pathSummaryWriter = null;
        nodeFactory = null;

        // Shutdown pool.
        threadPool.shutdown();
        try {
          threadPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          throw new SirixThreadedException(e);
        }
      }
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx rollback() {
    acquireLockIfNecessary();
    try {
      nodeReadOnlyTrx.assertNotClosed();

      // Reset modification counter.
      modificationCount = 0L;

      // Close current page transaction.
      final long trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = pageTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

      final UberPage uberPage = pageTrx.rollback();

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      resourceManager.closeNodePageWriteTransaction(getId());
      nodeReadOnlyTrx.setPageReadTransaction(null);
      removeCommitFile();

      pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.YES, true);
      nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
      resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

      nodeFactory = null;
      nodeFactory = new JsonNodeFactoryImpl(hashFunction, pageTrx);

      reInstantiateIndexes();

      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  private void removeCommitFile() {
    try {
      final Path commitFile = resourceManager.getCommitFile();
      if (java.nio.file.Files.exists(commitFile))
        java.nio.file.Files.delete(resourceManager.getCommitFile());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public JsonNodeTrx commit() {
    return commit(null);
  }

  /**
   * Create new instances.
   *
   * @param trxID     transaction ID
   * @param revNumber revision number
   */
  void reInstantiate(final @Nonnegative long trxID, final @Nonnegative int revNumber) {
    // Reset page transaction to new uber page.
    resourceManager.closeNodePageWriteTransaction(getId());
    pageTrx = null;
    pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.NO, true);
    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
    resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

    nodeFactory = null;
    nodeFactory = new JsonNodeFactoryImpl(hashFunction, pageTrx);
    final boolean isBulkInsert = nodeHashing.isBulkInsert();
    nodeHashing = null;
    nodeHashing = new JsonNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);
    nodeHashing.setBulkInsert(isBulkInsert);

    updateOperationsUnordered.clear();
    updateOperationsOrdered.clear();

    reInstantiateIndexes();
  }

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (buildPathSummary) {
      pathSummaryWriter = null;
      pathSummaryWriter =
          new PathSummaryWriter<>(pageTrx, nodeReadOnlyTrx.getResourceManager(), nodeFactory, nodeReadOnlyTrx);
    }

    // Recreate index listeners.
    indexController.createIndexListeners(indexController.getIndexes().getIndexDefs(), this);
  }

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  private void checkAccessAndCommit() {
    nodeReadOnlyTrx.assertNotClosed();
    checkState();
    modificationCount++;
    intermediateCommitIfRequired();
  }

  // ////////////////////////////////////////////////////////////
  // insert operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for insert operations.
   *
   * @param structNode pointer of the new node to be inserted
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final StructNode structNode) {
    assert structNode != null;

    final StructNode parent = pageTrx.prepareRecordForModification(structNode.getParentKey(), PageKind.RECORDPAGE, -1);

    if (storeChildCount) {
      parent.incrementChildCount();
    }

    if (!structNode.hasLeftSibling()) {
      parent.setFirstChildKey(structNode.getNodeKey());
    }

    if (!structNode.hasRightSibling()) {
      parent.setLastChildKey(structNode.getNodeKey());
    }

    if (structNode.hasRightSibling()) {
      final StructNode rightSiblingNode =
          pageTrx.prepareRecordForModification(structNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
    }

    if (structNode.hasLeftSibling()) {
      final StructNode leftSiblingNode =
          pageTrx.prepareRecordForModification(structNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
      leftSiblingNode.setRightSiblingKey(structNode.getNodeKey());
    }
  }

  // ////////////////////////////////////////////////////////////
  // end of insert operation
  // ////////////////////////////////////////////////////////////

  // ////////////////////////////////////////////////////////////
  // remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for remove operations.
   *
   * @param oldNode pointer of the old node to be replaced
   * @throws SirixException if anything weird happens
   */
  private void adaptForRemove(final StructNode oldNode) {
    assert oldNode != null;

    // Adapt left sibling node if there is one.
    if (oldNode.hasLeftSibling()) {
      final StructNode leftSibling =
          pageTrx.prepareRecordForModification(oldNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
      leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      final StructNode rightSibling =
          pageTrx.prepareRecordForModification(oldNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
    }

    // Adapt parent, if node has left sibling now it is a first child, and right sibling will be a last child
    StructNode parent = pageTrx.prepareRecordForModification(oldNode.getParentKey(), PageKind.RECORDPAGE, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
    }
    if (!oldNode.hasRightSibling()) {
      parent.setLastChildKey(oldNode.getLeftSiblingKey());
    }

    if (storeChildCount) {
      parent.decrementChildCount();
    }

    // Remove non structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      // removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    pageTrx.removeRecord(oldNode.getNodeKey(), PageKind.RECORDPAGE, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Making an intermediate commit based on set attributes.
   *
   * @throws SirixException if commit fails
   */
  private void intermediateCommitIfRequired() {
    nodeReadOnlyTrx.assertNotClosed();
    if (maxNodeCount > 0 && modificationCount > maxNodeCount) {
      commit("autoCommit");
    }
  }

  /**
   * Get the current node.
   *
   * @return {@link Node} implementation
   */
  private ImmutableNode getCurrentNode() {
    return nodeReadOnlyTrx.getCurrentNode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("readTrx", nodeReadOnlyTrx.toString())
                      .add("hashKind", hashType)
                      .toString();
  }

  @Override
  protected JsonNodeReadOnlyTrx delegate() {
    return nodeReadOnlyTrx;
  }

  @Override
  public JsonNodeTrx addPreCommitHook(final PreCommitHook hook) {
    acquireLockIfNecessary();
    try {
      preCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx addPostCommitHook(final PostCommitHook hook) {
    acquireLockIfNecessary();
    try {
      postCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof JsonNodeTrxImpl wtx) {
      return Objects.equal(nodeReadOnlyTrx, wtx.nodeReadOnlyTrx);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeReadOnlyTrx);
  }

  @Override
  public PathSummaryReader getPathSummary() {
    acquireLockIfNecessary();
    try {
      return pathSummaryWriter.getPathSummary();
    } finally {
      unLockIfNecessary();
    }
  }

  @Override
  public JsonNodeTrx truncateTo(final int revision) {
    nodeReadOnlyTrx.assertNotClosed();

    // TODO

    return this;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    nodeReadOnlyTrx.assertNotClosed();

    return nodeReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public JsonNodeTrx commit(final String commitMessage) {
    nodeReadOnlyTrx.assertNotClosed();

    // Optionally lock while commiting and assigning new instances.
    acquireLockIfNecessary();
    try {
      state = State.Committing;

      // Execute pre-commit hooks.
      for (final PreCommitHook hook : preCommitHooks) {
        hook.preCommit(this);
      }

      // Reset modification counter.
      modificationCount = 0L;

      final UberPage uberPage = commitMessage == null ? pageTrx.commit() : pageTrx.commit(commitMessage);

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      if (resourceManager.getResourceConfig().storeDiffs()) {
        serializeUpdateDiffs();
      }

      // Reinstantiate everything.
      if (afterCommitState == AfterCommitState.KeepOpen) {
        reInstantiate(getId(), getRevisionNumber());
        state = State.Running;
      } else {
        state = State.Committed;
      }
    } finally {
      unLockIfNecessary();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }

    return this;
  }

  public void serializeUpdateDiffs() {
    final int revisionNumber = getRevisionNumber();
    if (!nodeHashing.isBulkInsert() && revisionNumber - 1 > 0) {
      final var diffSerializer = new JsonDiffSerializer((JsonResourceManager) resourceManager,
                                                        beforeBulkInsertionRevisionNumber != 0 && isAutoCommitting
                                                            ? beforeBulkInsertionRevisionNumber
                                                            : revisionNumber - 1,
                                                        revisionNumber,
                                                        storeDeweyIDs()
                                                            ? updateOperationsOrdered.values()
                                                            : updateOperationsUnordered.values());
      final var jsonDiff = diffSerializer.serialize(false);

      // Deserialize index definitions.
      final Path diff = resourceManager.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                       .resolve(
                                           "diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber + ".json");
      try {
        Files.createFile(diff);
        Files.writeString(diff, jsonDiff);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public PageTrx getPageWtx() {
    nodeReadOnlyTrx.assertNotClosed();
    return (PageTrx) nodeReadOnlyTrx.getPageTrx();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    nodeReadOnlyTrx.assertNotClosed();
    return getCurrentNode().getDeweyID();
  }

  @Override
  public JsonNodeTrx setBulkInsertion(boolean bulkInsertion) {
    nodeHashing.setBulkInsert(bulkInsertion);
    return this;
  }

  @Override
  public void adaptHashesInPostorderTraversal() {
    if (hashType != HashType.NONE) {
      final long nodeKey = getCurrentNode().getNodeKey();
      postOrderTraversalHashes();
      final ImmutableNode startNode = getCurrentNode();
      moveToParent();
      while (getCurrentNode().hasParent()) {
        moveToParent();
        nodeHashing.addParentHash(startNode);
      }
      moveTo(nodeKey);
    }
  }

  private static final class JsonNodeTrxThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@Nonnull final Runnable runnable) {
      final var thread = new Thread(runnable, "JsonNodeTrxCommitThread");

      thread.setPriority(Thread.NORM_PRIORITY);
      thread.setDaemon(false);

      return thread;
    }
  }
}
