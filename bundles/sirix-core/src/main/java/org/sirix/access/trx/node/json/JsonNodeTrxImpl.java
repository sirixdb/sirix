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
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.HashType;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.InternalResourceManager.Abort;
import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.access.trx.node.xml.InsertPos;
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
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.*;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
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
  private PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx;

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
   * Flag to decide whether to store child count
   */
  private final boolean storeChildCount;

  private boolean canRemoveValue;

  /**
   * Constructor.
   *
   * @param resourceManager the resource manager instance this transaction is bound to
   * @param maxNodeCount    maximum number of node modifications before auto commit
   * @param timeUnit        unit of the number of the next param {@code pMaxTime}
   * @param maxTime         maximum number of seconds before auto commit
   * @param nodeHashing     hashes node contents
   * @throws SirixIOException    if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  @SuppressWarnings("unchecked")
  JsonNodeTrxImpl(final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final InternalJsonNodeReadOnlyTrx nodeReadTrx, final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter,
      final @Nonnegative int maxNodeCount, final TimeUnit timeUnit, final @Nonnegative int maxTime,
      final @Nonnull JsonNodeHashing nodeHashing, final JsonNodeFactory nodeFactory) {
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
    pageWriteTrx = (PageTrx<Long, DataRecord, UnorderedKeyValuePage>) nodeReadOnlyTrx.getPageTrx();
    storeChildCount = this.resourceManager.getResourceConfig().getStoreChildCount();

    this.nodeFactory = Preconditions.checkNotNull(nodeFactory);

    // Only auto commit by node modifications if it is more then 0.
    this.maxNodeCount = maxNodeCount;
    this.modificationCount = 0L;

    if (maxTime > 0) {
      threadPool.scheduleAtFixedRate(this::commit, maxTime, maxTime, timeUnit);
    }

    // Synchronize commit and other public methods if needed.
    lock = maxTime > 0 ? new ReentrantLock() : null;

    hashType = resourceManager.getResourceConfig().hashType;
    useTextCompression = resourceManager.getResourceConfig().useTextCompression;

    deweyIDManager = new JsonDeweyIDManager(this);

    updateOperationsOrdered = new TreeMap<>();
    updateOperationsUnordered = new HashMap<>();

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
  private void acquireLock() {
    if (lock != null) {
      lock.lock();
    }
  }

  /**
   * Release a lock if necessary.
   */
  private void unLock() {
    if (lock != null) {
      lock.unlock();
    }
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, true);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, boolean doImplicitCommit) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, doImplicitCommit);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, true);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, boolean doImplicitCommit) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, false);
  }

  private JsonNodeTrx insertSubtree(final JsonReader reader, final InsertPosition insertionPosition,
      boolean doImplicitCommit) {
    nodeReadOnlyTrx.assertNotClosed();
    checkNotNull(reader);
    assert insertionPosition != null;

    if (hashType != HashType.NONE && (maxNodeCount != 0 || lock != null)) {
      throw new IllegalStateException("Calling insertSubtree() with auto-commit and setting hashes is not allowed,"
                                          + " as the hashes are calculated after the insertion, which is not possible.");
    }

    acquireLock();
    try {
      final var peekedJsonToken = reader.peek();

      if (peekedJsonToken != JsonToken.BEGIN_OBJECT && peekedJsonToken != JsonToken.BEGIN_ARRAY)
        throw new SirixUsageException("JSON to insert must begin with an array or object.");

      final var nodeKind = getKind();
      var skipRootJsonToken = false;

      // $CASES-OMITTED$
      switch (insertionPosition) {
        case AS_FIRST_CHILD -> {
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
        case AS_RIGHT_SIBLING -> {
          final NodeKind parentKind = getParentKind();
          if (parentKind != NodeKind.ARRAY) {
            throw new IllegalStateException("Current parent node must an array.");
          }
        }
        default -> throw new UnsupportedOperationException();
      }

      checkAccessAndCommit();
      nodeHashing.setBulkInsert(true);
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
        case AS_RIGHT_SIBLING:
          moveToRightSibling();
          break;
        default:
          // May not happen.
      }

      adaptUpdateOperationsForInsert(getDeweyID(), getNodeKey());

      adaptHashesInPostorderTraversal();

      if (doImplicitCommit) {
        commit();
      }
      nodeHashing.setBulkInsert(false);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      unLock();
    }
    return this;
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
    acquireLock();
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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsRightSibling() {
    acquireLock();
    try {
      checkAccessAndCommit();

      if (getParentKind() != NodeKind.ARRAY) {
        throw new SirixUsageException("Insert is not allowed if parent node is not an array node!");
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsFirstChild(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLock();
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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      nodeReadOnlyTrx.setCurrentNode(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
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
    final ObjectKeyNode objectKeyNode =
        (ObjectKeyNode) pageWriteTrx.prepareEntryForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
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
  public JsonNodeTrx insertObjectRecordAsRightSibling(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    acquireLock();
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

      insertAsRightSibling(node);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsFirstChild() {
    acquireLock();
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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsRightSibling() {
    acquireLock();
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

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx replaceObjectRecordValue(String key, ObjectRecordValue<?> value) {
    checkNotNull(value);
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or an arry node!");

      checkAccessAndCommit();

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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
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

  private void adaptNodesAndHashesForInsertAsFirstChild(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    nodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node, InsertPos.ASFIRSTCHILD);
    nodeReadOnlyTrx.setCurrentNode(node);
    nodeHashing.adaptHashesWithAdd();
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final String value) {
    checkNotNull(value);
    acquireLock();
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

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsFirstChild(boolean value) {
    acquireLock();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key or array node!");

      checkAccessAndCommit();

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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsRightSibling(boolean value) {
    acquireLock();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final BooleanNode node = nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  private void checkPrecondition() {
    if (getParentKind() != NodeKind.ARRAY)
      throw new SirixUsageException("Insert is not allowed if parent node is not an array node!");
  }

  private void insertAsRightSibling(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    nodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node, InsertPos.ASRIGHTSIBLING);
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

    moveTo(node.getNodeKey());

    nodeReadOnlyTrx.setCurrentNode(node);

    indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);
  }

  @Override
  public JsonNodeTrx insertNumberValueAsFirstChild(Number value) {
    checkNotNull(value);
    acquireLock();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      checkAccessAndCommit();

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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsRightSibling(Number value) {
    acquireLock();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getParentKey();
      final long leftSibKey = structNode.getNodeKey();
      final long rightSibKey = structNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final NumberNode node = nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value, id);

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsFirstChild() {
    acquireLock();
    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      checkAccessAndCommit();

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

      adaptNodesAndHashesForInsertAsFirstChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsRightSibling() {
    acquireLock();
    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final NullNode node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);

      insertAsRightSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      return this;
    } finally {
      unLock();
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
    acquireLock();
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
        pageWriteTrx.removeEntry(currentNode.getNodeKey(), PageKind.RECORDPAGE, -1);
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
      unLock();
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
    if (getCurrentNode() instanceof ImmutableNameNode) {
      final ImmutableNameNode node = ((ImmutableNameNode) getCurrentNode());
      indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
      final NodeKind nodeKind = node.getKind();
      final NamePage page = ((NamePage) pageWriteTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(node.getLocalNameKey(), nodeKind, pageWriteTrx);

      assert nodeKind != NodeKind.JSON_DOCUMENT;
      if (buildPathSummary) {
        pathSummaryWriter.remove(node, nodeKind, page);
      }
    }
  }

  @Override
  public JsonNodeTrx setObjectKeyName(final String key) {
    checkNotNull(key);
    acquireLock();
    try {
      if (getKind() != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Not allowed if current node is not an object key node!");
      checkAccessAndCommit();

      ObjectKeyNode node = (ObjectKeyNode) nodeReadOnlyTrx.getCurrentNode();
      final BigInteger oldHash = node.computeHash();

      // Remove old keys from mapping.
      final NodeKind nodeKind = node.getKind();
      final int oldNameKey = node.getNameKey();
      final NamePage page = ((NamePage) pageWriteTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(oldNameKey, nodeKind, pageWriteTrx);

      // Create new key for mapping.
      final int newNameKey = pageWriteTrx.createNameKey(key, node.getKind());

      // Set new keys for current node.
      node = (ObjectKeyNode) pageWriteTrx.prepareEntryForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
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
      unLock();
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
    acquireLock();
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
          (AbstractStringNode) pageWriteTrx.prepareEntryForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                        PageKind.RECORDPAGE,
                                                                        -1);
      node.setValue(byteVal);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx setBooleanValue(final boolean value) {
    acquireLock();
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
          (AbstractBooleanNode) pageWriteTrx.prepareEntryForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE,
                                                                         -1);
      node.setValue(value);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx setNumberValue(final Number value) {
    checkNotNull(value);
    acquireLock();
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
          (AbstractNumberNode) pageWriteTrx.prepareEntryForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                        PageKind.RECORDPAGE,
                                                                        -1);
      node.setValue(value);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx revertTo(final @Nonnegative int revision) {
    acquireLock();
    try {
      nodeReadOnlyTrx.assertNotClosed();
      resourceManager.assertAccess(revision);

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      resourceManager.closeNodePageWriteTransaction(getId());
      final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageTrx =
          resourceManager.createPageTransaction(trxID, revision, revNumber - 1, Abort.NO, true);
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
      unLock();
    }
  }

  @Override
  public void close() {
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public JsonNodeTrx rollback() {
    acquireLock();
    try {
      nodeReadOnlyTrx.assertNotClosed();

      // Reset modification counter.
      modificationCount = 0L;

      // Close current page transaction.
      final long trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = pageWriteTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

      final UberPage uberPage = pageWriteTrx.rollback();

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      resourceManager.closeNodePageWriteTransaction(getId());
      nodeReadOnlyTrx.setPageReadTransaction(null);
      removeCommitFile();

      pageWriteTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.YES, true);
      nodeReadOnlyTrx.setPageReadTransaction(pageWriteTrx);
      resourceManager.setNodePageWriteTransaction(getId(), pageWriteTrx);

      nodeFactory = null;
      nodeFactory = new JsonNodeFactoryImpl(hashFunction, pageWriteTrx);

      reInstantiateIndexes();

      return this;
    } finally {
      unLock();
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
    pageWriteTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.NO, true);
    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(pageWriteTrx);
    resourceManager.setNodePageWriteTransaction(getId(), pageWriteTrx);

    nodeFactory = null;
    nodeFactory = new JsonNodeFactoryImpl(hashFunction, pageWriteTrx);
    nodeHashing = new JsonNodeHashing(hashType, nodeReadOnlyTrx, pageWriteTrx);

    updateOperationsUnordered.clear();
    updateOperationsOrdered.clear();

    reInstantiateIndexes();
  }

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (buildPathSummary) {
      pathSummaryWriter = null;
      pathSummaryWriter =
          new PathSummaryWriter<>(pageWriteTrx, nodeReadOnlyTrx.getResourceManager(), nodeFactory, nodeReadOnlyTrx);
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
   * @param insertPos  determines the position where to insert
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final StructNode structNode, final InsertPos insertPos) {
    assert structNode != null;
    assert insertPos != null;

    final StructNode parent =
        (StructNode) pageWriteTrx.prepareEntryForModification(structNode.getParentKey(), PageKind.RECORDPAGE, -1);

    if (storeChildCount) {
      parent.incrementChildCount();
    }

    if (!structNode.hasLeftSibling()) {
      parent.setFirstChildKey(structNode.getNodeKey());
    }

    if (structNode.hasRightSibling()) {
      final StructNode rightSiblingNode =
          (StructNode) pageWriteTrx.prepareEntryForModification(structNode.getRightSiblingKey(),
                                                                PageKind.RECORDPAGE,
                                                                -1);
      rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
    }
    if (structNode.hasLeftSibling()) {
      final StructNode leftSiblingNode =
          (StructNode) pageWriteTrx.prepareEntryForModification(structNode.getLeftSiblingKey(),
                                                                PageKind.RECORDPAGE,
                                                                -1);
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
          (StructNode) pageWriteTrx.prepareEntryForModification(oldNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
      leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      final StructNode rightSibling =
          (StructNode) pageWriteTrx.prepareEntryForModification(oldNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
    }

    // Adapt parent, if node has now left sibling it is a first child.
    StructNode parent =
        (StructNode) pageWriteTrx.prepareEntryForModification(oldNode.getParentKey(), PageKind.RECORDPAGE, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
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
    pageWriteTrx.removeEntry(oldNode.getNodeKey(), PageKind.RECORDPAGE, -1);
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
    if ((maxNodeCount > 0) && (modificationCount > maxNodeCount)) {
      commit();
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
    acquireLock();
    try {
      preCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx addPostCommitHook(final PostCommitHook hook) {
    acquireLock();
    try {
      postCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof JsonNodeTrxImpl) {
      final JsonNodeTrxImpl wtx = (JsonNodeTrxImpl) obj;
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
    acquireLock();
    try {
      return pathSummaryWriter.getPathSummary();
    } finally {
      unLock();
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
    acquireLock();
    try {
      // Execute pre-commit hooks.
      for (final PreCommitHook hook : preCommitHooks) {
        hook.preCommit(this);
      }

      // Reset modification counter.
      modificationCount = 0L;

      final UberPage uberPage = commitMessage == null ? pageWriteTrx.commit() : pageWriteTrx.commit(commitMessage);

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      serializeUpdateDiffs();

      // Reinstantiate everything.
      reInstantiate(getId(), getRevisionNumber());
    } finally {
      unLock();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : postCommitHooks) {
      hook.postCommit(this);
    }

    return this;
  }

  public void serializeUpdateDiffs() {
    final int revisionNumber = getRevisionNumber();
    if (revisionNumber - 1 > 0) {
      final var diffSerializer = new JsonDiffSerializer((JsonResourceManager) resourceManager,
                                                        revisionNumber - 1,
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

  @SuppressWarnings("unchecked")
  @Override
  public PageTrx<Long, DataRecord, UnorderedKeyValuePage> getPageWtx() {
    nodeReadOnlyTrx.assertNotClosed();
    return (PageTrx<Long, DataRecord, UnorderedKeyValuePage>) nodeReadOnlyTrx.getPageTrx();
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
