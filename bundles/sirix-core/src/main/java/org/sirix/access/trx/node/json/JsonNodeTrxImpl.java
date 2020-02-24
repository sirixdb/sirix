/**
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
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
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.*;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.AbstractBooleanNode;
import org.sirix.node.json.AbstractNullNode;
import org.sirix.node.json.AbstractNumberNode;
import org.sirix.node.json.AbstractStringNode;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.json.StringNode;
import org.sirix.node.xml.ElementNode;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/**
 * <h1>JsonNodeTrxImpl</h1>
 *
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
final class JsonNodeTrxImpl extends AbstractForwardingJsonNodeReadOnlyTrx implements JsonNodeTrx {

  /** Prime for computing the hash. */
  private static final BigInteger PRIME = BigInteger.valueOf(77081);

  /** Maximum number of node modifications before auto commit. */
  private final int mMaxNodeCount;

  /** Modification counter. */
  long mModificationCount;

  /** Hash kind of Structure. */
  private final HashType mHashKind;

  /** Scheduled executor service. */
  private final ScheduledExecutorService mPool = Executors.newScheduledThreadPool(1, new JsonNodeTrxThreadFactory());

  /** {@link InternalJsonNodeReadOnlyTrx} reference. */
  final InternalJsonNodeReadOnlyTrx mNodeReadOnlyTrx;

  /** Determines if a bulk insert operation is done. */
  private boolean mBulkInsert;

  /** {@link PathSummaryWriter} instance. */
  private PathSummaryWriter<JsonNodeReadOnlyTrx> mPathSummaryWriter;

  /**
   * Determines if a path summary should be built and kept up-to-date or not.
   */
  private final boolean mBuildPathSummary;

  /** {@link JsonNodeFactory} to be able to create nodes. */
  private JsonNodeFactory mNodeFactory;

  /** An optional lock for all methods, if an automatic commit is issued. */
  private final Lock mLock;

  /** Determines if text values should be compressed or not. */
  private final boolean mCompression;

  /**
   * The {@link JsonIndexController} used within the session this {@link JsonNodeTrx} is bound to.
   */
  private final JsonIndexController mIndexController;

  /** The resource manager. */
  private final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> mResourceManager;

  /** The page write trx. */
  private PageTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /** Collection holding pre-commit hooks. */
  private final List<PreCommitHook> mPreCommitHooks = new ArrayList<>();

  /** Collection holding post-commit hooks. */
  private final List<PostCommitHook> mPostCommitHooks = new ArrayList<>();

  /** The hash function to use to hash node contents. */
  private final HashFunction mHashFunction;

  /**
   * Constructor.
   *
   * @param transactionID ID of transaction
   * @param resourceManager the resource manager instance this transaction is bound to
   * @param maxNodeCount maximum number of node modifications before auto commit
   * @param timeUnit unit of the number of the next param {@code pMaxTime}
   * @param maxTime maximum number of seconds before auto commit
   * @throws SirixIOException if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  @SuppressWarnings("unchecked")
  JsonNodeTrxImpl(final @Nonnegative long transactionID,
      final InternalResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final InternalJsonNodeReadOnlyTrx nodeReadTrx, final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter,
      final @Nonnegative int maxNodeCount, final TimeUnit timeUnit, final @Nonnegative int maxTime,
      final @Nonnull Node documentNode, final JsonNodeFactory nodeFactory) {
    // Do not accept negative values.
    Preconditions.checkArgument(maxNodeCount >= 0 && maxTime >= 0,
                                "Negative arguments for maxNodeCount and maxTime are not accepted.");

    mHashFunction = resourceManager.getResourceConfig().nodeHashFunction;
    mResourceManager = Preconditions.checkNotNull(resourceManager);
    mNodeReadOnlyTrx = Preconditions.checkNotNull(nodeReadTrx);
    mBuildPathSummary = resourceManager.getResourceConfig().withPathSummary;
    mPathSummaryWriter = pathSummaryWriter;

    mIndexController = resourceManager.getWtxIndexController(mNodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    mPageWriteTrx = (PageTrx<Long, Record, UnorderedKeyValuePage>) mNodeReadOnlyTrx.getPageTrx();

    mNodeFactory = Preconditions.checkNotNull(nodeFactory);

    // Only auto commit by node modifications if it is more then 0.
    mMaxNodeCount = maxNodeCount;
    mModificationCount = 0L;

    if (maxTime > 0) {
      mPool.scheduleAtFixedRate(this::commit, maxTime, maxTime, timeUnit);
    }

    // Synchronize commit and other public methods if needed.
    mLock = maxTime > 0 ? new ReentrantLock() : null;

    mHashKind = resourceManager.getResourceConfig().hashType;
    mCompression = resourceManager.getResourceConfig().useTextCompression;

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
    return mResourceManager.getUser();
  }

  @Override
  public Optional<User> getUserOfRevisionToRepresent() {
    return mNodeReadOnlyTrx.getUser();
  }

  /** Acquire a lock if necessary. */
  private void acquireLock() {
    if (mLock != null) {
      mLock.lock();
    }
  }

  /** Release a lock if necessary. */
  private void unLock() {
    if (mLock != null) {
      mLock.unlock();
    }
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING);
  }

  private JsonNodeTrx insertSubtree(final JsonReader reader, final InsertPosition insertionPosition) {
    mNodeReadOnlyTrx.assertNotClosed();
    checkNotNull(reader);
    assert insertionPosition != null;

    if (mHashKind != HashType.NONE && (mMaxNodeCount != 0 || mLock != null)) {
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

      switch (insertionPosition) {
        case AS_FIRST_CHILD:
          if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT) {
            throw new IllegalStateException(
                "Current node must either be the document root, an array or an object key.");
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

              if (nodeKind == NodeKind.ARRAY)
                skipRootJsonToken = true;

              break;
            // $CASES-OMITTED$
            default:
          }
          break;
        case AS_RIGHT_SIBLING:
          final NodeKind parentKind = getParentKind();
          if (parentKind != NodeKind.ARRAY) {
            throw new IllegalStateException("Current parent node must an array.");
          }
          break;
        // $CASES-OMITTED$
        default:
          throw new UnsupportedOperationException();
      }

      checkAccessAndCommit();
      mBulkInsert = true;
      var nodeKey = getCurrentNode().getNodeKey();
      final var shredderBuilder = new JsonShredder.Builder(this, reader, insertionPosition);

      if (skipRootJsonToken)
        shredderBuilder.skipRootJsonToken();

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
        case AS_LEFT_SIBLING:
          moveToLeftSibling();
          break;
        default:
          // May not happen.
      }

      if (mHashKind != HashType.NONE) {
        nodeKey = getCurrentNode().getNodeKey();
        postOrderTraversalHashes();
        final ImmutableNode startNode = getCurrentNode();
        moveToParent();
        while (getCurrentNode().hasParent()) {
          moveToParent();
          addParentHash(startNode);
        }
        moveTo(nodeKey);
      }

      commit();
      mBulkInsert = false;
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
    new PostOrderAxis(this, IncludeSelf.YES).forEach((unused) -> addHashAndDescendantCount());
  }

  /**
   * Add a hash.
   *
   * @param startNode start node
   */
  private void addParentHash(final ImmutableNode startNode) {
    switch (mHashKind) {
      case ROLLING:
        final BigInteger hashToAdd = startNode.computeHash();
        final Node node = (Node) mPageWriteTrx.prepareEntryForModification(
            mNodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setHash(node.getHash().add(hashToAdd.multiply(PRIME)));
        if (startNode instanceof StructNode) {
          ((StructNode) node).setDescendantCount(
              ((StructNode) node).getDescendantCount() + ((StructNode) startNode).getDescendantCount() + 1);
        }
        break;
      case POSTORDER:
        break;
      case NONE:
      default:
    }
  }

  /** Add a hash and the descendant count. */
  private void addHashAndDescendantCount() {
    switch (mHashKind) {
      case ROLLING:
        // Setup.
        final ImmutableJsonNode startNode = getCurrentNode();
        final long oldDescendantCount = mNodeReadOnlyTrx.getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;

        // Set start node.
        final BigInteger hashToAdd = startNode.computeHash();
        Node node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                     PageKind.RECORDPAGE, -1);
        node.setHash(hashToAdd);

        // Set parent node.
        if (startNode.hasParent()) {
          moveToParent();
          node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                  PageKind.RECORDPAGE, -1);
          final BigInteger hash = node.getHash() == null || BigInteger.ZERO.equals(node.getHash())
              ? node.computeHash()
              : node.getHash();
          node.setHash(hash.add(hashToAdd.multiply(PRIME)));

          setAddDescendants(startNode, node, descendantCount);
        }

        mNodeReadOnlyTrx.setCurrentNode(startNode);
        break;
      case POSTORDER:
        postorderAdd();
        break;
      case NONE:
      default:
    }
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

      final StructNode structNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey =
          kind == NodeKind.OBJECT_KEY ? Fixed.NULL_NODE_KEY.getStandardProperty() : structNode.getFirstChildKey();

      final ObjectNode node = mNodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey);

      adaptNodesAndHashesForInsertAsFirstChild(node);

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

      final StructNode currentNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final ObjectNode node = mNodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey);

      insertAsRightSibling(node);

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

      final StructNode structNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(structNode.getNodeKey(), key, NodeKind.OBJECT_KEY);

      final ObjectKeyNode node = mNodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey,
                                                                      key, Fixed.NULL_NODE_KEY.getStandardProperty());

      adaptNodesAndHashesForInsertAsFirstChild(node);

      mNodeReadOnlyTrx.setCurrentNode(node);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

      return this;
    } finally {
      unLock();
    }
  }

  private void setFirstChildOfObjectKeyNode(final ObjectKeyNode node) {
    final ObjectKeyNode objectKeyNode = (ObjectKeyNode) mPageWriteTrx.prepareEntryForModification(node.getNodeKey(),
                                                                                                  PageKind.RECORDPAGE,
                                                                                                  -1);
    objectKeyNode.setFirstChildKey(getNodeKey());
  }

  private void insertValue(final ObjectRecordValue<?> value) throws AssertionError {
    final NodeKind valueKind = value.getKind();

    switch (valueKind) {
      case OBJECT:
        insertObjectAsFirstChild();
        break;
      case ARRAY:
        insertArrayAsFirstChild();
        break;
      case STRING_VALUE:
        insertStringValueAsFirstChild((String) value.getValue());
        break;
      case BOOLEAN_VALUE:
        insertBooleanValueAsFirstChild((Boolean) value.getValue());
        break;
      case NUMBER_VALUE:
        insertNumberValueAsFirstChild((Number) value.getValue());
        break;
      case NULL_VALUE:
        insertNullValueAsFirstChild();
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError("Type not known.");
    }
  }

  private long getPathNodeKey(final long nodeKey, final String name, final NodeKind kind) {
    moveToParentObjectKeyArrayOrDocumentRoot();

    final long pathNodeKey = mBuildPathSummary ? mPathSummaryWriter.getPathNodeKey(new QNm(name), kind) : 0;

    mNodeReadOnlyTrx.moveTo(nodeKey);

    return pathNodeKey;
  }

  private void moveToParentObjectKeyArrayOrDocumentRoot() {
    while (mNodeReadOnlyTrx.getKind() != NodeKind.OBJECT_KEY && mNodeReadOnlyTrx.getKind() != NodeKind.ARRAY
        && mNodeReadOnlyTrx.getKind() != NodeKind.JSON_DOCUMENT) {
      mNodeReadOnlyTrx.moveToParent();
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

      final StructNode currentNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), key, NodeKind.OBJECT_KEY);
      moveTo(currentNode.getNodeKey());

      final ObjectKeyNode node = mNodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey,
                                                                      key, -1);

      insertAsRightSibling(node);

      insertValue(value);

      setFirstChildOfObjectKeyNode(node);

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

      final StructNode currentNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = currentNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "array", NodeKind.ARRAY);

      final ArrayNode node = mNodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey);

      adaptNodesAndHashesForInsertAsFirstChild(node);

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
      final NodeKind kind = getKind();
      checkPrecondition(kind);

      final StructNode currentNode = (StructNode) getCurrentNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode.getNodeKey(), "array", NodeKind.ARRAY);
      moveTo(currentNode.getNodeKey());

      final ArrayNode node = mNodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey);

      insertAsRightSibling(node);

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

      final StructNode structNode = mNodeReadOnlyTrx.getStructuralNode();

      final long pathNodeKey = structNode.getNodeKey();
      final long parentKey = structNode.getNodeKey();

      final byte[] textValue = getBytes(value);

      final AbstractStringNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        node = mNodeFactory.createJsonObjectStringNode(parentKey, textValue, mCompression);
      } else {
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = structNode.getFirstChildKey();
        node = mNodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue, mCompression);
      }

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      return this;
    } finally {
      unLock();
    }
  }

  private void adaptNodesAndHashesForInsertAsFirstChild(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    mNodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node, InsertPos.ASFIRSTCHILD);
    mNodeReadOnlyTrx.setCurrentNode(node);
    adaptHashesWithAdd();
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      checkAccessAndCommit();
      final NodeKind kind = getKind();
      checkPrecondition(kind);

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final byte[] textValue = getBytes(value);

      final StringNode node = mNodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, textValue,
                                                                mCompression);

      insertAsRightSibling(node);

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

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long pathNodeKey = currentNode.getNodeKey();
      final long parentKey = currentNode.getNodeKey();

      final AbstractBooleanNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        node = mNodeFactory.createJsonObjectBooleanNode(parentKey, value);
      } else {
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = currentNode.getFirstChildKey();
        node = mNodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value);
      }

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

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
      final NodeKind kind = getKind();
      checkPrecondition(kind);

      final StructNode currentNode = (StructNode) getCurrentNode();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final BooleanNode node = mNodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, value);

      insertAsRightSibling(node);

      return this;
    } finally {
      unLock();
    }
  }

  private void checkPrecondition(NodeKind kind) {
    if (kind == NodeKind.JSON_DOCUMENT || getParentKind() == NodeKind.OBJECT_KEY)
      throw new SirixUsageException(
          "Insert is not allowed if current node is the document-, an object- or parent is an object key node!");
  }

  private void insertAsRightSibling(final ImmutableJsonNode node) {
    // Adapt local nodes and hashes.
    mNodeReadOnlyTrx.setCurrentNode(node);
    adaptForInsert((StructNode) node, InsertPos.ASRIGHTSIBLING);
    mNodeReadOnlyTrx.setCurrentNode(node);
    adaptHashesWithAdd();

    // Get the path node key.
    moveToParentObjectKeyArrayOrDocumentRoot();
    final long pathNodeKey = isObjectKey() ? ((ImmutableObjectKeyNode) getNode()).getPathNodeKey() : -1;
    moveTo(node.getNodeKey());

    mNodeReadOnlyTrx.setCurrentNode(node);

    // Index text value.
    mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);
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
      final long pathNodeKey = currentNode.getNodeKey();
      final long parentKey = currentNode.getNodeKey();

      final AbstractNumberNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        node = mNodeFactory.createJsonObjectNumberNode(parentKey, value);
      } else {
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = currentNode.getFirstChildKey();
        node = mNodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value);
      }

      adaptNodesAndHashesForInsertAsFirstChild(node);

      // Index text value.
      mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

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
      final NodeKind kind = getKind();
      checkPrecondition(kind);

      final StructNode structNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getParentKey();
      final long leftSibKey = structNode.getNodeKey();
      final long rightSibKey = structNode.getRightSiblingKey();

      final NumberNode node = mNodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, value);

      insertAsRightSibling(node);

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

      final StructNode structNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();

      final AbstractNullNode node;
      if (kind == NodeKind.OBJECT_KEY) {
        node = mNodeFactory.createJsonObjectNullNode(parentKey);
      } else {
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = structNode.getRightSiblingKey();
        node = mNodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey);
      }

      adaptNodesAndHashesForInsertAsFirstChild(node);

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
      final NodeKind kind = getKind();
      checkPrecondition(kind);

      final StructNode currentNode = mNodeReadOnlyTrx.getStructuralNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final NullNode node = mNodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey);

      insertAsRightSibling(node);

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

      final var parentNodeKind = getParentKind();

      if (parentNodeKind != NodeKind.JSON_DOCUMENT && parentNodeKind != NodeKind.OBJECT
          && parentNodeKind != NodeKind.ARRAY)
        throw new SirixUsageException(
            "An object record value can not be removed, you have to remove the whole object record (parent of this value).");

      // Remove subtree.
      for (final var axis = new PostOrderAxis(this); axis.hasNext(); ) {
        axis.next();

        final var currentNode = axis.getCursor().getNode();

        // Remove name.
        removeName();

        // Remove text value.
        removeValue();

        // Then remove node.
        mPageWriteTrx.removeEntry(currentNode.getNodeKey(), PageKind.RECORDPAGE, -1);
      }

      // Remove the name of subtree-root.
      if (node.getKind() == NodeKind.OBJECT_KEY) {
        removeName();
      } else {
        removeValue();
      }

      // Adapt hashes and neighbour nodes as well as the name from the
      // NamePage mapping if it's not a text node.
      final ImmutableJsonNode jsonNode = (ImmutableJsonNode) node;
      mNodeReadOnlyTrx.setCurrentNode(jsonNode);
      adaptHashesWithRemove();
      adaptForRemove(node);
      mNodeReadOnlyTrx.setCurrentNode(jsonNode);

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
      mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);
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
      mIndexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
      final NodeKind nodeKind = node.getKind();
      final NamePage page = ((NamePage) mPageWriteTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(node.getLocalNameKey(), nodeKind, mPageWriteTrx);

      assert nodeKind != NodeKind.JSON_DOCUMENT;
      if (mBuildPathSummary) {
        mPathSummaryWriter.remove(node, nodeKind, page);
      }
    }
  }

  @Override
  public JsonNodeTrx setObjectKeyName(final String name) {
    checkNotNull(name);
    acquireLock();
    try {
      if (getKind() != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Not allowed if current node is not an object key node!");
      checkAccessAndCommit();

      ObjectKeyNode node = (ObjectKeyNode) mNodeReadOnlyTrx.getCurrentNode();
      final BigInteger oldHash = node.computeHash();

      // Remove old keys from mapping.
      final NodeKind nodeKind = node.getKind();
      final int oldNameKey = node.getNameKey();
      final NamePage page = ((NamePage) mPageWriteTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(oldNameKey, nodeKind, mPageWriteTrx);

      // Create new key for mapping.
      final int newNameKey = mPageWriteTrx.createNameKey(name, node.getKind());

      // Set new keys for current node.
      node = (ObjectKeyNode) mPageWriteTrx.prepareEntryForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setNameKey(newNameKey);

      // Adapt path summary.
      if (mBuildPathSummary) {
        mPathSummaryWriter.adaptPathForChangedNode(node, new QNm(name), -1, -1, newNameKey, OPType.SETNAME);
      }

      // Set path node key.
      node.setPathNodeKey(mBuildPathSummary ? mPathSummaryWriter.getNodeKey() : 0);

      mNodeReadOnlyTrx.setCurrentNode(node);
      adaptHashedWithUpdate(oldHash);

      return this;
    } finally {
      unLock();
    }
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
      mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = mNodeReadOnlyTrx.getCurrentNode().computeHash();
      final byte[] byteVal = getBytes(value);

      final AbstractStringNode node = (AbstractStringNode) mPageWriteTrx.prepareEntryForModification(
          mNodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(byteVal);

      mNodeReadOnlyTrx.setCurrentNode(node);
      adaptHashedWithUpdate(oldHash);

      // Index new value.
      mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

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
      mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = mNodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractBooleanNode node = (AbstractBooleanNode) mPageWriteTrx.prepareEntryForModification(
          mNodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(value);

      mNodeReadOnlyTrx.setCurrentNode(node);
      adaptHashedWithUpdate(oldHash);

      // Index new value.
      mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

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
      mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = mNodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractNumberNode node = (AbstractNumberNode) mPageWriteTrx.prepareEntryForModification(
          mNodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setValue(value);

      mNodeReadOnlyTrx.setCurrentNode(node);
      adaptHashedWithUpdate(oldHash);

      // Index new value.
      mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx revertTo(final @Nonnegative int revision) {
    acquireLock();
    try {
      mNodeReadOnlyTrx.assertNotClosed();
      mResourceManager.assertAccess(revision);

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      mResourceManager.closeNodePageWriteTransaction(getId());
      final PageTrx<Long, Record, UnorderedKeyValuePage> trx = mResourceManager.createPageTransaction(trxID, revision,
                                                                                                      revNumber - 1,
                                                                                                      Abort.NO, true);
      mNodeReadOnlyTrx.setPageReadTransaction(null);
      mNodeReadOnlyTrx.setPageReadTransaction(trx);
      mResourceManager.setNodePageWriteTransaction(getId(), trx);

      // Reset node factory.
      mNodeFactory = null;
      mNodeFactory = new JsonNodeFactoryImpl(mHashFunction, trx);

      // New index instances.
      reInstantiateIndexes();

      // Reset modification counter.
      mModificationCount = 0L;

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
        if (mModificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Release all state immediately.
        final long trxId = getId();
        mNodeReadOnlyTrx.close();
        mResourceManager.closeWriteTransaction(trxId);
        removeCommitFile();

        mPathSummaryWriter = null;
        mNodeFactory = null;

        // Shutdown pool.
        mPool.shutdown();
        try {
          mPool.awaitTermination(2, TimeUnit.SECONDS);
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
      mNodeReadOnlyTrx.assertNotClosed();

      // Reset modification counter.
      mModificationCount = 0L;

      // Close current page transaction.
      final long trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = mPageWriteTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

      final UberPage uberPage = mPageWriteTrx.rollback();

      // Remember succesfully committed uber page in resource manager.
      mResourceManager.setLastCommittedUberPage(uberPage);

      mResourceManager.closeNodePageWriteTransaction(getId());
      mNodeReadOnlyTrx.setPageReadTransaction(null);
      removeCommitFile();

      mPageWriteTrx = mResourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.YES, true);
      mNodeReadOnlyTrx.setPageReadTransaction(mPageWriteTrx);
      mResourceManager.setNodePageWriteTransaction(getId(), mPageWriteTrx);

      mNodeFactory = null;
      mNodeFactory = new JsonNodeFactoryImpl(mHashFunction, mPageWriteTrx);

      reInstantiateIndexes();

      return this;
    } finally {
      unLock();
    }
  }

  private void removeCommitFile() {
    try {
      final Path commitFile = mResourceManager.getCommitFile();
      if (java.nio.file.Files.exists(commitFile))
        java.nio.file.Files.delete(mResourceManager.getCommitFile());
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
   * @param trxID transaction ID
   * @param revNumber revision number
   */
  void reInstantiate(final @Nonnegative long trxID, final @Nonnegative int revNumber) {
    // Reset page transaction to new uber page.
    mResourceManager.closeNodePageWriteTransaction(getId());
    mPageWriteTrx = mResourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.NO, true);
    mNodeReadOnlyTrx.setPageReadTransaction(null);
    mNodeReadOnlyTrx.setPageReadTransaction(mPageWriteTrx);
    mResourceManager.setNodePageWriteTransaction(getId(), mPageWriteTrx);

    mNodeFactory = null;
    mNodeFactory = new JsonNodeFactoryImpl(mHashFunction, mPageWriteTrx);

    reInstantiateIndexes();
  }

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (mBuildPathSummary) {
      mPathSummaryWriter = null;
      mPathSummaryWriter = new PathSummaryWriter<>(mPageWriteTrx, mNodeReadOnlyTrx.getResourceManager(), mNodeFactory,
                                                   mNodeReadOnlyTrx);
    }

    // Recreate index listeners.
    mIndexController.createIndexListeners(mIndexController.getIndexes().getIndexDefs(), this);
  }

  // /**
  // * Modifying hashes in a postorder-traversal.
  // *
  // * @throws SirixIOException if an I/O error occurs
  // */
  // private void postOrderTraversalHashes() {
  // new PostOrderAxis(this, IncludeSelf.YES).forEach((unused) -> {
  // addHashAndDescendantCount();
  // });
  // }
  //
  // /**
  // * Add a hash.
  // *
  // * @param startNode start node
  // */
  // private void addParentHash(final ImmutableNode startNode) {
  // switch (mHashKind) {
  // case ROLLING:
  // final long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
  // final Node node =
  // (Node)
  // mPageWriteTrx.prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
  // PageKind.RECORDPAGE, -1);
  // node.setHash(node.getHash() + hashToAdd * PRIME);
  // if (startNode instanceof StructNode) {
  // ((StructNode) node).setDescendantCount(
  // ((StructNode) node).getDescendantCount() + ((StructNode) startNode).getDescendantCount() + 1);
  // }
  // break;
  // case POSTORDER:
  // break;
  // case NONE:
  // default:
  // }
  // }
  //
  // /** Add a hash and the descendant count. */
  // private void addHashAndDescendantCount() {
  // switch (mHashKind) {
  // case ROLLING:
  // // Setup.
  // final ImmutableNode startNode = getCurrentNode();
  // final long oldDescendantCount = mNodeReadTrx.getStructuralNode().getDescendantCount();
  // final long descendantCount = oldDescendantCount == 0
  // ? 1
  // : oldDescendantCount + 1;
  //
  // // Set start node.
  // final long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
  // Node node = (Node)
  // mPageWriteTrx.prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
  // PageKind.RECORDPAGE, -1);
  // node.setHash(hashToAdd);
  //
  // // Set parent node.
  // if (startNode.hasParent()) {
  // moveToParent();
  // node = (Node)
  // mPageWriteTrx.prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
  // PageKind.RECORDPAGE, -1);
  // node.setHash(node.getHash() + hashToAdd * PRIME);
  // setAddDescendants(startNode, node, descendantCount);
  // }
  //
  // mNodeReadTrx.setCurrentNode(startNode);
  // break;
  // case POSTORDER:
  // postorderAdd();
  // break;
  // case NONE:
  // default:
  // }
  // }

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  private void checkAccessAndCommit() {
    mNodeReadOnlyTrx.assertNotClosed();
    mModificationCount++;
    intermediateCommitIfRequired();
  }

  // ////////////////////////////////////////////////////////////
  // insert operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for insert operations.
   *
   * @param structNode pointer of the new node to be inserted
   * @param insertPos determines the position where to insert
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final StructNode structNode, final InsertPos insertPos) {
    assert structNode != null;
    assert insertPos != null;

    final StructNode parent = (StructNode) mPageWriteTrx.prepareEntryForModification(structNode.getParentKey(),
                                                                                     PageKind.RECORDPAGE, -1);
    parent.incrementChildCount();
    if (!structNode.hasLeftSibling()) {
      parent.setFirstChildKey(structNode.getNodeKey());
    }

    if (structNode.hasRightSibling()) {
      final StructNode rightSiblingNode = (StructNode) mPageWriteTrx.prepareEntryForModification(
          structNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
    }
    if (structNode.hasLeftSibling()) {
      final StructNode leftSiblingNode = (StructNode) mPageWriteTrx.prepareEntryForModification(
          structNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
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
      final StructNode leftSibling = (StructNode) mPageWriteTrx.prepareEntryForModification(oldNode.getLeftSiblingKey(),
                                                                                            PageKind.RECORDPAGE, -1);
      leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      final StructNode rightSibling = (StructNode) mPageWriteTrx.prepareEntryForModification(
          oldNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
    }

    // Adapt parent, if node has now left sibling it is a first child.
    StructNode parent = (StructNode) mPageWriteTrx.prepareEntryForModification(oldNode.getParentKey(),
                                                                               PageKind.RECORDPAGE, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
    }
    parent.decrementChildCount();

    // Remove non structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      // removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    mPageWriteTrx.removeEntry(oldNode.getNodeKey(), PageKind.RECORDPAGE, -1);
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
    mNodeReadOnlyTrx.assertNotClosed();
    if ((mMaxNodeCount > 0) && (mModificationCount > mMaxNodeCount)) {
      commit();
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashesWithAdd() {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingAdd();
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with remove.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashesWithRemove() {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingRemove();
          break;
        case POSTORDER:
          postorderRemove();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashedWithUpdate(final BigInteger oldHash) {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingUpdate(oldHash);
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Removal operation for postorder hash computation.
   *
   * @throws SirixIOException if anything weird happens
   */
  private void postorderRemove() {
    moveTo(getCurrentNode().getParentKey());
    postorderAdd();
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if anything weird happened
   */
  private void postorderAdd() {
    // start with hash to add
    final ImmutableJsonNode startNode = getCurrentNode();
    // long for adapting the hash of the parent
    BigInteger hashCodeForParent = BigInteger.ZERO;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE, -1);
      node.setHash(mNodeReadOnlyTrx.getCurrentNode().computeHash());
      moveTo(mNodeReadOnlyTrx.getCurrentNode().getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot = (StructNode) mPageWriteTrx.prepareEntryForModification(
          mNodeReadOnlyTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      hashCodeForParent = mNodeReadOnlyTrx.getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == NodeKind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          moveTo(currentElement.getAttributeKey(i));
          hashCodeForParent = mNodeReadOnlyTrx.getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          moveTo(currentElement.getNamespaceKey(i));
          hashCodeForParent = mNodeReadOnlyTrx.getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
        }
        moveTo(cursorToRoot.getNodeKey());
      }

      // Caring about the children of a node
      if (moveTo(mNodeReadOnlyTrx.getStructuralNode().getFirstChildKey()).hasMoved()) {
        do {
          hashCodeForParent = mNodeReadOnlyTrx.getCurrentNode().getHash().add(hashCodeForParent.multiply(PRIME));
        } while (moveTo(mNodeReadOnlyTrx.getStructuralNode().getRightSiblingKey()).hasMoved());
        moveTo(mNodeReadOnlyTrx.getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
      hashCodeForParent = BigInteger.ZERO;
    } while (moveTo(cursorToRoot.getParentKey()).hasMoved());

    mNodeReadOnlyTrx.setCurrentNode(startNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final BigInteger oldHash) {
    final ImmutableJsonNode newNode = getCurrentNode();
    final BigInteger hash = newNode.computeHash();
    BigInteger resultNew;

    // go the path to the root
    do {
      final Node node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == newNode.getNodeKey()) {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(oldHash));
        resultNew = Node.to128BitsAtMaximumBigInteger(resultNew.add(hash));
      } else {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(oldHash.multiply(PRIME)));
        resultNew = Node.to128BitsAtMaximumBigInteger(resultNew.add(hash.multiply(PRIME)));
      }
      node.setHash(resultNew);
    } while (moveTo(mNodeReadOnlyTrx.getCurrentNode().getParentKey()).hasMoved());

    mNodeReadOnlyTrx.setCurrentNode(newNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final ImmutableJsonNode startNode = getCurrentNode();
    BigInteger hashToRemove = startNode.getHash();
    BigInteger hashToAdd = BigInteger.ZERO;
    BigInteger newHash;
    // go the path to the root
    do {
      final Node node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // the begin node is always null
        newHash = BigInteger.ZERO;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // the parent node is just removed
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(hashToRemove.multiply(PRIME)));
        hashToRemove = Node.to128BitsAtMaximumBigInteger(node.getHash());
        setRemoveDescendants(startNode);
      } else {
        // the ancestors are all touched regarding the modification
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(hashToRemove.multiply(PRIME)));
        newHash = Node.to128BitsAtMaximumBigInteger(newHash.add(hashToAdd.multiply(PRIME)));
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      }
      node.setHash(newHash);
      hashToAdd = newHash;
    } while (moveTo(mNodeReadOnlyTrx.getCurrentNode().getParentKey()).hasMoved());

    mNodeReadOnlyTrx.setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startNode the node which has been removed
   */
  private void setRemoveDescendants(final ImmutableNode startNode) {
    assert startNode != null;
    if (startNode instanceof StructNode) {
      final StructNode node = ((StructNode) getCurrentNode());
      node.setDescendantCount(node.getDescendantCount() - ((StructNode) startNode).getDescendantCount() - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() {
    // start with hash to add
    final ImmutableJsonNode startNode = mNodeReadOnlyTrx.getCurrentNode();
    final long oldDescendantCount = mNodeReadOnlyTrx.getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;
    BigInteger hashToAdd = startNode.getHash() == null || BigInteger.ZERO.equals(startNode.getHash())
        ? startNode.computeHash()
        : startNode.getHash();
    BigInteger newHash;
    BigInteger possibleOldHash = BigInteger.ZERO;
    // go the path to the root
    do {
      final Node node = (Node) mPageWriteTrx.prepareEntryForModification(mNodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // at the beginning, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // at the parent level, just add the node
        possibleOldHash = node.getHash();
        newHash = Node.to128BitsAtMaximumBigInteger(possibleOldHash.add(hashToAdd.multiply(PRIME)));
        hashToAdd = newHash;
        setAddDescendants(startNode, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element
        // and add the new one
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(possibleOldHash.multiply(PRIME)));
        newHash = Node.to128BitsAtMaximumBigInteger(newHash.add(hashToAdd.multiply(PRIME)));
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startNode, node, descendantCount);
      }
      node.setHash(newHash);

    } while (moveTo(mNodeReadOnlyTrx.getCurrentNode().getParentKey()).hasMoved());
    mNodeReadOnlyTrx.setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after an add-operation.
   *
   * @param startNode the node which has been removed
   * @param nodeToModify node to modify
   * @param descendantCount the descendantCount to add
   */
  private static void setAddDescendants(final ImmutableNode startNode, final Node nodeToModify,
      final @Nonnegative long descendantCount) {
    assert startNode != null;
    assert descendantCount >= 0;
    assert nodeToModify != null;
    if (startNode instanceof StructNode) {
      final StructNode node = (StructNode) nodeToModify;
      final long oldDescendantCount = node.getDescendantCount();
      node.setDescendantCount(oldDescendantCount + descendantCount);
    }
  }

  /**
   * Get the current node.
   *
   * @return {@link Node} implementation
   */
  private ImmutableJsonNode getCurrentNode() {
    return mNodeReadOnlyTrx.getCurrentNode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("readTrx", mNodeReadOnlyTrx.toString())
                      .add("hashKind", mHashKind)
                      .toString();
  }

  @Override
  protected JsonNodeReadOnlyTrx delegate() {
    return mNodeReadOnlyTrx;
  }

  @Override
  public JsonNodeTrx addPreCommitHook(final PreCommitHook hook) {
    acquireLock();
    try {
      mPreCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx addPostCommitHook(final PostCommitHook hook) {
    acquireLock();
    try {
      mPostCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof JsonNodeTrxImpl) {
      final JsonNodeTrxImpl wtx = (JsonNodeTrxImpl) obj;
      return Objects.equal(mNodeReadOnlyTrx, wtx.mNodeReadOnlyTrx);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeReadOnlyTrx);
  }

  @Override
  public PathSummaryReader getPathSummary() {
    acquireLock();
    try {
      return mPathSummaryWriter.getPathSummary();
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeTrx truncateTo(final int revision) {
    mNodeReadOnlyTrx.assertNotClosed();

    // TODO

    return this;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    mNodeReadOnlyTrx.assertNotClosed();

    return mNodeReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public JsonNodeTrx commit(final String commitMessage) {
    mNodeReadOnlyTrx.assertNotClosed();

    // Optionally lock while commiting and assigning new instances.
    acquireLock();
    try {
      // Execute pre-commit hooks.
      for (final PreCommitHook hook : mPreCommitHooks) {
        hook.preCommit(this);
      }

      // Reset modification counter.
      mModificationCount = 0L;

      final UberPage uberPage = commitMessage == null ? mPageWriteTrx.commit() : mPageWriteTrx.commit(commitMessage);

      // Remember succesfully committed uber page in resource manager.
      mResourceManager.setLastCommittedUberPage(uberPage);

      // Reinstantiate everything.
      reInstantiate(getId(), getRevisionNumber());
    } finally {
      unLock();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : mPostCommitHooks) {
      hook.postCommit(this);
    }

    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PageTrx<Long, Record, UnorderedKeyValuePage> getPageWtx() {
    mNodeReadOnlyTrx.assertNotClosed();
    return (PageTrx<Long, Record, UnorderedKeyValuePage>) mNodeReadOnlyTrx.getPageTrx();
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
