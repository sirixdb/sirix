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

import com.google.common.hash.HashFunction;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.xdm.Item;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.*;
import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.api.PageTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.axis.PostOrderAxis;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffTuple;
import org.sirix.diff.JsonDiffSerializer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.*;
import org.sirix.page.NamePage;
import org.sirix.service.InsertPosition;
import org.sirix.service.json.shredder.JsonItemShredder;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.CREATE;

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
final class JsonNodeTrxImpl extends
    AbstractNodeTrxImpl<JsonNodeReadOnlyTrx, JsonNodeTrx, JsonNodeFactory, ImmutableNode, InternalJsonNodeReadOnlyTrx>
    implements InternalJsonNodeTrx, ForwardingJsonNodeReadOnlyTrx {

  /**
   * A factory that creates new {@link PageTrx} instances.
   */
  private final String databaseName;

  /**
   * Json DeweyID manager.
   */
  private final JsonDeweyIDManager deweyIDManager;

  /**
   * Determines if text values should be compressed or not.
   */
  private final boolean useTextCompression;

  /**
   * The hash function to use to hash node contents.
   */
  private final HashFunction hashFunction;

  /**
   * Flag to decide whether to store child count.
   */
  private final boolean storeChildCount;

  /**
   * Flag to decide if to store the full node history or not.
   */
  private final boolean storeNodeHistory;

  /**
   * Determines if a value can be replaced, used for replacing an object record.
   */
  private boolean canRemoveValue;

  /**
   * {@code true}, if transaction is auto-committing, {@code false} if not.
   */
  private final boolean isAutoCommitting;

  /**
   * The revision number before bulk-inserting nodes.
   */
  private int beforeBulkInsertionRevisionNumber;

  /**
   * Constructor.
   *
   * @param databaseName         The database name where the transaction operates.
   * @param resourceManager      the resource manager instance this transaction is bound to
   * @param nodeReadTrx          the read-only trx delegate
   * @param pathSummaryWriter    writes the path summary
   * @param maxNodeCount         maximum number of node modifications before auto commit
   * @param nodeHashing          hashes node contents
   * @param nodeFactory          to create nodes
   * @param afterCommitState     state after committing, keep open or close
   * @param nodeToRevisionsIndex the node to revisions index (when a node has changed)
   * @throws SirixIOException    if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  JsonNodeTrxImpl(final String databaseName,
      final InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager,
      final InternalJsonNodeReadOnlyTrx nodeReadTrx,
      @Nullable final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter, @NonNegative final int maxNodeCount,
      @Nullable final Lock transactionLock, final Duration afterCommitDelay, @NonNull final JsonNodeHashing nodeHashing,
      final JsonNodeFactory nodeFactory, @NonNull final AfterCommitState afterCommitState,
      final RecordToRevisionsIndex nodeToRevisionsIndex, final boolean isAutoCommitting) {
    super(new JsonNodeTrxThreadFactory(),
          resourceManager.getResourceConfig().hashType,
          nodeReadTrx,
          nodeReadTrx,
          resourceManager,
          afterCommitState,
          nodeHashing,
          pathSummaryWriter,
          nodeFactory,
          nodeToRevisionsIndex,
          transactionLock,
          afterCommitDelay,
          maxNodeCount);

    this.databaseName = checkNotNull(databaseName);

    hashFunction = resourceManager.getResourceConfig().nodeHashFunction;
    storeChildCount = resourceManager.getResourceConfig().storeChildCount();

    // Only auto commit by node modifications if it is more than 0.
    this.isAutoCommitting = isAutoCommitting;

    useTextCompression = resourceManager.getResourceConfig().useTextCompression;

    deweyIDManager = new JsonDeweyIDManager(this);
    storeNodeHistory = resourceManager.getResourceConfig().storeNodeHistory();
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader) {
    return insertSubtree(reader,
                         InsertPosition.AS_LEFT_SIBLING,
                         Commit.IMPLICIT,
                         CheckParentNode.YES,
                         SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader) {
    return insertSubtree(reader,
                         InsertPosition.AS_RIGHT_SIBLING,
                         Commit.IMPLICIT,
                         CheckParentNode.YES,
                         SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode, SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  private JsonNodeTrx insertSubtree(final JsonReader reader, final InsertPosition insertionPosition, Commit commit,
      final CheckParentNode checkParentNode, final SkipRootToken doSkipRootJsonToken) {
    nodeReadOnlyTrx.assertNotClosed();
    checkNotNull(reader);
    assert insertionPosition != null;

    runLocked(() -> {
      try {
        assertRunning();
        final var peekedJsonToken = reader.peek();

        if (peekedJsonToken != JsonToken.BEGIN_OBJECT && peekedJsonToken != JsonToken.BEGIN_ARRAY)
          throw new SirixUsageException("JSON to insert must begin with an array or object.");

        var skipRootJsonToken = doSkipRootJsonToken;
        final var nodeKind = getKind();

        // $CASES-OMITTED$
        switch (insertionPosition) {
          case AS_FIRST_CHILD, AS_LAST_CHILD -> {
            if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT) {
              throw new IllegalStateException(
                  "Current node must either be the document root, an array or an object key.");
            }
            switch (peekedJsonToken) {
              case BEGIN_OBJECT:
                if (nodeKind == NodeKind.OBJECT)
                  skipRootJsonToken = SkipRootToken.YES;
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
            if (checkParentNode == CheckParentNode.YES) {
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

        if (skipRootJsonToken == SkipRootToken.YES) {
          shredderBuilder.skipRootJsonToken();
        }

        final var shredder = shredderBuilder.build();
        shredder.call();
        moveTo(nodeKey);

        switch (insertionPosition) {
          case AS_FIRST_CHILD -> moveToFirstChild();
          case AS_LAST_CHILD -> moveToLastChild();
          case AS_LEFT_SIBLING -> moveToLeftSibling();
          case AS_RIGHT_SIBLING -> moveToRightSibling();
          default -> {
            // May not happen.
          }
        }

        adaptUpdateOperationsForInsert(getDeweyID(), getNodeKey());

        // bulk inserts will be disabled for auto-commits after the first commit
        if (!isAutoCommitting) {
          adaptHashesInPostorderTraversal();
        }

        nodeHashing.setBulkInsert(false);

        if (commit == Commit.IMPLICIT) {
          commit();
        }

        //      for (final long unused : new DescendantAxis(nodeReadOnlyTrx)) {
        //        System.out.println(nodeReadOnlyTrx.getDeweyID());
        //      }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    return this;
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final Item item) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final Item item, Commit commit) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final Item item) {
    return insertSubtree(item, InsertPosition.AS_LAST_CHILD, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final Item item, Commit commit) {
    return insertSubtree(item, InsertPosition.AS_LAST_CHILD, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(item, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final Item item) {
    return insertSubtree(item, InsertPosition.AS_LEFT_SIBLING, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final Item item, Commit commit) {
    return insertSubtree(item, InsertPosition.AS_LEFT_SIBLING, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(item, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final Item item) {
    return insertSubtree(item, InsertPosition.AS_RIGHT_SIBLING, Commit.IMPLICIT, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final Item item, Commit commit) {
    return insertSubtree(item, InsertPosition.AS_RIGHT_SIBLING, commit, CheckParentNode.YES, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final Item item, Commit commit, CheckParentNode checkParentNode) {
    return insertSubtree(item, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, SkipRootToken.NO);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  private JsonNodeTrx insertSubtree(final Item item, final InsertPosition insertionPosition, Commit commit,
      final CheckParentNode checkParentNode, final SkipRootToken doSkipRootToken) {
    nodeReadOnlyTrx.assertNotClosed();
    checkNotNull(item);
    assert insertionPosition != null;

    runLocked(() -> {
      assertRunning();

      if (!item.itemType().isArray() && !item.itemType().isObject())
        throw new SirixUsageException("JSON to insert must begin with an array or object.");

      final var nodeKind = getKind();
      var skipRootJsonToken = doSkipRootToken;

      // $CASES-OMITTED$
      switch (insertionPosition) {
        case AS_FIRST_CHILD, AS_LAST_CHILD -> {
          if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT) {
            throw new IllegalStateException("Current node must either be the document root, an array or an object key.");
          }
          if (item.itemType().isObject()) {
            if (nodeKind == NodeKind.OBJECT)
              skipRootJsonToken = SkipRootToken.YES;
          } else if (item.itemType().isArray()) {
            if (nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.JSON_DOCUMENT) {
              throw new IllegalStateException("Current node in storage must be an array node.");
            }
          }
        }
        case AS_LEFT_SIBLING, AS_RIGHT_SIBLING -> {
          if (checkParentNode == CheckParentNode.YES) {
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
      final var shredderBuilder = new JsonItemShredder.Builder(this, item, insertionPosition);

      if (skipRootJsonToken == SkipRootToken.YES) {
        shredderBuilder.skipRootJsonToken();
      }

      final var shredder = shredderBuilder.build();
      shredder.call();
      moveTo(nodeKey);

      switch (insertionPosition) {
        case AS_FIRST_CHILD -> moveToFirstChild();
        case AS_LAST_CHILD -> moveToLastChild();
        case AS_LEFT_SIBLING -> moveToLeftSibling();
        case AS_RIGHT_SIBLING -> moveToRightSibling();
        default -> {
          // May not happen.
        }
      }

      adaptUpdateOperationsForInsert(getDeweyID(), getNodeKey());

      // bulk inserts will be disabled for auto-commits after the first commit
      if (!isAutoCommitting) {
        adaptHashesInPostorderTraversal();
      }

      nodeHashing.setBulkInsert(false);

      if (commit == Commit.IMPLICIT) {
        commit();
      }

      //      for (final long unused : new DescendantAxis(nodeReadOnlyTrx)) {
      //        System.out.println(nodeReadOnlyTrx.getDeweyID());
      //      }
    });
    return this;
  }

  @Override
  public JsonNodeTrx insertObjectAsFirstChild() {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsLastChild() {
    if (lock != null) {
      lock.lock();
    }

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
          : (structNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
              ? deweyIDManager.newFirstChildID()
              : deweyIDManager.newLastChildID());

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsLeftSibling() {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectAsRightSibling() {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsFirstChild(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(structNode, key, NodeKind.OBJECT_KEY);

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLastChild(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNode();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();

      final long pathNodeKey = getPathNodeKey(structNode, key, NodeKind.OBJECT_KEY);

      final SirixDeweyID id = structNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
          ? deweyIDManager.newFirstChildID()
          : deweyIDManager.newLastChildID();

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void adaptUpdateOperationsForInsert(SirixDeweyID id, long newNodeKey) {
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

  private void adaptUpdateOperationsForReplace(SirixDeweyID id, long oldNodeKey, long newNodeKey) {
    if (id == null) {
      updateOperationsUnordered.put(newNodeKey,
                                    new DiffTuple(DiffFactory.DiffType.REPLACEDNEW, newNodeKey, oldNodeKey, null));
    } else {
      updateOperationsOrdered.put(id,
                                  new DiffTuple(DiffFactory.DiffType.REPLACEDNEW,
                                                newNodeKey,
                                                oldNodeKey,
                                                new DiffDepth(id.getLevel(), id.getLevel())));
    }
  }

  private void setFirstChildOfObjectKeyNode(final ObjectKeyNode node) {
    final ObjectKeyNode objectKeyNode = pageTrx.prepareRecordForModification(node.getNodeKey(), IndexType.DOCUMENT, -1);
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

  private long getPathNodeKey(final ImmutableNode node, final String name, final NodeKind kind) {
    if (buildPathSummary) {
      moveToParentObjectKeyArrayOrDocumentRoot();
      final long pathNodeKey = pathSummaryWriter.getPathNodeKey(new QNm(name), kind);
      nodeReadOnlyTrx.setCurrentNode(node);
      return pathNodeKey;
    }

    return 0;
  }

  private void moveToParentObjectKeyArrayOrDocumentRoot() {
    var nodeKind = nodeReadOnlyTrx.getKind();
    while (nodeKind != NodeKind.OBJECT_KEY && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.JSON_DOCUMENT) {
      nodeReadOnlyTrx.moveToParent();
      nodeKind = nodeReadOnlyTrx.getKind();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLeftSibling(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    if (lock != null) {
      lock.lock();
    }

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
      final long pathNodeKey = getPathNodeKey(currentNode, key, NodeKind.OBJECT_KEY);
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsRightSibling(final String key, final ObjectRecordValue<?> value) {
    checkNotNull(key);
    if (lock != null) {
      lock.lock();
    }

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
      final long pathNodeKey = getPathNodeKey(currentNode, key, NodeKind.OBJECT_KEY);
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsFirstChild() {
    if (lock != null) {
      lock.lock();
    }

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

      final long pathNodeKey = getPathNodeKey(currentNode, "__array__", NodeKind.ARRAY);

      final SirixDeweyID id = currentNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsLastChild() {
    if (lock != null) {
      lock.lock();
    }

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

      final long pathNodeKey = getPathNodeKey(currentNode, "__array__", NodeKind.ARRAY);

      final SirixDeweyID id = currentNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : currentNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
              ? deweyIDManager.newFirstChildID()
              : deweyIDManager.newLastChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      adaptNodesAndHashesForInsertAsChild(node);

      indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsLeftSibling() {
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode, "array", NodeKind.ARRAY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertArrayAsRightSibling() {
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      checkPrecondition();

      final StructNode currentNode = (StructNode) getCurrentNode();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(currentNode, "array", NodeKind.ARRAY);
      moveTo(currentNode.getNodeKey());

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);

      insertAsSibling(node);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx replaceObjectRecordValue(ObjectRecordValue<?> value) {
    checkNotNull(value);

    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Replacing is only permitted for record object key nodes.");

      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
      moveToFirstChild();

      canRemoveValue = true;

      final long oldValueNodeKey = getNodeKey();
      remove();
      moveTo(nodeKey);
      insertValue(value);

      final ImmutableNode node = getCurrentNode();

      adaptUpdateOperationsForReplace(node.getDeweyID(), oldValueNodeKey, node.getNodeKey());

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsLastChild(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

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
        id = structNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
            ? deweyIDManager.newFirstChildID()
            : deweyIDManager.newLastChildID();
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsFirstChild(boolean value) {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLastChild(boolean value) {
    if (lock != null) {
      lock.lock();
    }

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
        id = structNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
            ? deweyIDManager.newFirstChildID()
            : deweyIDManager.newLastChildID();
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLeftSibling(boolean value) {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsRightSibling(boolean value) {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLastChild(Number value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

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
        id = currentNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
            ? deweyIDManager.newFirstChildID()
            : deweyIDManager.newLastChildID();
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLeftSibling(Number value) {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    return nodeReadOnlyTrx;
  }

  @Override
  public JsonNodeTrx insertNumberValueAsRightSibling(Number value) {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsFirstChild() {
    if (lock != null) {
      lock.lock();
    }

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
        final long rightSibKey = structNode.getFirstChildKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsLastChild() {
    if (lock != null) {
      lock.lock();
    }

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
        id = structNode.getFirstChildKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
            ? deweyIDManager.newFirstChildID()
            : deweyIDManager.newLastChildID();
        final long leftSibKey = structNode.getLeftSiblingKey();
        final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);
      }

      adaptNodesAndHashesForInsertAsChild(node);

      if (getParentKind() != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, node.getNodeKey());
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsLeftSibling() {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNullValueAsRightSibling() {
    if (lock != null) {
      lock.lock();
    }

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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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
    if (lock != null) {
      lock.lock();
    }

    try {
      final StructNode node = (StructNode) getCurrentNode();
      if (node.getKind() == NodeKind.JSON_DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      }

      final var parentNodeKind = getParentKind();

      if ((parentNodeKind != NodeKind.JSON_DOCUMENT && parentNodeKind != NodeKind.OBJECT
          && parentNodeKind != NodeKind.ARRAY) && !canRemoveValue) {
        throw new SirixUsageException(
            "An object record value can not be removed, you have to remove the whole object record (parent of this value).");
      }

      canRemoveValue = false;

      if (parentNodeKind != NodeKind.OBJECT_KEY) {
        adaptUpdateOperationsForRemove(node.getDeweyID(), node.getNodeKey());
      }

      // Remove subtree.
      for (final var axis = new PostOrderAxis(this); axis.hasNext(); ) {
        axis.nextLong();

        final var currentNode = axis.getCursor().getNode();

        // Remove name.
        removeName();

        // Remove text value.
        removeValue();

        // Then remove node.
        pageTrx.removeRecord(currentNode.getNodeKey(), IndexType.DOCUMENT, -1);

        if (storeNodeHistory) {
          nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(currentNode.getNodeKey());
        }
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

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
      }

      if (node.hasRightSibling()) {
        moveTo(node.getRightSiblingKey());
      } else if (node.hasLeftSibling()) {
        moveTo(node.getLeftSiblingKey());
      } else {
        moveTo(node.getParentKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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

      moveToParent();

      final var node = getNode();

      if (node.getKind() == NodeKind.ARRAY) {
        pathNodeKey = ((ImmutableArrayNode) node).getPathNodeKey();
      } else if (node.getKind() == NodeKind.OBJECT_KEY) {
        pathNodeKey = ((ImmutableObjectKeyNode) node).getPathNodeKey();
      } else {
        pathNodeKey = -1;
      }

      moveTo(nodeKey);
      indexController.notifyChange(ChangeType.DELETE, node, pathNodeKey);
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
    if (lock != null) {
      lock.lock();
    }

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
      node = pageTrx.prepareRecordForModification(node.getNodeKey(), IndexType.DOCUMENT, -1);
      node.setNameKey(newNameKey);
      node.setName(key);
      node.setPreviousRevision(pageTrx.getRevisionToRepresent());

      // Adapt path summary.
      if (buildPathSummary) {
        pathSummaryWriter.adaptPathForChangedNode(node, new QNm(key), -1, -1, newNameKey, OPType.SETNAME);
      }

      // Set path node key.
      node.setPathNodeKey(buildPathSummary ? pathSummaryWriter.getNodeKey() : 0);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
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
    if (lock != null) {
      lock.lock();
    }

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
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setValue(byteVal);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(pageTrx.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx setBooleanValue(final boolean value) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getKind() != NodeKind.BOOLEAN_VALUE && getKind() != NodeKind.OBJECT_BOOLEAN_VALUE) {
        throw new SirixUsageException("Not allowed if current node is not a boolean value node!");
      }

      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      // Remove old value from indexes.
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractBooleanNode node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setValue(value);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(pageTrx.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx setNumberValue(final Number value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getKind() != NodeKind.NUMBER_VALUE && getKind() != NodeKind.OBJECT_NUMBER_VALUE) {
        throw new SirixUsageException(
            "Not allowed if current node is not a number value and not an object number value node!");
      }
      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      // Remove old value from indexes.
      indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

      final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();

      final AbstractNumberNode node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setValue(value);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(pageTrx.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

      adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
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

    final StructNode parent = pageTrx.prepareRecordForModification(structNode.getParentKey(), IndexType.DOCUMENT, -1);

    if (storeChildCount) {
      parent.incrementChildCount();
    }

    if (structNode.hasLeftSibling()) {
      final StructNode leftSiblingNode =
          pageTrx.prepareRecordForModification(structNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      leftSiblingNode.setRightSiblingKey(structNode.getNodeKey());
    } else {
      parent.setFirstChildKey(structNode.getNodeKey());
    }

    if (structNode.hasRightSibling()) {
      final StructNode rightSiblingNode =
          pageTrx.prepareRecordForModification(structNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
      rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
    } else {
      parent.setLastChildKey(structNode.getNodeKey());
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
          pageTrx.prepareRecordForModification(oldNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      final StructNode rightSibling =
          pageTrx.prepareRecordForModification(oldNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
      rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
    }

    // Adapt parent, if node has left sibling now it is a first child, and right sibling will be a last child
    StructNode parent = pageTrx.prepareRecordForModification(oldNode.getParentKey(), IndexType.DOCUMENT, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
    }
    if (!oldNode.hasRightSibling()) {
      parent.setLastChildKey(oldNode.getLeftSiblingKey());
    }

    if (storeChildCount) {
      parent.decrementChildCount();
    }

    // Remove non-structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      // removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    pageTrx.removeRecord(oldNode.getNodeKey(), IndexType.DOCUMENT, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  @Override
  protected void serializeUpdateDiffs(final int revisionNumber) {
    if (!nodeHashing.isBulkInsert() && revisionNumber - 1 > 0) {
      final var diffSerializer = new JsonDiffSerializer(this.databaseName,
                                                        (JsonResourceSession) resourceManager,
                                                        beforeBulkInsertionRevisionNumber != 0 && isAutoCommitting
                                                            ? beforeBulkInsertionRevisionNumber
                                                            : revisionNumber - 1,
                                                        revisionNumber,
                                                        storeDeweyIDs()
                                                            ? updateOperationsOrdered.values()
                                                            : updateOperationsUnordered.values());
      final var jsonDiff = diffSerializer.serialize(false);

      final Path diff = resourceManager.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                       .resolve(
                                           "diffFromRev" + (revisionNumber - 1) + "toRev" + revisionNumber + ".json");
      try {
        Files.writeString(diff, jsonDiff, CREATE);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }

      if (storeDeweyIDs()) {
        updateOperationsOrdered.clear();
      } else {
        updateOperationsUnordered.clear();
      }
    }
  }

  @Override
  protected JsonNodeTrx self() {
    return this;
  }

  @Override
  protected AbstractNodeHashing<ImmutableNode, JsonNodeReadOnlyTrx> reInstantiateNodeHashing(HashType hashType,
      PageTrx pageTrx) {
    return new JsonNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);
  }

  @Override
  protected JsonNodeFactory reInstantiateNodeFactory(PageTrx pageTrx) {
    return new JsonNodeFactoryImpl(hashFunction, pageTrx);
  }

  private static final class JsonNodeTrxThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@NonNull final Runnable runnable) {
      final var thread = new Thread(runnable, "JsonNodeTrxCommitThread");

      thread.setPriority(Thread.NORM_PRIORITY);
      thread.setDaemon(false);

      return thread;
    }
  }
}
