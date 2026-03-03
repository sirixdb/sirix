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

package io.sirix.access.trx.node.json;

import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractNodeHashing;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.trx.node.RecordToRevisionsIndex;
import io.sirix.access.trx.node.json.objectvalue.ByteStringValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.Axis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.PostOrderAxis;
import io.sirix.diff.DiffDepth;
import io.sirix.diff.DiffFactory;
import io.sirix.diff.DiffTuple;
import io.sirix.diff.JsonDiffSerializer;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.index.path.summary.PathSummaryWriter.OPType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.BooleanValueNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.page.NamePage;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import io.sirix.service.json.shredder.JsonItemShredder;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Objects.requireNonNull;

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
@SuppressWarnings("resource")
final class JsonNodeTrxImpl extends
    AbstractNodeTrxImpl<JsonNodeReadOnlyTrx, JsonNodeTrx, JsonNodeFactory, ImmutableNode, InternalJsonNodeReadOnlyTrx>
    implements InternalJsonNodeTrx, ForwardingJsonNodeReadOnlyTrx {

  private final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

  /**
   * A factory that creates new {@link StorageEngineWriter} instances.
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
  private final LongHashFunction hashFunction;

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
   * Insert not allowed exception because of absance of parent in array.
   */
  private static final String INSERT_NOT_ALLOWED_SINCE_PARENT_NOT_IN_AN_ARRAY_NODE =
      "Insert is not allowed if parent node is not an array node!";

  private static final QNm ARRAY_PATH_QNM = new QNm("__array__");
  private static final QNm ARRAY_SIBLING_PATH_QNM = new QNm("array");
  private static final Str STR_TRUE = new Str("true");
  private static final Str STR_FALSE = new Str("false");

  /**
   * Constructor.
   *
   * @param databaseName The database name where the transaction operates.
   * @param resourceSession the resource session instance this transaction is bound to
   * @param nodeReadTrx the read-only trx delegate
   * @param pathSummaryWriter writes the path summary
   * @param maxNodeCount maximum number of node modifications before auto commit
   * @param nodeHashing hashes node contents
   * @param nodeFactory to create nodes
   * @param afterCommitState state after committing, keep open or close
   * @param nodeToRevisionsIndex the node to revisions index (when a node has changed)
   * @throws SirixIOException if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  JsonNodeTrxImpl(final String databaseName,
      final InternalResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceSession,
      final InternalJsonNodeReadOnlyTrx nodeReadTrx,
      @Nullable final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter, @NonNegative final int maxNodeCount,
      @Nullable final Lock transactionLock, final Duration afterCommitDelay, @NonNull final JsonNodeHashing nodeHashing,
      final JsonNodeFactory nodeFactory, @NonNull final AfterCommitState afterCommitState,
      final RecordToRevisionsIndex nodeToRevisionsIndex, final boolean isAutoCommitting) {
    super(new JsonNodeTrxThreadFactory(), resourceSession.getResourceConfig().hashType, nodeReadTrx, nodeReadTrx,
        resourceSession, afterCommitState, nodeHashing, pathSummaryWriter, nodeFactory, nodeToRevisionsIndex,
        transactionLock, afterCommitDelay, maxNodeCount);
    this.databaseName = requireNonNull(databaseName);

    hashFunction = resourceSession.getResourceConfig().nodeHashFunction;
    storeChildCount = resourceSession.getResourceConfig().storeChildCount();

    // Only auto commit by node modifications if it is more than 0.
    this.isAutoCommitting = isAutoCommitting;

    useTextCompression = resourceSession.getResourceConfig().useTextCompression;

    deweyIDManager = new JsonDeweyIDManager(this);
    storeNodeHistory = resourceSession.getResourceConfig().storeNodeHistory();

    // Register index listeners for any existing indexes.
    // This is critical for subsequent write transactions to update indexes on node modifications.
    final var existingIndexDefs = indexController.getIndexes().getIndexDefs();
    if (!existingIndexDefs.isEmpty()) {
      indexController.createIndexListeners(existingIndexDefs, this);
    }

    // Auto-create CAS indexes for valid time paths on bootstrap
    createValidTimeIndexesIfNeeded(resourceSession.getResourceConfig());

    // Wire write singleton binder for zero-allocation write path.
    if (nodeFactory instanceof JsonNodeFactoryImpl factoryImpl) {
      wireWriteSingletonBinder(factoryImpl, storageEngineWriter);
    }
  }

  /**
   * Creates CAS indexes for valid time paths if configured and this is a new resource.
   *
   * <p>
   * This is called during the first write transaction on a new resource to ensure that bitemporal
   * queries can use optimized index-based lookups for valid time.
   * </p>
   *
   * @param resourceConfig the resource configuration
   */
  private void createValidTimeIndexesIfNeeded(ResourceConfiguration resourceConfig) {
    // Only create indexes on bootstrap (revision 0)
    if (nodeReadOnlyTrx.getRevisionNumber() != 0) {
      return;
    }

    // Check if valid time configuration exists
    final var validTimeConfig = resourceConfig.getValidTimeConfig();
    if (validTimeConfig == null) {
      return;
    }

    // Check if indexes already exist
    final var existingIndexes = indexController.getIndexes().getIndexDefs();
    if (!existingIndexes.isEmpty()) {
      return;
    }

    // Create CAS indexes for validFrom and validTo paths
    final var indexDefs = createValidTimeIndexDefs(validTimeConfig);
    if (!indexDefs.isEmpty()) {
      // Note: we just register the index definitions here, they will be populated
      // when data is inserted. For bootstrap with no data yet, this just sets up
      // the index listeners for future insertions.
      for (IndexDef indexDef : indexDefs) {
        indexController.getIndexes().add(indexDef);
      }
      indexController.createIndexListeners(indexDefs, this);
    }
  }

  /**
   * Creates index definitions for valid time paths.
   *
   * @param validTimeConfig the valid time configuration
   * @return set of index definitions for validFrom and validTo paths
   */
  private Set<IndexDef> createValidTimeIndexDefs(io.sirix.access.ValidTimeConfig validTimeConfig) {
    final Set<IndexDef> indexDefs = new HashSet<>();

    // Use xs:dateTime type for the CAS indexes since valid time values are typically timestamps
    final var dateTimeType = io.brackit.query.jdm.Type.DATI;

    // Create index for validFrom path
    final var validFromPath = validTimeConfig.getNormalizedValidFromPath();
    final var validFromPaths =
        Set.of(io.brackit.query.util.path.Path.parse(validFromPath, io.brackit.query.util.path.PathParser.Type.JSON));
    indexDefs.add(
        io.sirix.index.IndexDefs.createCASIdxDef(false, dateTimeType, validFromPaths, 0, IndexDef.DbType.JSON));

    // Create index for validTo path
    final var validToPath = validTimeConfig.getNormalizedValidToPath();
    final var validToPaths =
        Set.of(io.brackit.query.util.path.Path.parse(validToPath, io.brackit.query.util.path.PathParser.Type.JSON));
    indexDefs.add(io.sirix.index.IndexDefs.createCASIdxDef(false, dateTimeType, validToPaths, 1, IndexDef.DbType.JSON));

    return indexDefs;
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonReader reader, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonReader reader, Commit commit,
      CheckParentNode checkParentNode, SkipRootToken skipRootToken) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  private JsonNodeTrx insertSubtree(final JsonReader reader, final InsertPosition insertionPosition,
      final Commit commit, final CheckParentNode checkParentNode, final SkipRootToken doSkipRootJsonToken) {
    requireNonNull(reader);

    return insertSubtreeInternal(insertionPosition, commit, checkParentNode, doSkipRootJsonToken, () -> {
      final var peekedJsonToken = reader.peek();
      if (peekedJsonToken != JsonToken.BEGIN_OBJECT && peekedJsonToken != JsonToken.BEGIN_ARRAY) {
        throw new SirixUsageException("JSON to insert must begin with an array or object.");
      }
      return new InputShape(peekedJsonToken == JsonToken.BEGIN_OBJECT, peekedJsonToken == JsonToken.BEGIN_ARRAY);
    }, (skipToken, position) -> {
      final var shredderBuilder = new JsonShredder.Builder(this, reader, position);
      if (skipToken == SkipRootToken.YES) {
        shredderBuilder.skipRootJsonToken();
      }
      shredderBuilder.build().call();
    });
  }

  // ==================== LDJSON Methods ====================

  @Override
  public LdjsonResult insertLdjsonAsFirstChild(final JsonParser parser, final Commit commit) {
    return insertLdjson(parser, InsertPosition.AS_FIRST_CHILD, commit);
  }

  @Override
  public LdjsonResult insertLdjsonAsLastChild(final JsonParser parser, final Commit commit) {
    return insertLdjson(parser, InsertPosition.AS_LAST_CHILD, commit);
  }

  private LdjsonResult insertLdjson(final JsonParser parser, final InsertPosition insertionPosition,
      final Commit commit) {
    requireNonNull(parser);
    final var resultHolder = new LdjsonResult[1];

    insertSubtreeInternal(insertionPosition, commit, CheckParentNode.NO, SkipRootToken.NO, () -> {
      // LDJSON wrapper is an array
      return new InputShape(false, true);
    }, (skipToken, position) -> {
      final var shredderBuilder = new JacksonJsonShredder.Builder(this, parser, position);
      shredderBuilder.ldjsonMode();
      final var shredder = shredderBuilder.build();
      shredder.call();
      resultHolder[0] = new LdjsonResult(shredder.getDocumentCount(), shredder.getLdjsonArrayKey());
    });

    return resultHolder[0];
  }

  // ==================== Jackson JsonParser Methods ====================

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(parser, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(parser, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final JsonParser parser, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(parser, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final JsonParser parser, Commit commit,
      CheckParentNode checkParentNode, SkipRootToken skipRootToken) {
    return insertSubtree(parser, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  private JsonNodeTrx insertSubtree(final JsonParser parser, final InsertPosition insertionPosition,
      final Commit commit, final CheckParentNode checkParentNode, final SkipRootToken doSkipRootJsonToken) {
    requireNonNull(parser);
    final var tokenHolder = new com.fasterxml.jackson.core.JsonToken[1];

    return insertSubtreeInternal(insertionPosition, commit, checkParentNode, doSkipRootJsonToken, () -> {
      final com.fasterxml.jackson.core.JsonToken firstToken = parser.nextToken();
      if (firstToken != com.fasterxml.jackson.core.JsonToken.START_OBJECT
          && firstToken != com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
        throw new SirixUsageException("JSON to insert must begin with an array or object.");
      }
      tokenHolder[0] = firstToken;
      return new InputShape(firstToken == com.fasterxml.jackson.core.JsonToken.START_OBJECT,
          firstToken == com.fasterxml.jackson.core.JsonToken.START_ARRAY);
    }, (skipToken, position) -> {
      final var shredderBuilder = new JacksonJsonShredder.Builder(this, parser, position);
      shredderBuilder.firstToken(tokenHolder[0]);
      if (skipToken == SkipRootToken.YES) {
        shredderBuilder.skipRootJsonToken();
      }
      shredderBuilder.build().call();
    });
  }

  @Override
  public JsonNodeTrx insertSubtreeAsFirstChild(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_FIRST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLastChild(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_LAST_CHILD, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsLeftSibling(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_LEFT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  @Override
  public JsonNodeTrx insertSubtreeAsRightSibling(final Item item, Commit commit, CheckParentNode checkParentNode,
      SkipRootToken skipRootToken) {
    return insertSubtree(item, InsertPosition.AS_RIGHT_SIBLING, commit, checkParentNode, skipRootToken);
  }

  private JsonNodeTrx insertSubtree(final Item item, final InsertPosition insertionPosition, final Commit commit,
      final CheckParentNode checkParentNode, final SkipRootToken doSkipRootToken) {
    requireNonNull(item);

    return insertSubtreeInternal(insertionPosition, commit, checkParentNode, doSkipRootToken, () -> {
      if (!item.itemType().isObject() && !item.itemType().isArray()) {
        throw new SirixUsageException("JSON to insert must begin with an array or object.");
      }
      return new InputShape(item.itemType().isObject(), item.itemType().isArray());
    }, (skipToken, position) -> {
      final var shredderBuilder = new JsonItemShredder.Builder(this, item, position);
      if (skipToken == SkipRootToken.YES) {
        shredderBuilder.skipRootJsonToken();
      }
      shredderBuilder.build().call();
    });
  }

  /**
   * Immutable holder for the shape of the input being inserted (object vs array).
   *
   * @param isObject {@code true} if the input starts with an object
   * @param isArray  {@code true} if the input starts with an array
   */
  private record InputShape(boolean isObject, boolean isArray) {
  }

  /**
   * Functional interface for validating the input source and determining its shape. May throw
   * checked exceptions (e.g. {@link IOException} from Gson/Jackson peek/nextToken).
   */
  @FunctionalInterface
  private interface InputValidator {
    InputShape validate() throws Exception;
  }

  /**
   * Functional interface for constructing and executing the appropriate shredder. May throw checked
   * exceptions (e.g. {@link IOException} from shredder I/O).
   */
  @FunctionalInterface
  private interface ShredderExecutor {
    void execute(SkipRootToken skipRootToken, InsertPosition insertionPosition) throws Exception;
  }

  /**
   * Common implementation for all {@code insertSubtree} overloads. Validates position constraints,
   * sets up bulk-insert state, delegates to the shredder, then adapts hashes and commits.
   *
   * @param insertionPosition where to insert relative to the current node
   * @param commit            whether to commit implicitly after insertion
   * @param checkParentNode   whether to validate the parent node type for sibling insertions
   * @param doSkipRootToken   whether to skip the root JSON token of the input
   * @param inputValidator    validates the input and returns its shape (object vs array)
   * @param shredderExecutor  constructs and runs the appropriate shredder
   * @return this transaction for fluent chaining
   */
  private JsonNodeTrx insertSubtreeInternal(final InsertPosition insertionPosition, final Commit commit,
      final CheckParentNode checkParentNode, final SkipRootToken doSkipRootToken,
      final InputValidator inputValidator, final ShredderExecutor shredderExecutor) {
    nodeReadOnlyTrx.assertNotClosed();
    assert insertionPosition != null;

    runLocked(() -> {
      try {
        assertRunning();

        final InputShape inputShape = inputValidator.validate();
        var skipRootJsonToken = doSkipRootToken;
        final var nodeKind = getKind();

        // $CASES-OMITTED$
        switch (insertionPosition) {
          case AS_FIRST_CHILD, AS_LAST_CHILD -> {
            if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT) {
              throw new IllegalStateException(
                  "Current node must either be the document root, an array or an object key.");
            }
            if (inputShape.isObject()) {
              if (nodeKind == NodeKind.OBJECT) {
                skipRootJsonToken = SkipRootToken.YES;
              }
            } else if (inputShape.isArray()) {
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
        final long nodeKey = getNodeKey();

        shredderExecutor.execute(skipRootJsonToken, insertionPosition);

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

      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new SirixException(e);
      }
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

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = kind == NodeKind.OBJECT_KEY
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : structNode.getFirstChildKey();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (kind != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = kind == NodeKind.OBJECT_KEY
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long firstChildKey = structNode.getFirstChildKey();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : (firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
              ? deweyIDManager.newFirstChildID()
              : deweyIDManager.newLastChildID());

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (kind != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
        if (getParentKind() != NodeKind.ARRAY) {
          throw new SirixUsageException(INSERT_NOT_ALLOWED_SINCE_PARENT_NOT_IN_AN_ARRAY_NODE);
        }
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
        if (getParentKind() != NodeKind.ARRAY) {
          throw new SirixUsageException(INSERT_NOT_ALLOWED_SINCE_PARENT_NOT_IN_AN_ARRAY_NODE);
        }
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
    requireNonNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(parentKey, key, NodeKind.OBJECT_KEY);

      final SirixDeweyID id = deweyIDManager.newFirstChildID();

      final ObjectKeyNode node = nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey,
          key, Fixed.NULL_NODE_KEY.getStandardProperty(), id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      insertValue(value);

      setFirstChildOfObjectKeyNode(nodeKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
    requireNonNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long firstChildKey = structNode.getFirstChildKey(); // capture before getPathNodeKey moves cursor

      final long pathNodeKey = getPathNodeKey(parentKey, key, NodeKind.OBJECT_KEY);

      final SirixDeweyID id = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
          ? deweyIDManager.newFirstChildID()
          : deweyIDManager.newLastChildID();

      final ObjectKeyNode node = nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey,
          key, Fixed.NULL_NODE_KEY.getStandardProperty(), id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      insertValue(value);

      setFirstChildOfObjectKeyNode(nodeKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void adaptUpdateOperationsForInsert(SirixDeweyID id, long newNodeKey) {
    final var diffTuple = new DiffTuple(DiffFactory.DiffType.INSERTED, newNodeKey, 0, id == null
        ? null
        : new DiffDepth(id.getLevel(), 0));
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
      updateOperationsOrdered.put(id, new DiffTuple(DiffFactory.DiffType.REPLACEDNEW, newNodeKey, oldNodeKey,
          new DiffDepth(id.getLevel(), id.getLevel())));
    }
  }

  private void setFirstChildOfObjectKeyNode(final long objectKeyNodeKey) {
    final ObjectKeyNode objectKeyNode = storageEngineWriter.prepareRecordForModification(objectKeyNodeKey, IndexType.DOCUMENT, -1);
    objectKeyNode.setFirstChildKey(getNodeKey());
    persistUpdatedRecord(objectKeyNode);
  }

  private void insertValue(final ObjectRecordValue<?> value) throws AssertionError {
    final NodeKind valueKind = value.getKind();

    // $CASES-OMITTED$
    switch (valueKind) {
      case OBJECT -> insertObjectAsFirstChild();
      case ARRAY -> insertArrayAsFirstChild();
      case STRING_VALUE -> {
        if (value instanceof ByteStringValue bsv) {
          insertStringValueAsFirstChild(bsv.getValue(), bsv.getOffset(), bsv.getLength());
        } else {
          final Object raw = value.getValue();
          if (raw instanceof byte[] utf8) {
            insertStringValueAsFirstChild(utf8);
          } else if (raw instanceof String s) {
            insertStringValueAsFirstChild(s);
          } else {
            throw new IllegalStateException("STRING_VALUE payload must be String or byte[], got: "
                + (raw == null ? "null" : raw.getClass().getName()));
          }
        }
      }
      case BOOLEAN_VALUE -> insertBooleanValueAsFirstChild((Boolean) value.getValue());
      case NUMBER_VALUE -> insertNumberValueAsFirstChild((Number) value.getValue());
      case NULL_VALUE -> insertNullValueAsFirstChild();
      default -> throw new AssertionError("Type not known.");
    }
  }

  /**
   * Get the path node key by restoring the cursor position via moveTo instead of setCurrentNode.
   * This avoids retaining a reference to the singleton structural node across cursor moves.
   *
   * @param restoreNodeKey the node key to restore the cursor to after path summary lookup
   * @param name the name for the path summary
   * @param kind the node kind
   * @return the path node key, or 0 if path summary is not built
   */
  private long getPathNodeKey(final long restoreNodeKey, final String name, final NodeKind kind) {
    return getPathNodeKey(restoreNodeKey, new QNm(name), kind);
  }

  private long getPathNodeKey(final long restoreNodeKey, final QNm name, final NodeKind kind) {
    if (buildPathSummary) {
      moveToParentObjectKeyArrayOrDocumentRoot();
      final long pathNodeKey = pathSummaryWriter.getPathNodeKey(name, kind);
      nodeReadOnlyTrx.moveTo(restoreNodeKey);
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
    requireNonNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long rightSibKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(rightSibKey, key, NodeKind.OBJECT_KEY);
      moveTo(rightSibKey);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ObjectKeyNode node =
          nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, key, -1, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      insertValue(value);

      setFirstChildOfObjectKeyNode(nodeKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
    requireNonNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(leftSibKey, key, NodeKind.OBJECT_KEY);
      moveTo(leftSibKey);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ObjectKeyNode node =
          nodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, key, -1, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      insertValue(value);

      setFirstChildOfObjectKeyNode(nodeKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = currentNode.getFirstChildKey();
      final NodeKind currentKind = currentNode.getKind();

      final long pathNodeKey = getPathNodeKey(parentKey, ARRAY_PATH_QNM, NodeKind.ARRAY);

      final SirixDeweyID id = currentKind == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      if (kind != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final NodeKind currentKind = currentNode.getKind();
      final long firstChildKey = currentNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(parentKey, ARRAY_PATH_QNM, NodeKind.ARRAY);

      final SirixDeweyID id = currentKind == NodeKind.OBJECT_KEY
          ? deweyIDManager.newRecordValueID()
          : firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
              ? deweyIDManager.newFirstChildID()
              : deweyIDManager.newLastChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      if (kind != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
        checkPrecondition();
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(rightSibKey, ARRAY_SIBLING_PATH_QNM, NodeKind.ARRAY);
      moveTo(rightSibKey);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
        checkPrecondition();
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(leftSibKey, ARRAY_SIBLING_PATH_QNM, NodeKind.ARRAY);
      moveTo(leftSibKey);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
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
    requireNonNull(value);

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

      adaptUpdateOperationsForReplace(getDeweyID(), oldValueNodeKey, getNodeKey());

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final String value) {
    requireNonNull(value);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, getBytes(value), null, false, true, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsLastChild(final String value) {
    requireNonNull(value);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, getBytes(value), null, false, true, false);
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final byte[] utf8Value) {
    requireNonNull(utf8Value);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, utf8Value, null, false, true, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsLastChild(final byte[] utf8Value) {
    requireNonNull(utf8Value);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, utf8Value, null, false, true, false);
  }

  private long getPathNodeKey(StructNode structNode) {
    final long pathNodeKey;

    if (structNode.getKind() == NodeKind.ARRAY) {
      pathNodeKey = ((ArrayNode) structNode).getPathNodeKey();
    } else if (structNode.getKind() == NodeKind.OBJECT_KEY) {
      pathNodeKey = ((ObjectKeyNode) structNode).getPathNodeKey();
    } else if (structNode.getKind() == NodeKind.JSON_DOCUMENT) {
      pathNodeKey = 0;
    } else {
      pathNodeKey = -1;
    }
    return pathNodeKey;
  }

  private void adaptNodesAndHashesForInsertAsChild(final long nodeKey, final long parentKey,
      final long leftSibKey, final long rightSibKey) {
    // Pass structural keys directly — eliminates moveTo before adaptForInsert.
    // Old code did: moveTo(nodeKey) → adaptForInsert(getStructuralNodeView()) → moveTo(nodeKey) → hash.
    // New code: adaptForInsert(keys) → hash(nodeKey) → moveTo(nodeKey).
    // Net: eliminated 1 moveTo (the first one before adaptForInsert).
    adaptForInsert(nodeKey, parentKey, leftSibKey, rightSibKey, false);
    nodeHashing.adaptHashesWithAdd(nodeKey);
    // Restore cursor to new node only if hashing did not already do so (HashType.NONE path).
    if (nodeReadOnlyTrx.getNodeKey() != nodeKey) {
      nodeReadOnlyTrx.moveTo(nodeKey);
    }
  }

  @Override
  public JsonNodeTrx insertStringValueAsLeftSibling(final String value) {
    requireNonNull(value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, getBytes(value), null, false, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final String value) {
    requireNonNull(value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, getBytes(value), null, false, false);
  }

  @Override
  public JsonNodeTrx insertStringValueAsLeftSibling(final byte[] utf8Value) {
    requireNonNull(utf8Value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, utf8Value, null, false, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final byte[] utf8Value) {
    requireNonNull(utf8Value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, utf8Value, null, false, false);
  }

  @Override
  public JsonNodeTrx insertStringValueAsFirstChild(final byte[] buf, final int off, final int len) {
    requireNonNull(buf);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, buf, off, len, null, false, true, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsLastChild(final byte[] buf, final int off, final int len) {
    requireNonNull(buf);
    return insertPrimitiveAsChild(PrimitiveNodeType.STRING, buf, off, len, null, false, true, false);
  }

  @Override
  public JsonNodeTrx insertStringValueAsLeftSibling(final byte[] buf, final int off, final int len) {
    requireNonNull(buf);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, buf, off, len, null, false, true);
  }

  @Override
  public JsonNodeTrx insertStringValueAsRightSibling(final byte[] buf, final int off, final int len) {
    requireNonNull(buf);
    return insertPrimitiveAsSibling(PrimitiveNodeType.STRING, buf, off, len, null, false, false);
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsFirstChild(boolean value) {
    return insertPrimitiveAsChild(PrimitiveNodeType.BOOLEAN, null, null, value, true, true);
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLastChild(boolean value) {
    return insertPrimitiveAsChild(PrimitiveNodeType.BOOLEAN, null, null, value, true, false);
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsLeftSibling(boolean value) {
    return insertPrimitiveAsSibling(PrimitiveNodeType.BOOLEAN, null, null, value, true);
  }

  @Override
  public JsonNodeTrx insertBooleanValueAsRightSibling(boolean value) {
    return insertPrimitiveAsSibling(PrimitiveNodeType.BOOLEAN, null, null, value, false);
  }

  private void checkPrecondition() {
    if (!nodeHashing.isBulkInsert() && getParentKind() != NodeKind.ARRAY) {
      throw new SirixUsageException(INSERT_NOT_ALLOWED_SINCE_PARENT_NOT_IN_AN_ARRAY_NODE);
    }
  }

  private void insertAsSibling(final long nodeKey, final long parentKey,
      final long leftSibKey, final long rightSibKey) {
    insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey, false);
  }

  private void insertAsSibling(final long nodeKey, final long parentKey,
      final long leftSibKey, final long rightSibKey, final boolean useParentPathNodeKeyIfAvailable) {
    final boolean notifyPrimitiveIndexes = indexController.hasAnyPrimitiveIndex();
    final boolean resolveParentPathNodeKey = useParentPathNodeKeyIfAvailable && notifyPrimitiveIndexes && buildPathSummary;
    final long parentPathNodeKey = adaptForInsert(nodeKey, parentKey, leftSibKey, rightSibKey, resolveParentPathNodeKey);
    nodeHashing.adaptHashesWithAdd(nodeKey);
    if (nodeReadOnlyTrx.getNodeKey() != nodeKey) {
      nodeReadOnlyTrx.moveTo(nodeKey);
    }

    if (notifyPrimitiveIndexes) {
      final long pathNodeKey;
      if (buildPathSummary) {
        if (resolveParentPathNodeKey && parentPathNodeKey != -1) {
          pathNodeKey = parentPathNodeKey;
        } else {
          moveToParentObjectKeyArrayOrDocumentRoot();
          pathNodeKey = getPathNodeKey(nodeReadOnlyTrx.getStructuralNodeView());
          if (nodeReadOnlyTrx.getNodeKey() != nodeKey) {
            nodeReadOnlyTrx.moveTo(nodeKey);
          }
        }
      } else {
        pathNodeKey = 0;
      }
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
          (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(),
          pathNodeKey);
    }
  }

  /**
   * Enumeration of primitive node types to avoid lambda allocation on the insert hot path.
   */
  private enum PrimitiveNodeType {
    STRING, NUMBER, BOOLEAN, NULL
  }

  /**
   * Create an object-key child node (direct child of ObjectKeyNode) based on primitive type.
   * For STRING type, uses (stringValue, stringOff, stringLen); other types ignore off/len.
   */
  private StructNode createObjectKeyNode(final PrimitiveNodeType type, final long parentKey,
      final byte[] stringValue, final int stringOff, final int stringLen,
      final Number numberValue, final boolean booleanValue, final SirixDeweyID id) {
    return switch (type) {
      case STRING -> nodeFactory.createJsonObjectStringNode(parentKey, stringValue, stringOff, stringLen,
          useTextCompression, id);
      case NUMBER -> nodeFactory.createJsonObjectNumberNode(parentKey, numberValue, id);
      case BOOLEAN -> nodeFactory.createJsonObjectBooleanNode(parentKey, booleanValue, id);
      case NULL -> nodeFactory.createJsonObjectNullNode(parentKey, id);
    };
  }

  /**
   * Create a sibling node (child of array) based on primitive type.
   * For STRING type, uses (stringValue, stringOff, stringLen); other types ignore off/len.
   */
  private StructNode createSiblingNode(final PrimitiveNodeType type, final long parentKey,
      final long leftSibKey, final long rightSibKey,
      final byte[] stringValue, final int stringOff, final int stringLen,
      final Number numberValue, final boolean booleanValue, final SirixDeweyID id) {
    return switch (type) {
      case STRING -> nodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey,
          stringValue, stringOff, stringLen, useTextCompression, id);
      case NUMBER -> nodeFactory.createJsonNumberNode(parentKey, leftSibKey, rightSibKey, numberValue, id);
      case BOOLEAN -> nodeFactory.createJsonBooleanNode(parentKey, leftSibKey, rightSibKey, booleanValue, id);
      case NULL -> nodeFactory.createJsonNullNode(parentKey, leftSibKey, rightSibKey, id);
    };
  }

  private JsonNodeTrx insertPrimitiveAsChild(final PrimitiveNodeType type, final byte[] stringValue,
      final Number numberValue, final boolean booleanValue, final boolean notifyIndex, final boolean isFirstChild) {
    return insertPrimitiveAsChild(type, stringValue, 0, stringValue != null ? stringValue.length : 0,
        numberValue, booleanValue, notifyIndex, isFirstChild);
  }

  private JsonNodeTrx insertPrimitiveAsChild(final PrimitiveNodeType type,
      final byte[] stringValue, final int stringOff, final int stringLen,
      final Number numberValue, final boolean booleanValue, final boolean notifyIndex, final boolean isFirstChild) {
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      if (kind != NodeKind.OBJECT_KEY && kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT)
        throw new SirixUsageException("Insert is not allowed if current node is not an object-key- or array-node!");

      if (kind != NodeKind.OBJECT_KEY) {
        if (nodeHashing.isBulkInsert()) {
          checkAccessAndCommitBulk();
        } else {
          checkAccessAndCommit();
        }
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long pathNodeKey = (notifyIndex && indexController.hasAnyPrimitiveIndex())
          ? getPathNodeKey(structNode)
          : 0;
      final long parentKey = structNode.getNodeKey();
      final long firstChildKey = structNode.getFirstChildKey();
      final long lastChildKey = structNode.getLastChildKey();

      final SirixDeweyID id;
      final StructNode node;
      final long leftSibKey;
      final long rightSibKey;
      if (kind == NodeKind.OBJECT_KEY) {
        id = deweyIDManager.newRecordValueID();
        leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = createObjectKeyNode(type, parentKey, stringValue, stringOff, stringLen, numberValue, booleanValue, id);
      } else if (isFirstChild) {
        id = deweyIDManager.newFirstChildID();
        leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        rightSibKey = firstChildKey;
        node = createSiblingNode(type, parentKey, leftSibKey, rightSibKey, stringValue, stringOff, stringLen, numberValue, booleanValue, id);
      } else {
        id = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
            ? deweyIDManager.newFirstChildID()
            : deweyIDManager.newLastChildID();
        leftSibKey = lastChildKey;
        rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        node = createSiblingNode(type, parentKey, leftSibKey, rightSibKey, stringValue, stringOff, stringLen, numberValue, booleanValue, id);
      }

      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (notifyIndex && indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      if (kind != NodeKind.OBJECT_KEY && !nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private JsonNodeTrx insertPrimitiveAsSibling(final PrimitiveNodeType type, final byte[] stringValue,
      final Number numberValue, final boolean booleanValue, final boolean isLeftSibling) {
    return insertPrimitiveAsSibling(type, stringValue, 0, stringValue != null ? stringValue.length : 0,
        numberValue, booleanValue, isLeftSibling);
  }

  private JsonNodeTrx insertPrimitiveAsSibling(final PrimitiveNodeType type,
      final byte[] stringValue, final int stringOff, final int stringLen,
      final Number numberValue, final boolean booleanValue, final boolean isLeftSibling) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
        checkPrecondition();
      }

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long parentKey = currentNode.getParentKey();

      final long leftSibKey;
      final long rightSibKey;
      final SirixDeweyID id;
      if (isLeftSibling) {
        leftSibKey = currentNode.getLeftSiblingKey();
        rightSibKey = currentNode.getNodeKey();
        id = deweyIDManager.newLeftSiblingID();
      } else {
        leftSibKey = currentNode.getNodeKey();
        rightSibKey = currentNode.getRightSiblingKey();
        id = deweyIDManager.newRightSiblingID();
      }

      final StructNode node = createSiblingNode(type, parentKey, leftSibKey, rightSibKey,
          stringValue, stringOff, stringLen, numberValue, booleanValue, id);
      final long nodeKey = node.getNodeKey();

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey, true);

      if (!nodeHashing.isBulkInsert()) {
        adaptUpdateOperationsForInsert(id, nodeKey);
      }

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addToRecordToRevisionsIndex(nodeKey);
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx insertNumberValueAsFirstChild(Number value) {
    requireNonNull(value);
    return insertPrimitiveAsChild(PrimitiveNodeType.NUMBER, null, value, false, true, true);
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLastChild(Number value) {
    requireNonNull(value);
    return insertPrimitiveAsChild(PrimitiveNodeType.NUMBER, null, value, false, true, false);
  }

  @Override
  public JsonNodeTrx insertNumberValueAsLeftSibling(Number value) {
    requireNonNull(value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.NUMBER, null, value, false, true);
  }

  @Override
  public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    return nodeReadOnlyTrx;
  }

  @Override
  public JsonNodeTrx insertNumberValueAsRightSibling(Number value) {
    requireNonNull(value);
    return insertPrimitiveAsSibling(PrimitiveNodeType.NUMBER, null, value, false, false);
  }

  @Override
  public JsonNodeTrx insertNullValueAsFirstChild() {
    return insertPrimitiveAsChild(PrimitiveNodeType.NULL, null, null, false, false, true);
  }

  @Override
  public JsonNodeTrx insertNullValueAsLastChild() {
    return insertPrimitiveAsChild(PrimitiveNodeType.NULL, null, null, false, false, false);
  }

  @Override
  public JsonNodeTrx insertNullValueAsLeftSibling() {
    return insertPrimitiveAsSibling(PrimitiveNodeType.NULL, null, null, false, true);
  }

  @Override
  public JsonNodeTrx insertNullValueAsRightSibling() {
    return insertPrimitiveAsSibling(PrimitiveNodeType.NULL, null, null, false, false);
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
  public JsonNodeTrx moveSubtreeToFirstChild(final long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (fromKey < 0 || fromKey > getMaxNodeKey()) {
        throw new IllegalArgumentException("Argument must be a valid node key!");
      }
      if (fromKey == getNodeKey()) {
        throw new IllegalArgumentException("Can't move itself to first child of itself!");
      }

      final DataRecord node = storageEngineWriter.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist!");
      }

      if (node instanceof StructNode toMove) {
        final NodeKind anchorKind = getKind();
        if (anchorKind != NodeKind.OBJECT && anchorKind != NodeKind.ARRAY
            && anchorKind != NodeKind.OBJECT_KEY && anchorKind != NodeKind.JSON_DOCUMENT) {
          throw new SirixUsageException(
              "Move is not allowed if the anchor node is not an OBJECT, ARRAY, OBJECT_KEY, or JSON_DOCUMENT node!");
        }

        checkMoveAncestors(toMove);
        checkAccessAndCommit();

        final StructNode nodeAnchor = nodeReadOnlyTrx.getStructuralNode();

        if (nodeAnchor.getFirstChildKey() != toMove.getNodeKey()) {
          // Adapt index-structures (before move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.DELETE);

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers (no text node merging for JSON).
          adaptForMove(toMove, nodeAnchor, MovePosition.AS_FIRST_CHILD);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            pathSummaryWriter.adaptPathForChangedNode(moved, getName(), moved.getURIKey(),
                moved.getPrefixKey(), moved.getLocalNameKey(), PathSummaryWriter.OPType.MOVED);
          }

          // Adapt index-structures (after move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.INSERT);

          // Compute and assign new DeweyIDs.
          if (storeDeweyIDs()) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException("Node to move must be a StructNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx moveSubtreeToRightSibling(final long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (fromKey < 0 || fromKey > getMaxNodeKey()) {
        throw new IllegalArgumentException("Argument must be a valid node key!");
      }
      if (fromKey == getNodeKey()) {
        throw new IllegalArgumentException("Can't move itself to right sibling of itself!");
      }

      final DataRecord node = storageEngineWriter.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist: " + fromKey);
      }

      if (node instanceof StructNode toMove) {
        checkMoveAncestors(toMove);
        checkAccessAndCommit();

        final StructNode nodeAnchor = nodeReadOnlyTrx.getStructuralNode();

        if (nodeAnchor.getRightSiblingKey() != toMove.getNodeKey()) {
          final long parentKey = nodeAnchor.getParentKey();

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers (no text node merging for JSON).
          adaptForMove(toMove, nodeAnchor, MovePosition.AS_RIGHT_SIBLING);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            final PathSummaryWriter.OPType type = moved.getParentKey() == parentKey
                ? PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL
                : PathSummaryWriter.OPType.MOVED;

            if (type != PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL) {
              pathSummaryWriter.adaptPathForChangedNode(moved, getName(), moved.getURIKey(),
                  moved.getPrefixKey(), moved.getLocalNameKey(), type);
            }
          }

          // Recompute DeweyIDs if they are used.
          if (storeDeweyIDs()) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException("Node to move must be a StructNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx moveSubtreeToLeftSibling(final long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()) {
        moveToLeftSibling();
        return moveSubtreeToRightSibling(fromKey);
      } else {
        moveToParent();
        return moveSubtreeToFirstChild(fromKey);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Checks that the node to move is not an ancestor of the current node (prevents cycles).
   *
   * @param nodeToMove the node to be moved
   * @throws IllegalStateException if moving an ancestor is detected
   */
  private void checkMoveAncestors(final Node nodeToMove) {
    assert nodeToMove != null;
    final long startNodeKey = getNodeKey();
    while (hasParent()) {
      moveToParent();
      if (getNodeKey() == nodeToMove.getNodeKey()) {
        throw new IllegalStateException("Moving one of the ancestor nodes is not permitted!");
      }
    }
    moveTo(startNodeKey);
  }

  /**
   * Adapt subtree regarding the index-structures for move operations.
   *
   * @param node  node which is moved
   * @param type  the type of change (DELETE from old position or INSERT into new position)
   */
  private void adaptSubtreeForMove(final Node node, final IndexController.ChangeType type) {
    assert type != null;
    final long beforeNodeKey = getNodeKey();
    moveTo(node.getNodeKey());
    final Axis axis = new DescendantAxis(this, IncludeSelf.YES);
    while (axis.hasNext()) {
      axis.nextLong();
      final ImmutableNode currentNode = nodeReadOnlyTrx.getNode();
      long pathNodeKey = -1;
      if (currentNode instanceof ValueNode
          && currentNode.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        final long nodeKey = currentNode.getNodeKey();
        moveToParent();
        pathNodeKey = getPathNodeKey();
        moveTo(nodeKey);
      } else if (currentNode instanceof NameNode nameNode) {
        pathNodeKey = nameNode.getPathNodeKey();
      }
      if (pathNodeKey != -1 && indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(type, currentNode, pathNodeKey);
      }
    }
    moveTo(beforeNodeKey);
  }

  /**
   * Adapt hashes for move operation ("remove" phase).
   *
   * @param nodeToMove node which implements {@link StructNode} and is moved
   */
  private void adaptHashesForMove(final StructNode nodeToMove) {
    assert nodeToMove != null;
    nodeReadOnlyTrx.setCurrentNode((ImmutableJsonNode) nodeToMove);
    nodeHashing.adaptHashesWithRemove();
  }

  /**
   * Insert position for move operations.
   */
  private enum MovePosition {
    AS_FIRST_CHILD,
    AS_RIGHT_SIBLING
  }

  /**
   * Adapts pointers for move operations. JSON-specific: no text node merging.
   *
   * @param fromNode root {@link StructNode} of the subtree to be moved
   * @param toNode   the {@link StructNode} which is the anchor of the new subtree
   * @param pos      determines if it has to be inserted as a first child or a right sibling
   */
  private void adaptForMove(final StructNode fromNode, final StructNode toNode,
      final MovePosition pos) {
    assert fromNode != null;
    assert toNode != null;
    assert pos != null;

    // === Source side: detach fromNode from its current position ===

    // Modify parent's child count.
    final StructNode parent =
        storageEngineWriter.prepareRecordForModification(fromNode.getParentKey(), IndexType.DOCUMENT, -1);
    switch (pos) {
      case AS_RIGHT_SIBLING -> {
        if (fromNode.getParentKey() != toNode.getParentKey() && storeChildCount) {
          parent.decrementChildCount();
        }
      }
      case AS_FIRST_CHILD -> {
        if (fromNode.getParentKey() != toNode.getNodeKey() && storeChildCount) {
          parent.decrementChildCount();
        }
      }
    }
    // Adapt first child key of former parent.
    if (parent.getFirstChildKey() == fromNode.getNodeKey()) {
      parent.setFirstChildKey(fromNode.getRightSiblingKey());
    }
    persistUpdatedRecord(parent);

    // Adapt left sibling key of former right sibling.
    if (fromNode.hasRightSibling()) {
      final StructNode rightSibling =
          storageEngineWriter.prepareRecordForModification(fromNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
      rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
      persistUpdatedRecord(rightSibling);
    }

    // Adapt right sibling key of former left sibling.
    if (fromNode.hasLeftSibling()) {
      final StructNode leftSibling =
          storageEngineWriter.prepareRecordForModification(fromNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
      persistUpdatedRecord(leftSibling);
    }

    // No text node merging for JSON (unlike XML).

    // === Target side: insert fromNode at new position ===
    switch (pos) {
      case AS_FIRST_CHILD -> processMoveAsFirstChild(fromNode, toNode);
      case AS_RIGHT_SIBLING -> processMoveAsRightSibling(fromNode, toNode);
    }
  }

  /**
   * Process moving a node as the first child of the target node.
   */
  private void processMoveAsFirstChild(final StructNode fromNode, final StructNode toNode) {
    // Increment child count of new parent (if not already parent).
    StructNode newParent =
        storageEngineWriter.prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
    if (fromNode.getParentKey() != toNode.getNodeKey() && storeChildCount) {
      newParent.incrementChildCount();
    }

    if (toNode.hasFirstChild()) {
      // Adapt left sibling key of former first child.
      final StructNode oldFirstChild =
          storageEngineWriter.prepareRecordForModification(toNode.getFirstChildKey(), IndexType.DOCUMENT, -1);
      oldFirstChild.setLeftSiblingKey(fromNode.getNodeKey());
      final long oldFirstChildNodeKey = oldFirstChild.getNodeKey();
      persistUpdatedRecord(oldFirstChild);

      // Adapt right sibling key of moved node.
      final StructNode moved =
          storageEngineWriter.prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
      moved.setRightSiblingKey(oldFirstChildNodeKey);
      persistUpdatedRecord(moved);
    } else {
      // Adapt right sibling key of moved node (no siblings).
      final StructNode moved =
          storageEngineWriter.prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
      moved.setRightSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
      persistUpdatedRecord(moved);
    }

    // Set fromNode as new first child of parent.
    newParent = storageEngineWriter.prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
    newParent.setFirstChildKey(fromNode.getNodeKey());
    persistUpdatedRecord(newParent);

    // Adapt left sibling key and parent key of moved node.
    final StructNode moved =
        storageEngineWriter.prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
    moved.setLeftSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    moved.setParentKey(toNode.getNodeKey());
    persistUpdatedRecord(moved);
  }

  /**
   * Process moving a node as the right sibling of the target node.
   */
  private void processMoveAsRightSibling(final StructNode fromNode, final StructNode toNode) {
    // Increment child count of parent if moving between different parents.
    if (fromNode.getParentKey() != toNode.getParentKey() && storeChildCount) {
      final StructNode parentNode =
          storageEngineWriter.prepareRecordForModification(toNode.getParentKey(), IndexType.DOCUMENT, -1);
      parentNode.incrementChildCount();
      persistUpdatedRecord(parentNode);
    }

    // Adapt right sibling key of anchor node.
    final StructNode insertAnchor =
        storageEngineWriter.prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
    final long rightSiblKey = insertAnchor.getRightSiblingKey();
    insertAnchor.setRightSiblingKey(fromNode.getNodeKey());
    persistUpdatedRecord(insertAnchor);

    final long insertAnchorNodeKey = insertAnchor.getNodeKey();

    if (rightSiblKey > -1) {
      // Adapt left sibling key of former right sibling.
      final StructNode oldRightSibling =
          storageEngineWriter.prepareRecordForModification(rightSiblKey, IndexType.DOCUMENT, -1);
      oldRightSibling.setLeftSiblingKey(fromNode.getNodeKey());
      persistUpdatedRecord(oldRightSibling);
    }

    // Adapt right- and left-sibling key of moved node.
    final StructNode movedNode =
        storageEngineWriter.prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
    movedNode.setRightSiblingKey(rightSiblKey);
    movedNode.setLeftSiblingKey(insertAnchorNodeKey);
    persistUpdatedRecord(movedNode);

    // Adapt parent key of moved node.
    final StructNode movedForParent =
        storageEngineWriter.prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
    movedForParent.setParentKey(toNode.getParentKey());
    persistUpdatedRecord(movedForParent);
  }

  @Override
  public JsonNodeTrx remove() {
    checkAccessAndCommit();
    if (lock != null) {
      lock.lock();
    }

    // CRITICAL FIX: Acquire a separate guard on the current node's page
    // to prevent it from being modified/evicted during the PostOrderAxis traversal
    final var nodePageGuard = storageEngineWriter.acquireGuardForCurrentNode();

    try {
      final StructNode node = nodeReadOnlyTrx.getStructuralNode();
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
      for (final var axis = new PostOrderAxis(this); axis.hasNext();) {
        final long currentNodeKey = axis.nextLong();

        // Remove name.
        removeName();

        // Remove text value.
        removeValue();

        // Then remove node.
        storageEngineWriter.removeRecord(currentNodeKey, IndexType.DOCUMENT, -1);

        if (storeNodeHistory) {
          nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(currentNodeKey);
        }
      }

      // Remove the name of subtree-root.
      if (node.getKind() == NodeKind.OBJECT_KEY) {
        removeName();
      } else {
        removeValue();
      }

      // Adapt hashes and neighbour nodes as well as the name from the NamePage mapping if it's not a text
      // node.
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
      // Release the guard on the node's page
      nodePageGuard.close();
    }
  }

  private void adaptUpdateOperationsForRemove(SirixDeweyID id, final long oldNodeKey) {
    moveToNext();
    final var diffTuple = new DiffTuple(DiffFactory.DiffType.DELETED, 0, oldNodeKey, id == null
        ? null
        : new DiffDepth(0, id.getLevel()));
    if (id == null) {
      updateOperationsUnordered.values().removeIf(currDiffTuple -> currDiffTuple.getNewNodeKey() == oldNodeKey);
      updateOperationsUnordered.put(oldNodeKey, diffTuple);
    } else {
      updateOperationsOrdered.values().removeIf(currDiffTuple -> currDiffTuple.getNewNodeKey() == oldNodeKey);
      updateOperationsOrdered.put(id, diffTuple);
    }
    moveTo(oldNodeKey);
  }

  private void notifyPrimitiveIndexChange(final IndexController.ChangeType type, final ImmutableNode node,
      final long pathNodeKey) {
    if (!indexController.hasPathIndex() && !indexController.hasNameIndex() && !indexController.hasCASIndex()) {
      return;
    }

    final NodeKind kind = node.getKind();
    final long nodeKey = node.getNodeKey();

    final QNm name;
    if (kind == NodeKind.OBJECT_KEY && indexController.hasNameIndex() && node instanceof ObjectKeyNode objectKeyNode) {
      // getName() may be null for flyweight-bound nodes (cachedName is a Java field, not in MemorySegment).
      // Fall back to resolving the name from the name key via the storage engine.
      QNm resolvedName = objectKeyNode.getName();
      if (resolvedName == null) {
        final int nameKey = objectKeyNode.getNameKey();
        final String localName = storageEngineWriter.getName(nameKey, NodeKind.OBJECT_KEY);
        if (localName != null) {
          resolvedName = new QNm(localName);
        }
      }
      name = resolvedName;
    } else {
      name = null;
    }

    final Str value;
    if (indexController.hasCASIndex()) {
      value = switch (kind) {
        case STRING_VALUE, OBJECT_STRING_VALUE -> node instanceof ValueNode valueNode
            ? new Str(valueNode.getValue())
            : null;
        case BOOLEAN_VALUE -> node instanceof BooleanNode boolNode
            ? (boolNode.getValue()
                ? STR_TRUE
                : STR_FALSE)
            : null;
        case OBJECT_BOOLEAN_VALUE -> node instanceof ObjectBooleanNode boolNode
            ? (boolNode.getValue()
                ? STR_TRUE
                : STR_FALSE)
            : null;
        case NUMBER_VALUE -> node instanceof NumberNode numberNode
            ? new Str(String.valueOf(numberNode.getValue()))
            : null;
        case OBJECT_NUMBER_VALUE -> node instanceof ObjectNumberNode numberNode
            ? new Str(String.valueOf(numberNode.getValue()))
            : null;
        default -> null;
      };
    } else {
      value = null;
    }

    indexController.notifyChange(type, nodeKey, kind, pathNodeKey, name, value);
  }

  private void removeValue() {
    final NodeKind nodeKind = getKind();
    if (nodeKind == NodeKind.OBJECT_STRING_VALUE || nodeKind == NodeKind.OBJECT_NUMBER_VALUE
        || nodeKind == NodeKind.OBJECT_BOOLEAN_VALUE || nodeKind == NodeKind.STRING_VALUE
        || nodeKind == NodeKind.NUMBER_VALUE || nodeKind == NodeKind.BOOLEAN_VALUE) {
      final long nodeKey = getNodeKey();
      final StructNode currentValueNode = nodeReadOnlyTrx.getStructuralNode();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(nodeReadOnlyTrx.getStructuralNode());

      moveTo(nodeKey);
      // Pass the VALUE node, not the parent node - CAS index needs the value to extract.
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) currentValueNode, pathNodeKey);
    }
  }

  /**
   * Remove a name from the {@link NamePage} reference and the path summary if needed.
   *
   * @throws SirixException if Sirix fails
   */
  private void removeName() {
    if (getKind() == NodeKind.OBJECT_KEY) {
      final ObjectKeyNode node = (ObjectKeyNode) nodeReadOnlyTrx.getStructuralNode();
      // Ensure the name is resolved for index listener (ObjectKeyNode may have null cachedName when
      // loaded from disk)
      if (node.getName() == null) {
        final String resolvedName = storageEngineWriter.getName(node.getLocalNameKey(), node.getKind());
        if (resolvedName != null) {
          node.setName(resolvedName);
        }
      }

      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, node.getPathNodeKey());
      final NodeKind nodeKind = node.getKind();
      final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      page.removeName(node.getLocalNameKey(), nodeKind, storageEngineWriter);

      assert nodeKind != NodeKind.JSON_DOCUMENT;
      if (buildPathSummary) {
        pathSummaryWriter.remove(node);
      }
    }
  }

  @Override
  public JsonNodeTrx setObjectKeyName(final String key) {
    requireNonNull(key);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getKind() != NodeKind.OBJECT_KEY)
        throw new SirixUsageException("Not allowed if current node is not an object key node!");
      checkAccessAndCommit();

      ObjectKeyNode node = (ObjectKeyNode) nodeReadOnlyTrx.getStructuralNode();
      final long oldHash = node.computeHash(bytes);

      // Remove old keys from mapping.
      final NodeKind nodeKind = node.getKind();
      final int oldNameKey = node.getNameKey();
      final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      page.removeName(oldNameKey, nodeKind, storageEngineWriter);

      // Create new key for mapping.
      final int newNameKey = storageEngineWriter.createNameKey(key, node.getKind());

      // Mutate current singleton-backed node directly.
      node.setNameKey(newNameKey);
      node.setName(key);
      node.setPreviousRevision(storageEngineWriter.getRevisionToRepresent());

      // Adapt path summary.
      if (buildPathSummary) {
        pathSummaryWriter.adaptPathForChangedNode(node, new QNm(key), -1, -1, newNameKey, OPType.SETNAME);
      }

      // Set path node key.
      node.setPathNodeKey(buildPathSummary
          ? pathSummaryWriter.getNodeKey()
          : 0);

      nodeReadOnlyTrx.setCurrentNode(node);
      persistUpdatedRecord(node);
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
    final var diffTuple = new DiffTuple(DiffFactory.DiffType.UPDATED, nodeKey, nodeKey, id == null
        ? null
        : new DiffDepth(id.getLevel(), id.getLevel()));
    if (id == null && updateOperationsUnordered.get(nodeKey) == null) {
      updateOperationsUnordered.put(nodeKey, diffTuple);
    } else if (id != null && hasNoUpdatingNodeWithGivenNodeKey(nodeKey)) {
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
    return currDiffTuple -> (currDiffTuple.getNewNodeKey() == nodeKey
        && currDiffTuple.getDiff() == DiffFactory.DiffType.INSERTED)
        || (currDiffTuple.getOldNodeKey() == nodeKey && currDiffTuple.getDiff() == DiffFactory.DiffType.DELETED);
  }

  @Override
  public JsonNodeTrx setStringValue(final String value) {
    requireNonNull(value);
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

      final ValueNode node = (ValueNode) nodeReadOnlyTrx.getStructuralNode();
      // Remove old value from indexes before mutating the node.
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);
      final long oldHash = node.computeHash(bytes);
      final byte[] byteVal = getBytes(value);
      node.setRawValue(byteVal);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      persistUpdatedRecord(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, (ImmutableNode) node, pathNodeKey);

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

      final BooleanValueNode node = (BooleanValueNode) nodeReadOnlyTrx.getStructuralNode();
      // Remove old value from indexes before mutating the node.
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);
      final long oldHash = node.computeHash(bytes);
      node.setValue(value);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      persistUpdatedRecord(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, (ImmutableNode) node, pathNodeKey);

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
    requireNonNull(value);
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

      final NumericValueNode node = (NumericValueNode) nodeReadOnlyTrx.getStructuralNode();
      // Remove old value from indexes before mutating the node.
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);
      final long oldHash = node.computeHash(bytes);
      node.setValue(value);
      node.setPreviousRevision(node.getLastModifiedRevisionNumber());
      node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

      nodeReadOnlyTrx.setCurrentNode(node);
      persistUpdatedRecord(node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      // Index new value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, (ImmutableNode) node, pathNodeKey);

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
   * Adapting everything for insert operations. Accepts the new node's structural keys directly
   * to avoid a moveTo call — the caller already knows these values from the factory call.
   *
   * @param structNodeKey the new node's key
   * @param parentKey     the new node's parent key
   * @param leftSibKey    the new node's left sibling key
   * @param rightSibKey   the new node's right sibling key
   * @param resolveParentPathNodeKey whether to resolve and return the parent path node key
   * @return the parent path node key if available, otherwise {@code -1}
   * @throws SirixIOException if anything weird happens
   */
  private long adaptForInsert(final long structNodeKey, final long parentKey,
      final long leftSibKey, final long rightSibKey, final boolean resolveParentPathNodeKey) {
    final boolean hasLeft = leftSibKey != Fixed.NULL_NODE_KEY.getStandardProperty();
    final boolean hasRight = rightSibKey != Fixed.NULL_NODE_KEY.getStandardProperty();

    // Phase 1: Update parent — childCount + firstChild/lastChild if no siblings.
    // Complete all parent modifications BEFORE acquiring any sibling singletons.
    final StructNode parent = storageEngineWriter.prepareRecordForModificationDocument(parentKey);
    final long parentPathNodeKey = resolveParentPathNodeKey
        ? getPathNodeKey(parent)
        : -1;
    if (storeChildCount) {
      parent.incrementChildCount();
    }
    if (!hasLeft) {
      parent.setFirstChildKey(structNodeKey);
    }
    if (!hasRight) {
      parent.setLastChildKey(structNodeKey);
    }
    // No persistUpdatedRecord — bound write singletons mutate directly on heap

    // Phase 2: Update left sibling
    if (hasLeft) {
      final StructNode leftSib = storageEngineWriter.prepareRecordForModificationDocument(leftSibKey);
      leftSib.setRightSiblingKey(structNodeKey);
    }

    // Phase 3: Update right sibling
    if (hasRight) {
      final StructNode rightSib = storageEngineWriter.prepareRecordForModificationDocument(rightSibKey);
      rightSib.setLeftSiblingKey(structNodeKey);
    }

    return parentPathNodeKey;
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
    // Capture all needed values from oldNode before any prepareRecordForModification calls.
    // With write-path singletons, subsequent calls for the same kind would overwrite the singleton.
    final long leftSibKey = oldNode.getLeftSiblingKey();
    final long rightSibKey = oldNode.getRightSiblingKey();
    final long parentKey = oldNode.getParentKey();
    final boolean hasLeft = oldNode.hasLeftSibling();
    final boolean hasRight = oldNode.hasRightSibling();

    // Phase 1: Adapt left sibling node if there is one.
    if (hasLeft) {
      final StructNode leftSibling = storageEngineWriter.prepareRecordForModification(leftSibKey, IndexType.DOCUMENT, -1);
      leftSibling.setRightSiblingKey(rightSibKey);
      persistUpdatedRecord(leftSibling);
    }

    // Phase 2: Adapt right sibling node if there is one.
    if (hasRight) {
      final StructNode rightSibling = storageEngineWriter.prepareRecordForModification(rightSibKey, IndexType.DOCUMENT, -1);
      rightSibling.setLeftSiblingKey(leftSibKey);
      persistUpdatedRecord(rightSibling);
    }

    // Phase 3: Adapt parent
    final StructNode parent = storageEngineWriter.prepareRecordForModification(parentKey, IndexType.DOCUMENT, -1);
    if (!hasLeft) {
      parent.setFirstChildKey(rightSibKey);
    }
    if (!hasRight) {
      parent.setLastChildKey(leftSibKey);
    }
    if (storeChildCount) {
      parent.decrementChildCount();
    }
    persistUpdatedRecord(parent);

    // Remove non-structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      // removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    storageEngineWriter.removeRecord(oldNode.getNodeKey(), IndexType.DOCUMENT, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  @Override
  protected void serializeUpdateDiffs(final int revisionNumber) {
    if (!nodeHashing.isBulkInsert() && revisionNumber - 1 > 0) {
      // Determine the old revision number for the diff:
      // - After bulk insert with auto-commit, use the pre-bulk-insert revision
      // - Otherwise, use the previous revision
      final int oldRevisionNumber = beforeBulkInsertionRevisionNumber != 0 && isAutoCommitting
          ? beforeBulkInsertionRevisionNumber
          : revisionNumber - 1;

      final var diffSerializer = new JsonDiffSerializer(this.databaseName, (JsonResourceSession) resourceSession,
          oldRevisionNumber, revisionNumber, storeDeweyIDs()
              ? updateOperationsOrdered.values()
              : updateOperationsUnordered.values());
      final var jsonDiff = diffSerializer.serialize(false);

      // Use the same old revision number for the file name as for the diff content
      final Path diff = resourceSession.getResourceConfig()
                                       .getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                                       .resolve("diffFromRev" + oldRevisionNumber + "toRev" + revisionNumber + ".json");
      try {
        Files.writeString(diff, jsonDiff, CREATE);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }

      // Reset beforeBulkInsertionRevisionNumber after writing the diff file
      // so that subsequent commits use the normal previous revision
      if (beforeBulkInsertionRevisionNumber != 0) {
        beforeBulkInsertionRevisionNumber = 0;
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
  protected AbstractNodeHashing<ImmutableNode, JsonNodeReadOnlyTrx> reInstantiateNodeHashing(
      StorageEngineWriter storageEngineWriter) {
    return new JsonNodeHashing(resourceSession.getResourceConfig(), nodeReadOnlyTrx, storageEngineWriter);
  }

  @Override
  protected JsonNodeFactory reInstantiateNodeFactory(StorageEngineWriter storageEngineWriter) {
    final var factory = new JsonNodeFactoryImpl(hashFunction, storageEngineWriter);
    wireWriteSingletonBinder(factory, storageEngineWriter);
    return factory;
  }

  private static void wireWriteSingletonBinder(final JsonNodeFactoryImpl factory,
      final StorageEngineWriter storageEngineWriter) {
    storageEngineWriter.setWriteSingletonBinder(factory::bindWriteSingleton);
  }

  @Override
  public JsonNodeTrx copySubtreeAsFirstChild(final JsonNodeReadOnlyTrx rtx) {
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
      copy(rtx, InsertPosition.AS_FIRST_CHILD);
      moveTo(nodeKey);
      moveToFirstChild();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx copySubtreeAsLeftSibling(final JsonNodeReadOnlyTrx rtx) {
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
      copy(rtx, InsertPosition.AS_LEFT_SIBLING);
      moveTo(nodeKey);
      moveToFirstChild();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public JsonNodeTrx copySubtreeAsRightSibling(final JsonNodeReadOnlyTrx rtx) {
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
      copy(rtx, InsertPosition.AS_RIGHT_SIBLING);
      moveTo(nodeKey);
      moveToRightSibling();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Helper method for copy-operations.
   *
   * @param trx the source {@link JsonNodeReadOnlyTrx}
   * @param insert the insertion strategy
   * @throws SirixException if anything fails in sirix
   */
  private void copy(final JsonNodeReadOnlyTrx trx, final InsertPosition insert) {
    assert trx != null;
    assert insert != null;
    final JsonNodeReadOnlyTrx rtx = trx.getResourceSession().beginNodeReadOnlyTrx(trx.getRevisionNumber());
    assert rtx.getRevisionNumber() == trx.getRevisionNumber();
    rtx.moveTo(trx.getNodeKey());
    assert rtx.getNodeKey() == trx.getNodeKey();
    if (rtx.getKind() == NodeKind.JSON_DOCUMENT) {
      rtx.moveToFirstChild();
    }

    final NodeKind kind = rtx.getKind();
    switch (kind) {
      case NULL_VALUE -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertNullValueAsFirstChild();
          case AS_LEFT_SIBLING -> insertNullValueAsLeftSibling();
          case AS_RIGHT_SIBLING -> insertNullValueAsRightSibling();
          default -> throw new IllegalStateException();
        }
      }
      case STRING_VALUE -> {
        final String textValue = rtx.getValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertStringValueAsFirstChild(textValue);
          case AS_LEFT_SIBLING -> insertStringValueAsLeftSibling(textValue);
          case AS_RIGHT_SIBLING -> insertStringValueAsRightSibling(textValue);
          default -> throw new IllegalStateException();
        }
      }
      case BOOLEAN_VALUE -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertBooleanValueAsFirstChild(rtx.getBooleanValue());
          case AS_LEFT_SIBLING -> insertBooleanValueAsLeftSibling(rtx.getBooleanValue());
          case AS_RIGHT_SIBLING -> insertBooleanValueAsRightSibling(rtx.getBooleanValue());
          default -> throw new IllegalStateException();
        }
      }
      case NUMBER_VALUE -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertNumberValueAsFirstChild(rtx.getNumberValue());
          case AS_LEFT_SIBLING -> insertNumberValueAsLeftSibling(rtx.getNumberValue());
          case AS_RIGHT_SIBLING -> insertNumberValueAsRightSibling(rtx.getNumberValue());
          default -> throw new IllegalStateException();
        }
      }
      // $CASES-OMITTED$
      default -> throw new UnsupportedOperationException(
          "Copying complex JSON node kinds (OBJECT, ARRAY, OBJECT_KEY) via copySubtree* "
              + "is not yet implemented. Node kind: " + kind);
    }
    rtx.close();
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
