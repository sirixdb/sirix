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
import io.sirix.access.trx.node.AbstractNodeReadOnlyTrx;
import io.sirix.access.trx.node.AbstractNodeTrxImpl;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.access.trx.node.RecordToRevisionsIndex;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.ByteStringValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
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
import io.sirix.node.interfaces.immutable.ImmutableNameNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.page.NamePage;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import io.sirix.service.json.shredder.JsonItemShredder;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

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
   * @return {@code true} when the cursor is positioned on the synthetic primitive-value child of
   *         a fused {@code OBJECT_NAMED_*} record. The concrete class is probed because the
   *         narrower read-only interface does not expose the flag.
   */
  private boolean isFusedSyntheticChildCursor() {
    return nodeReadOnlyTrx instanceof AbstractNodeReadOnlyTrx<?, ?, ?> anrt
        && anrt.isFusedSyntheticChild();
  }

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
   * Interning cache for object-key names encountered during inserts. Typical JSON schemas
   * repeat a small set of field names across millions of records; allocating a fresh QNm
   * for every insert wastes tens of millions of objects and gave path summary lookup a
   * measurable QNm-construction overhead on the shred hot path.
   */
  private final java.util.HashMap<String, QNm> nameToQNm = new java.util.HashMap<>();

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
      @Nullable final PathSummaryWriter<JsonNodeReadOnlyTrx> pathSummaryWriter, final int maxNodeCount,
      @Nullable final Lock transactionLock, final Duration afterCommitDelay, final JsonNodeHashing nodeHashing,
      final JsonNodeFactory nodeFactory, final AfterCommitState afterCommitState,
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
            // play
            // the OBJECT/ARRAY role under fusion, so admit them as valid first/last-child
            // anchor points alongside their non-fused counterparts.
            if (nodeKind != NodeKind.JSON_DOCUMENT && nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.OBJECT
                && nodeKind != NodeKind.OBJECT_NAMED_OBJECT
                && nodeKind != NodeKind.OBJECT_NAMED_ARRAY) {
              throw new IllegalStateException(
                  "Current node must either be the document root, an array or an object key.");
            }
            if (inputShape.isObject()) {
              if (nodeKind == NodeKind.OBJECT || nodeKind == NodeKind.OBJECT_NAMED_OBJECT) {
                skipRootJsonToken = SkipRootToken.YES;
              }
            } else if (inputShape.isArray()) {
              if (nodeKind != NodeKind.ARRAY && nodeKind != NodeKind.JSON_DOCUMENT
                  && nodeKind != NodeKind.OBJECT_NAMED_ARRAY) {
                throw new IllegalStateException("Current node in storage must be an array node.");
              }
            }
          }
          case AS_LEFT_SIBLING, AS_RIGHT_SIBLING -> {
            if (checkParentNode == CheckParentNode.YES) {
              final NodeKind parentKind = getParentKind();
              if (parentKind != NodeKind.ARRAY && parentKind != NodeKind.OBJECT_NAMED_ARRAY) {
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

      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_NAMED_OBJECT && kind != NodeKind.ARRAY
          && kind != NodeKind.OBJECT_NAMED_ARRAY)
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
      final long rightSibKey = kind == NodeKind.OBJECT_NAMED_OBJECT
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : structNode.getFirstChildKey();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_NAMED_OBJECT
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (kind != NodeKind.OBJECT_NAMED_OBJECT && !nodeHashing.isBulkInsert()) {
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

      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_NAMED_OBJECT && kind != NodeKind.ARRAY
          && kind != NodeKind.OBJECT_NAMED_ARRAY)
        throw new SirixUsageException(
            "Insert is not allowed if current node is not the document-, an object key- or a json array node!");

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = kind == NodeKind.OBJECT_NAMED_OBJECT
          ? Fixed.NULL_NODE_KEY.getStandardProperty()
          : structNode.getLastChildKey();
      final long rightSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long firstChildKey = structNode.getFirstChildKey();

      final SirixDeweyID id = structNode.getKind() == NodeKind.OBJECT_NAMED_OBJECT
          ? deweyIDManager.newRecordValueID()
          : (firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
              ? deweyIDManager.newFirstChildID()
              : deweyIDManager.newLastChildID());

      final ObjectNode node = nodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (kind != NodeKind.OBJECT_NAMED_OBJECT && !nodeHashing.isBulkInsert()) {
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
        final NodeKind pk = getParentKind();
        if (pk != NodeKind.ARRAY && pk != NodeKind.OBJECT_NAMED_ARRAY) {
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
        final NodeKind pk = getParentKind();
        if (pk != NodeKind.ARRAY && pk != NodeKind.OBJECT_NAMED_ARRAY) {
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
    // Primitive-valued fields are emitted as a single fused OBJECT_NAMED_* record. Legacy
    // OBJECT_*_VALUE child kinds were removed in iter#32, so the OBJECT_KEY + primitive-child
    // emission is no longer reachable on disk — route to the fused entry point instead.
    if (isPrimitiveValueKind(value.getKind())) {
      return insertObjectRecordWithPrimitiveAsFirstChild(key, value);
    }
    // P2: Object/Array-valued fields emit a single fused OBJECT_NAMED_OBJECT/ARRAY record
    // (kindIds 52/53). The fused record IS the OBJECT_KEY+OBJECT (or +ARRAY) pair. After this
    // returns, the cursor is positioned ON the fused parent — its inner fields will be inserted
    // as its first child via subsequent shredder calls.
    if (value.getKind() == NodeKind.OBJECT) {
      return insertObjectRecordStructuralAsFirstChild(key, NodeKind.OBJECT);
    }
    if (value.getKind() == NodeKind.ARRAY) {
      return insertObjectRecordStructuralAsFirstChild(key, NodeKind.ARRAY);
    }
    throw new IllegalArgumentException("Unexpected ObjectRecordValue kind: " + value.getKind());
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLastChild(final String key, final ObjectRecordValue<?> value) {
    requireNonNull(key);
    if (isPrimitiveValueKind(value.getKind())) {
      return insertObjectRecordWithPrimitiveAsLastChild(key, value);
    }
    if (value.getKind() == NodeKind.OBJECT) {
      return insertObjectRecordStructuralAsLastChild(key, NodeKind.OBJECT);
    }
    if (value.getKind() == NodeKind.ARRAY) {
      return insertObjectRecordStructuralAsLastChild(key, NodeKind.ARRAY);
    }
    throw new IllegalArgumentException("Unexpected ObjectRecordValue kind: " + value.getKind());
  }

  /**
   * P2 fused-structural emission: insert a single OBJECT_NAMED_OBJECT (kindId 52) or
   * OBJECT_NAMED_ARRAY (kindId 53) record as the first child of the current OBJECT cursor.
   *
   * <p>The fused record IS the legacy OBJECT_KEY + OBJECT/ARRAY pair collapsed into one slot.
   * After return the cursor is positioned ON the fused parent so subsequent inner-field inserts
   * land as its first child.
   *
   * @param key      the field name
   * @param valueKind {@link NodeKind#OBJECT} or {@link NodeKind#ARRAY} — selects 52 vs 53
   */
  private JsonNodeTrx insertObjectRecordStructuralAsFirstChild(final String key, final NodeKind valueKind) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      // OBJECT_NAMED_OBJECT IS the parent OBJECT under fusion — accept it as valid cursor.
      if (kind != NodeKind.OBJECT && kind != NodeKind.OBJECT_NAMED_OBJECT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");
      }
      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long objectKeyPathNodeKey = getPathNodeKey(parentKey, key, NodeKind.OBJECT_NAMED_OBJECT);
      // OBJECT_NAMED_ARRAY plays both OBJECT_KEY and ARRAY roles.
      // The path summary still needs the anonymous-array (`__array__/ARRAY`) layer so user paths
      // like `/features/[]/...` resolve. Anchor the fused node's pathNodeKey at the ARRAY layer
      // so child fields nest below it correctly.
      final long pathNodeKey = (valueKind == NodeKind.ARRAY && buildPathSummary)
          ? pathSummaryWriter.getArrayChildPathNodeKey(objectKeyPathNodeKey)
          : objectKeyPathNodeKey;
      final SirixDeweyID id = deweyIDManager.newFirstChildID();

      final long nodeKey;
      if (valueKind == NodeKind.OBJECT) {
        final ObjectNamedObjectNode node = nodeFactory.createJsonObjectNamedObjectNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      } else {
        final ObjectNamedArrayNode node = nodeFactory.createJsonObjectNamedArrayNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      }

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      moveTo(nodeKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

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

  /**
   * P2 fused-structural emission: insert as last child. Walks to existing last child (when
   * present) and emits the fused structural as its right sibling; falls back to first-child
   * insertion when the object is empty.
   */
  private JsonNodeTrx insertObjectRecordStructuralAsLastChild(final String key, final NodeKind valueKind) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      if (kind != NodeKind.OBJECT && kind != NodeKind.OBJECT_NAMED_OBJECT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");
      }
      final StructNode obj = nodeReadOnlyTrx.getStructuralNodeView();
      final long lastChildKey = obj.getLastChildKey();
      if (lastChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        return insertObjectRecordStructuralAsFirstChild(key, valueKind);
      }
      moveTo(lastChildKey);
      return insertObjectRecordStructuralAsRightSibling(key, valueKind);
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
    // The fused-replace path (remove + insertObjectRecordAs* + REPLACEDNEW) leaves a stray
    // DELETED tuple for {@code oldNodeKey} from the inner remove(). The REPLACE diff already
    // captures the old → new transition; downstream replay ({@link
    // io.sirix.service.json.shredder.JsonResourceCopy#executeReplace}) only needs the REPLACE
    // entry, otherwise it would receive a no-op DELETE on the just-replaced node and surface
    // it as a phantom delete in the diff JSON.
    if (id == null) {
      updateOperationsUnordered.values().removeIf(t ->
          t.getDiff() == DiffFactory.DiffType.DELETED && t.getOldNodeKey() == oldNodeKey);
      updateOperationsUnordered.put(newNodeKey,
          new DiffTuple(DiffFactory.DiffType.REPLACEDNEW, newNodeKey, oldNodeKey, null));
    } else {
      updateOperationsOrdered.values().removeIf(t ->
          t.getDiff() == DiffFactory.DiffType.DELETED && t.getOldNodeKey() == oldNodeKey);
      updateOperationsOrdered.put(id, new DiffTuple(DiffFactory.DiffType.REPLACEDNEW, newNodeKey, oldNodeKey,
          new DiffDepth(id.getLevel(), id.getLevel())));
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
    // Intern QNm per unique String name — hot during shred, cache keeps the
    // per-insert allocation out of the fast path.
    QNm qnm = nameToQNm.get(name);
    if (qnm == null) {
      qnm = new QNm(name);
      nameToQNm.put(name, qnm);
    }
    return getPathNodeKey(restoreNodeKey, qnm, kind);
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
    // play the
    // OBJECT_KEY+OBJECT (or ARRAY) role under fusion — stop walking up at them so the
    // path-summary writer anchors child path nodes at the correct ancestor.
    while (nodeKind != NodeKind.OBJECT_NAMED_OBJECT
        && nodeKind != NodeKind.OBJECT_NAMED_OBJECT
        && nodeKind != NodeKind.OBJECT_NAMED_ARRAY
        && nodeKind != NodeKind.ARRAY
        && nodeKind != NodeKind.JSON_DOCUMENT) {
      nodeReadOnlyTrx.moveToParent();
      nodeKind = nodeReadOnlyTrx.getKind();
    }
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsLeftSibling(final String key, final ObjectRecordValue<?> value) {
    requireNonNull(key);
    if (isPrimitiveValueKind(value.getKind())) {
      return insertObjectRecordWithPrimitiveAsLeftSibling(key, value);
    }
    if (value.getKind() == NodeKind.OBJECT) {
      return insertObjectRecordStructuralAsLeftSibling(key, NodeKind.OBJECT);
    }
    if (value.getKind() == NodeKind.ARRAY) {
      return insertObjectRecordStructuralAsLeftSibling(key, NodeKind.ARRAY);
    }
    throw new IllegalArgumentException("Unexpected ObjectRecordValue kind: " + value.getKind());
  }

  @Override
  public JsonNodeTrx insertObjectRecordAsRightSibling(final String key, final ObjectRecordValue<?> value) {
    requireNonNull(key);
    if (isPrimitiveValueKind(value.getKind())) {
      return insertObjectRecordWithPrimitiveAsRightSibling(key, value);
    }
    if (value.getKind() == NodeKind.OBJECT) {
      return insertObjectRecordStructuralAsRightSibling(key, NodeKind.OBJECT);
    }
    if (value.getKind() == NodeKind.ARRAY) {
      return insertObjectRecordStructuralAsRightSibling(key, NodeKind.ARRAY);
    }
    throw new IllegalArgumentException("Unexpected ObjectRecordValue kind: " + value.getKind());
  }

  /**
   * P2 fused-structural left-sibling emission. Cursor must currently be on an OBJECT_KEY or
   * any OBJECT_NAMED_* (primitive or structural) record.
   */
  private JsonNodeTrx insertObjectRecordStructuralAsLeftSibling(final String key, final NodeKind valueKind) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      if (!kind.playsObjectKeyRole()) {
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");
      }
      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long parentKey = currentNode.getParentKey();
      final long rightSibKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();

      moveToParent();
      final long objectKeyPathNodeKey = getPathNodeKey(rightSibKey, key, NodeKind.OBJECT_NAMED_OBJECT);
      // array-valued field needs the `__array__/ARRAY` path-summary layer
      // anchored under the OBJECT_KEY layer the field name created. Anchor the fused node's
      // pathNodeKey at the ARRAY layer so child fields (`/foo/[]/bar`) resolve correctly.
      final long pathNodeKey = (valueKind == NodeKind.ARRAY && buildPathSummary)
          ? pathSummaryWriter.getArrayChildPathNodeKey(objectKeyPathNodeKey)
          : objectKeyPathNodeKey;
      moveTo(rightSibKey);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();

      final long nodeKey;
      if (valueKind == NodeKind.OBJECT) {
        final ObjectNamedObjectNode node = nodeFactory.createJsonObjectNamedObjectNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      } else {
        final ObjectNamedArrayNode node = nodeFactory.createJsonObjectNamedArrayNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      }

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      moveTo(nodeKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

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

  /**
   * P2 fused-structural right-sibling emission. Cursor must currently be on an OBJECT_KEY or
   * any OBJECT_NAMED_* (primitive or structural) record.
   */
  private JsonNodeTrx insertObjectRecordStructuralAsRightSibling(final String key, final NodeKind valueKind) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      if (!kind.playsObjectKeyRole()) {
        throw new SirixUsageException("Insert is not allowed if current node is not an object key node!");
      }
      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long objectKeyPathNodeKey = getPathNodeKey(leftSibKey, key, NodeKind.OBJECT_NAMED_OBJECT);
      // array-valued field needs the `__array__/ARRAY` path-summary layer
      // anchored under the OBJECT_KEY layer the field name created.
      final long pathNodeKey = (valueKind == NodeKind.ARRAY && buildPathSummary)
          ? pathSummaryWriter.getArrayChildPathNodeKey(objectKeyPathNodeKey)
          : objectKeyPathNodeKey;
      moveTo(leftSibKey);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final long nodeKey;
      if (valueKind == NodeKind.OBJECT) {
        final ObjectNamedObjectNode node = nodeFactory.createJsonObjectNamedObjectNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      } else {
        final ObjectNamedArrayNode node = nodeFactory.createJsonObjectNamedArrayNode(parentKey,
            leftSibKey, rightSibKey, pathNodeKey, key, id);
        nodeKey = node.getNodeKey();
      }

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      moveTo(nodeKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

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
  public JsonNodeTrx insertObjectRecordWithPrimitiveAsFirstChild(final String key,
      final ObjectRecordValue<?> value) {
    requireNonNull(key);
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      // P2: OBJECT_NAMED_OBJECT (fused structural) acts as an object for child-insertion. The
      // legacy two-record (OBJECT_KEY+OBJECT) shape is collapsed into a single record; that
      // single record IS the parent object for inner fields.
      if (kind != NodeKind.OBJECT && kind != NodeKind.OBJECT_NAMED_OBJECT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an object node!");
      }

      checkAccessAndCommit();

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = structNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = structNode.getFirstChildKey();

      final long pathNodeKey = getPathNodeKey(parentKey, key, NodeKind.OBJECT_NAMED_OBJECT);

      final SirixDeweyID id = deweyIDManager.newFirstChildID();

      final long nodeKey = createFusedObjectNamedNode(key, value, parentKey, leftSibKey, rightSibKey,
          pathNodeKey, id);

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      // Fused records carry the inline primitive on the same slot; path-statistics must observe
      // the value exactly like the non-fused insertPrimitiveAsChild path does.
      recordFusedPrimitiveStat(pathNodeKey, value, nodeKey);

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
  public JsonNodeTrx insertObjectRecordWithPrimitiveAsRightSibling(final String key,
      final ObjectRecordValue<?> value) {
    requireNonNull(key);
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();

      // Accept any record that plays the OBJECT_KEY role (legacy OBJECT_KEY OR any fused
      // OBJECT_NAMED_* — primitive 48-51 OR structural 52/53). Sibling-insertion semantics
      // are identical because all of these are physically a single record under one OBJECT.
      if (!kind.playsObjectKeyRole()) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an object-key or named object-primitive node!");
      }

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();

      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(leftSibKey, key, NodeKind.OBJECT_NAMED_OBJECT);
      moveTo(leftSibKey);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();

      final long nodeKey = createFusedObjectNamedNode(key, value, parentKey, leftSibKey, rightSibKey,
          pathNodeKey, id);

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      // Fused records carry the inline primitive on the same slot; path-statistics must observe
      // the value exactly like the non-fused insertPrimitiveAsSibling path does.
      recordFusedPrimitiveStat(pathNodeKey, value, nodeKey);

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

  /**
   * Record a path-statistics observation for the inline primitive payload of a freshly inserted
   * fused {@code OBJECT_NAMED_*} record. No-op when stats are disabled, the path node key is
   * non-positive, or the {@link PathSummaryWriter} is null.
   *
   * <p>Mirrors {@link #recordPrimitiveStat} for the non-fused path: fusion keeps the same logical
   * value-observation semantics, only the physical node count is halved.</p>
   */
  private void recordFusedPrimitiveStat(final long pathNodeKey, final ObjectRecordValue<?> value,
      final long valueNodeKey) {
    if (pathSummaryWriter == null || !pathSummaryWriter.isPathStatisticsEnabled() || pathNodeKey <= 0) {
      return;
    }
    final NodeKind valueKind = value.getKind();
    switch (valueKind) {
      case STRING_VALUE -> {
        final byte[] rawValue;
        if (value instanceof ByteStringValue bsv) {
          // Slice exactly the [off, off+len) window of the (possibly reusable) buffer
          // so that path-stats observes the actual UTF-8 payload, never a stale tail.
          final int off = bsv.getOffset();
          final int len = bsv.getLength();
          if (off == 0 && len == bsv.getValue().length) {
            rawValue = bsv.getValue();
          } else {
            rawValue = new byte[len];
            System.arraycopy(bsv.getValue(), off, rawValue, 0, len);
          }
        } else {
          final Object raw = value.getValue();
          if (raw instanceof byte[] utf8) {
            rawValue = utf8;
          } else if (raw instanceof String s) {
            rawValue = s.getBytes(Constants.DEFAULT_ENCODING);
          } else {
            return;
          }
        }
        pathSummaryWriter.recordValue(pathNodeKey, rawValue, valueNodeKey);
      }
      case NUMBER_VALUE -> {
        final Object raw = value.getValue();
        if (raw instanceof Number n) {
          pathSummaryWriter.recordValue(pathNodeKey, n.longValue(), valueNodeKey);
        }
      }
      case BOOLEAN_VALUE -> {
        final Object raw = value.getValue();
        if (raw instanceof Boolean b) {
          pathSummaryWriter.recordBooleanValue(pathNodeKey, b, valueNodeKey);
        }
      }
      case NULL_VALUE -> pathSummaryWriter.recordNullValue(pathNodeKey, valueNodeKey);
      default -> { /* non-primitive payload: shouldn't reach the fused path */ }
    }
  }

  /**
   * True for object-record values whose payload is a primitive ({@link NodeKind#STRING_VALUE},
   * {@link NodeKind#NUMBER_VALUE}, {@link NodeKind#BOOLEAN_VALUE}, {@link NodeKind#NULL_VALUE}).
   * Drives fused OBJECT_NAMED_* dispatch in the public {@code insertObjectRecordAs*} entry points.
   */
  private static boolean isPrimitiveValueKind(final NodeKind valueKind) {
    return valueKind == NodeKind.STRING_VALUE
        || valueKind == NodeKind.NUMBER_VALUE
        || valueKind == NodeKind.BOOLEAN_VALUE
        || valueKind == NodeKind.NULL_VALUE;
  }

  /**
   * Fused {@code OBJECT_NAMED_*} variant of {@link #insertObjectRecordAsLastChild(String,
   * ObjectRecordValue)}. Walks to the existing last child (when present) and emits the fused
   * record as its right sibling; falls back to {@link #insertObjectRecordWithPrimitiveAsFirstChild}
   * when the object is empty.
   */
  private JsonNodeTrx insertObjectRecordWithPrimitiveAsLastChild(final String key,
      final ObjectRecordValue<?> value) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      if (kind != NodeKind.OBJECT && kind != NodeKind.OBJECT_NAMED_OBJECT) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an object node!");
      }
      final StructNode obj = nodeReadOnlyTrx.getStructuralNodeView();
      final long lastChildKey = obj.getLastChildKey();
      if (lastChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        return insertObjectRecordWithPrimitiveAsFirstChild(key, value);
      }
      moveTo(lastChildKey);
      return insertObjectRecordWithPrimitiveAsRightSibling(key, value);
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Fused {@code OBJECT_NAMED_*} variant of {@link #insertObjectRecordAsLeftSibling(String,
   * ObjectRecordValue)}. Mirrors {@link #insertObjectRecordWithPrimitiveAsRightSibling} but
   * places the new record to the LEFT of the current cursor.
   */
  private JsonNodeTrx insertObjectRecordWithPrimitiveAsLeftSibling(final String key,
      final ObjectRecordValue<?> value) {
    if (lock != null) {
      lock.lock();
    }
    try {
      final NodeKind kind = getKind();
      if (!kind.playsObjectKeyRole()) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an object-key or named object-primitive node!");
      }

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNodeView();
      final long parentKey = currentNode.getParentKey();
      final long rightSibKey = currentNode.getNodeKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(rightSibKey, key, NodeKind.OBJECT_NAMED_OBJECT);
      moveTo(rightSibKey);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();
      final long nodeKey = createFusedObjectNamedNode(key, value, parentKey, leftSibKey, rightSibKey,
          pathNodeKey, id);

      insertAsSibling(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      recordFusedPrimitiveStat(pathNodeKey, value, nodeKey);

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

  /**
   * Create a fused OBJECT_NAMED_* node based on the primitive value kind.
   *
   * @return the new node's key
   * @throws IllegalArgumentException if the value wraps a non-primitive (object / array)
   */
  private long createFusedObjectNamedNode(final String key, final ObjectRecordValue<?> value,
      final long parentKey, final long leftSibKey, final long rightSibKey, final long pathNodeKey,
      final SirixDeweyID id) {
    final NodeKind valueKind = value.getKind();
    return switch (valueKind) {
      case BOOLEAN_VALUE -> nodeFactory.createJsonObjectNamedBooleanNode(parentKey, leftSibKey,
          rightSibKey, pathNodeKey, key, (Boolean) value.getValue(), id).getNodeKey();
      case NUMBER_VALUE -> nodeFactory.createJsonObjectNamedNumberNode(parentKey, leftSibKey,
          rightSibKey, pathNodeKey, key, (Number) value.getValue(), id).getNodeKey();
      case STRING_VALUE -> {
        // Honour ByteStringValue's (buffer, off, len) slice — the underlying buffer may be a
        // larger reusable scratch shared across records.
        if (value instanceof ByteStringValue bsv) {
          yield nodeFactory.createJsonObjectNamedStringNode(parentKey, leftSibKey, rightSibKey,
              pathNodeKey, key, bsv.getValue(), bsv.getOffset(), bsv.getLength(), id).getNodeKey();
        }
        final byte[] rawValue;
        final Object raw = value.getValue();
        if (raw instanceof byte[] utf8) {
          rawValue = utf8;
        } else if (raw instanceof String s) {
          rawValue = s.getBytes(Constants.DEFAULT_ENCODING);
        } else {
          throw new IllegalStateException(
              "STRING_VALUE payload must be String or byte[], got: "
                  + (raw == null ? "null" : raw.getClass().getName()));
        }
        yield nodeFactory.createJsonObjectNamedStringNode(parentKey, leftSibKey, rightSibKey,
            pathNodeKey, key, rawValue, id).getNodeKey();
      }
      case NULL_VALUE -> nodeFactory.createJsonObjectNamedNullNode(parentKey, leftSibKey, rightSibKey,
          pathNodeKey, key, id).getNodeKey();
      default -> throw new IllegalArgumentException(
          "Fused OBJECT_NAMED_* insert requires a primitive value kind, got: " + valueKind);
    };
  }

  @Override
  public JsonNodeTrx insertArrayAsFirstChild() {
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();
      // OBJECT_NAMED_ARRAY plays the ARRAY role under fusion — its
      // first/last-child slot is the array body, exactly like ARRAY. Accept it.
      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_NAMED_OBJECT && kind != NodeKind.ARRAY
          && kind != NodeKind.OBJECT_NAMED_ARRAY)
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

      final SirixDeweyID id = currentKind == NodeKind.OBJECT_NAMED_OBJECT
          ? deweyIDManager.newRecordValueID()
          : deweyIDManager.newFirstChildID();

      final ArrayNode node = nodeFactory.createJsonArrayNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, id);
      final long nodeKey = node.getNodeKey();

      adaptNodesAndHashesForInsertAsChild(nodeKey, parentKey, leftSibKey, rightSibKey);

      if (indexController.hasAnyPrimitiveIndex()) {
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT,
            (ImmutableNode) nodeReadOnlyTrx.getStructuralNodeView(), pathNodeKey);
      }

      if (kind != NodeKind.OBJECT_NAMED_OBJECT && !nodeHashing.isBulkInsert()) {
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
      // OBJECT_NAMED_ARRAY plays the ARRAY role under fusion — its
      // first/last-child slot is the array body, exactly like ARRAY. Accept it.
      if (kind != NodeKind.JSON_DOCUMENT && kind != NodeKind.OBJECT_NAMED_OBJECT && kind != NodeKind.ARRAY
          && kind != NodeKind.OBJECT_NAMED_ARRAY)
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

      final SirixDeweyID id = currentKind == NodeKind.OBJECT_NAMED_OBJECT
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

      if (kind != NodeKind.OBJECT_NAMED_OBJECT && !nodeHashing.isBulkInsert()) {
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

      if (!kind.playsObjectKeyRole())
        throw new SirixUsageException("Replacing is only permitted for record object key nodes.");

      checkAccessAndCommit();
      final long nodeKey = getNodeKey();

      // Phase 4: every record reaching this point is a fused OBJECT_NAMED_* (kindIds 48-53).
      // The fused record IS both the field name and the value (primitive leaves carry inline
      // payload; structural variants carry a name + nested subtree). Replacing the value means
      // remove + re-emit at the same anchor with the new value. A same-kind primitive update
      // takes the fast path so the fused record keeps its nodeKey (preserving node identity
      // for sdb:item-history, indexes, and temporal axes — without this, {@code replace json
      // value} on a fused field would mint a new physical node and break "track a single
      // field's history" workflows).
      if (kind == NodeKind.OBJECT_NAMED_STRING && value instanceof StringValue sv) {
        setStringValue(sv.getValue());
        return this;
      }
      if (kind == NodeKind.OBJECT_NAMED_NUMBER && value instanceof NumberValue nv) {
        setNumberValue(nv.getValue());
        return this;
      }
      if (kind == NodeKind.OBJECT_NAMED_BOOLEAN && value instanceof BooleanValue bv) {
        setBooleanValue(bv.getValue());
        return this;
      }
      if (kind == NodeKind.OBJECT_NAMED_NULL && value instanceof NullValue) {
        return this;
      }

      final String keyName = getName().getLocalName();
      final long oldValueNodeKey = nodeKey; // fused record IS the value holder
      final boolean hasLeft = hasLeftSibling();
      final long anchorKey = hasLeft ? getLeftSiblingKey() : getParentKey();
      final InsertPosition insertPos = hasLeft
          ? InsertPosition.AS_RIGHT_SIBLING
          : InsertPosition.AS_FIRST_CHILD;
      canRemoveValue = true;
      remove();
      moveTo(anchorKey);
      if (insertPos == InsertPosition.AS_FIRST_CHILD) {
        insertObjectRecordAsFirstChild(keyName, value);
      } else {
        insertObjectRecordAsRightSibling(keyName, value);
      }
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
    final NodeKind kind = structNode.getKind();

    if (kind == NodeKind.ARRAY) {
      pathNodeKey = ((ArrayNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_NUMBER) {
      // iter#30 fused leaf — pathNodeKey lives on the fused record itself.
      pathNodeKey = ((ObjectNamedNumberNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_STRING) {
      pathNodeKey = ((ObjectNamedStringNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_BOOLEAN) {
      pathNodeKey = ((ObjectNamedBooleanNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_NULL) {
      pathNodeKey = ((ObjectNamedNullNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_OBJECT) {
      // iter#32 P2 structural fused — pathNodeKey on the fused record.
      pathNodeKey = ((ObjectNamedObjectNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.OBJECT_NAMED_ARRAY) {
      pathNodeKey = ((ObjectNamedArrayNode) structNode).getPathNodeKey();
    } else if (kind == NodeKind.JSON_DOCUMENT) {
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
    if (!nodeHashing.isBulkInsert()) {
      final NodeKind pk = getParentKind();
      if (pk != NodeKind.ARRAY && pk != NodeKind.OBJECT_NAMED_ARRAY) {
        throw new SirixUsageException(INSERT_NOT_ALLOWED_SINCE_PARENT_NOT_IN_AN_ARRAY_NODE);
      }
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
   * Record a primitive value observation in the PathSummary statistics for the path
   * identified by {@code pathNodeKey}. No-op when stats are disabled, the path node
   * key is non-positive, or the PathSummaryWriter is null (resource without summary).
   *
   * <p>For STRING values, avoids an extra {@code byte[]} allocation when the offset
   * is 0 and the length matches the full array (the common case).
   */
  private void recordPrimitiveStat(final long pathNodeKey, final PrimitiveNodeType type,
      final byte[] stringValue, final int stringOff, final int stringLen,
      final Number numberValue, final boolean booleanValue, final long valueNodeKey) {
    if (pathSummaryWriter == null || pathNodeKey <= 0) {
      return;
    }
    switch (type) {
      case STRING -> {
        if (stringValue == null) {
          return;
        }
        if (stringOff == 0 && stringLen == stringValue.length) {
          pathSummaryWriter.recordValue(pathNodeKey, stringValue, valueNodeKey);
        } else {
          pathSummaryWriter.recordValue(pathNodeKey,
              java.util.Arrays.copyOfRange(stringValue, stringOff, stringOff + stringLen),
              valueNodeKey);
        }
      }
      case NUMBER -> {
        if (numberValue != null) {
          pathSummaryWriter.recordValue(pathNodeKey, numberValue.longValue(), valueNodeKey);
        }
      }
      case BOOLEAN -> pathSummaryWriter.recordBooleanValue(pathNodeKey, booleanValue, valueNodeKey);
      case NULL -> pathSummaryWriter.recordNullValue(pathNodeKey, valueNodeKey);
    }
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

      if (kind == NodeKind.OBJECT_NAMED_OBJECT) {
        throw new SirixUsageException(
            "Inserting a primitive value as the child of an OBJECT_KEY is no longer supported. "
                + "Use insertObjectRecordWithPrimitiveAs* to emit a fused OBJECT_NAMED_* record.");
      }

      if (kind != NodeKind.ARRAY && kind != NodeKind.JSON_DOCUMENT
          && kind != NodeKind.OBJECT_NAMED_ARRAY) {
        throw new SirixUsageException("Insert is not allowed if current node is not an array-node!");
      }

      if (nodeHashing.isBulkInsert()) {
        checkAccessAndCommitBulk();
      } else {
        checkAccessAndCommit();
      }

      final StructNode structNode = nodeReadOnlyTrx.getStructuralNodeView();
      final boolean pathStatsEnabled =
          pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
      final long pathNodeKey = (notifyIndex && indexController.hasAnyPrimitiveIndex()) || pathStatsEnabled
          ? getPathNodeKey(structNode)
          : 0;
      final long parentKey = structNode.getNodeKey();
      final long firstChildKey = structNode.getFirstChildKey();
      final long lastChildKey = structNode.getLastChildKey();

      final SirixDeweyID id;
      final StructNode node;
      final long leftSibKey;
      final long rightSibKey;
      if (isFirstChild) {
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

      if (pathStatsEnabled) {
        recordPrimitiveStat(pathNodeKey, type, stringValue, stringOff, stringLen, numberValue, booleanValue, nodeKey);
      }

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

      if (pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled()) {
        // Stats live on the parent container's path node (OBJECT_KEY / ARRAY); fetch
        // via a short cursor walk so we don't need to teach StructNode about paths.
        nodeReadOnlyTrx.moveTo(parentKey);
        final long parentPathNodeKey = getPathNodeKey(nodeReadOnlyTrx.getStructuralNodeView());
        nodeReadOnlyTrx.moveTo(nodeKey);
        recordPrimitiveStat(parentPathNodeKey, type, stringValue, stringOff, stringLen,
            numberValue, booleanValue, nodeKey);
      }

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
        // play the
        // OBJECT/ARRAY role under fusion, so admit them as valid move anchors.
        if (anchorKind != NodeKind.OBJECT && anchorKind != NodeKind.ARRAY
            && anchorKind != NodeKind.OBJECT_NAMED_OBJECT && anchorKind != NodeKind.JSON_DOCUMENT
            && anchorKind != NodeKind.OBJECT_NAMED_OBJECT && anchorKind != NodeKind.OBJECT_NAMED_ARRAY) {
          throw new SirixUsageException(
              "Move is not allowed if the anchor node is not an OBJECT, ARRAY, OBJECT_KEY, or JSON_DOCUMENT node!");
        }

        checkMoveAncestors(toMove);
        checkAccessAndCommit();

        final StructNode nodeAnchor = nodeReadOnlyTrx.getStructuralNode();

        if (nodeAnchor.getFirstChildKey() != toMove.getNodeKey()) {
          // Save original parent key before the move (toMove may be a flyweight singleton that gets
          // mutated during adaptForMove).
          final long originalParentKey = toMove.getParentKey();

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
          } else if (buildPathSummary
              && originalParentKey != nodeAnchor.getNodeKey()) {
            // Non-NameNode (OBJECT/ARRAY) moved to a different parent: adapt descendant NameNodes.
            adaptDescendantNameNodePaths(toMove);
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
          final long originalParentKey = toMove.getParentKey();

          // Adapt index-structures (before move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.DELETE);

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers (no text node merging for JSON).
          adaptForMove(toMove, nodeAnchor, MovePosition.AS_RIGHT_SIBLING);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            final PathSummaryWriter.OPType type = originalParentKey == parentKey
                ? PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL
                : PathSummaryWriter.OPType.MOVED;

            if (type != PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL) {
              pathSummaryWriter.adaptPathForChangedNode(moved, getName(), moved.getURIKey(),
                  moved.getPrefixKey(), moved.getLocalNameKey(), type);
            }
          } else if (buildPathSummary && originalParentKey != parentKey) {
            // Non-NameNode (OBJECT/ARRAY) moved to a different parent: adapt descendant NameNodes.
            adaptDescendantNameNodePaths(toMove);
          }

          // Adapt index-structures (after move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.INSERT);

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
   * Adapts path summary for descendant NameNode children when a non-NameNode (OBJECT or ARRAY) is
   * moved to a different parent. Without this, nested OBJECT_KEY nodes would retain stale
   * pathNodeKey references after the container node moves.
   *
   * <p>Only processes the <b>shallowest</b> NameNode descendants (first OBJECT_KEY layer reached
   * in each branch), because {@code adaptPathForChangedNode} internally handles deeper descendants
   * via its own descendant traversal.</p>
   *
   * @param movedNode the non-NameNode that was moved
   */
  private void adaptDescendantNameNodePaths(final StructNode movedNode) {
    final long savedKey = getNodeKey();
    moveTo(movedNode.getNodeKey());
    collectAndAdaptShallowNameNodes(movedNode.getNodeKey());
    moveTo(savedKey);
  }

  /**
   * Recursively finds the shallowest NameNode descendants under a non-NameNode container and
   * adapts their path summary entries. Stops recursing once a NameNode is found (because
   * {@code adaptPathForChangedNode} handles deeper descendants).
   */
  private void collectAndAdaptShallowNameNodes(final long parentKey) {
    moveTo(parentKey);
    if (!hasFirstChild()) {
      return;
    }
    moveToFirstChild();
    do {
      final long childKey = getNodeKey();
      final ImmutableNode childNode = nodeReadOnlyTrx.getNode();
      if (childNode instanceof NameNode nameNode) {
        // Found a NameNode — adapt its path (which also fixes all its descendants).
        // No need to recurse deeper.
        moveTo(childKey);
        pathSummaryWriter.adaptPathForChangedNode(nameNode, getName(), nameNode.getURIKey(),
            nameNode.getPrefixKey(), nameNode.getLocalNameKey(), PathSummaryWriter.OPType.MOVED);
        moveTo(childKey);
      } else if (childNode instanceof StructNode structChild && structChild.hasFirstChild()) {
        // Non-NameNode with children (e.g., OBJECT inside ARRAY) — recurse to find NameNodes.
        collectAndAdaptShallowNameNodes(childKey);
      }
      moveTo(childKey);
    } while (hasRightSibling() && moveToRightSibling());
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

      // OBJECT_NAMED_OBJECT and OBJECT_NAMED_ARRAY (records) play the
      // OBJECT/ARRAY role under fusion — their direct children are full object-fields, removable
      // exactly the same way as legacy OBJECT/ARRAY children. Accept them as valid removable-
      // child parents alongside the legacy kinds.
      if ((parentNodeKind != NodeKind.JSON_DOCUMENT && parentNodeKind != NodeKind.OBJECT
          && parentNodeKind != NodeKind.ARRAY
          && parentNodeKind != NodeKind.OBJECT_NAMED_OBJECT
          && parentNodeKind != NodeKind.OBJECT_NAMED_ARRAY) && !canRemoveValue) {
        throw new SirixUsageException(
            "An object record value can not be removed, you have to remove the whole object record (parent of this value).");
      }

      canRemoveValue = false;

      // iter#32: Legacy `parentNodeKind != OBJECT_KEY` was meant to skip the DELETE
      // diff entry when callers asked to remove the inner value-record of an
      // OBJECT_KEY pair (a half-removal that left a dangling key wrapper). Under
      // fusion the inner-value record no longer exists — every OBJECT_NAMED_*
      // record IS the field — so this skip-condition does not apply. Removing a
      // child of OBJECT_NAMED_OBJECT or OBJECT_NAMED_ARRAY is a real object/array
      // field removal and must register a DELETE tuple, otherwise downstream
      // consumers (BasicJsonDiff, jn:diff serializer) lose the entry entirely.
      adaptUpdateOperationsForRemove(node.getDeweyID(), node.getNodeKey());

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

      if (node.getKind().playsObjectKeyRole()) {
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
    if (indexController.hasNameIndex()) {
      if (kind == NodeKind.OBJECT_NAMED_BOOLEAN || kind == NodeKind.OBJECT_NAMED_NUMBER
          || kind == NodeKind.OBJECT_NAMED_STRING || kind == NodeKind.OBJECT_NAMED_NULL
          || kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
        // Fused OBJECT_NAMED_* plays the field-name role — resolve via the stored nameKey
        // in the OBJECT_NAMED_OBJECT namespace bucket (the canonical "object field" tag).
        name = resolveFusedName(node);
      } else {
        name = null;
      }
    } else {
      name = null;
    }

    final Str value;
    if (indexController.hasCASIndex()) {
      value = switch (kind) {
        case STRING_VALUE -> node instanceof ValueNode valueNode
            ? new Str(valueNode.getValue())
            : null;
        case BOOLEAN_VALUE -> node instanceof BooleanNode boolNode
            ? (boolNode.getValue()
                ? STR_TRUE
                : STR_FALSE)
            : null;
        case NUMBER_VALUE -> node instanceof NumberNode numberNode
            ? new Str(String.valueOf(numberNode.getValue()))
            : null;
        // Fused OBJECT_NAMED_* — value is inline on the fused record.
        case OBJECT_NAMED_STRING -> node instanceof ObjectNamedStringNode namedStr
            ? new Str(new String(namedStr.getRawValue(), Constants.DEFAULT_ENCODING))
            : null;
        case OBJECT_NAMED_BOOLEAN -> node instanceof ObjectNamedBooleanNode namedBool
            ? (namedBool.getValue() ? STR_TRUE : STR_FALSE)
            : null;
        case OBJECT_NAMED_NUMBER -> node instanceof ObjectNamedNumberNode namedNum
            ? new Str(String.valueOf(namedNum.getValue()))
            : null;
        case OBJECT_NAMED_NULL, OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY -> null;
        default -> null;
      };
    } else {
      value = null;
    }

    indexController.notifyChange(type, nodeKey, kind, pathNodeKey, name, value);
  }

  /**
   * Resolve the {@link QNm} for a fused {@code OBJECT_NAMED_*} record via the storage
   * engine's name index (the nameKey was created under the canonical fused-named bucket at
   * shred time).
   */
  private QNm resolveFusedName(final ImmutableNode node) {
    final int nameKey;
    if (node instanceof ObjectNamedNumberNode n) {
      nameKey = n.getNameKey();
    } else if (node instanceof ObjectNamedStringNode n) {
      nameKey = n.getNameKey();
    } else if (node instanceof ObjectNamedBooleanNode n) {
      nameKey = n.getNameKey();
    } else if (node instanceof ObjectNamedNullNode n) {
      nameKey = n.getNameKey();
    } else if (node instanceof ObjectNamedObjectNode n) {
      nameKey = n.getNameKey();
    } else if (node instanceof ObjectNamedArrayNode n) {
      nameKey = n.getNameKey();
    } else {
      return null;
    }
    final String localName = storageEngineWriter.getName(nameKey, NodeKind.OBJECT_NAMED_OBJECT);
    return localName == null ? null : new QNm(localName);
  }

  private void removeValue() {
    final NodeKind nodeKind = getKind();
    if (nodeKind == NodeKind.STRING_VALUE
        || nodeKind == NodeKind.NUMBER_VALUE || nodeKind == NodeKind.BOOLEAN_VALUE) {
      final long nodeKey = getNodeKey();
      final StructNode currentValueNode = nodeReadOnlyTrx.getStructuralNode();

      moveToParent();
      final long pathNodeKey = getPathNodeKey(nodeReadOnlyTrx.getStructuralNode());

      moveTo(nodeKey);
      // Pass the VALUE node, not the parent node - CAS index needs the value to extract.
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) currentValueNode, pathNodeKey);

      // Stats-decrement (best-effort; dirty flag on PathNode triggers rebound at read time).
      if (pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled() && pathNodeKey > 0) {
        switch (nodeKind) {
          case STRING_VALUE -> {
            if (currentValueNode instanceof ValueNode vn) {
              pathSummaryWriter.removeValue(pathNodeKey, vn.getRawValue());
            }
          }
          case NUMBER_VALUE -> {
            if (currentValueNode instanceof NumberNode n && n.getValue() != null) {
              pathSummaryWriter.removeValue(pathNodeKey, n.getValue().longValue());
            }
          }
          case BOOLEAN_VALUE -> {
            if (currentValueNode instanceof BooleanNode b) {
              pathSummaryWriter.removeBooleanValue(pathNodeKey, b.getValue());
            }
          }
          default -> { /* no stats for other kinds */ }
        }
      }
    }
  }

  /**
   * Remove a name from the {@link NamePage} reference and the path summary if needed.
   *
   * @throws SirixException if Sirix fails
   */
  private void removeName() {
    final NodeKind kind = getKind();
    if (kind.isFusedObjectNamed()) {
      // Fused OBJECT_NAMED_* records play the OBJECT_KEY role and carry both name and inline
      // value on one slot; mirror the OBJECT_KEY branch plus a path-stats decrement for the value.
      final NameNode node = (NameNode) nodeReadOnlyTrx.getStructuralNode();
      final long pathNodeKey = node.getPathNodeKey();
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);

      final int nameKey = node.getLocalNameKey();
      final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      page.removeName(nameKey, NodeKind.OBJECT_NAMED_OBJECT, storageEngineWriter);

      if (buildPathSummary) {
        pathSummaryWriter.remove((ImmutableNameNode) node);

        if (pathSummaryWriter.isPathStatisticsEnabled() && pathNodeKey > 0) {
          switch (kind) {
            case OBJECT_NAMED_STRING ->
                pathSummaryWriter.removeValue(pathNodeKey, ((ObjectNamedStringNode) node).getRawValue());
            case OBJECT_NAMED_NUMBER -> {
              final Number v = ((ObjectNamedNumberNode) node).getValue();
              if (v != null) {
                pathSummaryWriter.removeValue(pathNodeKey, v.longValue());
              }
            }
            case OBJECT_NAMED_BOOLEAN ->
                pathSummaryWriter.removeBooleanValue(pathNodeKey, ((ObjectNamedBooleanNode) node).getValue());
            case OBJECT_NAMED_NULL -> pathSummaryWriter.removeNullValue(pathNodeKey);
            default -> throw new AssertionError(kind);
          }
        }
      }
    } else if (kind == NodeKind.OBJECT_NAMED_OBJECT || kind == NodeKind.OBJECT_NAMED_ARRAY) {
      // Fused structural records (OBJECT_NAMED_OBJECT/ARRAY) carry a name + nested subtree.
      // The subtree is removed by the caller's PostOrderAxis cascade; here we only need to
      // decrement the NamePage entry, fire the index event, and (when path-summary is enabled)
      // remove the name from the path-summary tree. There is no inline primitive value to
      // decrement in path statistics.
      final NameNode node = (NameNode) nodeReadOnlyTrx.getStructuralNode();
      final long pathNodeKey = node.getPathNodeKey();
      notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);

      final int nameKey = node.getLocalNameKey();
      final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      page.removeName(nameKey, NodeKind.OBJECT_NAMED_OBJECT, storageEngineWriter);

      if (buildPathSummary) {
        // OBJECT_NAMED_ARRAY's pathNodeKey points at the `__array__/ARRAY` entry (level N+1)
        // because insertion bumped both that and its OBJECT_KEY parent (level N). Symmetric
        // remove must decrement BOTH entries so refcounts stay balanced.
        // Snapshot the OBJECT_KEY parent BEFORE removing the ARRAY entry, since the standard
        // remove(...) may collapse the entire ARRAY subtree on the last reference.
        final long objectKeyParentForFusedArray =
            kind == NodeKind.OBJECT_NAMED_ARRAY ? pathSummaryWriter.lookupArrayPathParentKey(pathNodeKey) : -1L;
        pathSummaryWriter.remove((ImmutableNameNode) node);
        if (objectKeyParentForFusedArray >= 0) {
          pathSummaryWriter.decrementObjectKeyRefByKey(objectKeyParentForFusedArray);
        }
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
      final NodeKind currentKind = getKind();
      if (!currentKind.playsObjectKeyRole()) {
        throw new SirixUsageException("Not allowed if current node is not an object key node!");
      }
      checkAccessAndCommit();

      final ImmutableJsonNode node = (ImmutableJsonNode) nodeReadOnlyTrx.getStructuralNode();
      final NameNode nameNode = (NameNode) node;
      final long oldHash = node.computeHash(bytes);

      // Fused NamePage entries live in the OBJECT_KEY namespace, so remove+create always uses OBJECT_KEY.
      final NodeKind nameNamespaceKind = NodeKind.OBJECT_NAMED_OBJECT;
      final int oldNameKey = nameNode.getLocalNameKey();
      final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      page.removeName(oldNameKey, nameNamespaceKind, storageEngineWriter);

      final int newNameKey = storageEngineWriter.createNameKey(key, nameNamespaceKind);

      final QNm renamed = new QNm(key);
      nameNode.setLocalNameKey(newNameKey);
      nameNode.setName(renamed);
      nameNode.setPreviousRevision(storageEngineWriter.getRevisionToRepresent());

      // Adapt path summary.
      if (buildPathSummary) {
        // OBJECT_NAMED_ARRAY's pathNodeKey points at the `__array__/ARRAY`
        // layer (one level deeper than the field-name OBJECT_KEY entry). Renaming the field
        // means renaming the OBJECT_KEY parent of that ARRAY entry — the ARRAY entry itself
        // continues to carry the `__array__` synthetic name.
        if (currentKind == NodeKind.OBJECT_NAMED_ARRAY) {
          final long arrayPathNodeKey = nameNode.getPathNodeKey();
          final long objectKeyPathParent =
              pathSummaryWriter.lookupArrayPathParentKey(arrayPathNodeKey);
          if (objectKeyPathParent >= 0) {
            pathSummaryWriter.renameObjectKeyPathEntry(objectKeyPathParent, renamed, newNameKey);
          }
        } else {
          pathSummaryWriter.adaptPathForChangedNode((ImmutableNameNode) nameNode, renamed, -1, -1, newNameKey,
              OPType.SETNAME);
        }
      }

      // Set path node key. For OBJECT_NAMED_ARRAY the ARRAY layer key is unchanged; for other
      // kinds adaptPathForChangedNode positioned the path-summary cursor on the (possibly newly
      // created) target path node.
      if (currentKind != NodeKind.OBJECT_NAMED_ARRAY) {
        nameNode.setPathNodeKey(buildPathSummary
            ? pathSummaryWriter.getNodeKey()
            : 0);
      }

      nodeReadOnlyTrx.setCurrentNode(node);
      persistUpdatedRecord((DataRecord) node);
      nodeHashing.adaptHashedWithUpdate(oldHash);

      final long updatedNodeKey = ((Node) node).getNodeKey();
      adaptUpdateOperationsForUpdate(((ImmutableJsonNode) node).getDeweyID(),
          updatedNodeKey);

      if (storeNodeHistory) {
        nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(updatedNodeKey);
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
      final NodeKind kind = getKind();
      if (kind != NodeKind.STRING_VALUE
          && kind != NodeKind.OBJECT_NAMED_STRING) {
        throw new SirixUsageException(
            "Not allowed if current node is not a string value node!");
      }
      if (kind == NodeKind.OBJECT_NAMED_STRING) {
        // Fused record carries (name, value) inline. Updating only the value is semantically
        // an in-place update — diff shows an UPDATE instead of REPLACE, and avoids
        // remove+reinsert cost.
        return setStringValueFused(value);
      }

      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      final ValueNode node = (ValueNode) nodeReadOnlyTrx.getStructuralNode();
      final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
      // Capture old bytes before mutation so stats can rebound on value change.
      final byte[] oldBytes = statsOn ? node.getRawValue().clone() : null;
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
      if (statsOn) {
        pathSummaryWriter.removeValue(pathNodeKey, oldBytes);
        pathSummaryWriter.recordValue(pathNodeKey, byteVal, node.getNodeKey());
      }

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
      final NodeKind kind = getKind();
      if (kind != NodeKind.BOOLEAN_VALUE
          && kind != NodeKind.OBJECT_NAMED_BOOLEAN) {
        throw new SirixUsageException("Not allowed if current node is not a boolean value node!");
      }
      if (kind == NodeKind.OBJECT_NAMED_BOOLEAN) {
        // In-place boolean update on fused record — see setStringValueFused comment.
        return setBooleanValueFused(value);
      }

      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      final BooleanValueNode node = (BooleanValueNode) nodeReadOnlyTrx.getStructuralNode();
      final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
      final boolean oldValue = statsOn && node.getValue();
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
      if (statsOn) {
        pathSummaryWriter.removeBooleanValue(pathNodeKey, oldValue);
        pathSummaryWriter.recordBooleanValue(pathNodeKey, value, node.getNodeKey());
      }

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
      final NodeKind kind = getKind();
      if (kind != NodeKind.NUMBER_VALUE
          && kind != NodeKind.OBJECT_NAMED_NUMBER) {
        throw new SirixUsageException(
            "Not allowed if current node is not a number value node!");
      }
      if (kind == NodeKind.OBJECT_NAMED_NUMBER) {
        // In-place number update on fused record — see setStringValueFused comment.
        return setNumberValueFused(value);
      }
      checkAccessAndCommit();

      final long nodeKey = getNodeKey();
      moveToParent();
      final long pathNodeKey = getPathNodeKey();
      moveTo(nodeKey);

      final NumericValueNode node = (NumericValueNode) nodeReadOnlyTrx.getStructuralNode();
      final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
      final long oldValueAsLong = statsOn && node.getValue() != null ? node.getValue().longValue() : 0L;
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
      if (statsOn) {
        pathSummaryWriter.removeValue(pathNodeKey, oldValueAsLong);
        pathSummaryWriter.recordValue(pathNodeKey, value.longValue(), node.getNodeKey());
      }

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
  // fused OBJECT_NAMED_* in-place value setters
  // ////////////////////////////////////////////////////////////
  //
  // These mirror the legacy OBJECT_*_VALUE setters: update the primitive value in-place
  // on the same record (so the nodeKey is preserved), index DELETE+INSERT around the
  // mutation, update path statistics, and emit an UPDATED diff event. Preserving the
  // nodeKey means a semantic "value change" surfaces to the user as an update in
  // BasicJsonDiff, matching legacy-mode behavior. The alternative — remove+reinsert via
  // replaceObjectRecordValue — would emit REPLACE, shift subsequent nodeKeys, and pay
  // both a delete and an insert of the full record.

  private JsonNodeTrx setStringValueFused(final String value) {
    checkAccessAndCommit();

    final long nodeKey = getNodeKey();
    ObjectNamedStringNode node =
        storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    final long pathNodeKey = node.getPathNodeKey();

    final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
    final byte[] oldBytes = statsOn ? node.getRawValue().clone() : null;

    notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, pathNodeKey);
    final long oldHash = node.computeHash(bytes);
    final byte[] byteVal = getBytes(value);
    node.setRawValue(byteVal);
    node.setPreviousRevision(node.getLastModifiedRevisionNumber());
    node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

    // setRawValue may have resize-unbound a bound flyweight — re-acquire the current
    // singleton so the rest of the update path sees the rebound record.
    node = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    nodeReadOnlyTrx.setCurrentNode(node);
    persistUpdatedRecord(node);
    nodeHashing.adaptHashedWithUpdate(oldHash);

    notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);
    if (statsOn) {
      pathSummaryWriter.removeValue(pathNodeKey, oldBytes);
      pathSummaryWriter.recordValue(pathNodeKey, byteVal, node.getNodeKey());
    }

    adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

    if (storeNodeHistory) {
      nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
    }

    return this;
  }

  private JsonNodeTrx setBooleanValueFused(final boolean value) {
    checkAccessAndCommit();

    final long nodeKey = getNodeKey();
    ObjectNamedBooleanNode node =
        storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    final long pathNodeKey = node.getPathNodeKey();

    final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
    final boolean oldValue = statsOn && node.getValue();

    notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, pathNodeKey);
    final long oldHash = node.computeHash(bytes);
    node.setValue(value);
    node.setPreviousRevision(node.getLastModifiedRevisionNumber());
    node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

    // Re-acquire in case setValue resize-unbound the flyweight.
    node = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    nodeReadOnlyTrx.setCurrentNode(node);
    persistUpdatedRecord(node);
    nodeHashing.adaptHashedWithUpdate(oldHash);

    notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);
    if (statsOn) {
      pathSummaryWriter.removeBooleanValue(pathNodeKey, oldValue);
      pathSummaryWriter.recordBooleanValue(pathNodeKey, value, node.getNodeKey());
    }

    adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

    if (storeNodeHistory) {
      nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
    }

    return this;
  }

  private JsonNodeTrx setNumberValueFused(final Number value) {
    checkAccessAndCommit();

    final long nodeKey = getNodeKey();
    ObjectNamedNumberNode node =
        storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    final long pathNodeKey = node.getPathNodeKey();

    final boolean statsOn = pathSummaryWriter != null && pathSummaryWriter.isPathStatisticsEnabled();
    final long oldValueAsLong =
        statsOn && node.getValue() != null ? node.getValue().longValue() : 0L;

    notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, pathNodeKey);
    final long oldHash = node.computeHash(bytes);
    node.setValue(value);
    node.setPreviousRevision(node.getLastModifiedRevisionNumber());
    node.setLastModifiedRevision(storageEngineWriter.getRevisionNumber());

    // Re-acquire in case setValue resize-unbound the flyweight.
    node = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    nodeReadOnlyTrx.setCurrentNode(node);
    persistUpdatedRecord(node);
    nodeHashing.adaptHashedWithUpdate(oldHash);

    notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);
    if (statsOn) {
      pathSummaryWriter.removeValue(pathNodeKey, oldValueAsLong);
      pathSummaryWriter.recordValue(pathNodeKey, value.longValue(), node.getNodeKey());
    }

    adaptUpdateOperationsForUpdate(node.getDeweyID(), node.getNodeKey());

    if (storeNodeHistory) {
      nodeToRevisionsIndex.addRevisionToRecordToRevisionsIndex(node.getNodeKey());
    }

    return this;
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

    try {
      copyNode(rtx, insert);
    } finally {
      rtx.close();
    }
  }

  /**
   * Recursively copies a single node and its entire subtree at the given insertion position.
   * After this method returns, the write-transaction cursor is positioned on the newly inserted
   * subtree root.
   *
   * @param rtx    source read-only transaction positioned at the node to copy
   * @param insert where to insert relative to the current write-transaction cursor
   */
  private void copyNode(final JsonNodeReadOnlyTrx rtx, final InsertPosition insert) {
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
        final boolean boolVal = rtx.getBooleanValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertBooleanValueAsFirstChild(boolVal);
          case AS_LEFT_SIBLING -> insertBooleanValueAsLeftSibling(boolVal);
          case AS_RIGHT_SIBLING -> insertBooleanValueAsRightSibling(boolVal);
          default -> throw new IllegalStateException();
        }
      }
      case NUMBER_VALUE -> {
        final Number numVal = rtx.getNumberValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertNumberValueAsFirstChild(numVal);
          case AS_LEFT_SIBLING -> insertNumberValueAsLeftSibling(numVal);
          case AS_RIGHT_SIBLING -> insertNumberValueAsRightSibling(numVal);
          default -> throw new IllegalStateException();
        }
      }
      case OBJECT -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertObjectAsFirstChild();
          case AS_LEFT_SIBLING -> insertObjectAsLeftSibling();
          case AS_RIGHT_SIBLING -> insertObjectAsRightSibling();
          default -> throw new IllegalStateException();
        }
        copyChildren(rtx);
      }
      case ARRAY -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertArrayAsFirstChild();
          case AS_LEFT_SIBLING -> insertArrayAsLeftSibling();
          case AS_RIGHT_SIBLING -> insertArrayAsRightSibling();
          default -> throw new IllegalStateException();
        }
        copyChildren(rtx);
      }
      case OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING, OBJECT_NAMED_NULL ->
          copyFusedObjectRecord(rtx, insert, kind);
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY ->
          copyFusedStructuralRecord(rtx, insert, kind);
      // $CASES-OMITTED$
      default -> throw new UnsupportedOperationException(
          "Copying node kind " + kind + " is not supported.");
    }
  }

  /**
   * Copy a fused OBJECT_NAMED_OBJECT/OBJECT_NAMED_ARRAY record by emitting an equivalent fused
   * structural record at the destination, then recursively copying its children. The fused
   * record IS the OBJECT_KEY+OBJECT/ARRAY pair in legacy semantics, so a moveToFirstChild after
   * creation puts the cursor on the first inner field (or returns false if empty), but for
   * copy-children purposes the cursor must STAY on the structural record so that subsequent
   * inner-field inserts land as its children.
   */
  private void copyFusedStructuralRecord(final JsonNodeReadOnlyTrx rtx, final InsertPosition insert,
      final NodeKind kind) {
    final String keyName = rtx.getName().getLocalName();
    final long sourceKey = rtx.getNodeKey();
    final ObjectRecordValue<?> value = kind == NodeKind.OBJECT_NAMED_OBJECT
        ? ObjectValue.INSTANCE
        : ArrayValue.INSTANCE;

    // Emit the fused structural record. The cursor lands ON the fused record after each call.
    switch (insert) {
      case AS_FIRST_CHILD -> insertObjectRecordAsFirstChild(keyName, value);
      case AS_LEFT_SIBLING -> insertObjectRecordAsLeftSibling(keyName, value);
      case AS_RIGHT_SIBLING -> insertObjectRecordAsRightSibling(keyName, value);
      default -> throw new IllegalStateException("unsupported copy insert position: " + insert);
    }

    // Recursively copy children of the fused record into the newly inserted fused parent. The
    // wtx cursor is already on the fused record (which plays the OBJECT/ARRAY role under
    // fusion), so copyChildren will use it as the parent for subsequent first-child inserts.
    rtx.moveTo(sourceKey);
    copyChildren(rtx);
    rtx.moveTo(sourceKey);
  }

  /**
   * Copy a fused OBJECT_NAMED_* record by emitting an equivalent fused record in the
   * destination transaction. The destination's shredder default (fusion on) will re-fuse
   * when the name + primitive pair are inserted via
   * {@link #insertObjectRecordWithPrimitiveAsFirstChild(String, ObjectRecordValue)} /
   * {@link #insertObjectRecordWithPrimitiveAsRightSibling(String, ObjectRecordValue)}.
   */
  private void copyFusedObjectRecord(final JsonNodeReadOnlyTrx rtx, final InsertPosition insert,
      final NodeKind kind) {
    final String keyName = rtx.getName().getLocalName();
    final ObjectRecordValue<?> value = switch (kind) {
      case OBJECT_NAMED_BOOLEAN -> BooleanValue.of(rtx.getBooleanValue());
      case OBJECT_NAMED_NUMBER -> new NumberValue(rtx.getNumberValue());
      case OBJECT_NAMED_STRING -> new StringValue(rtx.getValue());
      case OBJECT_NAMED_NULL -> NullValue.INSTANCE;
      default -> throw new IllegalStateException("unexpected fused kind: " + kind);
    };
    switch (insert) {
      case AS_FIRST_CHILD -> insertObjectRecordWithPrimitiveAsFirstChild(keyName, value);
      case AS_LEFT_SIBLING, AS_RIGHT_SIBLING -> insertObjectRecordWithPrimitiveAsRightSibling(keyName, value);
      default -> throw new IllegalStateException("unsupported copy insert position: " + insert);
    }
  }

  /**
   * Copies all children of the source node (at rtx's current position) as children of the
   * write-transaction's current node. After this method, both cursors are restored to their
   * original positions.
   *
   * @param rtx source read-only transaction positioned at the parent whose children are to be copied
   */
  private void copyChildren(final JsonNodeReadOnlyTrx rtx) {
    if (!rtx.hasFirstChild()) {
      return;
    }

    final long wtxParentKey = getNodeKey();
    final long rtxParentKey = rtx.getNodeKey();

    rtx.moveToFirstChild();
    copyNode(rtx, InsertPosition.AS_FIRST_CHILD);

    while (rtx.hasRightSibling()) {
      rtx.moveToRightSibling();
      copyNode(rtx, InsertPosition.AS_RIGHT_SIBLING);
    }

    // Restore both cursors to the parent nodes.
    rtx.moveTo(rtxParentKey);
    moveTo(wtxParentKey);
  }

  private static final class JsonNodeTrxThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(final Runnable runnable) {
      final var thread = new Thread(runnable, "JsonNodeTrxCommitThread");

      thread.setPriority(Thread.NORM_PRIORITY);
      thread.setDaemon(false);

      return thread;
    }
  }
}
