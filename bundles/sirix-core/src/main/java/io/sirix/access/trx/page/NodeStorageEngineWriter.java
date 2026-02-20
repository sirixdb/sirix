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

package io.sirix.access.trx.page;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.exception.SirixIOException;
import io.sirix.io.SerializationBufferPool;
import io.sirix.node.PooledBytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.utils.OS;
import io.sirix.index.IndexType;
import io.sirix.io.Writer;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.node.layout.FixedSlotRecordMaterializer;
import io.sirix.node.layout.FixedSlotRecordProjector;
import io.sirix.node.layout.NodeKindLayout;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.ReusableNodeProxy;
import io.sirix.page.CASPage;
import io.sirix.page.DeweyIDPage;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.IndirectPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.PathSummaryPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Implements the {@link StorageEngineWriter} interface to provide write capabilities to the
 * persistent storage layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class NodeStorageEngineWriter extends AbstractForwardingStorageEngineReader implements StorageEngineWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeStorageEngineWriter.class);

  /**
   * Debug flag for memory leak tracking.
   * 
   * @see DiagnosticSettings#MEMORY_LEAK_TRACKING
   */
  private static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;

  private BytesOut<?> bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
  private MemorySegmentBytesOut demotionBuffer;
  private MemorySegment fixedSlotProjectionBuffer;


  /**
   * Page writer to serialize.
   */
  private final Writer storagePageReaderWriter;

  /**
   * Transaction intent log.
   */
  TransactionIntentLog log;

  /**
   * Last reference to the actual revRoot.
   */
  private final RevisionRootPage newRevisionRootPage;

  /**
   * {@link NodeStorageEngineReader} instance.
   */
  private final NodeStorageEngineReader storageEngineReader;

  /**
   * Determines if transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Pending fsync future for async durability. For auto-commit mode, fsync runs in background; next
   * commit waits for it.
   */
  private volatile java.util.concurrent.CompletableFuture<Void> pendingFsync;

  /**
   * {@link XmlIndexController} instance.
   */
  private final IndexController<?, ?> indexController;

  /**
   * The indirect page trie writer - manages the trie structure of IndirectPages.
   */
  private final TrieWriter trieWriter;

  /**
   * The HOT trie writer - manages HOT (Height Optimized Trie) structure. Used for cache-friendly
   * secondary indexes (PATH, CAS, NAME).
   */
  private final HOTTrieWriter hotTrieWriter;

  /**
   * The revision to represent.
   */
  private final int representRevision;

  /**
   * Trie type for routing page operations.
   */
  public enum TrieType {
    /** Traditional bit-decomposed IndirectPage trie (existing approach). */
    KEYED_TRIE,
    /** HOT (Height Optimized Trie) for cache-friendly secondary indexes (new approach). */
    HOT
  }

  /**
   * {@code true} if this page write trx will be bound to a node trx, {@code false} otherwise
   */
  private final boolean isBoundToNodeTrx;

  private record IndexLogKeyToPageContainer(IndexType indexType, long recordPageKey, int indexNumber,
      int revisionNumber, PageContainer pageContainer) {
  }

  /**
   * The most recent page container.
   */
  private IndexLogKeyToPageContainer mostRecentPageContainer;

  /**
   * The second most recent page container.
   */
  private IndexLogKeyToPageContainer secondMostRecentPageContainer;

  /**
   * The most recent path summary page container.
   */
  private IndexLogKeyToPageContainer mostRecentPathSummaryPageContainer;

  // Write-path singletons — one per XML/JSON node kind (lazily allocated)
  private ObjectNode writeObjectSingleton;
  private ArrayNode writeArraySingleton;
  private ObjectKeyNode writeObjectKeySingleton;
  private StringNode writeStringSingleton;
  private NumberNode writeNumberSingleton;
  private BooleanNode writeBooleanSingleton;
  private NullNode writeNullSingleton;
  private ObjectStringNode writeObjectStringSingleton;
  private ObjectNumberNode writeObjectNumberSingleton;
  private ObjectBooleanNode writeObjectBooleanSingleton;
  private ObjectNullNode writeObjectNullSingleton;
  private JsonDocumentRootNode writeJsonDocumentSingleton;
  private XmlDocumentRootNode writeXmlDocumentSingleton;
  private ElementNode writeElementSingleton;
  private TextNode writeTextSingleton;
  private CommentNode writeCommentSingleton;
  private AttributeNode writeAttributeSingleton;
  private NamespaceNode writeNamespaceSingleton;
  private PINode writePISingleton;

  private final LinkedHashMap<IndexLogKey, PageContainer> pageContainerCache;

  /**
   * Constructor.
   *
   * @param writer the page writer
   * @param log the transaction intent log
   * @param revisionRootPage the revision root page
   * @param storageEngineReader the page reading transaction used as a delegate
   * @param indexController the index controller, which is used to update indexes
   * @param representRevision the revision to represent
   * @param isBoundToNodeTrx {@code true} if this page write trx will be bound to a node trx,
   *        {@code false} otherwise
   */
  NodeStorageEngineWriter(final Writer writer, final TransactionIntentLog log, final RevisionRootPage revisionRootPage,
      final NodeStorageEngineReader storageEngineReader, final IndexController<?, ?> indexController, final int representRevision,
      final boolean isBoundToNodeTrx) {
    this.trieWriter = new TrieWriter();
    this.hotTrieWriter = new HOTTrieWriter();
    storagePageReaderWriter = requireNonNull(writer);
    this.log = requireNonNull(log);
    newRevisionRootPage = requireNonNull(revisionRootPage);
    this.storageEngineReader = requireNonNull(storageEngineReader);
    this.indexController = requireNonNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    this.representRevision = representRevision;
    this.isBoundToNodeTrx = isBoundToNodeTrx;
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = mostRecentPageContainer;
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    pageContainerCache = new LinkedHashMap<>(100, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<IndexLogKey, PageContainer> eldest) {
        if (size() > 100) {
          // When evicting PageContainer from local cache, ensure pages are properly tracked
          // Pages should be in TIL (will be closed on commit/rollback) or in global cache
          PageContainer container = eldest.getValue();
          if (container != null) {
            // Pages in local cache should already be in TIL (appended via appendLogRecord)
            // No action needed - TIL will handle cleanup
          }
          return true;
        }
        return false;
      }
    };
  }

  /**
   * Determine which trie type to use for the given index.
   * 
   * <p>
   * Currently all indexes use KEYED_TRIE. HOT is available but requires HOTLeafPage to implement
   * setRecord()/getRecord() compatible with existing API.
   * </p>
   * 
   * <p>
   * TODO to enable HOT:
   * </p>
   * <ul>
   * <li>Implement HOTLeafPage.setRecord() to store DataRecords</li>
   * <li>Implement HOTLeafPage.getRecord() to retrieve DataRecords</li>
   * <li>Bridge between HOT key-value storage and DataRecord storage</li>
   * </ul>
   *
   * @param indexType the index type
   * @param indexNumber the index number
   * @return the trie type to use
   */
  @SuppressWarnings("unused") // indexType/indexNumber will be used when HOT is enabled
  private TrieType getTrieType(@NonNull IndexType indexType, int indexNumber) {
    // HOT is not yet fully integrated with DataRecord storage.
    // HOTLeafPage needs to implement setRecord()/getRecord() to be compatible.
    // Once implemented, enable with:
    // return switch (indexType) {
    // case PATH, CAS, NAME -> TrieType.HOT;
    // default -> TrieType.KEYED_TRIE;
    // };
    return TrieType.KEYED_TRIE;
  }

  /**
   * Get the HOT trie writer for HOT index operations.
   * 
   * @return the HOT trie writer
   */
  public HOTTrieWriter getHOTTrieWriter() {
    return hotTrieWriter;
  }

  @Override
  public BytesOut<?> newBufferedBytesInstance() {
    bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
    return bufferBytes;
  }

  @Override
  public int getRevisionToRepresent() {
    storageEngineReader.assertNotClosed();
    return representRevision;
  }

  @Override
  public TransactionIntentLog getLog() {
    storageEngineReader.assertNotClosed();
    return log;
  }

  @Override
  public int getRevisionNumber() {
    storageEngineReader.assertNotClosed();
    return newRevisionRootPage.getRevision();
  }

  @Override
  public DataRecord prepareRecordForModification(final long recordKey, @NonNull final IndexType indexType,
      final int index) {
    storageEngineReader.assertNotClosed();
    checkArgument(recordKey >= 0, "recordKey must be >= 0!");
    requireNonNull(indexType);

    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final var modifiedPage = cont.getModifiedAsKeyValuePage();

    // Check records[] first — this is the fast path and returns a stable reference.
    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);
    DataRecord record = modifiedPage.getRecord(recordOffset);
    if (record != null) {
      return record;
    }

    // Try fixed-slot singleton path — zero allocation hot path.
    // The write singleton is populated from fixed-slot bytes (authoritative state).
    // Callers must use the returned object immediately, mutate, and persist via
    // updateRecordSlot before the next prepareRecordForModification call for the
    // same node kind (use-persist-discard rule).
    if (modifiedPage instanceof KeyValueLeafPage modifiedLeafPage && modifiedLeafPage.isFixedSlotFormat(recordOffset)) {
      final NodeKind nodeKind = modifiedLeafPage.getFixedSlotNodeKind(recordOffset);
      if (nodeKind != null) {
        final long dataOffset = modifiedLeafPage.getSlotDataOffset(recordOffset);
        if (dataOffset >= 0) {
          final DataRecord singleton = getOrCreateWriteSingleton(nodeKind);
          if (singleton != null) {
            final int dataLength = modifiedLeafPage.getSlotDataLength(recordOffset);
            final byte[] deweyIdBytes = modifiedLeafPage.getDeweyIdAsByteArray(recordOffset);
            if (FixedSlotRecordMaterializer.populateExisting(singleton, nodeKind, recordKey,
                modifiedLeafPage.getSlotMemory(), dataOffset, dataLength, deweyIdBytes,
                storageEngineReader.getResourceSession().getResourceConfig())) {
              return singleton; // Zero-allocation hot path
            }
          }
        }
      }
    }

    // Try compact-format / records[] on the modified page.
    record = storageEngineReader.getValue(modifiedPage, recordKey);
    if (record != null) {
      modifiedPage.setRecord(record);
      return record;
    }

    // Fall back to complete (on-disk) page.
    final DataRecord oldRecord = storageEngineReader.getValue(cont.getCompleteAsKeyValuePage(), recordKey);
    if (oldRecord == null) {
      throw new SirixIOException("Cannot retrieve record from cache: (key: " + recordKey + ") (indexType: " + indexType
          + ") (index: " + index + ")");
    }
    record = oldRecord;

    // Project disk-read records to fixed-slot format if eligible.
    // Fixed-slot bytes are now authoritative. Don't cache in records[] —
    // next access will use write singleton via fixed-slot path.
    if (modifiedPage instanceof KeyValueLeafPage modifiedLeafPage && indexType == IndexType.DOCUMENT) {
      final ResourceConfiguration resourceConfiguration = storageEngineReader.getResourceSession().getResourceConfig();
      if (tryProjectRecordIntoFixedSlot(record, modifiedLeafPage, resourceConfiguration)) {
        return record;
      }
    }
    // Non-fixed-slot: cache in records[] for stable reference identity.
    modifiedPage.setRecord(record);

    return record;
  }

  @Override
  public void updateRecordSlot(@NonNull final DataRecord record, @NonNull final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();
    requireNonNull(record);
    requireNonNull(indexType);
    checkArgument(record.getNodeKey() >= 0, "recordKey must be >= 0!");

    final long recordKey = record.getNodeKey();
    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer container = prepareRecordPage(recordPageKey, index, indexType);
    final KeyValuePage<DataRecord> modifiedPage = container.getModifiedAsKeyValuePage();

    if (!(modifiedPage instanceof KeyValueLeafPage modifiedLeafPage)) {
      modifiedPage.setRecord(record);
      return;
    }

    final ResourceConfiguration resourceConfiguration = storageEngineReader.getResourceSession().getResourceConfig();
    if (tryProjectRecordIntoFixedSlot(record, modifiedLeafPage, resourceConfiguration)) {
      // Fixed-slot projection succeeded — fixed-slot bytes are the authoritative
      // in-memory representation. No records[] entry needed; compactFixedSlotsForCommit
      // handles serialization at commit time.
      return;
    }

    // Non-fixed-slot records (payload-bearing kinds like StringNode/NumberNode):
    // store in records[] for commit-time serialization via processEntries().
    modifiedPage.setRecord(record);
  }

  private boolean tryProjectRecordIntoFixedSlot(final DataRecord record, final KeyValueLeafPage modifiedLeafPage,
      final ResourceConfiguration resourceConfiguration) {
    if (!(record.getKind() instanceof NodeKind nodeKind)) {
      return false;
    }

    final NodeKindLayout layout = nodeKind.layoutDescriptor();
    if (!layout.isFixedSlotSupported()) {
      return false;
    }

    // Only VALUE_BLOB payload refs are supported for inline projection.
    if (!layout.hasSupportedPayloads()) {
      return false;
    }

    final int fixedSlotSize = layout.fixedSlotSizeInBytes();
    if (fixedSlotSize <= 0) {
      return false;
    }

    // Compute inline payload length for payload-bearing nodes (strings, numbers).
    final int inlinePayloadLength = FixedSlotRecordProjector.computeInlinePayloadLength(record, layout);
    if (inlinePayloadLength < 0) {
      return false;
    }

    final int totalSlotSize = fixedSlotSize + inlinePayloadLength;
    if (totalSlotSize > PageConstants.MAX_RECORD_SIZE) {
      return false;
    }

    final int recordOffset = StorageEngineReader.recordPageOffset(record.getNodeKey());
    long dataOffset = modifiedLeafPage.getSlotDataOffset(recordOffset);
    int dataLength = (dataOffset >= 0)
        ? modifiedLeafPage.getSlotDataLength(recordOffset)
        : -1;
    if (dataOffset < 0 || dataLength != totalSlotSize) {
      final MemorySegment projectionBuffer = ensureFixedSlotProjectionBuffer(totalSlotSize);
      modifiedLeafPage.setSlot(projectionBuffer.asSlice(0, totalSlotSize), recordOffset);
      dataOffset = modifiedLeafPage.getSlotDataOffset(recordOffset);
      dataLength = (dataOffset >= 0)
          ? modifiedLeafPage.getSlotDataLength(recordOffset)
          : -1;
      if (dataOffset < 0 || dataLength != totalSlotSize) {
        return false;
      }
    }

    if (!FixedSlotRecordProjector.project(record, layout, modifiedLeafPage.getSlotMemory(), dataOffset)) {
      return false;
    }
    modifiedLeafPage.markSlotAsFixedFormat(recordOffset, nodeKind);

    if (resourceConfiguration.areDeweyIDsStored) {
      final byte[] deweyId = record.getDeweyIDAsBytes();
      if (deweyId != null && deweyId.length > 0) {
        modifiedLeafPage.setDeweyId(deweyId, recordOffset);
      }
    }

    return true;
  }

  private MemorySegment ensureFixedSlotProjectionBuffer(final int requiredSize) {
    if (fixedSlotProjectionBuffer == null || fixedSlotProjectionBuffer.byteSize() < requiredSize) {
      int newSize = 256;
      while (newSize < requiredSize) {
        newSize <<= 1;
      }
      fixedSlotProjectionBuffer = MemorySegment.ofArray(new byte[newSize]);
    }
    return fixedSlotProjectionBuffer;
  }

  @Override
  public DataRecord createRecord(@NonNull final DataRecord record, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    storageEngineReader.assertNotClosed();

    // Allocate record key and increment record count.
    // For RECORD_TO_REVISIONS: Use the record's own nodeKey (document node key) for page allocation.
    // This is critical because later lookups via prepareRecordForModification use the document node
    // key.
    // Using an auto-allocated key would cause a mismatch where the record can't be found.
    // $CASES-OMITTED$
    final long createdRecordKey = switch (indexType) {
      case DOCUMENT -> newRevisionRootPage.incrementAndGetMaxNodeKeyInDocumentIndex();
      case CHANGED_NODES -> newRevisionRootPage.incrementAndGetMaxNodeKeyInChangedNodesIndex();
      case RECORD_TO_REVISIONS -> {
        // CRITICAL FIX: Use the document node key for page allocation, not an auto-allocated key.
        // The RevisionReferencesNode stores document node key as its nodeKey, and we need to
        // be able to look it up later using that same key in prepareRecordForModification.
        // Still update max key for proper tracking.
        long documentNodeKey = record.getNodeKey();
        long currentMax = newRevisionRootPage.getMaxNodeKeyInRecordToRevisionsIndex();
        if (documentNodeKey > currentMax) {
          newRevisionRootPage.setMaxNodeKeyInRecordToRevisionsIndex(documentNodeKey);
        }
        yield documentNodeKey;
      }
      case PATH_SUMMARY -> {
        // CRITICAL FIX: Use accessor method instead of direct .getPage() call
        // PageReference.getPage() can return null after TIL.put() nulls it
        // Accessor methods use loadPage() which handles TIL lookups
        final PathSummaryPage pathSummaryPage = storageEngineReader.getPathSummaryPage(newRevisionRootPage);
        yield pathSummaryPage.incrementAndGetMaxNodeKey(index);
      }
      case CAS -> {
        final CASPage casPage = storageEngineReader.getCASPage(newRevisionRootPage);
        yield casPage.incrementAndGetMaxNodeKey(index);
      }
      case PATH -> {
        final PathPage pathPage = storageEngineReader.getPathPage(newRevisionRootPage);
        yield pathPage.incrementAndGetMaxNodeKey(index);
      }
      case NAME -> {
        final NamePage namePage = storageEngineReader.getNamePage(newRevisionRootPage);
        yield namePage.incrementAndGetMaxNodeKey(index);
      }
      default -> throw new IllegalStateException();
    };

    final long recordPageKey = storageEngineReader.pageKey(createdRecordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final KeyValuePage<DataRecord> modified = cont.getModifiedAsKeyValuePage();
    final boolean storedAsReusableProxy =
        persistReusableProxyRecordIfSupported(record, createdRecordKey, indexType, modified);
    if (!storedAsReusableProxy) {
      modified.setRecord(record);

      if (modified instanceof KeyValueLeafPage modifiedLeafPage && modifiedLeafPage.shouldDemoteRecords(indexType)) {
        if (demotionBuffer == null) {
          demotionBuffer = new MemorySegmentBytesOut(256);
        }
        modifiedLeafPage.demoteRecordsToSlots(storageEngineReader.getResourceSession().getResourceConfig(), demotionBuffer);
      }
    }

    return record;
  }

  private boolean persistReusableProxyRecordIfSupported(final DataRecord record, final long createdRecordKey,
      @NonNull final IndexType indexType, final KeyValuePage<DataRecord> modified) {
    if (indexType != IndexType.DOCUMENT || !(record instanceof ReusableNodeProxy)
        || !(modified instanceof KeyValueLeafPage modifiedLeafPage) || record.getNodeKey() != createdRecordKey) {
      return false;
    }

    if (demotionBuffer == null) {
      demotionBuffer = new MemorySegmentBytesOut(256);
    }
    demotionBuffer.clear();

    final ResourceConfiguration resourceConfiguration = storageEngineReader.getResourceSession().getResourceConfig();
    if (tryProjectRecordIntoFixedSlot(record, modifiedLeafPage, resourceConfiguration)) {
      return true;
    }

    resourceConfiguration.recordPersister.serialize(demotionBuffer, record, resourceConfiguration);
    final var serializedRecord = demotionBuffer.getDestination();

    if (serializedRecord.byteSize() > PageConstants.MAX_RECORD_SIZE) {
      final DataRecord detachedRecord = materializeDetachedRecord(serializedRecord, record, resourceConfiguration);
      modified.setRecord(detachedRecord);
      if (modifiedLeafPage.shouldDemoteRecords(indexType)) {
        modifiedLeafPage.demoteRecordsToSlots(resourceConfiguration, demotionBuffer);
      }
      return true;
    }

    final int recordOffset = StorageEngineReader.recordPageOffset(record.getNodeKey());
    modifiedLeafPage.setSlot(serializedRecord, recordOffset);
    modifiedLeafPage.markSlotAsCompactFormat(recordOffset);

    if (resourceConfiguration.areDeweyIDsStored) {
      final byte[] deweyId = record.getDeweyIDAsBytes();
      if (deweyId != null && deweyId.length > 0) {
        modifiedLeafPage.setDeweyId(deweyId, recordOffset);
      }
    }

    return true;
  }

  private DataRecord materializeDetachedRecord(final java.lang.foreign.MemorySegment serializedRecord,
      final DataRecord sourceRecord, final ResourceConfiguration resourceConfiguration) {
    return resourceConfiguration.recordPersister.deserialize(new MemorySegmentBytesIn(serializedRecord),
        sourceRecord.getNodeKey(), sourceRecord.getDeweyIDAsBytes(), resourceConfiguration);
  }

  @Override
  public void removeRecord(final long recordKey, @NonNull final IndexType indexType, final int index) {
    storageEngineReader.assertNotClosed();

    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final DataRecord node = getRecord(recordKey, indexType, index);
    if (node == null) {
      throw new IllegalStateException("Node not found: " + recordKey);
    }

    final Node delNode = new DeletedNode(
        new NodeDelegate(node.getNodeKey(), -1, null, -1, storageEngineReader.getRevisionNumber(), (SirixDeweyID) null));
    cont.getModifiedAsKeyValuePage().setRecord(delNode);
    cont.getCompleteAsKeyValuePage().setRecord(delNode);
  }

  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    storageEngineReader.assertNotClosed();

    checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
    requireNonNull(indexType);

    // Calculate page.
    final long recordPageKey = storageEngineReader.pageKey(recordKey, indexType);

    final PageContainer pageCont = getPageContainer(recordPageKey, index, indexType);

    if (pageCont == null) {
      return storageEngineReader.getRecord(recordKey, indexType, index);
    } else {
      DataRecord node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getModified()), recordKey);
      if (node == null) {
        node = getRecordForWriteAccess(((KeyValueLeafPage) pageCont.getComplete()), recordKey);
      }
      return (V) storageEngineReader.checkItemIfDeleted(node);
    }
  }

  private DataRecord getRecordForWriteAccess(final KeyValuePage<? extends DataRecord> page,
      final long recordKey) {
    final int recordOffset = StorageEngineReader.recordPageOffset(recordKey);
    final DataRecord cachedRecord = page.getRecord(recordOffset);
    if (cachedRecord != null) {
      return cachedRecord;
    }

    if (page instanceof KeyValueLeafPage leafPage && leafPage.isFixedSlotFormat(recordOffset)) {
      final NodeKind nodeKind = leafPage.getFixedSlotNodeKind(recordOffset);
      if (nodeKind == null) {
        throw new IllegalStateException("Fixed-slot bitmap set but node kind metadata is missing (key=" + recordKey
            + ", slot=" + recordOffset + ").");
      }

      final MemorySegment fixedSlotBytes = leafPage.getSlot(recordOffset);
      if (fixedSlotBytes == null) {
        throw new IllegalStateException(
            "Fixed-slot bitmap set but slot bytes are missing (key=" + recordKey + ", slot=" + recordOffset + ").");
      }

      final byte[] deweyIdBytes = leafPage.getDeweyIdAsByteArray(recordOffset);
      final DataRecord materialized = FixedSlotRecordMaterializer.materialize(nodeKind, recordKey, fixedSlotBytes,
          deweyIdBytes, storageEngineReader.getResourceSession().getResourceConfig());
      if (materialized == null) {
        throw new IllegalStateException("Unsupported fixed-slot write-path materialization for node kind " + nodeKind
            + " (key=" + recordKey + ", slot=" + recordOffset + ").");
      }
      return materialized;
    }

    return storageEngineReader.getValue(page, recordKey);
  }

  @Override
  public String getName(final int nameKey, @NonNull final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    final NamePage currentNamePage = getNamePage(newRevisionRootPage);
    return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind, storageEngineReader) == null)
        ? storageEngineReader.getName(nameKey, nodeKind)
        : currentNamePage.getName(nameKey, nodeKind, storageEngineReader);
  }

  @Override
  public int createNameKey(final @Nullable String name, @NonNull final NodeKind nodeKind) {
    storageEngineReader.assertNotClosed();
    requireNonNull(nodeKind);
    final String string = name == null
        ? ""
        : name;
    final NamePage namePage = getNamePage(newRevisionRootPage);
    return namePage.setName(string, nodeKind, this);
  }

  @Override
  public void commit(final @Nullable PageReference reference) {
    if (reference == null) {
      return;
    }

    PageContainer container = log.get(reference);

    if (container == null) {
      return;
    }

    final var page = container.getModified();

    // Recursively commit indirectly referenced pages and then write self.
    page.commit(this);
    storagePageReaderWriter.write(getResourceSession().getResourceConfig(), reference, page, bufferBytes);

    // DIAGNOSTIC: Track page closes during commit
    if (DEBUG_MEMORY_LEAKS && container.getComplete() instanceof KeyValueLeafPage completePage) {
      LOGGER.debug("[WRITER-COMMIT] Closing complete page: pageKey={}, indexType={}, instance={}",
          completePage.getPageKey(), completePage.getIndexType(), System.identityHashCode(completePage));
    }
    container.getComplete().close();

    if (DEBUG_MEMORY_LEAKS && page instanceof KeyValueLeafPage kvPage) {
      LOGGER.debug("[WRITER-COMMIT] Closing modified page: pageKey={}, indexType={}, instance={}", kvPage.getPageKey(),
          kvPage.getIndexType(), System.identityHashCode(kvPage));
    }
    page.close();

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
      final boolean isAutoCommitting) {
    storageEngineReader.assertNotClosed();

    storageEngineReader.resourceSession.getCommitLock().lock();

    try {
      final Path commitFile = storageEngineReader.resourceSession.getCommitFile();

      // Issues with windows that it's not created in the first time?
      createIfAbsent(commitFile);

      final PageReference uberPageReference =
          new PageReference().setDatabaseId(storageEngineReader.getDatabaseId()).setResourceId(storageEngineReader.getResourceId());
      final UberPage uberPage = storageEngineReader.getUberPage();
      uberPageReference.setPage(uberPage);

      setUserIfPresent();
      setCommitMessageAndTimestampIfRequired(commitMessage, commitTimestamp);

      // PIPELINING: Serialize pages WHILE previous fsync may still be running.
      // This overlaps CPU work (serialization) with IO work (fsync).
      parallelSerializationOfKeyValuePages();

      // Recursively write indirectly referenced pages (serializes to buffers).
      uberPage.commit(this);

      // Wait for the previous commit's async UberPage fsync to complete.
      // This ensures the previous revision is fully durable before we proceed.
      final var fsync = pendingFsync;
      if (fsync != null) {
        fsync.join();
        pendingFsync = null;
      }

      // CRITICAL crash-safety invariant (write-ahead property):
      // All data pages MUST be flushed to durable storage BEFORE the UberPage is written.
      // If the process crashes between writing data pages and writing the UberPage, the OS
      // kernel may flush the UberPage before the data pages, leaving the database pointing at
      // pages that are not yet on disk.  An explicit forceAll() here prevents that window.
      storagePageReaderWriter.forceAll();

      // Write UberPage — all data pages are now durable, safe to update the root pointer.
      storagePageReaderWriter.writeUberPageReference(getResourceSession().getResourceConfig(), uberPageReference,
          uberPage, bufferBytes);

      if (isAutoCommitting) {
        // Auto-commit mode: queue an async fsync for the UberPage so the next commit's
        // serialization can overlap with this IO.  The next commit will call pendingFsync.join()
        // before writing its own UberPage, guaranteeing ordering.
        // Even if the process crashes before this fsync completes the database is consistent:
        // the old (pre-UberPage) state is recovered because the new UberPage is not yet on disk.
        pendingFsync = java.util.concurrent.CompletableFuture.runAsync(() -> {
          try {
            storagePageReaderWriter.forceAll();
          } catch (final Exception e) {
            LOGGER.error("Async fsync failed", e);
            throw e;
          }
        });
      } else {
        // Regular commit: flush the UberPage synchronously so durability is guaranteed
        // before commit() returns to the caller.
        storagePageReaderWriter.forceAll();
        pendingFsync = null;
      }

      final int revision = uberPage.getRevisionNumber();
      serializeIndexDefinitions(revision);

      // CRITICAL: Release current page guard BEFORE TIL.clear()
      // If guard is on a TIL page, the page won't close (guardCount > 0 check)
      storageEngineReader.closeCurrentPageGuard();

      if (DEBUG_MEMORY_LEAKS) {
        LOGGER.debug("[WRITER-COMMIT] Clearing TIL with {} entries before commit", log.size());
      }

      // Clear TransactionIntentLog - closes all modified pages
      log.clear();

      // Clear local cache (pages are already handled by log.clear())
      pageContainerCache.clear();

      // Null out cache references since pages have been returned to pool
      mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
      secondMostRecentPageContainer = mostRecentPageContainer;
      mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);

      // Delete commit file which denotes that a commit must write the log in the data file.
      try {
        deleteIfExists(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException("Commit file couldn't be deleted!");
      }
    } finally {
      storageEngineReader.resourceSession.getCommitLock().unlock();
    }

    return readUberPage();
  }

  private void setCommitMessageAndTimestampIfRequired(@Nullable String commitMessage,
      @Nullable Instant commitTimestamp) {
    if (commitMessage != null) {
      newRevisionRootPage.setCommitMessage(commitMessage);
    }

    if (commitTimestamp != null) {
      newRevisionRootPage.setCommitTimestamp(commitTimestamp);
    }
  }

  private void serializeIndexDefinitions(int revision) {
    if (!indexController.getIndexes().getIndexDefs().isEmpty()) {
      final Path indexes = storageEngineReader.getResourceSession().getResourceConfig().resourcePath.resolve(
          ResourceConfiguration.ResourcePaths.INDEXES.getPath()).resolve(revision + ".xml");

      try (final OutputStream out = newOutputStream(indexes, CREATE)) {
        indexController.serialize(out);
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be serialized!", e);
      }
    }
  }

  /**
   * Threshold for switching from sequential to parallel processing. For small commits, parallel
   * stream overhead exceeds benefits.
   */
  private static final int PARALLEL_SERIALIZATION_THRESHOLD = 4;

  private void parallelSerializationOfKeyValuePages() {
    final var resourceConfig = getResourceSession().getResourceConfig();

    // Filter KeyValueLeafPages first to get accurate count
    var keyValuePages = log.getList()
                           .stream()
                           .map(PageContainer::getModified)
                           .filter(page -> page instanceof KeyValueLeafPage)
                           .toList();

    // Adaptive: use sequential for small commits to avoid parallel stream overhead
    var stream = keyValuePages.size() < PARALLEL_SERIALIZATION_THRESHOLD
        ? keyValuePages.stream()
        : keyValuePages.parallelStream();

    stream.forEach(page -> {
      // Use pooled buffer instead of creating new Arena.ofAuto() per page
      var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
      try {
        var bytes = new PooledBytesOut(pooledSeg);
        PageKind.KEYVALUELEAFPAGE.serializePage(resourceConfig, bytes, page, SerializationType.DATA);
      } catch (final SirixIOException e) {
        throw e;
      } catch (final RuntimeException e) {
        throw e;
      } catch (final Exception e) {
        throw new SirixIOException(e);
      } finally {
        SerializationBufferPool.INSTANCE.release(pooledSeg);
      }
    });
  }

  private UberPage readUberPage() {
    return (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(),
        getResourceSession().getResourceConfig());
  }

  private void createIfAbsent(final Path file) {
    while (!Files.exists(file)) {
      try {
        Files.createFile(file);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  }

  private void setUserIfPresent() {
    final Optional<User> optionalUser = storageEngineReader.resourceSession.getUser();
    optionalUser.ifPresent(newRevisionRootPage::setUser);
  }

  @Override
  public UberPage rollback() {
    storageEngineReader.assertNotClosed();

    if (DEBUG_MEMORY_LEAKS) {
      LOGGER.debug("[WRITER-ROLLBACK] Rolling back transaction with {} TIL entries", log.size());
    }

    // CRITICAL: Release current page guard BEFORE TIL.clear()
    // If guard is on a TIL page, the page won't close (guardCount > 0 check)
    storageEngineReader.closeCurrentPageGuard();

    // Clear TransactionIntentLog - closes all modified pages
    log.clear();

    // Clear local cache and reset references (pages already handled by log.clear())
    pageContainerCache.clear();
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = mostRecentPageContainer;
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);

    return readUberPage();
  }

  @Override
  public void close() {
    if (!isClosed) {
      storageEngineReader.assertNotClosed();

      // Wait for any pending async fsync to complete before closing
      final var fsync = pendingFsync;
      if (fsync != null) {
        try {
          fsync.join();
        } catch (final java.util.concurrent.CompletionException e) {
          LOGGER.error("Pending async fsync failed during close", e);
        }
        pendingFsync = null;
      }

      // Release the demotion buffer's off-heap memory.
      if (demotionBuffer != null) {
        demotionBuffer.close();
        demotionBuffer = null;
      }

      // Don't clear the cached containers here - they've either been:
      // 1. Already cleared and returned to pool during commit(), or
      // 2. Will be cleared and returned to pool by log.close() below
      // Clearing them here could corrupt pages that have been returned to pool
      // and reused by other transactions.

      final UberPage lastUberPage = readUberPage();

      storageEngineReader.resourceSession.setLastCommittedUberPage(lastUberPage);

      if (!isBoundToNodeTrx) {
        storageEngineReader.resourceSession.closePageWriteTransaction(storageEngineReader.getTrxId());
      }

      // CRITICAL: Close storageEngineReader FIRST to release guards BEFORE TIL tries to close pages
      // If guards are active when TIL.close() runs, pages won't close (guardCount > 0 check)
      storageEngineReader.close();

      // Now TIL can close pages (guards released)
      log.close();

      // CRITICAL FIX: Clear cache AFTER log.close() to avoid OOM
      // log.close() needs the cache entries to properly unpin/close pages
      // Once closed, we must drop references to allow GC

      // CRITICAL: Close pages in pageContainerCache that are NOT in TIL or cache
      // This handles pages that were cached but TIL was cleared (e.g., after commit)
      for (PageContainer container : pageContainerCache.values()) {
        closeOrphanedPagesInContainer(container);
      }

      pageContainerCache.clear();
      mostRecentPageContainer = null;
      secondMostRecentPageContainer = null;
      mostRecentPathSummaryPageContainer = null;

      isClosed = true;
    }
  }

  /**
   * Close orphaned pages in a container (pages not in cache). If page is in cache, cache will manage
   * it - we just drop our reference. If page is NOT in cache, we must release guard and close it.
   */
  private void closeOrphanedPagesInContainer(PageContainer container) {
    if (container == null) {
      return;
    }

    if (container.getComplete() instanceof KeyValueLeafPage completePage && !completePage.isClosed()) {
      // Check if page is in cache
      PageReference ref = new PageReference().setKey(completePage.getPageKey())
                                             .setDatabaseId(storageEngineReader.getDatabaseId())
                                             .setResourceId(storageEngineReader.getResourceId());
      KeyValueLeafPage cachedPage = storageEngineReader.getBufferManager().getRecordPageCache().get(ref);

      if (cachedPage != completePage) {
        // Page is NOT in cache - orphaned, must release guard and close
        if (completePage.getGuardCount() > 0) {
          completePage.releaseGuard();
        }
        completePage.close();
      }
      // If page IS in cache, cache will manage it - just drop our reference
    }

    if (container.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != container.getComplete()
        && !modifiedPage.isClosed()) {
      // Check if page is in cache
      PageReference ref = new PageReference().setKey(modifiedPage.getPageKey())
                                             .setDatabaseId(storageEngineReader.getDatabaseId())
                                             .setResourceId(storageEngineReader.getResourceId());
      KeyValueLeafPage cachedPage = storageEngineReader.getBufferManager().getRecordPageCache().get(ref);

      if (cachedPage != modifiedPage) {
        // Page is NOT in cache - orphaned, must release guard and close
        if (modifiedPage.getGuardCount() > 0) {
          modifiedPage.releaseGuard();
        }
        modifiedPage.close();
      }
      // If page IS in cache, cache will manage it - just drop our reference
    }
  }

  @Override
  public DeweyIDPage getDeweyIDPage(@NonNull RevisionRootPage revisionRoot) {
    // TODO
    return null;
  }

  private PageContainer getPageContainer(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    PageContainer pageContainer =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());
    if (pageContainer != null) {
      return pageContainer;
    }

    return pageContainerCache.computeIfAbsent(
        new IndexLogKey(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision()), _ -> {
          final PageReference pageReference = storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);
          final var leafPageReference =
              storageEngineReader.getLeafPageReference(pageReference, recordPageKey, indexNumber, indexType, newRevisionRootPage);
          return log.get(leafPageReference);
        });
  }

  @Nullable
  private PageContainer getMostRecentPageContainer(IndexType indexType, long recordPageKey,
      @NonNegative int indexNumber, @NonNegative int revisionNumber) {
    if (indexType == IndexType.PATH_SUMMARY) {
      return mostRecentPathSummaryPageContainer != null && mostRecentPathSummaryPageContainer.indexType == indexType
          && mostRecentPathSummaryPageContainer.indexNumber == indexNumber
          && mostRecentPathSummaryPageContainer.recordPageKey == recordPageKey
          && mostRecentPathSummaryPageContainer.revisionNumber == revisionNumber
              ? mostRecentPathSummaryPageContainer.pageContainer
              : null;
    }

    var pageContainer = mostRecentPageContainer != null && mostRecentPageContainer.indexType == indexType
        && mostRecentPageContainer.recordPageKey == recordPageKey && mostRecentPageContainer.indexNumber == indexNumber
        && mostRecentPageContainer.revisionNumber == revisionNumber
            ? mostRecentPageContainer.pageContainer
            : null;
    if (pageContainer == null) {
      pageContainer = secondMostRecentPageContainer != null && secondMostRecentPageContainer.indexType == indexType
          && secondMostRecentPageContainer.recordPageKey == recordPageKey
          && secondMostRecentPageContainer.indexNumber == indexNumber
          && secondMostRecentPageContainer.revisionNumber == revisionNumber
              ? secondMostRecentPageContainer.pageContainer
              : null;
    }
    return pageContainer;
  }

  /**
   * Prepare record page.
   *
   * @param recordPageKey the key of the record page
   * @param indexNumber the index number if it's a record-page of an index, {@code -1}, else
   * @param indexType the index type
   * @return {@link PageContainer} instance
   * @throws SirixIOException if an I/O error occurs
   */
  private PageContainer prepareRecordPage(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    assert indexType != null;

    // Route to appropriate trie implementation
    TrieType trieType = getTrieType(indexType, indexNumber);
    if (trieType == TrieType.HOT) {
      return prepareRecordPageViaHOT(recordPageKey, indexNumber, indexType);
    }

    // Traditional KEYED_TRIE path (bit-decomposed)
    return prepareRecordPageViaKeyedTrie(recordPageKey, indexNumber, indexType);
  }

  /**
   * Prepare record page using traditional bit-decomposed KEYED_TRIE.
   */
  private PageContainer prepareRecordPageViaKeyedTrie(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {

    PageContainer mostRecentPageContainer1 =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());

    if (mostRecentPageContainer1 != null && !mostRecentPageContainer1.getModified().isClosed()
        && !mostRecentPageContainer1.getComplete().isClosed()) {
      return mostRecentPageContainer1;
    }

    final Function<IndexLogKey, PageContainer> fetchPageContainer = _ -> {
      final PageReference pageReference = storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);

      // Get the reference to the unordered key/value page storing the records.
      final PageReference reference = trieWriter.prepareLeafOfTree(this, log, getUberPage().getPageCountExp(indexType),
          pageReference, recordPageKey, indexNumber, indexType, newRevisionRootPage);

      var pageContainer = log.get(reference);

      if (pageContainer != null) {
        return pageContainer;
      }

      if (reference.getKey() == Constants.NULL_ID_LONG) {
        // Direct allocation (no pool)
        final MemorySegmentAllocator allocator = OS.isWindows()
            ? WindowsMemorySegmentAllocator.getInstance()
            : LinuxMemorySegmentAllocator.getInstance();

        final KeyValueLeafPage completePage = new KeyValueLeafPage(recordPageKey, indexType,
            getResourceSession().getResourceConfig(), storageEngineReader.getRevisionNumber(), allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored
                ? allocator.allocate(SIXTYFOUR_KB)
                : null,
            false // Memory from allocator - release on close()
        );

        final KeyValueLeafPage modifyPage = new KeyValueLeafPage(recordPageKey, indexType,
            getResourceSession().getResourceConfig(), storageEngineReader.getRevisionNumber(), allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored
                ? allocator.allocate(SIXTYFOUR_KB)
                : null,
            false // Memory from allocator - release on close()
        );

        if (DEBUG_MEMORY_LEAKS && recordPageKey == 0) {
          LOGGER.debug("[WRITER-CREATE] Created Page 0 pair: indexType={}, rev={}, complete={}, modify={}", indexType,
              storageEngineReader.getRevisionNumber(), System.identityHashCode(completePage), System.identityHashCode(modifyPage));
        }

        pageContainer = PageContainer.getInstance(completePage, modifyPage);
        appendLogRecord(reference, pageContainer);
        return pageContainer;
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
        return pageContainer;
      }
    };

    var currPageContainer = pageContainerCache.computeIfAbsent(
        new IndexLogKey(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision()), fetchPageContainer);

    if (indexType == IndexType.PATH_SUMMARY) {
      mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(indexType, recordPageKey, indexNumber,
          newRevisionRootPage.getRevision(), currPageContainer);
    } else {
      secondMostRecentPageContainer = mostRecentPageContainer;
      mostRecentPageContainer = new IndexLogKeyToPageContainer(indexType, recordPageKey, indexNumber,
          newRevisionRootPage.getRevision(), currPageContainer);
    }

    return currPageContainer;
  }

  /**
   * Prepare record page using HOT (Height Optimized Trie).
   * 
   * <p>
   * This method uses the HOTTrieWriter for cache-friendly secondary index operations. HOT uses
   * semantic byte[] keys instead of bit-decomposed numeric keys.
   * </p>
   *
   * @param recordPageKey the record page key (used as fallback key encoding)
   * @param indexNumber the index number
   * @param indexType the index type (PATH, CAS, or NAME)
   * @return PageContainer with the HOT leaf page
   */
  private PageContainer prepareRecordPageViaHOT(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {

    // Get the root reference for this index
    final PageReference rootRef = storageEngineReader.getPageReference(newRevisionRootPage, indexType, indexNumber);

    // Encode the recordPageKey as a byte[] key for HOT navigation
    // For proper integration, the caller should pass the semantic key directly
    // This is a compatibility bridge for the existing API
    byte[] key = encodeRecordPageKeyForHOT(recordPageKey);

    // Use HOT trie writer to navigate and prepare the leaf
    PageContainer container =
        hotTrieWriter.prepareKeyedLeafForModification(this, log, rootRef, key, indexType, indexNumber);

    if (container != null) {
      return container;
    }

    // Fallback: If HOT navigation fails (e.g., empty tree),
    // create a new HOT leaf page
    HOTLeafPage newLeaf = new HOTLeafPage(recordPageKey, storageEngineReader.getRevisionNumber(), indexType);
    container = PageContainer.getInstance(newLeaf, newLeaf);

    PageReference newRef = new PageReference();
    newRef.setKey(recordPageKey);
    log.put(newRef, container);

    // Update root reference to point to new leaf
    if (rootRef.getKey() == Constants.NULL_ID_LONG) {
      rootRef.setKey(recordPageKey);
      log.put(rootRef, container);
    }

    return container;
  }

  /**
   * Encode a recordPageKey as a byte[] for HOT navigation.
   * 
   * <p>
   * This is a compatibility bridge. For optimal performance, callers should pass semantic keys
   * directly to HOT methods.
   * </p>
   *
   * @param recordPageKey the numeric page key
   * @return the key as a byte array (big-endian)
   */
  private static byte[] encodeRecordPageKeyForHOT(long recordPageKey) {
    byte[] key = new byte[8];
    key[0] = (byte) (recordPageKey >>> 56);
    key[1] = (byte) (recordPageKey >>> 48);
    key[2] = (byte) (recordPageKey >>> 40);
    key[3] = (byte) (recordPageKey >>> 32);
    key[4] = (byte) (recordPageKey >>> 24);
    key[5] = (byte) (recordPageKey >>> 16);
    key[6] = (byte) (recordPageKey >>> 8);
    key[7] = (byte) recordPageKey;
    return key;
  }

  /**
   * Dereference record page reference.
   *
   * @param reference reference to leaf, that is the record page
   * @return dereferenced page
   */
  @Override
  public PageContainer dereferenceRecordPageForModification(final PageReference reference) {
    final VersioningType versioningType = storageEngineReader.resourceSession.getResourceConfig().versioningType;
    final int mileStoneRevision = storageEngineReader.resourceSession.getResourceConfig().maxNumberOfRevisionsToRestore;

    // FULL versioning: Release any reader guard before loading for modification
    // This prevents double-guarding when the page is already in RecordPageCache from a read
    if (versioningType == VersioningType.FULL) {
      storageEngineReader.closeCurrentPageGuard();
    }

    final var result = storageEngineReader.getPageFragments(reference);

    // All fragments are guarded by getPageFragments() to prevent eviction during combining
    try {
      return versioningType.combineRecordPagesForModification(result.pages(), mileStoneRevision, this, reference, log);
    } finally {
      // Release guards on ALL fragments after combining
      for (var page : result.pages()) {
        KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        kvPage.releaseGuard();
        // For FULL versioning, the page might still have guards from concurrent readers
        // (which is fine - they'll release when done). For other versioning types,
        // fragments should have exactly 1 guard from getPageFragments.
        if (versioningType != VersioningType.FULL) {
          assert kvPage.getGuardCount() == 0
              : "Fragment should have guardCount=0 after release, but has " + kvPage.getGuardCount();
        }
      }
      // Note: Fragments remain in cache for potential reuse. ClockSweeper will evict them when
      // appropriate.
    }
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    storageEngineReader.assertNotClosed();
    return newRevisionRootPage;
  }

  @Override
  public @Nullable HOTLeafPage getHOTLeafPage(@NonNull IndexType indexType, int indexNumber) {
    storageEngineReader.assertNotClosed();

    // CRITICAL: Use newRevisionRootPage (not the delegate's rootPage) because
    // HOT pages are stored against the new revision's PathPage/CASPage/NamePage references.
    final RevisionRootPage actualRootPage = newRevisionRootPage;

    // Get the root reference for the index from the NEW revision root page
    final PageReference rootRef = switch (indexType) {
      case PATH -> {
        final PathPage pathPage = getPathPage(actualRootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferences().size()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = getCASPage(actualRootPage);
        if (casPage == null || indexNumber >= casPage.getReferences().size()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = getNamePage(actualRootPage);
        if (namePage == null || indexNumber >= namePage.getReferences().size()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };

    if (rootRef == null) {
      return null;
    }

    // Check transaction log for uncommitted pages (this is the key for write transactions!)
    final PageContainer container = log.get(rootRef);
    if (container != null) {
      // Try modified first (the one being written to), then complete
      Page modified = container.getModified();
      if (modified instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
      Page complete = container.getComplete();
      if (complete instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
    }

    // Check if page is swizzled (directly on reference)
    if (rootRef.getPage() instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }

    // For uncommitted pages with no storage key, we're done
    if (rootRef.getKey() < 0 && rootRef.getLogKey() < 0) {
      return null;
    }

    // Try buffer cache or load from storage (for previously committed data)
    return storageEngineReader.getHOTLeafPage(indexType, indexNumber);
  }

  @Override
  public io.sirix.page.interfaces.@Nullable Page loadHOTPage(@NonNull PageReference reference) {
    storageEngineReader.assertNotClosed();

    if (reference == null) {
      return null;
    }

    // Check transaction log first for uncommitted pages
    final PageContainer container = log.get(reference);
    if (container != null) {
      Page modified = container.getModified();
      if (modified instanceof HOTLeafPage || modified instanceof HOTIndirectPage) {
        return modified;
      }
      Page complete = container.getComplete();
      if (complete instanceof HOTLeafPage || complete instanceof HOTIndirectPage) {
        return complete;
      }
    }

    // Delegate to the reader
    return storageEngineReader.loadHOTPage(reference);
  }

  @Override
  protected @NonNull StorageEngineReader delegate() {
    return storageEngineReader;
  }

  @Override
  public StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  @Override
  public StorageEngineWriter appendLogRecord(@NonNull final PageReference reference,
      @NonNull final PageContainer pageContainer) {
    requireNonNull(pageContainer);
    log.put(reference, pageContainer);
    return this;
  }

  @Override
  public PageContainer getLogRecord(final PageReference reference) {
    requireNonNull(reference);
    return log.get(reference);
  }

  @Override
  public StorageEngineWriter truncateTo(final int revision) {
    storagePageReaderWriter.truncateTo(this, revision);
    return this;
  }

  @Override
  public int getTrxId() {
    return storageEngineReader.getTrxId();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return storageEngineReader.getCommitCredentials();
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void finalize() {
    // DIAGNOSTIC: Detect if NodeStorageEngineWriter is GC'd without being closed
    if (!isClosed && KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.warn(
          "⚠️  NodeStorageEngineWriter FINALIZED WITHOUT CLOSE: trxId={} instance={} TIL={} with {} containers in TIL",
          storageEngineReader.getTrxId(), System.identityHashCode(this), System.identityHashCode(log), log.getList().size());
    }
  }

  /**
   * Acquire a guard on the page containing the current node. This is needed when holding a reference
   * to a node across cursor movements.
   *
   * @return a PageGuard that must be closed when done with the node
   */
  public PageGuard acquireGuardForCurrentNode() {
    // The current node is in the storageEngineReader's currentPageGuard
    // We need to return a new guard on the same page
    // Get the page containing the current node from storageEngineReader
    final var currentPage = ((NodeStorageEngineReader) storageEngineReader).getCurrentPage();
    if (currentPage == null) {
      throw new IllegalStateException("No current page - cannot acquire guard");
    }
    return new PageGuard(currentPage);
  }

  private static final QNm EMPTY_QNM = new QNm("");

  /**
   * Get or lazily create a write-path singleton for the given node kind. Returns null for unsupported
   * kinds (non-XML/JSON).
   */
  private DataRecord getOrCreateWriteSingleton(final NodeKind kind) {
    final var hashFunction = storageEngineReader.getResourceSession().getResourceConfig().nodeHashFunction;
    return switch (kind) {
      case OBJECT -> {
        if (writeObjectSingleton == null) {
          writeObjectSingleton = new ObjectNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeObjectSingleton;
      }
      case ARRAY -> {
        if (writeArraySingleton == null) {
          writeArraySingleton = new ArrayNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeArraySingleton;
      }
      case OBJECT_KEY -> {
        if (writeObjectKeySingleton == null) {
          writeObjectKeySingleton = new ObjectKeyNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeObjectKeySingleton;
      }
      case STRING_VALUE -> {
        if (writeStringSingleton == null) {
          writeStringSingleton = new StringNode(0, 0, 0, 0, 0, 0, 0, null, hashFunction, (byte[]) null);
        }
        yield writeStringSingleton;
      }
      case NUMBER_VALUE -> {
        if (writeNumberSingleton == null) {
          writeNumberSingleton = new NumberNode(0, 0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeNumberSingleton;
      }
      case BOOLEAN_VALUE -> {
        if (writeBooleanSingleton == null) {
          writeBooleanSingleton = new BooleanNode(0, 0, 0, 0, 0, 0, 0, false, hashFunction, (byte[]) null);
        }
        yield writeBooleanSingleton;
      }
      case NULL_VALUE -> {
        if (writeNullSingleton == null) {
          writeNullSingleton = new NullNode(0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeNullSingleton;
      }
      case OBJECT_STRING_VALUE -> {
        if (writeObjectStringSingleton == null) {
          writeObjectStringSingleton = new ObjectStringNode(0, 0, 0, 0, 0, null, hashFunction, (byte[]) null);
        }
        yield writeObjectStringSingleton;
      }
      case OBJECT_NUMBER_VALUE -> {
        if (writeObjectNumberSingleton == null) {
          writeObjectNumberSingleton = new ObjectNumberNode(0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeObjectNumberSingleton;
      }
      case OBJECT_BOOLEAN_VALUE -> {
        if (writeObjectBooleanSingleton == null) {
          writeObjectBooleanSingleton = new ObjectBooleanNode(0, 0, 0, 0, 0, false, hashFunction, (byte[]) null);
        }
        yield writeObjectBooleanSingleton;
      }
      case OBJECT_NULL_VALUE -> {
        if (writeObjectNullSingleton == null) {
          writeObjectNullSingleton = new ObjectNullNode(0, 0, 0, 0, 0, hashFunction, (byte[]) null);
        }
        yield writeObjectNullSingleton;
      }
      case JSON_DOCUMENT -> {
        if (writeJsonDocumentSingleton == null) {
          writeJsonDocumentSingleton = new JsonDocumentRootNode(0, 0, 0, 0, 0, hashFunction);
        }
        yield writeJsonDocumentSingleton;
      }
      case XML_DOCUMENT -> {
        if (writeXmlDocumentSingleton == null) {
          writeXmlDocumentSingleton = new XmlDocumentRootNode(0, 0, 0, 0, 0, hashFunction);
        }
        yield writeXmlDocumentSingleton;
      }
      case ELEMENT -> {
        if (writeElementSingleton == null) {
          writeElementSingleton = new ElementNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction,
              (byte[]) null, null, null, EMPTY_QNM);
        }
        yield writeElementSingleton;
      }
      case TEXT -> {
        if (writeTextSingleton == null) {
          writeTextSingleton = new TextNode(0, 0, 0, 0, 0, 0, 0, new byte[0], false, hashFunction, (byte[]) null);
        }
        yield writeTextSingleton;
      }
      case COMMENT -> {
        if (writeCommentSingleton == null) {
          writeCommentSingleton = new CommentNode(0, 0, 0, 0, 0, 0, 0, new byte[0], false, hashFunction, (byte[]) null);
        }
        yield writeCommentSingleton;
      }
      case ATTRIBUTE -> {
        if (writeAttributeSingleton == null) {
          writeAttributeSingleton =
              new AttributeNode(0, 0, 0, 0, 0, 0, 0, 0, 0, new byte[0], hashFunction, (byte[]) null, EMPTY_QNM);
        }
        yield writeAttributeSingleton;
      }
      case NAMESPACE -> {
        if (writeNamespaceSingleton == null) {
          writeNamespaceSingleton =
              new NamespaceNode(0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, (byte[]) null, EMPTY_QNM);
        }
        yield writeNamespaceSingleton;
      }
      case PROCESSING_INSTRUCTION -> {
        if (writePISingleton == null) {
          writePISingleton = new PINode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new byte[0], false, hashFunction,
              (byte[]) null, EMPTY_QNM);
        }
        yield writePISingleton;
      }
      default -> null;
    };
  }

  /**
   * Package-private helper class for managing the trie structure of IndirectPages.
   * 
   * <p>
   * Sirix uses a trie (not a B+tree) to organize IndirectPages. Page keys are decomposed bit-by-bit
   * to determine the path through the trie levels. This enables efficient navigation and modification
   * of the multi-level page structure.
   * </p>
   * 
   * <p>
   * For example, with pageKey=1024 and level exponents [10, 10, 10]:
   * <ul>
   * <li>Level 0: offset = 1024 >> 10 = 1</li>
   * <li>Level 1: offset = (1024 - 1024) >> 0 = 0</li>
   * </ul>
   * </p>
   * 
   * <p>
   * This class creates new IndirectPages as needed when navigating to leaves, and manages the
   * transaction intent log for all modified pages.
   * </p>
   *
   * <p>
   * Note: Package-private access is needed for StorageEngineWriterFactory to call
   * preparePreviousRevisionRootPage() before NodeStorageEngineWriter is constructed.
   * </p>
   *
   * @author Johannes Lichtenberger
   */
  static final class TrieWriter {

    /**
     * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
     *
     * @param uberPage the uber page
     * @param storageEngineReader the page reading transaction
     * @param log the transaction intent log
     * @param baseRevision base revision
     * @param representRevision the revision to represent
     * @return new {@link RevisionRootPage} instance
     */
    RevisionRootPage preparePreviousRevisionRootPage(final UberPage uberPage, final NodeStorageEngineReader storageEngineReader,
        final TransactionIntentLog log, final @NonNegative int baseRevision, final @NonNegative int representRevision) {
      final RevisionRootPage revisionRootPage;

      if (uberPage.isBootstrap()) {
        revisionRootPage = storageEngineReader.loadRevRoot(baseRevision);
      } else {
        // Prepare revision root nodePageReference.
        revisionRootPage = new RevisionRootPage(storageEngineReader.loadRevRoot(baseRevision), representRevision + 1);

        // Link the prepared revision root nodePageReference with the prepared indirect tree.
        final var revRootRef =
            new PageReference().setDatabaseId(storageEngineReader.getDatabaseId()).setResourceId(storageEngineReader.getResourceId());
        log.put(revRootRef, PageContainer.getInstance(revisionRootPage, revisionRootPage));
      }

      // Return prepared revision root nodePageReference.
      return revisionRootPage;
    }

    /**
     * Prepare the leaf of the trie, navigating through IndirectPages using bit-decomposition.
     *
     * @param storageEngineReader the page reading transaction
     * @param log the transaction intent log
     * @param inpLevelPageCountExp array which holds the maximum number of indirect page references per
     *        trie level
     * @param startReference the reference to start the trie traversal from
     * @param pageKey page key to lookup (decomposed bit-by-bit)
     * @param index the index number or {@code -1} if a regular record page should be prepared
     * @param indexType the index type
     * @param revisionRootPage the revision root page
     * @return {@link PageReference} instance pointing to the leaf page
     */
    PageReference prepareLeafOfTree(final StorageEngineWriter storageEngineReader, final TransactionIntentLog log,
        final int[] inpLevelPageCountExp, final PageReference startReference, @NonNegative final long pageKey,
        final int index, final IndexType indexType, final RevisionRootPage revisionRootPage) {
      // Initial state pointing to the indirect nodePageReference of level 0.
      PageReference reference = startReference;

      int offset;
      long levelKey = pageKey;

      int maxHeight = storageEngineReader.getCurrentMaxIndirectPageTreeLevel(indexType, index, revisionRootPage);

      // Check if we need an additional level of indirect pages.
      if (pageKey == (1L << inpLevelPageCountExp[inpLevelPageCountExp.length - maxHeight - 1])) {
        maxHeight = incrementCurrentMaxIndirectPageTreeLevel(storageEngineReader, revisionRootPage, indexType, index);

        // Add a new indirect page to the top of the trie and to the transaction-log.
        final IndirectPage page = new IndirectPage();

        // Get the first reference.
        final PageReference newReference = page.getOrCreateReference(0);

        newReference.setKey(reference.getKey());
        newReference.setLogKey(reference.getLogKey());
        newReference.setPage(reference.getPage());
        newReference.setPageFragments(reference.getPageFragments());

        // Create new page reference, add it to the transaction-log and reassign it in the root pages
        // of the trie.
        final PageReference newPageReference =
            new PageReference().setDatabaseId(storageEngineReader.getDatabaseId()).setResourceId(storageEngineReader.getResourceId());
        log.put(newPageReference, PageContainer.getInstance(page, page));
        setNewIndirectPage(storageEngineReader, revisionRootPage, indexType, index, newPageReference);

        reference = newPageReference;
      }

      // Iterate through all levels using bit-decomposition.
      for (int level = inpLevelPageCountExp.length - maxHeight,
          height = inpLevelPageCountExp.length; level < height; level++) {
        offset = (int) (levelKey >> inpLevelPageCountExp[level]);
        levelKey -= (long) offset << inpLevelPageCountExp[level];
        final IndirectPage page = prepareIndirectPage(storageEngineReader, log, reference);
        reference = page.getOrCreateReference(offset);
      }

      // Return reference to leaf of indirect trie.
      return reference;
    }

    /**
     * Prepare indirect page, that is getting the referenced indirect page or creating a new page and
     * putting the whole path into the log.
     *
     * @param storageEngineReader the page reading transaction
     * @param log the transaction intent log
     * @param reference {@link PageReference} to get the indirect page from or to create a new one
     * @return {@link IndirectPage} reference
     */
    IndirectPage prepareIndirectPage(final StorageEngineReader storageEngineReader, final TransactionIntentLog log,
        final PageReference reference) {
      final PageContainer cont = log.get(reference);
      IndirectPage page = cont == null
          ? null
          : (IndirectPage) cont.getComplete();
      if (page == null) {
        if (reference.getKey() == Constants.NULL_ID_LONG) {
          page = new IndirectPage();
        } else {
          final IndirectPage indirectPage = storageEngineReader.dereferenceIndirectPageReference(reference);
          page = new IndirectPage(indirectPage);
        }
        log.put(reference, PageContainer.getInstance(page, page));
      }
      return page;
    }

    /**
     * Set a new indirect page in the appropriate index structure.
     */
    private void setNewIndirectPage(final StorageEngineReader storageEngineReader, final RevisionRootPage revisionRoot,
        final IndexType indexType, final int index, final PageReference pageReference) {
      // $CASES-OMITTED$
      switch (indexType) {
        case DOCUMENT -> revisionRoot.setOrCreateReference(0, pageReference);
        case CHANGED_NODES -> revisionRoot.setOrCreateReference(1, pageReference);
        case RECORD_TO_REVISIONS -> revisionRoot.setOrCreateReference(2, pageReference);
        case CAS -> storageEngineReader.getCASPage(revisionRoot).setOrCreateReference(index, pageReference);
        case PATH -> storageEngineReader.getPathPage(revisionRoot).setOrCreateReference(index, pageReference);
        case NAME -> storageEngineReader.getNamePage(revisionRoot).setOrCreateReference(index, pageReference);
        case PATH_SUMMARY -> storageEngineReader.getPathSummaryPage(revisionRoot).setOrCreateReference(index, pageReference);
        default ->
          throw new IllegalStateException("Only defined for node, path summary, text value and attribute value pages!");
      }
    }

    /**
     * Increment the current maximum indirect page trie level for the given index type.
     */
    private int incrementCurrentMaxIndirectPageTreeLevel(final StorageEngineReader storageEngineReader,
        final RevisionRootPage revisionRoot, final IndexType indexType, final int index) {
      // $CASES-OMITTED$
      return switch (indexType) {
        case DOCUMENT -> revisionRoot.incrementAndGetCurrentMaxLevelOfDocumentIndexIndirectPages();
        case CHANGED_NODES -> revisionRoot.incrementAndGetCurrentMaxLevelOfChangedNodesIndexIndirectPages();
        case RECORD_TO_REVISIONS -> revisionRoot.incrementAndGetCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();
        case CAS -> storageEngineReader.getCASPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case PATH -> storageEngineReader.getPathPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case NAME -> storageEngineReader.getNamePage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case PATH_SUMMARY ->
          storageEngineReader.getPathSummaryPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        default ->
          throw new IllegalStateException("Only defined for node, path summary, text value and attribute value pages!");
      };
    }
  }
}
