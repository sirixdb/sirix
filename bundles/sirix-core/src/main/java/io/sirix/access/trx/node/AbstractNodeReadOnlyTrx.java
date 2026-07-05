package io.sirix.access.trx.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.ItemList;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageGuard;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.service.xml.xpath.AtomicValue;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import org.jspecify.annotations.Nullable;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A skeletal implementation of a read-only node transaction.
 *
 * @param <T> the type of node cursor
 */
public abstract class AbstractNodeReadOnlyTrx<T extends NodeCursor & NodeReadOnlyTrx, W extends NodeTrx & NodeCursor, N extends ImmutableNode>
    implements InternalNodeReadOnlyTrx<N>, NodeCursor, NodeReadOnlyTrx {

  /**
   * ID of transaction.
   */
  protected final int id;

  /**
   * State of transaction including all cached stuff.
   *
   * <p>Volatile because auto-commit swaps the engine from the commit-timer thread
   * ({@code reInstantiate()} closes the old engine, then publishes the new one via
   * {@link #setPageReadTransaction(StorageEngineReader)}) while the owning thread may
   * concurrently read — a plain field could pin a stale (closed) engine forever.
   */
  protected volatile StorageEngineReader storageEngineReader;

  /**
   * The revision this transaction works on, cached from the engine at every engine handoff.
   * The value is immutable per engine instance, so reading it here instead of dereferencing
   * {@link #storageEngineReader} keeps {@link #getRevisionNumber()} safe against the
   * post-auto-commit swap window in which the field is momentarily {@code null} or still
   * points at the just-closed engine (auto-commit explicitly invites such cross-thread reads).
   */
  private volatile int revisionNumber;

  /**
   * The current node.
   */
  private N currentNode;

  /**
   * Resource manager this write transaction is bound to.
   */
  protected final InternalResourceSession<T, W> resourceSession;

  /**
   * Tracks whether the transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Read-transaction-exclusive item list.
   */
  protected final ItemList<AtomicValue> itemList;
  
  // ==================== CURSOR STATE ====================
  
  /**
   * The current node's key (used for delta decoding).
   */
  private long currentNodeKey;
  
  /**
   * The current node's kind.
   */
  private NodeKind currentNodeKind;
  
  /**
   * Page guard protecting the current page from eviction.
   * MUST be released when moving to a different node or closing the transaction.
   */
  private PageGuard currentPageGuard;
  
  /**
   * The page key of the currently held page guard.
   * Used to detect same-page moves and avoid guard release/reacquire overhead.
   */
  private long currentPageKey = -1;
  
  /**
   * The current page reference (same page as currentPageGuard).
   * Cached to avoid re-lookup when moving within the same page.
   */
  private KeyValueLeafPage currentPage;
  
  /**
   * Reusable BytesIn instance for reading node data.
   * Avoids allocation on every moveTo() call.
   */
  private final MemorySegmentBytesIn reusableBytesIn = new MemorySegmentBytesIn(MemorySegment.NULL);
  
  /**
   * Resource configuration cached for hash type checks.
   */
  protected final ResourceConfiguration resourceConfig;

  /**
   * Cached {@link NodeStorageEngineReader} resolved once from {@link #storageEngineReader}.
   * For read-only transactions, this is the reader itself.
   * For write transactions, this is the delegate reader extracted from the writer.
   * Used by {@link #moveTo(long)} to enable singleton mode without per-call instanceof checks.
   */
  private NodeStorageEngineReader cachedNodeReader;

  /**
   * Cached {@link StorageEngineWriter} reference, non-null only for write transactions.
   * Used by {@link #moveToSingletonSlowPath} to resolve TIL modified pages.
   */
  private StorageEngineWriter cachedWriter;

  /**
   * Constructor.
   *
   * @param trxId               the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode        the document root node
   * @param resourceSession     The resource manager for the current transaction
   * @param itemList            Read-transaction-exclusive item list.
   */
  protected AbstractNodeReadOnlyTrx(final int trxId, final StorageEngineReader pageReadTransaction,
      final N documentNode, final InternalResourceSession<T, W> resourceSession,
      final ItemList<AtomicValue> itemList) {
    this.itemList = itemList;
    this.resourceSession = requireNonNull(resourceSession);
    this.id = trxId;
    this.storageEngineReader = requireNonNull(pageReadTransaction);
    this.revisionNumber = pageReadTransaction.getActualRevisionRootPage().getRevision();
    this.currentNode = requireNonNull(documentNode);
    this.isClosed = false;
    this.resourceConfig = resourceSession.getResourceConfig();
    this.cachedNodeReader = resolveNodeReader(pageReadTransaction);
    this.cachedWriter = (pageReadTransaction instanceof StorageEngineWriter w) ? w : null;

    // Initialize cursor state from document node.
    this.currentNodeKey = documentNode.getNodeKey();
    this.currentNodeKind = documentNode.getKind();
  }

  @Override
  public N getCurrentNode() {
    if (currentNode != null) {
      return currentNode;
    }

    // When in singleton mode, create a snapshot (deep copy) of the singleton.
    // Snapshot semantics are required because singleton instances are reused across moveTo calls.
    if (SINGLETON_ENABLED && singletonMode && currentSingleton != null) {
      currentNode = createSingletonSnapshot();
      return currentNode;
    }

    return currentNode;
  }
  
  /**
   * Create a deep copy snapshot of the current singleton node.
   * The snapshot is a new object with all values copied, safe to hold across cursor moves.
   *
   * @return a snapshot of the current singleton
   */
  @SuppressWarnings("unchecked")
  private N createSingletonSnapshot() {
    return switch (currentNodeKind) {
      case OBJECT -> (N) ((ObjectNode) currentSingleton).toSnapshot();
      case ARRAY -> (N) ((ArrayNode) currentSingleton).toSnapshot();
      case STRING_VALUE -> (N) ((StringNode) currentSingleton).toSnapshot();
      case NUMBER_VALUE -> (N) ((NumberNode) currentSingleton).toSnapshot();
      case BOOLEAN_VALUE -> (N) ((BooleanNode) currentSingleton).toSnapshot();
      case NULL_VALUE -> (N) ((NullNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_BOOLEAN -> (N) ((ObjectNamedBooleanNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_NUMBER -> (N) ((ObjectNamedNumberNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_STRING -> (N) ((ObjectNamedStringNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_NULL -> (N) ((ObjectNamedNullNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_OBJECT -> (N) ((ObjectNamedObjectNode) currentSingleton).toSnapshot();
      case OBJECT_NAMED_ARRAY -> (N) ((ObjectNamedArrayNode) currentSingleton).toSnapshot();
      case JSON_DOCUMENT -> (N) ((JsonDocumentRootNode) currentSingleton).toSnapshot();
      case ELEMENT -> (N) ((ElementNode) currentSingleton).toSnapshot();
      case ATTRIBUTE -> (N) ((AttributeNode) currentSingleton).toSnapshot();
      case TEXT -> (N) ((TextNode) currentSingleton).toSnapshot();
      case COMMENT -> (N) ((CommentNode) currentSingleton).toSnapshot();
      case PROCESSING_INSTRUCTION -> (N) ((PINode) currentSingleton).toSnapshot();
      case NAMESPACE -> (N) ((NamespaceNode) currentSingleton).toSnapshot();
      case XML_DOCUMENT -> (N) ((XmlDocumentRootNode) currentSingleton).toSnapshot();
      default -> throw new IllegalStateException("Unexpected singleton kind: " + currentNodeKind);
    };
  }
  
  @Override
  public void setCurrentNode(final @Nullable N currentNode) {
    assertNotClosed();
    this.currentNode = currentNode;
    
    if (currentNode != null) {
      this.singletonMode = false;
      this.currentSingleton = null;
      this.currentNodeKey = currentNode.getNodeKey();
      this.currentNodeKind = currentNode.getKind();
      // Release page guard since we're not reading from slot anymore
      releaseCurrentPageGuard();
    }
  }

  @Override
  public boolean storeDeweyIDs() {
    return resourceSession.getResourceConfig().areDeweyIDsStored;
  }

  @Override
  public ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession() {
    return resourceSession;
  }

  @Override
  public Optional<User> getUser() {
    return storageEngineReader.getActualRevisionRootPage().getUser();
  }

  @Override
  public boolean moveToPrevious() {
    assertNotClosed();
    // Use flyweight getters to avoid node materialization
    if (hasLeftSibling()) {
      // Left sibling node.
      boolean leftSiblMove = moveTo(getLeftSiblingKey());
      // Now move down to rightmost descendant node if it has one.
      while (hasFirstChild()) {
        leftSiblMove = moveToLastChild();
      }
      return leftSiblMove;
    }
    // Parent node.
    return moveTo(getParentKey());
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    assertNotClosed();
    if (hasLeftSibling()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLeftSibling();
      final NodeKind leftSiblingKind = getKind();
      moveTo(savedNodeKey);
      return leftSiblingKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    if (fusedSyntheticChildMode) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getLeftSiblingKey();
    }
    return getStructuralNodeView().getLeftSiblingKey();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNodeView().hasLeftSibling();
  }

  @Override
  public boolean moveToLeftSibling() {
    assertNotClosed();
    // Use flyweight getter if available to avoid node materialization
    if (!hasLeftSibling()) {
      return false;
    }
    return moveTo(getLeftSiblingKey());
  }

  @Override
  public int keyForName(final String name) {
    assertNotClosed();
    return NamePageHash.generateHashForString(name);
  }

  @Override
  public String nameForKey(final int key) {
    assertNotClosed();
    return storageEngineReader.getName(key, getKind());
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode && currentSingleton != null) {
      if (currentSingleton instanceof NameNode nameNode) {
        return nameNode.getPathNodeKey();
      }
      if (currentSingleton instanceof ArrayNode arrayNode) {
        return arrayNode.getPathNodeKey();
      }
      if (currentNodeKind == NodeKind.XML_DOCUMENT || currentNodeKind == NodeKind.JSON_DOCUMENT) {
        return 0;
      }
      return -1;
    }

    final ImmutableNode node = getCurrentNode();
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node instanceof ArrayNode arrayNode) {
      return arrayNode.getPathNodeKey();
    }
    if (node.getKind() == NodeKind.XML_DOCUMENT || node.getKind() == NodeKind.JSON_DOCUMENT) {
      return 0;
    }
    return -1;
  }

  @Override
  public int getId() {
    assertNotClosed();
    return id;
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    // Served from the cached value instead of the engine: during the post-commit engine swap
    // (KEEP_OPEN auto-commit) the engine reference is transiently null or already closed, and
    // dereferencing it from another thread raced into "Transaction is already closed!" errors.
    return revisionNumber;
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(storageEngineReader.getActualRevisionRootPage().getRevisionTimestamp());
  }

  @Override
  public boolean moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public boolean moveToParent() {
    assertNotClosed();
    return moveTo(getParentKey());
  }

  @Override
  public boolean moveToFirstChild() {
    assertNotClosed();

    // iter#31 pivot to Option B: fused OBJECT_NAMED_* records are leaves. No synthetic
    // child is emitted. Callers read the inline primitive value directly via
    // rtx.getValue() / getNumberValue() / etc.

    // Use flyweight getter if available to avoid node materialization
    if (!hasFirstChild()) {
      return false;
    }
    return moveTo(getFirstChildKey());
  }

  /**
   * @return true if the cursor currently targets a fused {@code OBJECT_NAMED_*} record
   *         (and we are NOT already descended into its synthetic child).
   */
  private boolean isOnFusedNamedPrimitive() {
    if (fusedSyntheticChildMode) {
      return false;
    }
    final NodeKind kind = SINGLETON_ENABLED && singletonMode ? currentNodeKind
        : getCurrentNode() != null ? (NodeKind) getCurrentNode().getKind() : null;
    return kind == NodeKind.OBJECT_NAMED_BOOLEAN
        || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING
        || kind == NodeKind.OBJECT_NAMED_NULL;
  }

  /**
   * @return the underlying (non-synthetic) kind of the cursor, regardless of whether we are
   *         in fused synthetic-child mode.
   */
  protected NodeKind getFusedParentKind() {
    if (SINGLETON_ENABLED && singletonMode) {
      return currentNodeKind;
    }
    final N node = getCurrentNode();
    return node != null ? (NodeKind) node.getKind() : null;
  }

  /**
   * @return true if we are currently viewing the synthetic primitive-value child of a
   *         fused {@code OBJECT_NAMED_*} record (Option A virtual navigation).
   */
  public boolean isFusedSyntheticChild() {
    return fusedSyntheticChildMode;
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    assertNotClosed();

    // Any moveTo implicitly exits fused synthetic-child mode — the caller is redirecting the
    // cursor so the virtual-child state does not survive.
    fusedSyntheticChildMode = false;

    // Handle negative keys (item list) - fall back to object mode
    if (nodeKey < 0) {
      return moveToItemList(nodeKey);
    }

    // Use singleton mode for READ-ONLY transactions (cachedWriter == null).
    // Write transactions fall through to moveToLegacy for now — the writer's overridden
    // getRecord() provides TIL-aware page resolution that moveToSingleton needs.
    if (SINGLETON_ENABLED && cachedNodeReader != null && cachedWriter == null) {
      return moveToSingleton(nodeKey, cachedNodeReader);
    }

    // Write path: use moveToSingletonWrite for TIL-aware singleton mode
    if (SINGLETON_ENABLED && cachedWriter != null && cachedNodeReader != null) {
      return moveToSingletonWrite(nodeKey, cachedNodeReader, cachedWriter);
    }

    // Fallback to traditional object mode
    return moveToLegacy(nodeKey);
  }
  
  /**
   * Toggle for singleton mode. Set to true to enable singleton node reuse.
   * Singleton mode uses mutable singleton nodes that are repopulated on each moveTo().
   * When combined with cache checking, uses cached records when available.
   */
  private static final boolean SINGLETON_ENABLED = true;
  
  /**
   * Whether currently in singleton mode (using singleton nodes).
   */
  private boolean singletonMode = false;
  
  /**
   * The current singleton node (set when in singletonMode).
   */
  private ImmutableNode currentSingleton;

  /**
   * Array-based singleton lookup indexed by NodeKind.getId().
   * Replaces the 19-case switch in getSingletonForKind with O(1) array access.
   * Lazily populated on first access per kind. Max NodeKind ID is 58 (VECTOR_INDEX_METADATA);
   * sized to 64 so iter#30 OBJECT_NAMED_* IDs (48-51) fit headroom.
   */
  private final ImmutableNode[] singletonByKindId = new ImmutableNode[64];

  /**
   * Option-A virtual-child mode for fused {@code OBJECT_NAMED_*} kinds.
   *
   * <p>When {@code true}, the cursor is conceptually positioned on the synthetic primitive
   * value child of a fused node, while physically still bound to the fused record. In this
   * mode: {@code getKind()} returns the corresponding {@code OBJECT_*_VALUE} kind,
   * {@code getValue()} / {@code getBooleanValue()} / {@code getNumberValue()} report the
   * primitive payload, {@code hasFirstChild()} / {@code hasLeftSibling()} /
   * {@code hasRightSibling()} return false (synthetic child is a leaf), and
   * {@code moveToParent()} clears the flag so we "return" to the fused node proper.
   *
   * <p>The flag is cleared on every call to {@link #moveTo(long)} entry as well as on
   * {@link #close()} / {@link #releaseCurrentPageGuard()}.
   */
  private boolean fusedSyntheticChildMode = false;
  
  /**
   * Move to a node using singleton mode (zero allocation).
   * Repopulates a mutable singleton instance from serialized data.
   * NO allocation happens here - only when getCurrentNode() is called.
   *
   * @param nodeKey the node key to move to
   * @param reader  the storage engine reader
   * @return true if the move was successful
   */
  private boolean moveToSingleton(final long nodeKey, final NodeStorageEngineReader reader) {
    // Inline pageKey: all index types use exponent 10, avoids assertNotClosed + switch overhead
    final long targetPageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    final int slotOffset = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));

    MemorySegment data;
    KeyValueLeafPage page;

    // OPTIMIZATION: Check if we're moving within the same page
    if (currentPageKey == targetPageKey && currentPage != null && !currentPage.isClosed()) {
      // Same page! Skip guard management entirely
      page = currentPage;

      // Check records[] first: Java objects are authoritative during write transactions
      // (modifications via prepareRecordForModification are NOT synced back to page heap)
      final DataRecord fromRecords = page.getRecord(slotOffset);
      if (fromRecords != null) {
        if (fromRecords.getKind() == NodeKind.DELETE) {
          return false;
        }
        @SuppressWarnings("unchecked")
        final N node = (N) fromRecords;
        this.currentNode = node;
        this.currentNodeKind = (NodeKind) fromRecords.getKind();
        this.currentNodeKey = nodeKey;
        this.currentSingleton = null;
        this.singletonMode = false;
        return true;
      }

      data = page.getSlot(slotOffset);
      if (data == null) {
        // Slot not found on current page - try overflow or fail
        return moveToSingletonSlowPath(nodeKey, reader);
      }
    } else {
      // Different page - use the slow path with guard management
      return moveToSingletonSlowPath(nodeKey, reader);
    }

    // Read node kind from first byte
    byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    NodeKind kind = NodeKind.getKind(kindByte);

    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      return false;
    }

    // Get singleton instance for this node type
    ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      // No singleton for this type (e.g., document root), fall back to legacy
      return moveToLegacy(nodeKey);
    }

    // Check if this is a flyweight record in slotted page
    final boolean isFlyweightSlot = page.getSlottedPage() != null && page.getSlotNodeKindId(slotOffset) > 0
        && singleton instanceof FlyweightNode;
    if (isFlyweightSlot) {
      final FlyweightNode fn = (FlyweightNode) singleton;
      // Bind flyweight directly to slotted page (zero-copy, no legacy parsing)
      final int heapOffset = PageLayout.getDirHeapOffset(page.getSlottedPage(), slotOffset);
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(page.getSlottedPage(), recordBase, nodeKey, slotOffset);
      // Propagate FSST symbol table for compressed string nodes
      propagateFsstToFlyweight(fn, page);
      // Propagate DeweyID from page to flyweight node (stored inline after record data).
      // setDeweyIDBytes stores raw bytes lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOffset));
      }
    } else {
      // Legacy format: populate from serialized data (NO ALLOCATION)
      // Reuse BytesIn instance - just reset to new segment and offset (skip kind byte)
      reusableBytesIn.reset(data, 1);
      // Only fetch DeweyID if actually stored (avoids byte[] allocation)
      byte[] deweyId = resourceConfig.areDeweyIDsStored ? page.getDeweyIdAsByteArray(slotOffset) : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state - we're in singleton mode now (page guard unchanged)
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null;  // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;

    return true;
  }

  /**
   * Slow path for moveToSingleton when moving to a different page (read-only transactions only).
   * Uses the reader's lookupSlotWithGuard with guard management.
   */
  private boolean moveToSingletonSlowPath(final long nodeKey, final NodeStorageEngineReader reader) {
    var slotLocation = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
    if (slotLocation == null) {
      return false;
    }

    return moveToSingletonFromPage(nodeKey, slotLocation.page(), reader,
        nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT, slotLocation.guard());
  }

  /**
   * Move to a node on a given page using singleton mode.
   * Shared logic for both write (TIL modified page) and read (guarded page) paths.
   *
   * @param nodeKey the node key
   * @param page the page to read from
   * @param reader the storage engine reader (for pageKey calculation)
   * @param pageKey the pre-calculated page key
   * @param newGuard the new page guard (null for TIL pages which don't need guarding)
   * @return true if move succeeded
   */
  private boolean moveToSingletonFromPage(final long nodeKey, final KeyValueLeafPage page,
      final NodeStorageEngineReader reader, final long pageKey, final @Nullable PageGuard newGuard) {
    final int slotOff = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));

    // Check records[] first: Java objects are authoritative during write transactions
    // (modifications via prepareRecordForModification are NOT synced back to page heap)
    final DataRecord fromRecords = page.getRecord(slotOff);
    if (fromRecords != null) {
      if (fromRecords.getKind() == NodeKind.DELETE) {
        if (newGuard != null) {
          newGuard.close();
        }
        return false;
      }
      releaseCurrentPageGuard();
      @SuppressWarnings("unchecked")
      final N node = (N) fromRecords;
      this.currentNode = node;
      this.currentNodeKind = (NodeKind) fromRecords.getKind();
      this.currentNodeKey = nodeKey;
      this.currentSingleton = null;
      this.singletonMode = false;
      this.currentPageGuard = newGuard;
      this.currentPage = page;
      this.currentPageKey = pageKey;
      return true;
    }

    // Get slot data from page heap
    MemorySegment data = page.getSlot(slotOff);
    boolean isOverflowData = false;
    if (data == null) {
      // Large-value overflow record (#1076): no slot, but the page carries a reference to an
      // OverflowPage whose bytes are in record-persister format (kind byte + payload) — exactly
      // what the legacy populate branch below parses. Without this, moveTo of an overflow record
      // returned false and hasLeftSibling()/moveToLeftSibling() loops spun forever.
      data = reader.readOverflowDataOrNull(page, nodeKey);
      if (data == null) {
        if (newGuard != null) {
          newGuard.close();
        }
        return false;
      }
      isOverflowData = true;
    }

    // Read node kind from first byte
    byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    NodeKind kind = NodeKind.getKind(kindByte);

    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      if (newGuard != null) {
        newGuard.close();
      }
      return false;
    }

    // Get singleton instance for this node type
    ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      // No singleton for this type (e.g., document root), fall back to legacy
      if (newGuard != null) {
        newGuard.close();
      }
      return moveToLegacy(nodeKey);
    }

    // Release previous page guard ONLY NOW (after we know the new page is valid)
    releaseCurrentPageGuard();

    // Check if this is a flyweight record in slotted page. Overflow data is NEVER flyweight
    // slot layout — and the directory entry of a cleared (overflowed) slot may still carry a
    // stale nodeKindId, so the flag must be forced off for overflow-resolved records.
    final boolean isFlyweight = !isOverflowData && page.getSlottedPage() != null
        && page.getSlotNodeKindId(slotOff) > 0
        && singleton instanceof FlyweightNode;
    if (isFlyweight) {
      final FlyweightNode fn = (FlyweightNode) singleton;
      // Bind flyweight directly to slotted page (zero-copy, no legacy parsing)
      final int heapOffset = PageLayout.getDirHeapOffset(page.getSlottedPage(), slotOff);
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(page.getSlottedPage(), recordBase, nodeKey, slotOff);
      // Propagate FSST symbol table for compressed string nodes
      propagateFsstToFlyweight(fn, page);
      // Propagate DeweyID from page to flyweight node (stored inline after record data).
      // setDeweyIDBytes stores raw bytes lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOff));
      }
    } else {
      // Legacy format: populate from serialized data (NO ALLOCATION)
      reusableBytesIn.reset(data, 1);
      byte[] deweyId = resourceConfig.areDeweyIDsStored
          ? page.getDeweyIdAsByteArray(slotOff) : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state - we're in singleton mode now with new page
    this.currentPageGuard = newGuard;
    this.currentPage = page;
    this.currentPageKey = pageKey;
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null;  // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;

    return true;
  }

  /**
   * Write-transaction singleton moveTo. Uses the writer's TIL for page resolution (modified pages).
   * Same-page optimization caches the modified page between calls.
   * Falls back to moveToLegacy if the page is not in TIL.
   *
   * @param nodeKey the node key to move to
   * @param reader the underlying storage engine reader (for pageKey calculation)
   * @param writer the storage engine writer (for TIL page resolution)
   * @return true if the move was successful
   */
  private boolean moveToSingletonWrite(final long nodeKey, final NodeStorageEngineReader reader,
      final StorageEngineWriter writer) {
    // Inline pageKey: all index types use exponent 10, avoids assertNotClosed + switch overhead
    final long targetPageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    final int slotOffset = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));
    KeyValueLeafPage page;

    // Resolve through the writer's TIL on EVERY move (getModifiedPageForRead has an O(1)
    // most-recent-container fast path). Do NOT reuse currentPage by (pageKey, !isClosed) alone:
    // after an async epoch rotation the TIL container is CoW'd to a NEW modified-page instance
    // while the superseded frozen instance stays OPEN for the background flush — an
    // isClosed()-based reuse check keeps serving the frozen instance for the rest of the epoch,
    // splitting reads (stale frozen page) from writes (CoW page). For a hot parent whose
    // firstChildKey advances with every insert, that durably corrupts the sibling chain: each
    // new node links to the stale first child, silently orphaning everything inserted after the
    // epoch boundary (#1077). On the synchronous path the superseded instance is closed by
    // TransactionIntentLog.put, which is why the old fast path appeared safe.
    page = writer.getModifiedPageForRead(targetPageKey, IndexType.DOCUMENT, -1);
    if (page == null) {
      // Page not in TIL — fall back to legacy (allocating) moveTo
      return moveToLegacy(nodeKey);
    }
    if (page != currentPage) {
      // Release previous guard (if any) and update page tracking
      // TIL pages don't need guarding — they're pinned by the transaction
      if (currentPageGuard != null) {
        currentPageGuard.close();
        currentPageGuard = null;
      }
      currentPage = page;
      currentPageKey = targetPageKey;
    }

    // Check records[] first: authoritative for writes (prepareRecordForModification stores here)
    final DataRecord fromRecords = page.getRecord(slotOffset);
    if (fromRecords != null) {
      if (fromRecords.getKind() == NodeKind.DELETE) {
        return false;
      }
      @SuppressWarnings("unchecked")
      final N node = (N) fromRecords;
      this.currentNode = node;
      this.currentNodeKind = (NodeKind) fromRecords.getKind();
      this.currentNodeKey = nodeKey;
      this.currentSingleton = null;
      this.singletonMode = false;
      return true;
    }

    // Inline slot lookup: avoid KeyValueLeafPage.getSlot's asSlice allocation,
    // which is hot on shred (every moveTo allocates a MemorySegment view just
    // to read one byte). Read the kind byte directly from the slotted page.
    final MemorySegment sp = page.getSlottedPage();
    if (sp == null || !PageLayout.isSlotPopulated(sp, slotOffset)) {
      return moveToLegacy(nodeKey);
    }
    final int heapOffset = PageLayout.getDirHeapOffset(sp, slotOffset);
    final int recordAbs = PageLayout.HEAP_START + heapOffset;
    final byte kindByte = sp.get(java.lang.foreign.ValueLayout.JAVA_BYTE, recordAbs);
    final NodeKind kind = NodeKind.getKind(kindByte);

    if (kind == NodeKind.DELETE) {
      return false;
    }

    // Get singleton instance for this node type
    final ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      return moveToLegacy(nodeKey);
    }

    // Bind singleton to page data (zero allocation)
    if (singleton instanceof FlyweightNode fn) {
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(sp, recordBase, nodeKey, slotOffset);
      // Propagate DeweyID lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOffset));
      }
    } else {
      // Legacy format: populate singleton from serialized data (NO ALLOCATION)
      // Size the view from the record length (skip kind byte).
      final int recordLength = PageLayout.getRecordOnlyLength(sp, slotOffset);
      if (recordLength <= 0) {
        return moveToLegacy(nodeKey);
      }
      reusableBytesIn.reset(sp.asSlice(recordAbs, recordLength), 1);
      final byte[] deweyId = resourceConfig.areDeweyIDsStored
          ? page.getDeweyIdAsByteArray(slotOffset) : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state — singleton mode, no guard needed for TIL pages
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null;
    this.singletonMode = true;

    return true;
  }

  /**
   * Propagate FSST symbol table from page to a flyweight string node.
   * Required for lazy decompression of FSST-compressed strings in singleton mode.
   */
  private static void propagateFsstToFlyweight(final FlyweightNode fn, final KeyValueLeafPage page) {
    final byte[] fsstTable = page.getFsstSymbolTable();
    if (fsstTable != null && fsstTable.length > 0) {
      if (fn instanceof StringNode sn) {
        sn.setFsstSymbolTable(fsstTable);
      } else if (fn instanceof ObjectNamedStringNode ons) {
        ons.setFsstSymbolTable(fsstTable);
      }
    }
  }

  /**
   * Get the singleton instance for a given node kind.
   * Lazily creates singletons on first use.
   *
   * @param kind the node kind
   * @return the singleton instance, or null if not supported
   */
  private ImmutableNode getSingletonForKind(NodeKind kind) {
    final int id = kind.getId() & 0xFF;
    if (id >= singletonByKindId.length) {
      return null;
    }
    ImmutableNode singleton = singletonByKindId[id];
    if (singleton != null) {
      return singleton;
    }
    singleton = createSingletonForKind(kind);
    if (singleton != null) {
      singletonByKindId[id] = singleton;
    }
    return singleton;
  }

  /**
   * Create a singleton instance for the given node kind (cold path, called once per kind).
   */
  private ImmutableNode createSingletonForKind(NodeKind kind) {
    return switch (kind) {
      case OBJECT -> new ObjectNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case ARRAY -> new ArrayNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case STRING_VALUE -> new StringNode(0, 0, 0, 0, 0, 0, 0, null,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case NUMBER_VALUE -> new NumberNode(0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case BOOLEAN_VALUE -> new BooleanNode(0, 0, 0, 0, 0, 0, 0, false,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case NULL_VALUE -> new NullNode(0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_NAMED_BOOLEAN -> new ObjectNamedBooleanNode(0, resourceConfig.nodeHashFunction);
      case OBJECT_NAMED_NUMBER -> new ObjectNamedNumberNode(0, resourceConfig.nodeHashFunction);
      case OBJECT_NAMED_STRING -> new ObjectNamedStringNode(0, resourceConfig.nodeHashFunction);
      case OBJECT_NAMED_NULL -> new ObjectNamedNullNode(0, resourceConfig.nodeHashFunction);
      case OBJECT_NAMED_OBJECT -> new ObjectNamedObjectNode(0, resourceConfig.nodeHashFunction);
      case OBJECT_NAMED_ARRAY -> new ObjectNamedArrayNode(0, resourceConfig.nodeHashFunction);
      case JSON_DOCUMENT -> new JsonDocumentRootNode(0, resourceConfig.nodeHashFunction);
      case ELEMENT -> new ElementNode(0, resourceConfig.nodeHashFunction);
      case ATTRIBUTE -> new AttributeNode(0, resourceConfig.nodeHashFunction);
      case TEXT -> new TextNode(0, resourceConfig.nodeHashFunction);
      case COMMENT -> new CommentNode(0, resourceConfig.nodeHashFunction);
      case PROCESSING_INSTRUCTION -> new PINode(0, resourceConfig.nodeHashFunction);
      case NAMESPACE -> new NamespaceNode(0, resourceConfig.nodeHashFunction);
      case XML_DOCUMENT -> new XmlDocumentRootNode(0, resourceConfig.nodeHashFunction);
      default -> null;
    };
  }
  
  /**
   * Populate a singleton node from serialized data.
   *
   * @param singleton the singleton to populate
   * @param source    the BytesIn source positioned after the kind byte
   * @param nodeKey   the node key
   * @param deweyId   the DeweyID bytes
   * @param kind      the node kind
   */
  private void populateSingleton(ImmutableNode singleton, BytesIn<?> source, 
                                  long nodeKey, byte[] deweyId, NodeKind kind,
                                  KeyValueLeafPage page) {
    switch (kind) {
      case OBJECT -> ((ObjectNode) singleton).readFrom(source, nodeKey, deweyId, 
          resourceConfig.nodeHashFunction, resourceConfig);
      case ARRAY -> ((ArrayNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case STRING_VALUE -> {
        StringNode stringNode = (StringNode) singleton;
        stringNode.readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
        // Propagate FSST symbol table for decompression
        byte[] fsstSymbolTable = page.getFsstSymbolTable();
        if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
          stringNode.setFsstSymbolTable(fsstSymbolTable);
        }
      }
      case NUMBER_VALUE -> ((NumberNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case BOOLEAN_VALUE -> ((BooleanNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case NULL_VALUE -> ((NullNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_BOOLEAN -> ((ObjectNamedBooleanNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_NUMBER -> ((ObjectNamedNumberNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_STRING -> {
        ObjectNamedStringNode namedStr = (ObjectNamedStringNode) singleton;
        namedStr.readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
        byte[] fsstSymbolTable = page.getFsstSymbolTable();
        if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
          namedStr.setFsstSymbolTable(fsstSymbolTable);
        }
      }
      case OBJECT_NAMED_NULL -> ((ObjectNamedNullNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_OBJECT -> ((ObjectNamedObjectNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_ARRAY -> ((ObjectNamedArrayNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case JSON_DOCUMENT -> ((JsonDocumentRootNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case ELEMENT -> ((ElementNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case ATTRIBUTE -> ((AttributeNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case TEXT -> ((TextNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case COMMENT -> ((CommentNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case PROCESSING_INSTRUCTION -> ((PINode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case NAMESPACE -> ((NamespaceNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case XML_DOCUMENT -> ((XmlDocumentRootNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      default -> throw new IllegalStateException("Unexpected singleton kind: " + kind);
    }
  }
  
  /**
   * Move to an item in the item list (negative keys).
   * Falls back to object mode since item list uses objects.
   *
   * @param nodeKey the negative node key
   * @return true if the move was successful
   */
  private boolean moveToItemList(final long nodeKey) {
    if (itemList.size() > 0) {
      DataRecord item = itemList.getItem(nodeKey);
      if (item != null) {
        // Move succeeded - release previous page guard and switch to object mode
        releaseCurrentPageGuard();
        //noinspection unchecked
        setCurrentNode((N) item);
        this.currentNodeKey = nodeKey;
        return true;
      }
    }
    // Item not found - keep the current position unchanged
    return false;
  }
  
  /**
   * Legacy object-based moveTo for when flyweight mode is not available.
   *
   * @param nodeKey the node key to move to
   * @return true if the move was successful
   */
  private boolean moveToLegacy(final long nodeKey) {
    DataRecord newNode;
    try {
      newNode = storageEngineReader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    } catch (final SirixIOException | UncheckedIOException | IllegalArgumentException e) {
      newNode = null;
    }

    if (newNode == null) {
      return false;
    } else {
      // Only release guard if we were in singleton mode.
      if (singletonMode) {
        releaseCurrentPageGuard();
        singletonMode = false;
      }
      //noinspection unchecked
      setCurrentNode((N) newNode);
      this.currentNodeKey = nodeKey;
      return true;
    }
  }
  
  /**
   * Release the current page guard if one is held.
   * This allows the page to be evicted if needed.
   */
  protected void releaseCurrentPageGuard() {
    if (currentPageGuard != null) {
      currentPageGuard.close();
      currentPageGuard = null;
      currentPage = null;
      currentPageKey = -1;
    }
  }
  
  @Override
  public boolean moveToRightSibling() {
    assertNotClosed();
    // Use flyweight getter if available to avoid node materialization
    if (!hasRightSibling()) {
      return false;
    }
    return moveTo(getRightSiblingKey());
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return currentNodeKey;
    }
    return getCurrentNode().getNodeKey();
  }

  @Override
  public long getHash() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return currentSingleton != null ? currentSingleton.getHash() : 0L;
    }
    return currentNode != null ? currentNode.getHash() : 0L;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return currentNodeKind;
    }
    final N node = getCurrentNode();
    return node != null ? (NodeKind) node.getKind() : null;
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  public void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  /**
   * Get the {@link StorageEngineReader}.
   *
   * @return current {@link StorageEngineReader}
   */
  public StorageEngineReader getPageTransaction() {
    assertNotClosed();
    return storageEngineReader;
  }

  /**
   * Replace the current {@link NodeStorageEngineReader}.
   *
   * @param pageReadTransaction {@link NodeStorageEngineReader} instance
   */
  public final void setPageReadTransaction(@Nullable final StorageEngineReader pageReadTransaction) {
    assertNotClosed();
    if (pageReadTransaction != null) {
      // Refresh BEFORE publishing the engine so a concurrent getRevisionNumber() never sees the
      // new engine paired with the old revision. While the engine reference is null (the swap
      // window) the previous revision stays readable — it was the true value an instant ago.
      revisionNumber = pageReadTransaction.getActualRevisionRootPage().getRevision();
    }
    storageEngineReader = pageReadTransaction;
    cachedNodeReader = resolveNodeReader(pageReadTransaction);
    cachedWriter = (pageReadTransaction instanceof StorageEngineWriter w) ? w : null;
  }

  /**
   * Resolve the underlying {@link NodeStorageEngineReader} from a storage engine reader.
   * For read-only transactions, this is the reader itself.
   * For write transactions (where the reader is a {@link StorageEngineWriter}),
   * extracts the delegate reader via {@link StorageEngineWriter#getStorageEngineReader()}.
   */
  private static NodeStorageEngineReader resolveNodeReader(@Nullable final StorageEngineReader reader) {
    if (reader instanceof NodeStorageEngineReader r) {
      return r;
    }
    if (reader instanceof StorageEngineWriter w
        && w.getStorageEngineReader() instanceof NodeStorageEngineReader r) {
      return r;
    }
    return null;
  }

  @Override
  public final long getMaxNodeKey() {
    assertNotClosed();
    return storageEngineReader.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    N node = getCurrentNode();
    if (node instanceof StructNode structNode) {
      return structNode;
    }
    return new io.sirix.node.NullNode(node);
  }

  @Override
  public final StructNode getStructuralNodeView() {
    if (currentNode instanceof StructNode structNode) {
      return structNode;
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode structNode) {
      return structNode;
    }
    return getStructuralNode();
  }

  @Override
  public boolean moveToNextFollowing() {
    assertNotClosed();
    // Use flyweight getters to avoid node materialization
    while (!hasRightSibling() && hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final long key) {
    assertNotClosed();
    // Save current position using flyweight-compatible getters
    final long savedNodeKey = getNodeKey();
    final boolean retVal = moveTo(key);
    // Restore to the saved position
    moveTo(savedNodeKey);
    return retVal;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    // Synthetic child of a fused record is a leaf — no further descent.
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNodeView().hasFirstChild();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    // Synthetic child has no siblings; real siblings live on the fused parent.
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNodeView().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    if (fusedSyntheticChildMode) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getRightSiblingKey();
    }
    return getStructuralNodeView().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    if (fusedSyntheticChildMode) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getFirstChildKey();
    }
    return getStructuralNodeView().getFirstChildKey();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    // From the synthetic primitive child, parent is the fused node's own nodeKey (we are still
    // physically bound to it). Callers use this to navigate back up.
    if (fusedSyntheticChildMode) {
      return currentNodeKey;
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton != null) {
      return currentSingleton.getParentKey();
    }
    return getCurrentNode().getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    final long parentKey = getParentKey();
    if (parentKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    final long savedNodeKey = getNodeKey();
    moveToParent();
    final NodeKind parentKind = getKind();
    moveTo(savedNodeKey);
    return parentKind;
  }

  @Override
  public boolean moveToNext() {
    assertNotClosed();
    // Use flyweight getter if available
    if (hasRightSibling()) {
      // Right sibling node.
      return moveTo(getRightSiblingKey());
    }
    // Next following node.
    return moveToNextFollowing();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    // Use flyweight getter - if it has a first child, it also has a last child
    return hasFirstChild();
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    if (hasLastChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLastChild();
      final NodeKind lastChildKind = getKind();
      moveTo(savedNodeKey);
      return lastChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    if (hasFirstChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToFirstChild();
      final NodeKind firstChildKind = getKind();
      moveTo(savedNodeKey);
      return firstChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    if (hasLastChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLastChild();
      final long lastChildNodeKey = getNodeKey();
      moveTo(savedNodeKey);
      return lastChildNodeKey;
    }
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    assertNotClosed();
    return getStructuralNodeView().getChildCount();
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return hasFirstChild();
    }
    return getStructuralNodeView().hasFirstChild();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
    return getStructuralNodeView().getDescendantCount();
  }

  @Override
  public NodeKind getPathKind() {
    assertNotClosed();
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getRightSiblingKind() {
    assertNotClosed();
    if (hasRightSibling()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToRightSibling();
      final NodeKind rightSiblingKind = getKind();
      moveTo(savedNodeKey);
      return rightSiblingKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public StorageEngineReader getStorageEngineReader() {
    assertNotClosed();
    return storageEngineReader;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    assertNotClosed();
    return storageEngineReader.getCommitCredentials();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return currentSingleton != null ? currentSingleton.getDeweyID() : null;
    }
    return currentNode != null ? currentNode.getDeweyID() : null;
  }

  @Override
  public int getPreviousRevisionNumber() {
    assertNotClosed();
    return getCurrentNode().getPreviousRevisionNumber();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
  
  /**
   * Check if flyweight mode is currently active.
   * Package-private for testing purposes.
   *
   * @return always {@code false}; flyweight mode has been removed.
   */
  boolean isFlyweightMode() {
    return false;
  }
  
  /**
   * Check if singleton mode is currently active.
   * Package-private for testing purposes.
   *
   * @return true if singleton mode is active (using mutable singleton nodes)
   */
  boolean isSingletonMode() {
    return singletonMode;
  }
  
  /**
   * Check if zero-allocation mode is active.
   * Package-private for testing purposes.
   *
   * @return true if singleton mode is active
   */
  boolean isZeroAllocationMode() {
    return singletonMode;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Release page guard first to allow page eviction.
      releaseCurrentPageGuard();

      // Pure read-only trx: close the underlying StorageEngineReader so its
      // RevisionEpochTracker ticket gets deregistered. Without this, every rtx
      // permanently consumes one of the global 4096 tracker slots and a long-running
      // workload eventually fails to open new transactions.
      //
      // wtx-attached rtx (cachedWriter != null) is owned by the surrounding
      // AbstractNodeTrxImpl, which closes the writer separately — closing the reader
      // here would tear it down before the writer finishes its close path
      // (await async commit, write last UberPage, close TIL).
      if (cachedWriter == null && storageEngineReader != null) {
        storageEngineReader.close();
      }

      // Callback on session to make sure everything is cleaned up.
      resourceSession.closeReadTransaction(id);

      setPageReadTransaction(null);

      // Immediately release all references.
      storageEngineReader = null;
      currentNode = null;

      // Close state.
      isClosed = true;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AbstractNodeReadOnlyTrx<?, ?, ?> that = (AbstractNodeReadOnlyTrx<?, ?, ?>) o;
    return getNodeKey() == that.getNodeKey()
        && storageEngineReader.getRevisionNumber() == that.storageEngineReader.getRevisionNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNodeKey(), storageEngineReader.getRevisionNumber());
  }
}
