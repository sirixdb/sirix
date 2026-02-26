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
import io.sirix.node.DeltaVarIntCodec;
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
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
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
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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
   */
  protected StorageEngineReader storageEngineReader;

  /**
   * The current node (used as fallback when flyweight mode is disabled).
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
  
  // ==================== FLYWEIGHT CURSOR STATE ====================
  // These fields enable zero-allocation navigation by reading directly from MemorySegment
  
  /**
   * The raw MemorySegment containing the current node's serialized data.
   * When in flyweight mode, getters read directly from this segment.
   */
  private MemorySegment currentSlot;
  
  /**
   * The current node's key (used for delta decoding).
   */
  private long currentNodeKey;
  
  /**
   * The current node's kind.
   */
  private NodeKind currentNodeKind;
  
  /**
   * The current node's DeweyID bytes (may be null if DeweyIDs not stored).
   */
  private byte[] currentDeweyId;
  
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
   * Whether the transaction is in flyweight mode (reading from currentSlot).
   * When false, falls back to using currentNode object.
   */
  private boolean flyweightMode = true;
  
  /**
   * Preallocated array for caching field offsets within currentSlot.
   * Indices are defined by FIELD_* constants.
   * This avoids re-parsing varints on each getter call.
   */
  protected final int[] cachedFieldOffsets = new int[16];
  
  // Field offset indices for cachedFieldOffsets array
  protected static final int FIELD_PARENT_KEY = 0;
  protected static final int FIELD_PREV_REVISION = 1;
  protected static final int FIELD_LAST_MOD_REVISION = 2;
  protected static final int FIELD_RIGHT_SIBLING_KEY = 3;
  protected static final int FIELD_LEFT_SIBLING_KEY = 4;
  protected static final int FIELD_FIRST_CHILD_KEY = 5;
  protected static final int FIELD_LAST_CHILD_KEY = 6;
  protected static final int FIELD_CHILD_COUNT = 7;
  protected static final int FIELD_DESCENDANT_COUNT = 8;
  protected static final int FIELD_HASH = 9;
  protected static final int FIELD_NAME_KEY = 10;
  protected static final int FIELD_PATH_NODE_KEY = 11;
  protected static final int FIELD_VALUE = 12;
  protected static final int FIELD_END = 13;  // End marker for offset validation
  
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
  protected AbstractNodeReadOnlyTrx(final @NonNegative int trxId, final @NonNull StorageEngineReader pageReadTransaction,
      final @NonNull N documentNode, final InternalResourceSession<T, W> resourceSession,
      final ItemList<AtomicValue> itemList) {
    this.itemList = itemList;
    this.resourceSession = requireNonNull(resourceSession);
    this.id = trxId;
    this.storageEngineReader = requireNonNull(pageReadTransaction);
    this.currentNode = requireNonNull(documentNode);
    this.isClosed = false;
    this.resourceConfig = resourceSession.getResourceConfig();
    this.cachedNodeReader = resolveNodeReader(pageReadTransaction);
    this.cachedWriter = (pageReadTransaction instanceof StorageEngineWriter w) ? w : null;

    // Initialize flyweight state from document node
    this.currentNodeKey = documentNode.getNodeKey();
    this.currentNodeKind = documentNode.getKind();
    this.flyweightMode = false;  // Start with object mode for document node
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

    if (FLYWEIGHT_ENABLED && flyweightMode && currentSlot != null) {
      currentNode = deserializeToSnapshot();
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
      case OBJECT_KEY -> (N) ((ObjectKeyNode) currentSingleton).toSnapshot();
      case STRING_VALUE -> (N) ((StringNode) currentSingleton).toSnapshot();
      case NUMBER_VALUE -> (N) ((NumberNode) currentSingleton).toSnapshot();
      case BOOLEAN_VALUE -> (N) ((BooleanNode) currentSingleton).toSnapshot();
      case NULL_VALUE -> (N) ((NullNode) currentSingleton).toSnapshot();
      case OBJECT_STRING_VALUE -> (N) ((ObjectStringNode) currentSingleton).toSnapshot();
      case OBJECT_NUMBER_VALUE -> (N) ((ObjectNumberNode) currentSingleton).toSnapshot();
      case OBJECT_BOOLEAN_VALUE -> (N) ((ObjectBooleanNode) currentSingleton).toSnapshot();
      case OBJECT_NULL_VALUE -> (N) ((ObjectNullNode) currentSingleton).toSnapshot();
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
  
  /**
   * Deserialize the current slot to a node object (snapshot).
   * This is the escape hatch for code that needs a stable node reference.
   * Called by getCurrentNode() when in flyweight mode.
   *
   * @return the deserialized node
   */
  @SuppressWarnings("unchecked")
  protected N deserializeToSnapshot() {
    // Use the same deserialization as normal read path
    var bytesIn = new MemorySegmentBytesIn(currentSlot);
    var record = resourceConfig.recordPersister.deserialize(bytesIn, currentNodeKey, currentDeweyId, resourceConfig);
    return (N) record;
  }

  @Override
  public void setCurrentNode(final @Nullable N currentNode) {
    assertNotClosed();
    this.currentNode = currentNode;
    
    if (currentNode != null) {
      // Disable flyweight and singleton modes - use the provided node object
      this.flyweightMode = false;
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
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getLeftSiblingKey();
    }
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] >= 0) {
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY], currentNodeKey);
    }
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    if ((SINGLETON_ENABLED && singletonMode) || (FLYWEIGHT_ENABLED && flyweightMode)) {
      return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasLeftSibling();
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
    final ImmutableNode node = getCurrentNode();
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node instanceof ObjectKeyNode objectKeyNode) {
      return objectKeyNode.getPathNodeKey();
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
    return storageEngineReader.getActualRevisionRootPage().getRevision();
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
    // Use flyweight getter if available to avoid node materialization
    if (!hasFirstChild()) {
      return false;
    }
    return moveTo(getFirstChildKey());
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    assertNotClosed();

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
   * Toggle for flyweight mode. Set to true to enable zero-allocation optimization.
   * Note: Flyweight mode reads directly from MemorySegment but has varint parsing overhead.
   */
  private static final boolean FLYWEIGHT_ENABLED = false;
  
  /**
   * Toggle for singleton mode. Set to true to enable singleton node reuse.
   * Singleton mode uses mutable singleton nodes that are repopulated on each moveTo().
   * When combined with cache checking, uses cached records when available.
   */
  private static final boolean SINGLETON_ENABLED = true;
  
  // ==================== SINGLETON NODE INSTANCES ====================
  // These mutable singleton nodes are reused across moveTo() operations.
  // Each JSON node type has a dedicated singleton instance.
  
  private ObjectNode singletonObject;
  private ArrayNode singletonArray;
  private ObjectKeyNode singletonObjectKey;
  private StringNode singletonString;
  private NumberNode singletonNumber;
  private BooleanNode singletonBoolean;
  private NullNode singletonNull;
  private ObjectStringNode singletonObjectString;
  private ObjectNumberNode singletonObjectNumber;
  private ObjectBooleanNode singletonObjectBoolean;
  private ObjectNullNode singletonObjectNull;
  private JsonDocumentRootNode singletonJsonDocRoot;
  private ElementNode singletonElement;
  private AttributeNode singletonAttribute;
  private TextNode singletonText;
  private CommentNode singletonComment;
  private PINode singletonPI;
  private NamespaceNode singletonNamespace;
  private XmlDocumentRootNode singletonXmlDocRoot;
  
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
   * Lazily populated on first access per kind. Max NodeKind ID is 55.
   */
  private final ImmutableNode[] singletonByKindId = new ImmutableNode[56];
  
  /**
   * Move to a node using flyweight mode (zero allocation).
   * Reads directly from MemorySegment without creating node objects.
   *
   * @param nodeKey the node key to move to
   * @param reader  the storage engine reader
   * @return true if the move was successful
   */
  private boolean moveToFlyweight(final long nodeKey, final NodeStorageEngineReader reader) {
    // Try singleton mode first (preferred for JSON nodes)
    if (SINGLETON_ENABLED) {
      return moveToSingleton(nodeKey, reader);
    }
    
    if (!FLYWEIGHT_ENABLED) {
      return moveToLegacy(nodeKey);
    }
    
    // Lookup slot directly without deserializing
    // NOTE: We acquire the new guard BEFORE releasing the old one to ensure
    // the transaction state remains valid if the lookup fails.
    var slotLocation = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
    if (slotLocation == null) {
      // Node not found - keep the current position unchanged
      return false;
    }
    
    // Read node kind from first byte
    MemorySegment data = slotLocation.data();
    byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    NodeKind kind = NodeKind.getKind(kindByte);
    
    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      // Release the newly acquired guard, keep current position unchanged
      slotLocation.guard().close();
      return false;
    }
    
    // Move succeeded - now release the previous page guard
    releaseCurrentPageGuard();
    
    // Update flyweight state (NO ALLOCATION)
    this.currentSlot = data;
    this.currentNodeKey = nodeKey;
    this.currentNodeKind = kind;
    this.currentDeweyId = slotLocation.page().getDeweyIdAsByteArray(slotLocation.offset());
    this.currentPageGuard = slotLocation.guard();
    this.flyweightMode = true;
    this.currentNode = null;  // Invalidate cached node object
    
    // Parse and cache field offsets for fast getter access
    parseFieldOffsets();
    
    return true;
  }
  
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
        this.flyweightMode = false;
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
    this.flyweightMode = false;

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
      this.flyweightMode = false;
      this.currentPageGuard = newGuard;
      this.currentPage = page;
      this.currentPageKey = pageKey;
      return true;
    }

    // Get slot data from page heap
    MemorySegment data = page.getSlot(slotOff);
    if (data == null) {
      if (newGuard != null) {
        newGuard.close();
      }
      return false;
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

    // Check if this is a flyweight record in slotted page
    final boolean isFlyweight = page.getSlottedPage() != null && page.getSlotNodeKindId(slotOff) > 0
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
    this.flyweightMode = false;

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

    // Same-page fast path: reuse cached modified page
    if (currentPageKey == targetPageKey && currentPage != null && !currentPage.isClosed()) {
      page = currentPage;
    } else {
      // Different page: get modified page from writer's TIL
      page = writer.getModifiedPageForRead(targetPageKey, IndexType.DOCUMENT, -1);
      if (page == null) {
        // Page not in TIL — fall back to legacy (allocating) moveTo
        return moveToLegacy(nodeKey);
      }
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
      this.flyweightMode = false;
      return true;
    }

    // Check slot data on modified page
    final MemorySegment data = page.getSlot(slotOffset);
    if (data == null) {
      // Not in modified page heap either — fall back to legacy
      return moveToLegacy(nodeKey);
    }

    // Read node kind from first byte
    final byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
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
    // Cache slottedPage locally to avoid repeated virtual calls to getSlottedPage()
    final MemorySegment sp = page.getSlottedPage();
    if (sp != null && singleton instanceof FlyweightNode fn) {
      final int heapOffset = PageLayout.getDirHeapOffset(sp, slotOffset);
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(sp, recordBase, nodeKey, slotOffset);
      // Propagate DeweyID lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOffset));
      }
    } else {
      // Legacy format: populate singleton from serialized data (NO ALLOCATION)
      reusableBytesIn.reset(data, 1);
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
    this.flyweightMode = false;

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
      } else if (fn instanceof ObjectStringNode osn) {
        osn.setFsstSymbolTable(fsstTable);
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
      case OBJECT -> singletonObject = new ObjectNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case ARRAY -> singletonArray = new ArrayNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_KEY -> singletonObjectKey = new ObjectKeyNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case STRING_VALUE -> singletonString = new StringNode(0, 0, 0, 0, 0, 0, 0, null,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case NUMBER_VALUE -> singletonNumber = new NumberNode(0, 0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case BOOLEAN_VALUE -> singletonBoolean = new BooleanNode(0, 0, 0, 0, 0, 0, 0, false,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case NULL_VALUE -> singletonNull = new NullNode(0, 0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_STRING_VALUE -> singletonObjectString = new ObjectStringNode(0, 0, 0, 0, 0, null,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_NUMBER_VALUE -> singletonObjectNumber = new ObjectNumberNode(0, 0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_BOOLEAN_VALUE -> singletonObjectBoolean = new ObjectBooleanNode(0, 0, 0, 0, 0, false,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case OBJECT_NULL_VALUE -> singletonObjectNull = new ObjectNullNode(0, 0, 0, 0, 0,
          resourceConfig.nodeHashFunction, (byte[]) null);
      case JSON_DOCUMENT -> singletonJsonDocRoot = new JsonDocumentRootNode(0, resourceConfig.nodeHashFunction);
      case ELEMENT -> singletonElement = new ElementNode(0, resourceConfig.nodeHashFunction);
      case ATTRIBUTE -> singletonAttribute = new AttributeNode(0, resourceConfig.nodeHashFunction);
      case TEXT -> singletonText = new TextNode(0, resourceConfig.nodeHashFunction);
      case COMMENT -> singletonComment = new CommentNode(0, resourceConfig.nodeHashFunction);
      case PROCESSING_INSTRUCTION -> singletonPI = new PINode(0, resourceConfig.nodeHashFunction);
      case NAMESPACE -> singletonNamespace = new NamespaceNode(0, resourceConfig.nodeHashFunction);
      case XML_DOCUMENT -> singletonXmlDocRoot = new XmlDocumentRootNode(0, resourceConfig.nodeHashFunction);
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
      case OBJECT_KEY -> ((ObjectKeyNode) singleton).readFrom(source, nodeKey, deweyId,
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
      case OBJECT_STRING_VALUE -> {
        ObjectStringNode objectStringNode = (ObjectStringNode) singleton;
        objectStringNode.readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
        // Propagate FSST symbol table for decompression
        byte[] fsstSymbolTable = page.getFsstSymbolTable();
        if (fsstSymbolTable != null && fsstSymbolTable.length > 0) {
          objectStringNode.setFsstSymbolTable(fsstSymbolTable);
        }
      }
      case OBJECT_NUMBER_VALUE -> ((ObjectNumberNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_BOOLEAN_VALUE -> ((ObjectBooleanNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NULL_VALUE -> ((ObjectNullNode) singleton).readFrom(source, nodeKey, deweyId,
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
        flyweightMode = false;
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
      // Only release guard if we were in flyweight/singleton mode
      if (flyweightMode || singletonMode) {
        releaseCurrentPageGuard();
        flyweightMode = false;
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
  
  /**
   * Parse the field offsets from the current slot for fast getter access.
   * This is called once per moveTo() and caches the byte offset of each field.
   * <p>
   * The slot format starts with:
   * - Byte 0: NodeKind byte
   * - Byte 1+: Node-specific fields (varints)
   * <p>
   * Field order varies by node kind, but common structural nodes follow:
   * - parentKey (delta varint)
   * - prevRev (signed varint)
   * - lastModRev (signed varint)
   * - [pathNodeKey for ARRAY] (delta varint)
   * - rightSiblingKey (delta varint)
   * - leftSiblingKey (delta varint)
   * - firstChildKey (delta varint) for structural nodes
   * - lastChildKey (delta varint) for structural nodes
   * - childCount (signed varint) if storeChildCount
   * - hash (8 bytes fixed) if hashType != NONE
   * - descendantCount (signed varint) if hashType != NONE
   */
  protected void parseFieldOffsets() {
    // Start after NodeKind byte
    int offset = 1;
    
    switch (currentNodeKind) {
      case OBJECT -> parseObjectNodeOffsets(offset);
      case ARRAY -> parseArrayNodeOffsets(offset);
      case OBJECT_KEY -> parseObjectKeyNodeOffsets(offset);
      // Non-object value nodes have siblings (used in arrays)
      case STRING_VALUE -> parseStringValueNodeOffsets(offset);
      case NUMBER_VALUE -> parseNumberValueNodeOffsets(offset);
      case BOOLEAN_VALUE -> parseBooleanValueNodeOffsets(offset);
      case NULL_VALUE -> parseNullValueNodeOffsets(offset);
      // Object value nodes have no siblings (single child of ObjectKeyNode)
      case OBJECT_STRING_VALUE -> parseObjectStringValueNodeOffsets(offset);
      case OBJECT_NUMBER_VALUE -> parseObjectNumberValueNodeOffsets(offset);
      case OBJECT_BOOLEAN_VALUE -> parseObjectBooleanValueNodeOffsets(offset);
      case OBJECT_NULL_VALUE -> parseObjectNullValueNodeOffsets(offset);
      case JSON_DOCUMENT -> {
        // JSON_DOCUMENT has special serialization - fall back to object mode
        // for simplicity. Document root is typically only visited once.
        java.util.Arrays.fill(cachedFieldOffsets, -1);
      }
      default -> {
        // For unsupported node kinds, set all offsets to -1 (use object fallback)
        java.util.Arrays.fill(cachedFieldOffsets, -1);
      }
    }
  }
  
  /**
   * Parse field offsets for OBJECT node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey,
   *         firstChildKey, lastChildKey, [childCount], [hash, descendantCount]
   */
  private void parseObjectNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    // Optional childCount
    if (resourceConfig.storeChildCount()) {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    }
    
    // Optional hash and descendant count
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;  // Fixed 8 bytes for hash
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    // OBJECT nodes don't have nameKey/pathNodeKey/value
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for ARRAY node.
   * Format: parentKey, prevRev, lastModRev, pathNodeKey, rightSiblingKey, leftSiblingKey,
   *         firstChildKey, lastChildKey, [childCount], [hash, descendantCount]
   */
  private void parseArrayNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.storeChildCount()) {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    }
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_KEY node.
   * Format: parentKey, prevRev, lastModRev, pathNodeKey, rightSiblingKey,
   *         leftSiblingKey, firstChildKey, nameKey, [hash, descendantCount]
   */
  private void parseObjectKeyNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_NAME_KEY] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    // OBJECT_KEY has no lastChildKey, childCount
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for STRING_VALUE/OBJECT_STRING_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], valueLength, value
   */
  private void parseStringValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    // Value starts at current offset (length + bytes)
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // Leaf nodes don't have children
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;  // Value parsing done on demand
  }
  
  /**
   * Parse field offsets for NUMBER_VALUE/OBJECT_NUMBER_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], numberValue
   */
  private void parseNumberValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for BOOLEAN_VALUE/OBJECT_BOOLEAN_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], booleanValue
   */
  private void parseBooleanValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for NULL_VALUE node (used in arrays).
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash]
   */
  private void parseNullValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  // ==================== OBJECT_* VALUE NODES (no siblings) ====================
  
  /**
   * Parse field offsets for OBJECT_STRING_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], stringValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectStringValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_NUMBER_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], numberValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectNumberValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_BOOLEAN_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], booleanValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectBooleanValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_NULL_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash]
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectNullValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for JSON_DOCUMENT node.
   * Format: firstChildKey (varint), descendantCount (8 bytes)
   * Note: JSON_DOCUMENT has fixed parent, prevRev, lastModRev values, not serialized.
   */
  private void parseJsonDocumentNodeOffsets(int offset) {
    // JSON_DOCUMENT has fixed values for these, not serialized:
    // - parentKey = NULL_NODE_KEY
    // - prevRev = NULL_REVISION_NUMBER
    // - lastModRev = NULL_REVISION_NUMBER
    cachedFieldOffsets[FIELD_PARENT_KEY] = -1;  // Fixed value, not serialized
    cachedFieldOffsets[FIELD_PREV_REVISION] = -1;  // Fixed value
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = -1;  // Fixed value
    
    // firstChildKey is a plain varint (not delta encoded)
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    // In JSON_DOCUMENT, firstChildKey == lastChildKey
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;  // Same as firstChildKey
    
    // descendantCount is always stored as 8-byte long
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
    offset += 8;
    
    // These fields don't exist for JSON_DOCUMENT
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_HASH] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
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
    if ((FLYWEIGHT_ENABLED && flyweightMode) || (SINGLETON_ENABLED && singletonMode)) {
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
    if (FLYWEIGHT_ENABLED && flyweightMode) {
      if (cachedFieldOffsets[FIELD_HASH] >= 0) {
        return DeltaVarIntCodec.readLongFromSegment(currentSlot, cachedFieldOffsets[FIELD_HASH]);
      }
      N node = getCurrentNode();
      return node != null ? node.getHash() : 0L;
    }
    return currentNode != null ? currentNode.getHash() : 0L;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    if ((FLYWEIGHT_ENABLED && flyweightMode) || (SINGLETON_ENABLED && singletonMode)) {
      return currentNodeKind;
    }
    return getCurrentNode().getKind();
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
    // In flyweight mode, materialize if needed
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
  public boolean hasNode(final @NonNegative long key) {
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
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_PARENT_KEY] >= 0) {
      return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getCurrentNode().hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    if ((SINGLETON_ENABLED && singletonMode) || (FLYWEIGHT_ENABLED && flyweightMode)) {
      return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    if ((SINGLETON_ENABLED && singletonMode) || (FLYWEIGHT_ENABLED && flyweightMode)) {
      return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getRightSiblingKey();
    }
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] >= 0) {
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY], currentNodeKey);
    }
    return getStructuralNode().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getFirstChildKey();
    }
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] >= 0) {
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_FIRST_CHILD_KEY], currentNodeKey);
    }
    return getStructuralNode().getFirstChildKey();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode && currentSingleton != null) {
      return currentSingleton.getParentKey();
    }
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_PARENT_KEY] >= 0) {
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_PARENT_KEY], currentNodeKey);
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
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_CHILD_COUNT] >= 0) {
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_CHILD_COUNT]);
    }
    return getStructuralNode().getChildCount();
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    if ((SINGLETON_ENABLED && singletonMode) || (FLYWEIGHT_ENABLED && flyweightMode)) {
      return hasFirstChild();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_DESCENDANT_COUNT] >= 0) {
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_DESCENDANT_COUNT]);
    }
    return getStructuralNode().getDescendantCount();
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
    if (FLYWEIGHT_ENABLED && flyweightMode) {
      if (currentDeweyId != null) {
        return new SirixDeweyID(currentDeweyId);
      }
      N node = getCurrentNode();
      return node != null ? node.getDeweyID() : null;
    }
    return currentNode != null ? currentNode.getDeweyID() : null;
  }

  @Override
  public int getPreviousRevisionNumber() {
    assertNotClosed();
    if (FLYWEIGHT_ENABLED && flyweightMode && cachedFieldOffsets[FIELD_PREV_REVISION] >= 0) {
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_PREV_REVISION]);
    }
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
   * @return true if flyweight mode is active (reading directly from MemorySegment)
   */
  boolean isFlyweightMode() {
    return flyweightMode;
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
   * Check if zero-allocation mode is active (either flyweight or singleton).
   * Package-private for testing purposes.
   *
   * @return true if any zero-allocation mode is active
   */
  boolean isZeroAllocationMode() {
    return flyweightMode || singletonMode;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Release flyweight state and page guard FIRST to allow page eviction
      releaseCurrentPageGuard();
      currentSlot = null;
      flyweightMode = false;
      
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
