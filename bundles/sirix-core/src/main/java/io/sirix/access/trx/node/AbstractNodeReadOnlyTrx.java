package io.sirix.access.trx.node;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.*;
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
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.xml.xpath.AtomicValue;
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
    implements InternalNodeReadOnlyTrx<N> {

  /**
   * ID of transaction.
   */
  protected final int id;

  /**
   * State of transaction including all cached stuff.
   */
  protected StorageEngineReader pageReadOnlyTrx;

  /**
   * The current node fallback object.
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

  // ==================== CURSOR SLOT STATE ====================

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
   * Slot offset of the current singleton node in {@link #currentPage}.
   */
  private int currentSlotOffset = -1;
  
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
   * Whether singleton-mode hot-path rebinding is enabled for this resource.
   * JSON and XML resources use singleton rebinding.
   */
  private final boolean singletonOptimizedResource;

  /**
   * Whether the current resource is an XML resource.
   */
  private final boolean xmlSingletonResource;

  /**
   * Tracks if Dewey bytes are already bound for the current XML singleton.
   */
  private boolean xmlSingletonDeweyBound = true;

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
    this.pageReadOnlyTrx = requireNonNull(pageReadTransaction);
    this.currentNode = requireNonNull(documentNode);
    this.isClosed = false;
    this.resourceConfig = resourceSession.getResourceConfig();
    
    // Initialize cursor state from document node.
    this.currentNodeKey = documentNode.getNodeKey();
    this.currentNodeKind = documentNode.getKind();
    this.xmlSingletonResource = documentNode.getKind() == NodeKind.XML_DOCUMENT;
    this.singletonOptimizedResource = documentNode.getKind() == NodeKind.JSON_DOCUMENT
        || xmlSingletonResource;
  }

  @Override
  public N getCurrentNode() {
    if (currentNode != null) {
      return currentNode;
    }

    // When in singleton mode, create a snapshot (deep copy) of the singleton.
    if (singletonMode && currentSingleton != null) {
      currentNode = createSingletonSnapshot();
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
      case XML_DOCUMENT -> (N) ((XmlDocumentRootNode) currentSingleton).toSnapshot();
      case ELEMENT -> (N) ((ElementNode) currentSingleton).toSnapshot();
      case ATTRIBUTE -> (N) ((AttributeNode) currentSingleton).toSnapshot();
      case NAMESPACE -> (N) ((NamespaceNode) currentSingleton).toSnapshot();
      case TEXT -> (N) ((TextNode) currentSingleton).toSnapshot();
      case COMMENT -> (N) ((CommentNode) currentSingleton).toSnapshot();
      case PROCESSING_INSTRUCTION -> (N) ((PINode) currentSingleton).toSnapshot();
      default -> throw new IllegalStateException("Unexpected singleton kind: " + currentNodeKind);
    };
  }
  
  @Override
  public void setCurrentNode(final @Nullable N currentNode) {
    assertNotClosed();
    this.currentNode = currentNode;

    if (currentNode != null) {
      // Disable singleton mode and use the provided node object.
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
    return pageReadOnlyTrx.getActualRevisionRootPage().getUser();
  }

  @Override
  public boolean moveToPrevious() {
    assertNotClosed();
    // Use cursor getters to avoid unnecessary materialization.
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
      // Save current position using cursor-compatible getters.
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
    if (singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getLeftSiblingKey();
    }
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    if (singletonMode) {
      return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  public boolean moveToLeftSibling() {
    assertNotClosed();
    // Use cursor getter and avoid unnecessary materialization.
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
    return pageReadOnlyTrx.getName(key, getKind());
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    final NodeKind kind = getKind();
    if (kind == NodeKind.XML_DOCUMENT || kind == NodeKind.JSON_DOCUMENT) {
      return 0;
    }

    final ImmutableNode node;
    if (singletonMode && currentSingleton != null) {
      node = currentSingleton;
    } else {
      node = getCurrentNode();
    }
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node instanceof ObjectKeyNode objectKeyNode) {
      return objectKeyNode.getPathNodeKey();
    }
    if (node instanceof ArrayNode arrayNode) {
      return arrayNode.getPathNodeKey();
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
    return pageReadOnlyTrx.getActualRevisionRootPage().getRevision();
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(pageReadOnlyTrx.getActualRevisionRootPage().getRevisionTimestamp());
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
    // Use cursor getter and avoid unnecessary materialization.
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
    
    if (singletonOptimizedResource && pageReadOnlyTrx instanceof NodeStorageEngineReader reader) {
      return moveToSingleton(nodeKey, reader);
    }

    // Fallback to traditional object mode.
    return moveToLegacy(nodeKey);
  }

  // ==================== SINGLETON NODE INSTANCES ====================
  // These mutable singleton nodes are reused across moveTo() operations.
  // Each supported JSON/XML node type has a dedicated singleton instance.

  private static final QNm EMPTY_QNM = new QNm("");
  
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
  private JsonDocumentRootNode singletonJsonDocumentRoot;
  private XmlDocumentRootNode singletonXmlDocumentRoot;
  private ElementNode singletonElement;
  private AttributeNode singletonAttribute;
  private NamespaceNode singletonNamespace;
  private TextNode singletonText;
  private CommentNode singletonComment;
  private PINode singletonPI;
  
  /**
   * Whether currently in singleton mode (using singleton nodes).
   */
  private boolean singletonMode = false;
  
  /**
   * The current singleton node (set when in singletonMode).
   */
  private ImmutableNode currentSingleton;

  
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
    // Calculate target page key to check for same-page access
    final long targetPageKey = reader.pageKey(nodeKey, IndexType.DOCUMENT);
    final int slotOffset = StorageEngineReader.recordPageOffset(nodeKey);
    
    MemorySegment data;
    KeyValueLeafPage page;
    
    // OPTIMIZATION: Check if we're moving within the same page
    if (currentPageKey == targetPageKey && currentPage != null && !currentPage.isClosed()) {
      // Same page! Skip guard management entirely
      page = currentPage;
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
    
    // Populate singleton from serialized data (NO ALLOCATION)
    // Note: NO guard management needed - we're on the same page
    // Reuse BytesIn instance - just reset to new segment and offset (skip kind byte)
    reusableBytesIn.reset(data, 1);
    // For XML resources, Dewey bytes are bound lazily on demand.
    byte[] deweyId = resourceConfig.areDeweyIDsStored && !xmlSingletonResource
        ? page.getDeweyIdAsByteArray(slotOffset)
        : null;
    populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    
    // Update state - we're in singleton mode now (page guard unchanged)
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentSlotOffset = slotOffset;
    this.xmlSingletonDeweyBound = !xmlSingletonResource || !resourceConfig.areDeweyIDsStored;
    this.currentNode = null;  // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;
    
    return true;
  }
  
  /**
   * Slow path for moveToSingleton when moving to a different page.
   * Handles guard acquisition and release.
   */
  private boolean moveToSingletonSlowPath(final long nodeKey, final NodeStorageEngineReader reader) {
    // Get raw slot data with full guard management
    var slotLocation = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
    if (slotLocation == null) {
      return false;
    }
    
    // Read node kind from first byte
    MemorySegment data = slotLocation.data();
    byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    NodeKind kind = NodeKind.getKind(kindByte);
    
    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      slotLocation.guard().close();
      return false;
    }
    
    // Get singleton instance for this node type
    ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      // No singleton for this type (e.g., document root), fall back to legacy
      slotLocation.guard().close();
      return moveToLegacy(nodeKey);
    }
    
    // Release previous page guard ONLY NOW (after we know the new page is valid)
    releaseCurrentPageGuard();
    
    // Populate singleton from serialized data (NO ALLOCATION)
    // Reuse BytesIn instance - just reset to new segment and offset (skip kind byte)
    reusableBytesIn.reset(data, 1);
    // For XML resources, Dewey bytes are bound lazily on demand.
    byte[] deweyId = resourceConfig.areDeweyIDsStored && !xmlSingletonResource
        ? slotLocation.page().getDeweyIdAsByteArray(slotLocation.offset())
        : null;
    populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, slotLocation.page());
    
    // Update state - we're in singleton mode now with new page
    this.currentPageGuard = slotLocation.guard();
    this.currentPage = slotLocation.page();
    this.currentPageKey = reader.pageKey(nodeKey, IndexType.DOCUMENT);
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentSlotOffset = slotLocation.offset();
    this.xmlSingletonDeweyBound = !xmlSingletonResource || !resourceConfig.areDeweyIDsStored;
    this.currentNode = null;  // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;
    
    return true;
  }
  
  /**
   * Get the singleton instance for a given node kind.
   * Lazily creates singletons on first use.
   *
   * @param kind the node kind
   * @return the singleton instance, or null if not supported
   */
  private ImmutableNode getSingletonForKind(NodeKind kind) {
    return switch (kind) {
      case OBJECT -> {
        if (singletonObject == null) {
          singletonObject = new ObjectNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObject;
      }
      case ARRAY -> {
        if (singletonArray == null) {
          singletonArray = new ArrayNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonArray;
      }
      case OBJECT_KEY -> {
        if (singletonObjectKey == null) {
          singletonObjectKey = new ObjectKeyNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObjectKey;
      }
      case STRING_VALUE -> {
        if (singletonString == null) {
          singletonString = new StringNode(0, 0, 0, 0, 0, 0, 0, null,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonString;
      }
      case NUMBER_VALUE -> {
        if (singletonNumber == null) {
          singletonNumber = new NumberNode(0, 0, 0, 0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonNumber;
      }
      case BOOLEAN_VALUE -> {
        if (singletonBoolean == null) {
          singletonBoolean = new BooleanNode(0, 0, 0, 0, 0, 0, 0, false,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonBoolean;
      }
      case NULL_VALUE -> {
        if (singletonNull == null) {
          singletonNull = new NullNode(0, 0, 0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonNull;
      }
      case OBJECT_STRING_VALUE -> {
        if (singletonObjectString == null) {
          singletonObjectString = new ObjectStringNode(0, 0, 0, 0, 0, null,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObjectString;
      }
      case OBJECT_NUMBER_VALUE -> {
        if (singletonObjectNumber == null) {
          singletonObjectNumber = new ObjectNumberNode(0, 0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObjectNumber;
      }
      case OBJECT_BOOLEAN_VALUE -> {
        if (singletonObjectBoolean == null) {
          singletonObjectBoolean = new ObjectBooleanNode(0, 0, 0, 0, 0, false,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObjectBoolean;
      }
      case OBJECT_NULL_VALUE -> {
        if (singletonObjectNull == null) {
          singletonObjectNull = new ObjectNullNode(0, 0, 0, 0, 0,
              resourceConfig.nodeHashFunction, (byte[]) null);
        }
        yield singletonObjectNull;
      }
      case JSON_DOCUMENT -> {
        if (singletonJsonDocumentRoot == null) {
          singletonJsonDocumentRoot = new JsonDocumentRootNode(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
              Fixed.NULL_NODE_KEY.getStandardProperty(),
              Fixed.NULL_NODE_KEY.getStandardProperty(),
              0,
              0,
              resourceConfig.nodeHashFunction);
        }
        yield singletonJsonDocumentRoot;
      }
      case XML_DOCUMENT -> {
        if (singletonXmlDocumentRoot == null) {
          singletonXmlDocumentRoot = new XmlDocumentRootNode(
              Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
              Fixed.NULL_NODE_KEY.getStandardProperty(),
              Fixed.NULL_NODE_KEY.getStandardProperty(),
              0,
              0,
              resourceConfig.nodeHashFunction);
        }
        yield singletonXmlDocumentRoot;
      }
      case ELEMENT -> {
        if (singletonElement == null) {
          singletonElement = new ElementNode(
              0, 0, 0, 0,
              0, 0, 0, 0,
              0, 0, 0,
              0, 0, 0, 0,
              resourceConfig.nodeHashFunction,
              (byte[]) null,
              null,
              null,
              EMPTY_QNM);
        }
        yield singletonElement;
      }
      case ATTRIBUTE -> {
        if (singletonAttribute == null) {
          singletonAttribute = new AttributeNode(
              0, 0, 0, 0,
              0, 0, 0, 0,
              0,
              new byte[0],
              resourceConfig.nodeHashFunction,
              (byte[]) null,
              EMPTY_QNM);
        }
        yield singletonAttribute;
      }
      case NAMESPACE -> {
        if (singletonNamespace == null) {
          singletonNamespace = new NamespaceNode(
              0, 0, 0, 0,
              0, 0, 0, 0,
              0,
              resourceConfig.nodeHashFunction,
              (byte[]) null,
              EMPTY_QNM);
        }
        yield singletonNamespace;
      }
      case TEXT -> {
        if (singletonText == null) {
          singletonText = new TextNode(
              0, 0, 0, 0,
              0, 0,
              0,
              new byte[0],
              false,
              resourceConfig.nodeHashFunction,
              (byte[]) null);
        }
        yield singletonText;
      }
      case COMMENT -> {
        if (singletonComment == null) {
          singletonComment = new CommentNode(
              0, 0, 0, 0,
              0, 0,
              0,
              new byte[0],
              false,
              resourceConfig.nodeHashFunction,
              (byte[]) null);
        }
        yield singletonComment;
      }
      case PROCESSING_INSTRUCTION -> {
        if (singletonPI == null) {
          singletonPI = new PINode(
              0, 0, 0, 0,
              0, 0, 0, 0,
              0, 0, 0,
              0,
              0, 0, 0,
              new byte[0],
              false,
              resourceConfig.nodeHashFunction,
              (byte[]) null,
              EMPTY_QNM);
        }
        yield singletonPI;
      }
      // Other types fall back to legacy mode.
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
      case JSON_DOCUMENT -> ((JsonDocumentRootNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case XML_DOCUMENT -> ((XmlDocumentRootNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case ELEMENT -> ((ElementNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig,
          null,
          null,
          EMPTY_QNM);
      case ATTRIBUTE -> ((AttributeNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case NAMESPACE -> ((NamespaceNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case TEXT -> ((TextNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case COMMENT -> ((CommentNode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
      case PROCESSING_INSTRUCTION -> ((PINode) singleton).readFrom(source,
          nodeKey,
          deweyId,
          resourceConfig.nodeHashFunction,
          resourceConfig);
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
        // Move succeeded - release previous page guard and switch to object mode.
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
   * Legacy object-based moveTo path.
   *
   * @param nodeKey the node key to move to
   * @return true if the move was successful
   */
  private boolean moveToLegacy(final long nodeKey) {
    DataRecord newNode;
    try {
      newNode = pageReadOnlyTrx.getRecord(nodeKey, IndexType.DOCUMENT, -1);
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
    }
    currentPage = null;
    currentPageKey = -1;
    currentSlotOffset = -1;
    xmlSingletonDeweyBound = true;
  }
  
  @Override
  public boolean moveToRightSibling() {
    assertNotClosed();
    // Use cursor getter and avoid unnecessary materialization.
    if (!hasRightSibling()) {
      return false;
    }
    return moveTo(getRightSiblingKey());
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    if (singletonMode) {
      return currentNodeKey;
    }
    return getCurrentNode().getNodeKey();
  }

  @Override
  public long getHash() {
    assertNotClosed();
    if (singletonMode) {
      return currentSingleton != null ? currentSingleton.getHash() : 0L;
    }
    return currentNode != null ? currentNode.getHash() : 0L;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    if (singletonMode) {
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
    return pageReadOnlyTrx;
  }

  /**
   * Replace the current {@link NodeStorageEngineReader}.
   *
   * @param pageReadTransaction {@link NodeStorageEngineReader} instance
   */
  public final void setPageReadTransaction(@Nullable final StorageEngineReader pageReadTransaction) {
    assertNotClosed();
    pageReadOnlyTrx = pageReadTransaction;
  }

  @Override
  public final long getMaxNodeKey() {
    assertNotClosed();
    return pageReadOnlyTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    if (singletonMode && currentSingleton instanceof StructNode structNode) {
      return structNode;
    }

    // Materialize a structural node only when needed.
    final N node = getCurrentNode();
    if (node instanceof final StructNode structNode) {
      return structNode;
    }
    return new io.sirix.node.NullNode(node);
  }

  @Override
  public boolean moveToNextFollowing() {
    assertNotClosed();
    // Use cursor getters to avoid unnecessary materialization.
    while (!hasRightSibling() && hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final @NonNegative long key) {
    assertNotClosed();
    // Save current position using cursor-compatible getters.
    final long savedNodeKey = getNodeKey();
    final boolean retVal = moveTo(key);
    // Restore to the saved position
    moveTo(savedNodeKey);
    return retVal;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    if (singletonMode && currentSingleton != null) {
      return currentSingleton.hasParent();
    }
    return getCurrentNode().hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    if (singletonMode) {
      return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    if (singletonMode) {
      return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    if (singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getRightSiblingKey();
    }
    return getStructuralNode().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    if (singletonMode && currentSingleton instanceof StructNode sn) {
      return sn.getFirstChildKey();
    }
    return getStructuralNode().getFirstChildKey();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    if (singletonMode && currentSingleton != null) {
      return currentSingleton.getParentKey();
    }
    return getCurrentNode().getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    if (getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    // Save current position using cursor-compatible getters.
    final long savedNodeKey = getNodeKey();
    moveToParent();
    final NodeKind parentKind = getKind();
    moveTo(savedNodeKey);
    return parentKind;
  }

  @Override
  public boolean moveToNext() {
    assertNotClosed();
    // Use cursor getter directly.
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
    // If it has a first child, it has a last child.
    return hasFirstChild();
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    if (hasLastChild()) {
      // Save current position using cursor-compatible getters.
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
      // Save current position using cursor-compatible getters.
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
      // Save current position using cursor-compatible getters.
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
    return getStructuralNode().getChildCount();
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    if (singletonMode) {
      return hasFirstChild();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
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
      // Save current position using cursor-compatible getters.
      final long savedNodeKey = getNodeKey();
      moveToRightSibling();
      final NodeKind rightSiblingKind = getKind();
      moveTo(savedNodeKey);
      return rightSiblingKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public StorageEngineReader getPageTrx() {
    assertNotClosed();
    return pageReadOnlyTrx;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    assertNotClosed();
    return pageReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    assertNotClosed();
    if (singletonMode) {
      bindXmlSingletonDeweyBytesIfNeeded();
      return currentSingleton != null ? currentSingleton.getDeweyID() : null;
    }
    return currentNode != null ? currentNode.getDeweyID() : null;
  }

  private void bindXmlSingletonDeweyBytesIfNeeded() {
    if (!xmlSingletonResource || xmlSingletonDeweyBound || currentSingleton == null
        || !resourceConfig.areDeweyIDsStored || currentPage == null || currentSlotOffset < 0) {
      return;
    }

    final byte[] deweyIdBytes = currentPage.getDeweyIdAsByteArray(currentSlotOffset);
    switch (currentNodeKind) {
      case XML_DOCUMENT -> ((XmlDocumentRootNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case ELEMENT -> ((ElementNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case ATTRIBUTE -> ((AttributeNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case NAMESPACE -> ((NamespaceNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case TEXT -> ((TextNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case COMMENT -> ((CommentNode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      case PROCESSING_INSTRUCTION -> ((PINode) currentSingleton).setDeweyIDBytes(deweyIdBytes);
      default -> {
        // Non-XML singleton kind for XML resource is unexpected but harmless.
      }
    }
    xmlSingletonDeweyBound = true;
  }

  @Override
  public int getPreviousRevisionNumber() {
    assertNotClosed();
    if (singletonMode && currentSingleton != null) {
      return currentSingleton.getPreviousRevisionNumber();
    }
    return getCurrentNode().getPreviousRevisionNumber();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
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
   * @return true if zero-allocation mode is active
   */
  boolean isZeroAllocationMode() {
    return singletonMode;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Release page guard first to allow page eviction.
      releaseCurrentPageGuard();
      
      // Callback on session to make sure everything is cleaned up.
      resourceSession.closeReadTransaction(id);

      setPageReadTransaction(null);

      // Immediately release all references.
      pageReadOnlyTrx = null;
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
        && pageReadOnlyTrx.getRevisionNumber() == that.pageReadOnlyTrx.getRevisionNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNodeKey(), pageReadOnlyTrx.getRevisionNumber());
  }
}
