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
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
   * <p>
   * Volatile because auto-commit swaps the engine from the commit-timer thread
   * ({@code reInstantiate()} closes the old engine, then publishes the new one via
   * {@link #setPageReadTransaction(StorageEngineReader)}) while the owning thread may concurrently
   * read — a plain field could pin a stale (closed) engine forever.
   */
  protected volatile StorageEngineReader storageEngineReader;

  /**
   * The revision this transaction works on, cached from the engine at every engine handoff. The value
   * is immutable per engine instance, so reading it here instead of dereferencing
   * {@link #storageEngineReader} keeps {@link #getRevisionNumber()} safe against the post-auto-commit
   * swap window in which the field is momentarily {@code null} or still points at the just-closed
   * engine (auto-commit explicitly invites such cross-thread reads).
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
   * One-shot latch making {@link #close()} run exactly once. {@link #isClosed} is only set at the END
   * of the close body (so {@code assertNotClosed} stays quiet during cleanup), which used to let a
   * concurrent or reentrant second close pass the {@code !isClosed} check and close the underlying
   * {@code StorageEngineReader} twice — double-deregistering its epoch-tracker ticket (issue #1102).
   * CASed 0 → 1 on entry; losers return immediately.
   */
  @SuppressWarnings("unused")
  private volatile int closeInitiated;

  private static final VarHandle CLOSE_INITIATED_VH;

  static {
    try {
      CLOSE_INITIATED_VH =
          MethodHandles.lookup().findVarHandle(AbstractNodeReadOnlyTrx.class, "closeInitiated", int.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

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
   * Page guard protecting the current page from eviction. MUST be released when moving to a different
   * node or closing the transaction.
   */
  private PageGuard currentPageGuard;

  /**
   * The page key of the currently held page guard. Used to detect same-page moves and avoid guard
   * release/reacquire overhead.
   */
  private long currentPageKey = -1;

  /**
   * The current page reference (same page as currentPageGuard). Cached to avoid re-lookup when moving
   * within the same page.
   */
  private KeyValueLeafPage currentPage;

  /**
   * Reusable BytesIn instance for reading node data. Avoids allocation on every moveTo() call.
   */
  private final MemorySegmentBytesIn reusableBytesIn = new MemorySegmentBytesIn(MemorySegment.NULL);

  // ==================== STRUCTURAL-KEY CACHE ====================
  //
  // The four structural keys are by far the hottest thing asked of a cursor on a read-only
  // traversal. A full-document serialization asks for the first-child and right-sibling key about
  // three times per node each — once in {@code DescendantAxis.nextKey}, once in the emitter, once
  // in {@code AbstractSerializer.serializeRevision} — and every ask paid the same price: an
  // {@code instanceof}, a megamorphic {@link StructNode} call over a dozen possible singleton
  // types, and a delta-varint decode out of the page's {@link MemorySegment}. Those repeats read a
  // value that cannot have changed: nothing but a reposition can alter what the cursor is looking
  // at.
  //
  // So each key is decoded at most once per position and kept in a primitive field, with
  // {@link #structKeysCached} recording which fields are live. Every reposition clears the mask, so
  // a stale key is never observable.
  //
  // Population is lazy rather than eager at {@code moveTo} time on purpose: a point read
  // ({@code moveTo} + {@code getValue}) touches no structural key at all and would otherwise pay
  // four decodes for nothing.

  private static final int FIRST_CHILD_CACHED = 1;
  private static final int RIGHT_SIBLING_CACHED = 1 << 1;
  private static final int LEFT_SIBLING_CACHED = 1 << 2;
  private static final int PARENT_CACHED = 1 << 3;

  /**
   * {@code Fixed.NULL_NODE_KEY}, hoisted so the hot comparisons are against a compile-time constant.
   */
  private static final long NULL_NODE_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Which of the {@code cached*Key} fields hold a value for the CURRENT position. */
  private int structKeysCached;

  private long cachedFirstChildKey;
  private long cachedRightSiblingKey;
  private long cachedLeftSiblingKey;
  private long cachedParentKey;

  /**
   * Whether decoded structural keys may be remembered at all.
   *
   * <p>
   * False for write transactions. A writer mutates the record under the cursor in place —
   * {@code setFirstChildKey} while inserting a child, {@code setRightSiblingKey} while linking a
   * sibling — without repositioning, so there is no moment at which the mask could be invalidated and
   * a cached key would go stale under it. Read-only cursors have no such mutation, which is exactly
   * why the repeats are safe to elide there.
   */
  private boolean structKeyCacheEnabled;

  /**
   * Drop every cached structural key. Called from each repositioning entry point; the cost is one
   * store of a zero, which is why it can sit unconditionally at the top of {@link #moveTo(long)}.
   */
  private void invalidateStructKeys() {
    structKeysCached = 0;
  }

  /**
   * Resource configuration cached for hash type checks.
   */
  protected final ResourceConfiguration resourceConfig;

  /**
   * Cached {@link NodeStorageEngineReader} resolved once from {@link #storageEngineReader}. For
   * read-only transactions, this is the reader itself. For write transactions, this is the delegate
   * reader extracted from the writer. Used by {@link #moveTo(long)} to enable singleton mode without
   * per-call instanceof checks.
   */
  private NodeStorageEngineReader cachedNodeReader;

  /**
   * Cached {@link StorageEngineWriter} reference, non-null only for write transactions. Used by
   * {@link #moveToSingletonSlowPath} to resolve TIL modified pages.
   */
  private StorageEngineWriter cachedWriter;

  /**
   * Constructor.
   *
   * @param trxId the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode the document root node
   * @param resourceSession The resource manager for the current transaction
   * @param itemList Read-transaction-exclusive item list.
   */
  protected AbstractNodeReadOnlyTrx(final int trxId, final StorageEngineReader pageReadTransaction,
      final N documentNode, final InternalResourceSession<T, W> resourceSession, final ItemList<AtomicValue> itemList) {
    this.itemList = itemList;
    this.resourceSession = requireNonNull(resourceSession);
    this.id = trxId;
    this.storageEngineReader = requireNonNull(pageReadTransaction);
    this.revisionNumber = pageReadTransaction.getActualRevisionRootPage().getRevision();
    this.currentNode = requireNonNull(documentNode);
    this.isClosed = false;
    this.resourceConfig = resourceSession.getResourceConfig();
    this.cachedNodeReader = resolveNodeReader(pageReadTransaction);
    this.cachedWriter = (pageReadTransaction instanceof StorageEngineWriter w)
        ? w
        : null;
    this.structKeyCacheEnabled = this.cachedWriter == null;

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
   * Create a deep copy snapshot of the current singleton node. The snapshot is a new object with all
   * values copied, safe to hold across cursor moves.
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
    invalidateStructKeys();
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
    if ((structKeysCached & LEFT_SIBLING_CACHED) != 0) {
      return cachedLeftSiblingKey;
    }
    return loadLeftSiblingKey();
  }

  /**
   * Decode the left-sibling key of the current position, remembering it when the cursor is allowed
   * to. Split out of {@link #getLeftSiblingKey()} so the cache hit — the overwhelmingly common case
   * on a traversal — inlines as a mask test and a field read.
   */
  private long loadLeftSiblingKey() {
    if (fusedSyntheticChildMode) {
      return NULL_NODE_KEY;
    }
    final long leftSiblingKey = SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn
        ? sn.getLeftSiblingKey()
        : getStructuralNodeView().getLeftSiblingKey();
    if (structKeyCacheEnabled) {
      cachedLeftSiblingKey = leftSiblingKey;
      structKeysCached |= LEFT_SIBLING_CACHED;
    }
    return leftSiblingKey;
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    if ((structKeysCached & LEFT_SIBLING_CACHED) != 0) {
      return cachedLeftSiblingKey != NULL_NODE_KEY;
    }
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return loadLeftSiblingKey() != NULL_NODE_KEY;
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
   * @return true if the cursor currently targets a fused {@code OBJECT_NAMED_*} record (and we are
   *         NOT already descended into its synthetic child).
   */
  private boolean isOnFusedNamedPrimitive() {
    if (fusedSyntheticChildMode) {
      return false;
    }
    final NodeKind kind = SINGLETON_ENABLED && singletonMode
        ? currentNodeKind
        : getCurrentNode() != null
            ? (NodeKind) getCurrentNode().getKind()
            : null;
    return kind == NodeKind.OBJECT_NAMED_BOOLEAN || kind == NodeKind.OBJECT_NAMED_NUMBER
        || kind == NodeKind.OBJECT_NAMED_STRING || kind == NodeKind.OBJECT_NAMED_NULL;
  }

  /**
   * @return the underlying (non-synthetic) kind of the cursor, regardless of whether we are in fused
   *         synthetic-child mode.
   */
  protected NodeKind getFusedParentKind() {
    if (SINGLETON_ENABLED && singletonMode) {
      return currentNodeKind;
    }
    final N node = getCurrentNode();
    return node != null
        ? (NodeKind) node.getKind()
        : null;
  }

  /**
   * @return true if we are currently viewing the synthetic primitive-value child of a fused
   *         {@code OBJECT_NAMED_*} record (Option A virtual navigation).
   */
  public boolean isFusedSyntheticChild() {
    return fusedSyntheticChildMode;
  }

  @Override
  public final void prepareForApprovedSelfMove() {
    prepareForMove();
  }

  @Override
  public final boolean tryMoveToLastAllocatedDocumentNode(final StorageEngineWriter writer, final long nodeKey) {
    if (cachedWriter != writer || writer.getAllocNodeKey() != nodeKey) {
      return false;
    }

    final KeyValueLeafPage page = writer.getAllocKvl();
    final int slotOffset = writer.getAllocSlotOffset();
    final long pageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    final int expectedSlotOffset = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));
    if (page == null || page.isClosed() || page.getPageKey() != pageKey || slotOffset != expectedSlotOffset) {
      return false;
    }

    // Insertion preflight (including an async epoch rotation, when due) runs before the factory
    // allocates this record. No commit check or second document allocation occurs between that
    // allocation and this immediate bind. We can therefore use the exact active KVL/slot without
    // resolving the page through the TIL again. The slot binder below rereads the directory entry,
    // so parent/sibling updates that resized the same page's heap cannot leave a stale offset.
    prepareForMove();
    return bindWritePageSlot(nodeKey, page, slotOffset, false, writer);
  }

  private void prepareForMove() {
    assertNotClosed();
    fusedSyntheticChildMode = false;
    invalidateStructKeys();
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    // Any move implicitly exits fused synthetic-child mode and invalidates structural keys decoded
    // at the old position. Keep this prelude shared with the approved write-cursor self-move path
    // so skipping only the physical rebind never skips observable cursor state changes.
    prepareForMove();

    // Handle negative keys (item list) - fall back to object mode
    if (nodeKey < 0) {
      return moveToItemList(nodeKey);
    }

    // Self-move: the cursor already sits on this node, so re-resolving it would repeat the whole
    // singleton bind -- page lookup, slot lookup, kind decode, flyweight rebind -- to arrive back
    // where it already is. Re-anchoring at a known node key is how the query layer keeps its place
    // (JsonDBObject.moveRtx does it on entry to every field access, and a scan does that once per
    // record), which put moveRtx at 71.8% inclusive of a warm filter scan.
    //
    // Placed AFTER the fused-synthetic-child reset and invalidateStructKeys() above, so the
    // observable side effects of a moveTo still happen -- only the redundant re-bind is skipped.
    //
    // READ-ONLY transactions only (cachedWriter == null). A write transaction must resolve through
    // its transaction-intent log on EVERY move: after an async epoch rotation the TIL container is
    // copied on write to a NEW modified-page instance while the superseded frozen instance stays
    // OPEN for the background flush, so an isClosed()-based reuse check keeps serving the frozen
    // page for the rest of the epoch -- splitting reads from writes and durably corrupting the
    // sibling chain (#1077). See the same warning in moveToSingletonWrite. Applying this
    // short-circuit to writers broke DiffFileCreationTest, HashTest and OverallTest exactly that
    // way. A read-only transaction has no TIL, and its guard keeps the bound page alive, so
    // reusing the binding is sound there.
    if (cachedWriter == null && nodeKey == currentNodeKey && currentPage != null && !currentPage.isClosed()
        && (currentSingleton != null || currentNode != null)) {
      return true;
    }

    // Use singleton mode for READ-ONLY transactions (cachedWriter == null).
    // Write transactions fall through to moveToLegacy for now — the writer's overridden
    // getRecord() provides TIL-aware page resolution that moveToSingleton needs.
    if (SINGLETON_ENABLED && cachedNodeReader != null && cachedWriter == null) {
      return moveToSingleton(nodeKey, cachedNodeReader);
    }

    // Write path: use moveToSingletonWrite for TIL-aware singleton mode
    if (SINGLETON_ENABLED && cachedWriter != null && cachedNodeReader != null) {
      return moveToSingletonWrite(nodeKey, cachedWriter);
    }

    // Fallback to traditional object mode
    return moveToLegacy(nodeKey);
  }

  /**
   * Toggle for singleton mode. Set to true to enable singleton node reuse. Singleton mode uses
   * mutable singleton nodes that are repopulated on each moveTo(). When combined with cache checking,
   * uses cached records when available.
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
   * Array-based singleton lookup indexed by NodeKind.getId(). Replaces the 19-case switch in
   * getSingletonForKind with O(1) array access. Lazily populated on first access per kind. Max
   * NodeKind ID is 58 (VECTOR_INDEX_METADATA); sized to 64 so iter#30 OBJECT_NAMED_* IDs (48-51) fit
   * headroom.
   */
  private final ImmutableNode[] singletonByKindId = new ImmutableNode[64];

  /**
   * Option-A virtual-child mode for fused {@code OBJECT_NAMED_*} kinds.
   *
   * <p>
   * When {@code true}, the cursor is conceptually positioned on the synthetic primitive value child
   * of a fused node, while physically still bound to the fused record. In this mode:
   * {@code getKind()} returns the corresponding {@code OBJECT_*_VALUE} kind, {@code getValue()} /
   * {@code getBooleanValue()} / {@code getNumberValue()} report the primitive payload,
   * {@code hasFirstChild()} / {@code hasLeftSibling()} / {@code hasRightSibling()} return false
   * (synthetic child is a leaf), and {@code moveToParent()} clears the flag so we "return" to the
   * fused node proper.
   *
   * <p>
   * The flag is cleared on every call to {@link #moveTo(long)} entry as well as on {@link #close()} /
   * {@link #releaseCurrentPageGuard()}.
   */
  private boolean fusedSyntheticChildMode = false;

  /**
   * Move to a node using singleton mode (zero allocation). Repopulates a mutable singleton instance
   * from serialized data. NO allocation happens here - only when getCurrentNode() is called.
   *
   * @param nodeKey the node key to move to
   * @param reader the storage engine reader
   * @return true if the move was successful
   */
  private boolean moveToSingleton(final long nodeKey, final NodeStorageEngineReader reader) {
    // Inline pageKey: all index types use exponent 10, avoids assertNotClosed + switch overhead
    final long targetPageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    final int slotOffset = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));

    final KeyValueLeafPage page;

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
    } else {
      // Different page - use the slow path with guard management
      return moveToSingletonSlowPath(nodeKey, reader);
    }

    if (page.isFusedOverflowDescriptor(slotOffset)) {
      return moveToSingletonSlowPath(nodeKey, reader);
    }

    // Inline slot lookup instead of KeyValueLeafPage.getSlot, which builds a MemorySegment VIEW
    // (asSlice) per call — one allocation on the single hottest step of a read-only traversal,
    // for a view the flyweight path never even looks at: it reads one kind byte and then binds
    // the singleton straight to the page. The write-side move (moveToSingletonWrite) already
    // reads the slot in place; this is the same treatment on the read side.
    final MemorySegment slottedPage = page.getSlottedPage();
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotOffset)) {
      // Slot not found on current page - try overflow or fail
      return moveToSingletonSlowPath(nodeKey, reader);
    }
    // A page loaded to answer one point lookup holds its records compressed until read. This is
    // the door that bypasses getSlot, so it is also the one that has to open the chunk itself; a
    // no-op branch on any eagerly decoded page.
    page.ensureChunkFor(slotOffset);
    // ONE 8-byte directory read for both halves. The heap offset and the packed length+kind sit in
    // adjacent ints of the same entry, so fetching them separately cost two bounds-checked segment
    // accesses on the hottest step of a traversal: profiled warm, this method is 48.9 % of scan CPU
    // and MemorySegment bounds checking a further 7.5 %.
    final long dirEntry = PageLayout.getDirEntry(slottedPage, slotOffset);
    final int heapOffset = PageLayout.dirEntryHeapOffset(dirEntry);
    final int nodeKindId = PageLayout.dirEntryNodeKindId(dirEntry);

    // The kind is already in the directory entry for a flyweight slot, so the byte does not have to
    // be fetched from the heap as well. That read is the scattered one -- the directory is dense and
    // sequentially walked, the heap offset is wherever the record happens to live -- so skipping it
    // removes a likely cache miss per move, not just an access. A zero id means "not a flyweight
    // slot", where the heap byte remains authoritative.
    final NodeKind kind = nodeKindId > 0
        ? NodeKind.getKind((byte) nodeKindId)
        : NodeKind.getKind(slottedPage.get(ValueLayout.JAVA_BYTE, PageLayout.HEAP_START + heapOffset));

    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      return false;
    }

    // Get singleton instance for this node type
    final ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      // No singleton for this type (e.g., document root), fall back to legacy
      return moveToLegacy(nodeKey);
    }

    final boolean isFlyweightSlot = nodeKindId > 0 && singleton instanceof FlyweightNode;
    if (isFlyweightSlot) {
      final FlyweightNode fn = (FlyweightNode) singleton;
      // Bind flyweight directly to slotted page (zero-copy, no legacy parsing)
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(slottedPage, recordBase, nodeKey, slotOffset);
      // Propagate FSST symbol table for compressed string nodes
      propagateFsstToFlyweight(fn, page, reader);
      // Propagate DeweyID from page to flyweight node (stored inline after record data).
      // setDeweyIDBytes stores raw bytes lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOffset));
      }
    } else {
      // Legacy format: populate from serialized data (NO ALLOCATION beyond the view the parser
      // needs). Size the view from the record length, as getSlot did; a non-positive length is
      // the empty-slot case getSlot reported as null, which the slow path resolves.
      final int recordLength = PageLayout.getRecordOnlyLength(slottedPage, slotOffset);
      if (recordLength <= 0) {
        return moveToSingletonSlowPath(nodeKey, reader);
      }
      // Reuse BytesIn instance - just reset to new segment and offset (skip kind byte)
      reusableBytesIn.reset(slottedPage.asSlice(PageLayout.HEAP_START + heapOffset, recordLength), 1);
      // Only fetch DeweyID if actually stored (avoids byte[] allocation)
      final byte[] deweyId = resourceConfig.areDeweyIDsStored
          ? page.getDeweyIdAsByteArray(slotOffset)
          : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state - we're in singleton mode now (page guard unchanged)
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null; // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;

    return true;
  }

  /**
   * Slow path for moveToSingleton when moving to a different page (read-only transactions only). Uses
   * the reader's lookupSlotWithGuard with guard management.
   */
  private boolean moveToSingletonSlowPath(final long nodeKey, final NodeStorageEngineReader reader) {
    var slotLocation = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
    if (slotLocation == null) {
      return false;
    }

    return moveToSingletonFromPage(nodeKey, slotLocation.page(), reader, nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
        slotLocation.guard());
  }

  /**
   * Move to a node on a given page using singleton mode. Shared logic for both write (TIL modified
   * page) and read (guarded page) paths.
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

    if (page.isFusedOverflowDescriptor(slotOff)) {
      if (newGuard != null) {
        newGuard.close();
      }
      return moveToLegacy(nodeKey);
    }

    // Locate the slot in place rather than through KeyValueLeafPage.getSlot, which materializes a
    // MemorySegment view per call; see the same inlining in moveToSingleton. An unpopulated slot
    // is what getSlot reported as null, and is handled identically below.
    final MemorySegment slottedPage = page.getSlottedPage();
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, slotOff)) {
      if (newGuard != null) {
        newGuard.close();
      }
      // Large-value overflow record (#1076): no slot, but the page carries an overflow
      // reference. Resolve through the object path — its record-persister deserialization
      // matches the overflow byte format for every node kind, whereas the singleton readFrom
      // parsers expect the legacy slot layout (which e.g. carries a hash field for
      // STRING_VALUE that the persister format does not). Without this, moveTo of an overflow
      // record returned false and hasLeftSibling()/moveToLeftSibling() loops spun forever.
      if (page.getPageReference(nodeKey) != null) {
        return moveToLegacy(nodeKey);
      }
      return false;
    }
    page.ensureChunkFor(slotOff);
    final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, slotOff);
    final int recordAbsOffset = PageLayout.HEAP_START + heapOffset;

    // Read node kind from first byte
    final byte kindByte = slottedPage.get(ValueLayout.JAVA_BYTE, recordAbsOffset);
    final NodeKind kind = NodeKind.getKind(kindByte);

    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      if (newGuard != null) {
        newGuard.close();
      }
      return false;
    }

    // Get singleton instance for this node type
    final ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      // No singleton for this type (e.g., document root), fall back to legacy
      if (newGuard != null) {
        newGuard.close();
      }
      return moveToLegacy(nodeKey);
    }

    // Check if this is a flyweight record in slotted page. Read the directory entry directly:
    // page.getSlotNodeKindId would re-null-check the page and re-test the population bitmap.
    final boolean isFlyweight =
        PageLayout.getDirNodeKindId(slottedPage, slotOff) > 0 && singleton instanceof FlyweightNode;

    // The legacy parser needs the record's length, and a non-positive one is the empty-slot case
    // getSlot used to report as null — decided here, while the new guard can still be released on
    // the way out.
    final int recordLength = isFlyweight
        ? 0
        : PageLayout.getRecordOnlyLength(slottedPage, slotOff);
    if (!isFlyweight && recordLength <= 0) {
      if (newGuard != null) {
        newGuard.close();
      }
      if (page.getPageReference(nodeKey) != null) {
        return moveToLegacy(nodeKey);
      }
      return false;
    }

    // Release previous page guard ONLY NOW (after we know the new page is valid)
    releaseCurrentPageGuard();

    if (isFlyweight) {
      final FlyweightNode fn = (FlyweightNode) singleton;
      // Bind flyweight directly to slotted page (zero-copy, no legacy parsing)
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(slottedPage, recordBase, nodeKey, slotOff);
      // Propagate FSST symbol table for compressed string nodes
      propagateFsstToFlyweight(fn, page, reader);
      // Propagate DeweyID from page to flyweight node (stored inline after record data).
      // setDeweyIDBytes stores raw bytes lazily — no SirixDeweyID parsing until getDeweyID() called.
      // MUST always set (even null) to clear stale DeweyID from previous singleton reuse.
      if (resourceConfig.areDeweyIDsStored && fn instanceof Node node) {
        node.setDeweyIDBytes(page.getDeweyIdAsByteArray(slotOff));
      }
    } else {
      // Legacy format: populate from serialized data (NO ALLOCATION)
      reusableBytesIn.reset(slottedPage.asSlice(recordAbsOffset, recordLength), 1);
      final byte[] deweyId = resourceConfig.areDeweyIDsStored
          ? page.getDeweyIdAsByteArray(slotOff)
          : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state - we're in singleton mode now with new page
    this.currentPageGuard = newGuard;
    this.currentPage = page;
    this.currentPageKey = pageKey;
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null; // Clear - will be created lazily by getCurrentNode()
    this.singletonMode = true;

    return true;
  }

  /**
   * Write-transaction singleton moveTo. Uses the writer for current-revision page resolution. Falls
   * back to moveToLegacy when the page is unchanged from the committed base revision.
   *
   * @param nodeKey the node key to move to
   * @param writer the storage engine writer (for TIL page resolution)
   * @return true if the move was successful
   */
  private boolean moveToSingletonWrite(final long nodeKey, final StorageEngineWriter writer) {
    // Inline pageKey: all index types use exponent 10, avoids assertNotClosed + switch overhead
    final long targetPageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    final int slotOffset = (int) (nodeKey & ((1 << Constants.NDP_NODE_COUNT_EXPONENT) - 1));
    final KeyValueLeafPage previousPage = currentPage;
    final long previousPageKey = currentPageKey;
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
      final boolean moved = moveToLegacy(nodeKey);
      if (moved) {
        writer.releasePageForRead(previousPage);
        currentPage = null;
        currentPageKey = -1;
      }
      return moved;
    }

    if (writer.isReadOnlyPageForRead(page)) {
      final boolean moved;
      try {
        moved = moveToDetachedWritePage(nodeKey, page, writer);
      } catch (final RuntimeException | Error failure) {
        if (page != previousPage) {
          writer.releasePageForRead(page);
        }
        currentPage = previousPage;
        currentPageKey = previousPageKey;
        throw failure;
      }
      if (moved) {
        if (previousPage != page) {
          writer.releasePageForRead(previousPage);
        }
        currentPage = page;
        currentPageKey = targetPageKey;
      } else {
        if (page != previousPage) {
          writer.releasePageForRead(page);
        }
        currentPage = previousPage;
        currentPageKey = previousPageKey;
      }
      return moved;
    }

    boolean moved = bindWritePageSlot(nodeKey, page, slotOffset, false, writer);
    if (!moved) {
      moved = moveToLegacyWrite(nodeKey, writer);
    }
    if (moved) {
      if (singletonMode) {
        if (previousPage != page) {
          writer.releasePageForRead(previousPage);
        }
      } else {
        writer.releasePageForRead(previousPage);
        writer.releasePageForRead(page);
        currentPage = null;
        currentPageKey = -1;
      }
    } else {
      if (page != previousPage) {
        writer.releasePageForRead(page);
      }
      currentPage = previousPage;
      currentPageKey = previousPageKey;
    }
    return moved;
  }

  private boolean moveToDetachedWritePage(final long nodeKey, final KeyValueLeafPage page,
      final StorageEngineWriter writer) {
    final DataRecord newNode = writer.getDetachedRecordForRead(page, nodeKey);
    if (newNode == null) {
      return false;
    }
    setCurrentNode((N) newNode);
    return true;
  }

  private boolean moveToLegacyWrite(final long nodeKey, final StorageEngineWriter writer) {
    DataRecord newNode;
    try {
      newNode = writer.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    } catch (final SirixIOException | UncheckedIOException | IllegalArgumentException e) {
      newNode = null;
    }
    if (newNode == null) {
      return false;
    }
    setCurrentNode((N) newNode);
    currentNodeKey = nodeKey;
    return true;
  }

  /**
   * Bind this cursor's own singleton to a record on an already-resolved write page.
   *
   * <p>
   * The node factory owns a separate set of reusable creation singletons. Never installing one of
   * those objects as the cursor node is essential: the next same-kind creation rebinds the factory
   * singleton in place. This method instead selects and binds the read cursor's private singleton,
   * exactly like the ordinary TIL-aware write move.
   * </p>
   */
  private boolean bindWritePageSlot(final long nodeKey, final KeyValueLeafPage page, final int slotOffset,
      final boolean fallbackToLegacy, final StorageEngineWriter writer) {
    final long targetPageKey = nodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;

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
      replaceCurrentWritePage(page, targetPageKey, writer);
      return true;
    }

    if (page.isFusedOverflowDescriptor(slotOffset)) {
      return false;
    }

    // Inline slot lookup: avoid KeyValueLeafPage.getSlot's asSlice allocation,
    // which is hot on shred (every moveTo allocates a MemorySegment view just
    // to read one byte). Read the kind byte directly from the slotted page.
    final MemorySegment sp = page.getSlottedPage();
    if (sp == null) {
      return fallbackToLegacy && moveToLegacy(nodeKey);
    }
    // One dense 8-byte directory load replaces three foreign-memory accesses (bitmap word, heap
    // offset, and scattered heap kind byte). Every populated record is at least its one-byte kind,
    // so dataLength==0 is an unambiguous empty-slot sentinel. The directory belongs to the eagerly
    // decoded META section; only the heap payload needs lazy-chunk materialization afterwards.
    final long dirEntry = PageLayout.getDirEntry(sp, slotOffset);
    if (PageLayout.dirEntryDataLength(dirEntry) == 0) {
      return fallbackToLegacy && moveToLegacy(nodeKey);
    }
    page.ensureChunkFor(slotOffset);
    final int heapOffset = PageLayout.dirEntryHeapOffset(dirEntry);
    final int nodeKindId = PageLayout.dirEntryNodeKindId(dirEntry);
    final NodeKind kind = nodeKindId > 0
        ? NodeKind.getKind((byte) nodeKindId)
        : NodeKind.getKind(sp.get(ValueLayout.JAVA_BYTE, PageLayout.HEAP_START + heapOffset));

    if (kind == NodeKind.DELETE) {
      return false;
    }

    // Get singleton instance for this node type
    final ImmutableNode singleton = getSingletonForKind(kind);
    if (singleton == null) {
      return fallbackToLegacy && moveToLegacy(nodeKey);
    }

    // Bind singleton to page data (zero allocation)
    if (singleton instanceof FlyweightNode fn) {
      final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
      fn.bind(sp, recordBase, nodeKey, slotOffset);
      // Fresh strings may already be FSST-compressed in the page. The cursor singleton is distinct
      // from the factory singleton that performed the write, so propagate the page table again.
      propagateFsstToFlyweight(fn, page, storageEngineReader);
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
        return fallbackToLegacy && moveToLegacy(nodeKey);
      }
      reusableBytesIn.reset(sp.asSlice(PageLayout.HEAP_START + heapOffset, recordLength), 1);
      final byte[] deweyId = resourceConfig.areDeweyIDsStored
          ? page.getDeweyIdAsByteArray(slotOffset)
          : null;
      populateSingleton(singleton, reusableBytesIn, nodeKey, deweyId, kind, page);
    }

    // Update state — singleton mode, no guard needed for TIL pages
    this.currentSingleton = singleton;
    this.currentNodeKind = kind;
    this.currentNodeKey = nodeKey;
    this.currentNode = null;
    this.singletonMode = true;
    replaceCurrentWritePage(page, targetPageKey, writer);

    return true;
  }

  private void replaceCurrentWritePage(final KeyValueLeafPage page, final long pageKey,
      final StorageEngineWriter writer) {
    final KeyValueLeafPage previousPage = currentPage;
    if (previousPage != page) {
      if (currentPageGuard != null) {
        currentPageGuard.close();
        currentPageGuard = null;
      }
      currentPage = page;
      writer.releasePageForRead(previousPage);
    }
    currentPageKey = pageKey;
  }

  /**
   * Propagate FSST symbol table from page to a flyweight string node. Required for lazy decompression
   * of FSST-compressed strings in singleton mode.
   */
  private static void propagateFsstToFlyweight(final FlyweightNode fn, final KeyValueLeafPage page,
      final StorageEngineReader reader) {
    if (fn instanceof StringNode sn) {
      final byte[] fsstTable = resolvedFsstSymbolTable(page, reader);
      // Null is meaningful: clear the table retained from this singleton's previous binding.
      sn.setFsstSymbolTable(fsstTable);
    } else if (fn instanceof ObjectNamedStringNode ons) {
      ons.setFsstSymbolTable(resolvedFsstSymbolTable(page, reader));
    }
  }

  /**
   * Resolve a cold page's dictionary reference before a singleton reads FSST payload bytes.
   * Already-resolved pages take one field read and one predictable branch; dictionary access is paid
   * only once, when a positive table id has not yet been resolved to bytes.
   */
  private static byte[] resolvedFsstSymbolTable(final KeyValueLeafPage page, final StorageEngineReader reader) {
    byte[] fsstTable = page.getFsstSymbolTable();
    if (fsstTable != null || page.getFsstSymbolTableId() == KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID) {
      return fsstTable;
    }

    reader.ensureFsstSymbolTable(page);
    fsstTable = page.getFsstSymbolTable();
    if (fsstTable == null) {
      throw new IllegalStateException("FSST symbol table " + page.getFsstSymbolTableId()
          + " remained unresolved for document record page " + page.getPageKey());
    }
    return fsstTable;
  }

  /**
   * Get the singleton instance for a given node kind. Lazily creates singletons on first use.
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
      case OBJECT -> new ObjectNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, resourceConfig.nodeHashFunction, (byte[]) null);
      case ARRAY -> new ArrayNode(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, resourceConfig.nodeHashFunction, (byte[]) null);
      case STRING_VALUE -> new StringNode(0, 0, 0, 0, 0, 0, 0, null, resourceConfig.nodeHashFunction, (byte[]) null);
      case NUMBER_VALUE -> new NumberNode(0, 0, 0, 0, 0, 0, 0, 0, resourceConfig.nodeHashFunction, (byte[]) null);
      case BOOLEAN_VALUE -> new BooleanNode(0, 0, 0, 0, 0, 0, 0, false, resourceConfig.nodeHashFunction, (byte[]) null);
      case NULL_VALUE -> new NullNode(0, 0, 0, 0, 0, 0, 0, resourceConfig.nodeHashFunction, (byte[]) null);
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
   * @param source the BytesIn source positioned after the kind byte
   * @param nodeKey the node key
   * @param deweyId the DeweyID bytes
   * @param kind the node kind
   */
  private void populateSingleton(ImmutableNode singleton, BytesIn<?> source, long nodeKey, byte[] deweyId,
      NodeKind kind, KeyValueLeafPage page) {
    switch (kind) {
      case OBJECT ->
        ((ObjectNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case ARRAY ->
        ((ArrayNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case STRING_VALUE -> {
        StringNode stringNode = (StringNode) singleton;
        stringNode.readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
        stringNode.setFsstSymbolTable(resolvedFsstSymbolTable(page, storageEngineReader));
      }
      case NUMBER_VALUE ->
        ((NumberNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case BOOLEAN_VALUE ->
        ((BooleanNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case NULL_VALUE ->
        ((NullNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_BOOLEAN -> ((ObjectNamedBooleanNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_NUMBER -> ((ObjectNamedNumberNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_STRING -> {
        ObjectNamedStringNode namedStr = (ObjectNamedStringNode) singleton;
        namedStr.readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
        namedStr.setFsstSymbolTable(resolvedFsstSymbolTable(page, storageEngineReader));
      }
      case OBJECT_NAMED_NULL -> ((ObjectNamedNullNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_OBJECT -> ((ObjectNamedObjectNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case OBJECT_NAMED_ARRAY -> ((ObjectNamedArrayNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case JSON_DOCUMENT -> ((JsonDocumentRootNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      case ELEMENT ->
        ((ElementNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case ATTRIBUTE ->
        ((AttributeNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case TEXT ->
        ((TextNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case COMMENT ->
        ((CommentNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case PROCESSING_INSTRUCTION ->
        ((PINode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case NAMESPACE ->
        ((NamespaceNode) singleton).readFrom(source, nodeKey, deweyId, resourceConfig.nodeHashFunction, resourceConfig);
      case XML_DOCUMENT -> ((XmlDocumentRootNode) singleton).readFrom(source, nodeKey, deweyId,
          resourceConfig.nodeHashFunction, resourceConfig);
      default -> throw new IllegalStateException("Unexpected singleton kind: " + kind);
    }
  }

  /**
   * Move to an item in the item list (negative keys). Falls back to object mode since item list uses
   * objects.
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
        // noinspection unchecked
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
      // noinspection unchecked
      setCurrentNode((N) newNode);
      this.currentNodeKey = nodeKey;
      return true;
    }
  }

  /**
   * Release the current page guard if one is held. This allows the page to be evicted if needed.
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
      return currentSingleton != null
          ? currentSingleton.getHash()
          : 0L;
    }
    return currentNode != null
        ? currentNode.getHash()
        : 0L;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    if (SINGLETON_ENABLED && singletonMode) {
      return currentNodeKind;
    }
    final N node = getCurrentNode();
    return node != null
        ? (NodeKind) node.getKind()
        : null;
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
    cachedWriter = (pageReadTransaction instanceof StorageEngineWriter w)
        ? w
        : null;
    structKeyCacheEnabled = cachedWriter == null;
    // The engine handoff can re-resolve the same node key against a different revision, so keys
    // decoded under the previous engine cannot be carried across it.
    invalidateStructKeys();
  }

  /**
   * Resolve the underlying {@link NodeStorageEngineReader} from a storage engine reader. For
   * read-only transactions, this is the reader itself. For write transactions (where the reader is a
   * {@link StorageEngineWriter}), extracts the delegate reader via
   * {@link StorageEngineWriter#getStorageEngineReader()}.
   */
  private static NodeStorageEngineReader resolveNodeReader(@Nullable final StorageEngineReader reader) {
    if (reader instanceof NodeStorageEngineReader r) {
      return r;
    }
    if (reader instanceof StorageEngineWriter w && w.getStorageEngineReader() instanceof NodeStorageEngineReader r) {
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
    return getCurrentNodeView() instanceof StructNode structNode
        ? structNode
        : getStructuralNode();
  }

  /**
   * Get a live, allocation-free view of the current node. In singleton mode this returns the reused
   * per-kind singleton bound to the current position instead of a deep-copy snapshot — callers must
   * consume it before the next cursor move and must never retain it. Non-singleton positions fall
   * back to {@link #getCurrentNode()}. Single source of the view dispatch chain
   * ({@link #getStructuralNodeView()} is expressed through it).
   *
   * @return live view of the current node
   */
  protected final ImmutableNode getCurrentNodeView() {
    if (currentNode != null) {
      return currentNode;
    }
    if (SINGLETON_ENABLED && singletonMode && currentSingleton != null) {
      return currentSingleton;
    }
    return getCurrentNode();
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
    if ((structKeysCached & PARENT_CACHED) != 0) {
      return cachedParentKey != NULL_NODE_KEY;
    }
    return loadParentKey() != NULL_NODE_KEY;
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    if ((structKeysCached & FIRST_CHILD_CACHED) != 0) {
      return cachedFirstChildKey != NULL_NODE_KEY;
    }
    // Synthetic child of a fused record is a leaf — no further descent.
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return loadFirstChildKey() != NULL_NODE_KEY;
    }
    return getStructuralNodeView().hasFirstChild();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    if ((structKeysCached & RIGHT_SIBLING_CACHED) != 0) {
      return cachedRightSiblingKey != NULL_NODE_KEY;
    }
    // Synthetic child has no siblings; real siblings live on the fused parent.
    if (fusedSyntheticChildMode) {
      return false;
    }
    if (SINGLETON_ENABLED && singletonMode) {
      return loadRightSiblingKey() != NULL_NODE_KEY;
    }
    return getStructuralNodeView().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    if ((structKeysCached & RIGHT_SIBLING_CACHED) != 0) {
      return cachedRightSiblingKey;
    }
    return loadRightSiblingKey();
  }

  /**
   * Decode the right-sibling key of the current position; see {@link #loadLeftSiblingKey()}.
   */
  private long loadRightSiblingKey() {
    if (fusedSyntheticChildMode) {
      return NULL_NODE_KEY;
    }
    final long rightSiblingKey = SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn
        ? sn.getRightSiblingKey()
        : getStructuralNodeView().getRightSiblingKey();
    if (structKeyCacheEnabled) {
      cachedRightSiblingKey = rightSiblingKey;
      structKeysCached |= RIGHT_SIBLING_CACHED;
    }
    return rightSiblingKey;
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    if ((structKeysCached & FIRST_CHILD_CACHED) != 0) {
      return cachedFirstChildKey;
    }
    return loadFirstChildKey();
  }

  /**
   * Decode the first-child key of the current position; see {@link #loadLeftSiblingKey()}.
   */
  private long loadFirstChildKey() {
    if (fusedSyntheticChildMode) {
      return NULL_NODE_KEY;
    }
    final long firstChildKey = SINGLETON_ENABLED && singletonMode && currentSingleton instanceof StructNode sn
        ? sn.getFirstChildKey()
        : getStructuralNodeView().getFirstChildKey();
    if (structKeyCacheEnabled) {
      cachedFirstChildKey = firstChildKey;
      structKeysCached |= FIRST_CHILD_CACHED;
    }
    return firstChildKey;
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    if ((structKeysCached & PARENT_CACHED) != 0) {
      return cachedParentKey;
    }
    return loadParentKey();
  }

  /**
   * Decode the parent key of the current position; see {@link #loadLeftSiblingKey()}. The fused
   * synthetic-child answer is deliberately NOT cached: it is the cursor's own node key rather than a
   * decoded field, and the mode is cleared by the same {@code moveTo} that clears the mask.
   */
  private long loadParentKey() {
    // From the synthetic primitive child, parent is the fused node's own nodeKey (we are still
    // physically bound to it). Callers use this to navigate back up.
    if (fusedSyntheticChildMode) {
      return currentNodeKey;
    }
    final long parentKey = SINGLETON_ENABLED && singletonMode && currentSingleton != null
        ? currentSingleton.getParentKey()
        : getCurrentNode().getParentKey();
    if (structKeyCacheEnabled) {
      cachedParentKey = parentKey;
      structKeysCached |= PARENT_CACHED;
    }
    return parentKey;
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
      return currentSingleton != null
          ? currentSingleton.getDeweyID()
          : null;
    }
    return currentNode != null
        ? currentNode.getDeweyID()
        : null;
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
   * Check if flyweight mode is currently active. Package-private for testing purposes.
   *
   * @return always {@code false}; flyweight mode has been removed.
   */
  boolean isFlyweightMode() {
    return false;
  }

  /**
   * Check if singleton mode is currently active. Package-private for testing purposes.
   *
   * @return true if singleton mode is active (using mutable singleton nodes)
   */
  boolean isSingletonMode() {
    return singletonMode;
  }

  /**
   * Check if zero-allocation mode is active. Package-private for testing purposes.
   *
   * @return true if singleton mode is active
   */
  boolean isZeroAllocationMode() {
    return singletonMode;
  }

  @Override
  public void close() {
    if (!CLOSE_INITIATED_VH.compareAndSet(this, 0, 1)) {
      return;
    }
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
