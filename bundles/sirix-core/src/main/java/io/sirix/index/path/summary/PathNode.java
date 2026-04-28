package io.sirix.index.path.summary;

import io.sirix.utils.ToStringHelper;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;

import static io.sirix.utils.Preconditions.checkArgument;

/**
 * Path node in the {@link PathSummaryReader}.
 *
 * <p>Self-contained: holds all structural and name state directly as inline fields
 * (formerly held by {@code NodeDelegate} / {@code StructNodeDelegate} / {@code NameNodeDelegate}).
 * Each PathNode is one heap object instead of three; structural reads are a single load
 * instead of two pointer chases.
 *
 * @author Johannes Lichtenberger
 */
public final class PathNode implements StructNode, NameNode {

  private static final int TYPE_KEY = NamePageHash.generateHashForString("xs:untyped");
  private static final long NULL_NODE_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final long INVALID_KEY = Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty();

  // ---------------------------------------------------------------------
  // Inlined NodeDelegate state.
  // ---------------------------------------------------------------------
  private long nodeKey;
  private long parentKey;
  private int typeKey = TYPE_KEY;
  private int previousRevision;
  private int lastModifiedRevision;
  private volatile @Nullable SirixDeweyID sirixDeweyID;
  private byte @Nullable [] deweyIDData;

  // ---------------------------------------------------------------------
  // Inlined StructNodeDelegate state.
  // ---------------------------------------------------------------------
  private long firstChildKey;
  private long lastChildKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long childCount;
  private long descendantCount;

  // ---------------------------------------------------------------------
  // Inlined NameNodeDelegate state.
  // ---------------------------------------------------------------------
  private int prefixKey;
  private int localNameKey;
  private int uriKey;
  private long pathNodeKey;

  // ---------------------------------------------------------------------
  // Path-node-specific state.
  // ---------------------------------------------------------------------
  private final NodeKind kind;
  private QNm name;
  private int references;
  private final int level;

  /** In-memory tree pointers — populated only by the cached PathSummaryReader graph. */
  private @Nullable PathNode firstChild;
  private @Nullable PathNode lastChild;
  private @Nullable PathNode parent;
  private @Nullable PathNode leftSibling;
  private @Nullable PathNode rightSibling;

  private @Nullable Path<QNm> path;

  /**
   * Per-path value statistics — populated only when the owning resource has
   * {@code ResourceConfiguration.withPathStatistics == true}. Maintained at commit
   * time via recordValue / removeValue hooks in {@link PathSummaryWriter}; read at
   * query time for the aggregate-short-circuit path in the vectorized executor.
   *
   * <p>Lazily allocated — a PathNode that has never seen a value carries a single
   * {@code null} reference instead of 11 zero-init fields + lazy HLL/bitmap.
   */
  private @Nullable PathStats stats;

  /**
   * Constructor — all structural and name state passed as primitives. The path-summary
   * factories own the construction; deserialization in {@link io.sirix.node.NodeKind}
   * uses this constructor directly.
   */
  public PathNode(final QNm name, final NodeKind kind, final int references, final int level,
      final long nodeKey, final long parentKey,
      final int previousRevision, final int lastModifiedRevision,
      final @Nullable SirixDeweyID deweyID,
      final long firstChildKey, final long lastChildKey,
      final long rightSiblingKey, final long leftSiblingKey,
      final long childCount, final long descendantCount,
      final int uriKey, final int prefixKey, final int localNameKey, final long pathNodeKey) {
    assert parentKey >= NULL_NODE_KEY;
    checkArgument(references > 0, "references must be > 0!");
    this.name = name;
    this.kind = kind;
    this.references = references;
    this.level = level;
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.sirixDeweyID = deweyID;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.uriKey = uriKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.pathNodeKey = pathNodeKey;
  }

  // ---------------------------------------------------------------------
  // Path-node-specific accessors.
  // ---------------------------------------------------------------------

  public Path<QNm> getPath() {
    return path;
  }

  public PathNode setPath(final Path<QNm> path) {
    this.path = path;
    return this;
  }

  public int getLevel() {
    return level;
  }

  public int getReferences() {
    return references;
  }

  public void setReferenceCount(final int references) {
    checkArgument(references > 0, "pReferences must be > 0!");
    this.references = references;
  }

  public void incrementReferenceCount() {
    references++;
  }

  public void decrementReferenceCount() {
    if (references <= 1) {
      throw new IllegalStateException();
    }
    references--;
  }

  public NodeKind getPathKind() {
    return kind;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PATH;
  }

  @Override
  public QNm getName() {
    return name;
  }

  @Override
  public void setName(final QNm qNm) {
    name = qNm;
  }

  // ---------------------------------------------------------------------
  // NameNode surface — direct field access.
  // ---------------------------------------------------------------------

  @Override
  public int getPrefixKey() {
    return prefixKey;
  }

  @Override
  public int getLocalNameKey() {
    return localNameKey;
  }

  @Override
  public int getURIKey() {
    return uriKey;
  }

  @Override
  public void setLocalNameKey(final int nameKey) {
    this.localNameKey = nameKey;
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    this.prefixKey = prefixKey;
  }

  @Override
  public void setURIKey(final int uriKey) {
    this.uriKey = uriKey;
  }

  @Override
  public long getPathNodeKey() {
    return pathNodeKey;
  }

  @Override
  public void setPathNodeKey(final long nodeKey) {
    this.pathNodeKey = nodeKey;
  }

  // ---------------------------------------------------------------------
  // Node / DataRecord surface — direct field access (formerly NodeDelegate).
  // ---------------------------------------------------------------------

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  @Override
  public long getParentKey() {
    return parentKey;
  }

  @Override
  public void setParentKey(final long parentKey) {
    assert parentKey >= NULL_NODE_KEY;
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != NULL_NODE_KEY;
  }

  public int getTypeKey() {
    return typeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    this.typeKey = typeKey;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final long hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long computeHash(final BytesOut<?> bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    // Double-checked locking lazy decode — preserves the contract NodeDelegate enforced.
    SirixDeweyID result = sirixDeweyID;
    final byte[] data = deweyIDData;
    if (result == null && data != null) {
      synchronized (this) {
        result = sirixDeweyID;
        if (result == null) {
          result = new SirixDeweyID(data);
          sirixDeweyID = result;
        }
      }
    }
    return result;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    final byte[] data = deweyIDData;
    if (data != null) {
      return data;
    }
    final SirixDeweyID id = sirixDeweyID;
    if (id != null) {
      final byte[] bytes = id.toBytes();
      deweyIDData = bytes;
      return bytes;
    }
    return null;
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDData = null;
  }

  @Override
  public void setDeweyIDBytes(final byte[] bytes) {
    this.deweyIDData = bytes;
    this.sirixDeweyID = null;
  }

  // ---------------------------------------------------------------------
  // StructNode surface — direct field access (formerly StructNodeDelegate).
  // ---------------------------------------------------------------------

  @Override
  public boolean hasFirstChild() {
    return firstChildKey != NULL_NODE_KEY;
  }

  @Override
  public boolean hasLastChild() {
    return lastChildKey != NULL_NODE_KEY;
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSiblingKey != NULL_NODE_KEY;
  }

  @Override
  public boolean hasRightSibling() {
    return rightSiblingKey != NULL_NODE_KEY;
  }

  @Override
  public long getChildCount() {
    return childCount;
  }

  @Override
  public long getFirstChildKey() {
    return firstChildKey;
  }

  @Override
  public long getLastChildKey() {
    return lastChildKey;
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSiblingKey;
  }

  @Override
  public long getRightSiblingKey() {
    return rightSiblingKey;
  }

  @Override
  public void setRightSiblingKey(final long key) {
    this.rightSiblingKey = key;
  }

  @Override
  public void setLeftSiblingKey(final long key) {
    this.leftSiblingKey = key;
  }

  @Override
  public void setFirstChildKey(final long key) {
    this.firstChildKey = key;
  }

  @Override
  public void setLastChildKey(final long key) {
    if (key == INVALID_KEY) {
      throw new UnsupportedOperationException();
    }
    this.lastChildKey = key;
  }

  @Override
  public void decrementChildCount() {
    childCount--;
  }

  @Override
  public void incrementChildCount() {
    childCount++;
  }

  @Override
  public long getDescendantCount() {
    return descendantCount;
  }

  @Override
  public void decrementDescendantCount() {
    descendantCount--;
  }

  @Override
  public void incrementDescendantCount() {
    descendantCount++;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    this.descendantCount = descendantCount;
  }

  // ---------------------------------------------------------------------
  // In-memory tree pointers (cached PathSummaryReader graph).
  // ---------------------------------------------------------------------

  public void setFirstChild(final PathNode pathNode) {
    firstChild = pathNode;
  }

  public void setLastChild(final PathNode pathNode) {
    lastChild = pathNode;
  }

  public void setParent(final PathNode pathNode) {
    parent = pathNode;
  }

  public void setLeftSibling(final PathNode pathNode) {
    leftSibling = pathNode;
  }

  public void setRightSibling(final PathNode pathNode) {
    rightSibling = pathNode;
  }

  public PathNode getFirstChild() {
    return firstChild;
  }

  public PathNode getLastChild() {
    return lastChild;
  }

  public PathNode getParent() {
    return parent;
  }

  public PathNode getLeftSibling() {
    return leftSibling;
  }

  public PathNode getRightSibling() {
    return rightSibling;
  }

  // ---------------------------------------------------------------------
  // hashCode / equals / toString.
  // ---------------------------------------------------------------------

  @Override
  public int hashCode() {
    int result = (int) (nodeKey ^ (nodeKey >>> 32));
    result = 31 * result + typeKey;
    result = 31 * result + (int) (parentKey ^ (parentKey >>> 32));
    result = 31 * result + prefixKey;
    result = 31 * result + localNameKey;
    result = 31 * result + uriKey;
    result = 31 * result + (int) (pathNodeKey ^ (pathNodeKey >>> 32));
    return result;
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof PathNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && typeKey == other.typeKey
        && parentKey == other.parentKey
        && prefixKey == other.prefixKey
        && localNameKey == other.localNameKey
        && uriKey == other.uriKey
        && pathNodeKey == other.pathNodeKey;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
        .add("nodeKey", nodeKey)
        .add("parentKey", parentKey)
        .add("typeKey", typeKey)
        .add("firstChild", firstChildKey)
        .add("lastChild", lastChildKey)
        .add("leftSib", leftSiblingKey)
        .add("rightSib", rightSiblingKey)
        .add("childCount", childCount)
        .add("descendantCount", descendantCount)
        .add("uriKey", uriKey)
        .add("prefixKey", prefixKey)
        .add("localNameKey", localNameKey)
        .add("pathNodeKey", pathNodeKey)
        .add("references", references)
        .add("kind", kind)
        .add("level", level)
        .toString();
  }

  // =====================================================================
  // Statistics maintenance (package-private — called by PathSummaryWriter).
  // All mutators route through the lazily-allocated PathStats record.
  // =====================================================================

  /** Lazily allocate the PathStats record. */
  private PathStats getOrCreateStats() {
    PathStats s = stats;
    if (s == null) {
      s = new PathStats();
      stats = s;
    }
    return s;
  }

  /** Record a long value observation (numeric path). */
  void recordLongValue(final long value) {
    final PathStats s = getOrCreateStats();
    s.count++;
    s.sum += value;
    if (value < s.min) {
      s.min = value;
    }
    if (value > s.max) {
      s.max = value;
    }
    if (s.hll == null) {
      s.hll = new HyperLogLogSketch();
    }
    s.hll.add(value);
  }

  /**
   * Record a byte-sequence value observation (string path). The caller's array is
   * cloned on min/max update so later mutations don't corrupt the bound.
   */
  void recordBytesValue(final byte[] value) {
    final PathStats s = getOrCreateStats();
    s.count++;
    if (s.minBytes == null || Arrays.compareUnsigned(value, s.minBytes) < 0) {
      s.minBytes = value.clone();
    }
    if (s.maxBytes == null || Arrays.compareUnsigned(value, s.maxBytes) > 0) {
      s.maxBytes = value.clone();
    }
    if (s.hll == null) {
      s.hll = new HyperLogLogSketch();
    }
    s.hll.add(value);
  }

  /** Record a boolean value (treated as 0/1 in numeric aggregates). */
  void recordBooleanValue(final boolean value) {
    recordLongValue(value ? 1L : 0L);
  }

  /** Record a null value observation. */
  void recordNullValue() {
    getOrCreateStats().nullCount++;
  }

  /**
   * Decrement value-count / sum on delete. If the deleted value matches the current
   * min or max bound, mark it dirty — the bound is possibly loose, reader rebounds on
   * demand. HLL is never decremented; estimate drifts upward on heavy deletion until a
   * rebuild runs.
   */
  void removeLongValue(final long value) {
    final PathStats s = stats;
    if (s == null) {
      return;
    }
    if (s.count > 0) {
      s.count--;
    }
    s.sum -= value;
    if (value == s.min) {
      s.minDirty = true;
    }
    if (value == s.max) {
      s.maxDirty = true;
    }
  }

  void removeBytesValue(final byte[] value) {
    final PathStats s = stats;
    if (s == null) {
      return;
    }
    if (s.count > 0) {
      s.count--;
    }
    if (s.minBytes != null && Arrays.equals(value, s.minBytes)) {
      s.minDirty = true;
    }
    if (s.maxBytes != null && Arrays.equals(value, s.maxBytes)) {
      s.maxDirty = true;
    }
  }

  void removeBooleanValue(final boolean value) {
    removeLongValue(value ? 1L : 0L);
  }

  void removeNullValue() {
    final PathStats s = stats;
    if (s != null && s.nullCount > 0) {
      s.nullCount--;
    }
  }

  // =====================================================================
  // Bulk-merge mutators — used by PathSummaryWriter.flushPendingStats.
  // =====================================================================

  void mergeLongStats(final long count, final long sum, final long min, final long max) {
    final PathStats s = getOrCreateStats();
    s.count += count;
    s.sum += sum;
    if (min < s.min) {
      s.min = min;
    }
    if (max > s.max) {
      s.max = max;
    }
  }

  void mergeBytesStats(final long count, final byte @Nullable [] minBytes, final byte @Nullable [] maxBytes) {
    final PathStats s = getOrCreateStats();
    s.count += count;
    if (minBytes != null
        && (s.minBytes == null || Arrays.compareUnsigned(minBytes, s.minBytes) < 0)) {
      s.minBytes = minBytes.clone();
    }
    if (maxBytes != null
        && (s.maxBytes == null || Arrays.compareUnsigned(maxBytes, s.maxBytes) > 0)) {
      s.maxBytes = maxBytes.clone();
    }
  }

  void incrementNullCount(final long delta) {
    getOrCreateStats().nullCount += delta;
  }

  void unionHll(final HyperLogLogSketch other) {
    if (other == null) {
      return;
    }
    final PathStats s = getOrCreateStats();
    if (s.hll == null) {
      s.hll = new HyperLogLogSketch();
    }
    s.hll.union(other);
  }

  void clearMinDirty(final long newMin) {
    final PathStats s = getOrCreateStats();
    s.min = newMin;
    s.minDirty = false;
  }

  void clearMaxDirty(final long newMax) {
    final PathStats s = getOrCreateStats();
    s.max = newMax;
    s.maxDirty = false;
  }

  /**
   * Merge the set of {@code leafPageKeys} into this PathNode's presence bitmap.
   * Lazily allocates the bitmap on first call.
   */
  void mergePageKeys(final IntSet leafPageKeys) {
    if (leafPageKeys == null || leafPageKeys.isEmpty()) {
      return;
    }
    final PathStats s = getOrCreateStats();
    if (s.pageKeys == null) {
      s.pageKeys = new RoaringBitmap();
    }
    final IntIterator it = leafPageKeys.iterator();
    while (it.hasNext()) {
      s.pageKeys.add(it.nextInt());
    }
  }

  /** Serializer-accessible setter; takes ownership of the supplied bitmap. */
  public void setPageKeys(final @Nullable RoaringBitmap pageKeys) {
    if (pageKeys == null) {
      if (stats != null) {
        stats.pageKeys = null;
      }
    } else {
      getOrCreateStats().pageKeys = pageKeys;
    }
  }

  public @Nullable RoaringBitmap getPageKeys() {
    final PathStats s = stats;
    return s == null ? null : s.pageKeys;
  }

  public int @Nullable [] getPageKeysArray() {
    final PathStats s = stats;
    return (s == null || s.pageKeys == null) ? null : s.pageKeys.toArray();
  }

  // =====================================================================
  // Statistics accessors (public — used by query-time short-circuit).
  // =====================================================================

  public @Nullable PathStats getStats() {
    return stats;
  }

  public void setStats(final @Nullable PathStats stats) {
    this.stats = stats;
  }

  public long getStatsValueCount() {
    final PathStats s = stats;
    return s == null ? 0L : s.count;
  }

  public long getStatsNullCount() {
    final PathStats s = stats;
    return s == null ? 0L : s.nullCount;
  }

  public long getStatsSum() {
    final PathStats s = stats;
    return s == null ? 0L : s.sum;
  }

  public long getStatsMin() {
    final PathStats s = stats;
    return s == null ? Long.MAX_VALUE : s.min;
  }

  public long getStatsMax() {
    final PathStats s = stats;
    return s == null ? Long.MIN_VALUE : s.max;
  }

  public byte @Nullable [] getStatsMinBytes() {
    final PathStats s = stats;
    return s == null ? null : s.minBytes;
  }

  public byte @Nullable [] getStatsMaxBytes() {
    final PathStats s = stats;
    return s == null ? null : s.maxBytes;
  }

  public @Nullable HyperLogLogSketch getHllSketch() {
    final PathStats s = stats;
    return s == null ? null : s.hll;
  }

  public boolean isStatsMinDirty() {
    final PathStats s = stats;
    return s != null && s.minDirty;
  }

  public boolean isStatsMaxDirty() {
    final PathStats s = stats;
    return s != null && s.maxDirty;
  }

  public boolean hasNumericStats() {
    final PathStats s = stats;
    return s != null && s.min != Long.MAX_VALUE;
  }

  public boolean hasBytesStats() {
    final PathStats s = stats;
    return s != null && s.minBytes != null;
  }
}
