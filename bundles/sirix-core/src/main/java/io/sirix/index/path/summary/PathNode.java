package io.sirix.index.path.summary;

import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Arrays;
import java.util.Objects;
import io.sirix.node.NodeKind;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.NameNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.RoaringBitmap;
import io.sirix.node.xml.AbstractStructForwardingNode;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Path node in the {@link PathSummaryReader}.
 *
 * @author Johannes Lichtenberger
 */
public final class PathNode extends AbstractStructForwardingNode implements NameNode {

  /**
   * {@link NodeDelegate} instance.
   */
  private final NodeDelegate nodeDel;

  /**
   * {@link StructNodeDelegate} instance.
   */
  private final StructNodeDelegate structNodeDel;

  /**
   * {@link NameNodeDelegate} instance.
   */
  private final NameNodeDelegate nameNodeDel;

  /**
   * Kind of node to index.
   */
  private final NodeKind kind;

  /**
   * The node name.
   */
  private QNm name;

  /**
   * Number of references to this path node.
   */
  private int references;

  /**
   * Level of this path node.
   */
  private final int level;

  private PathNode firstChild;

  private PathNode lastChild;

  private PathNode parent;

  private PathNode leftSibling;

  private PathNode rightSibling;

  private Path<QNm> path;

  // =====================================================================
  // Per-path value statistics — populated only when the owning resource has
  // ResourceConfiguration.withPathStatistics == true. Maintained at commit
  // time via recordValue / removeValue hooks in PathSummaryWriter; read at
  // query time for the aggregate-short-circuit path in the vectorized
  // executor.
  //
  // Sentinel values for the "empty" state:
  //   statsMin = Long.MAX_VALUE, statsMax = Long.MIN_VALUE
  //   statsMinBytes = null,      statsMaxBytes = null
  //   hll = null (lazily created on first value)
  // =====================================================================

  private long statsCount;
  private long statsNullCount;
  private long statsSum;
  private long statsMin = Long.MAX_VALUE;
  private long statsMax = Long.MIN_VALUE;
  private byte @Nullable [] statsMinBytes;
  private byte @Nullable [] statsMaxBytes;
  private @Nullable HyperLogLogSketch hll;
  /**
   * Set when a remove saw a value equal to {@link #statsMin} — the stored min may now be
   * too low. Reader rebounds on demand.
   */
  private boolean statsMinDirty;
  /**
   * Set when a remove saw a value equal to {@link #statsMax} — the stored max may now be
   * too high. Reader rebounds on demand.
   */
  private boolean statsMaxDirty;

  /**
   * Presence bitmap — set of leaf pageKeys that contain at least one node
   * with this PathNode's {@link #getPathNodeKey()}. Populated incrementally
   * at commit time from the caller's deferred stats; read at query time to
   * skip leaf-page fetches for pages that provably don't contribute. Lazily
   * allocated on the first recorded page to avoid the 60 B empty-bitmap
   * overhead on PathNodes that never see an analytical query.
   */
  private @Nullable RoaringBitmap pageKeys;

  /**
   * Constructor.
   *
   * @param name the full qualified name
   * @param nodeDel {@link NodeDelegate} instance
   * @param structNodeDel {@link StructNodeDelegate} instance
   * @param nameNodeDel {@link NameNodeDelegate} instance
   * @param kind kind of node to index
   * @param references number of references to this path node
   * @param level level of this path node
   */
  public PathNode(final QNm name, final NodeDelegate nodeDel, final StructNodeDelegate structNodeDel,
      final NameNodeDelegate nameNodeDel, final NodeKind kind, final int references,
      final int level) {
    this.name = name;
    this.nodeDel = requireNonNull(nodeDel);
    this.structNodeDel = requireNonNull(structNodeDel);
    this.nameNodeDel = requireNonNull(nameNodeDel);
    this.kind = requireNonNull(kind);
    checkArgument(references > 0, "references must be > 0!");
    this.references = references;
    this.level = level;
  }

  /**
   * Get the path up to the root path node.
   *
   * @return path up to the root
   */
  public Path<QNm> getPath() {
    return path;
  }

  /**
   * Set the path.
   * 
   * @param path path to set
   * @return this path instance
   */
  public PathNode setPath(Path<QNm> path) {
    this.path = path;
    return this;
  }

  /**
   * Level of this path node.
   *
   * @return level of this path node
   */
  public int getLevel() {
    return level;
  }

  /**
   * Get the number of references to this path node.
   *
   * @return number of references
   */
  public int getReferences() {
    return references;
  }

  /**
   * Set the reference count.
   *
   * @param references number of references
   */
  public void setReferenceCount(final int references) {
    checkArgument(references > 0, "pReferences must be > 0!");
    this.references = references;
  }

  /**
   * Increment the reference count.
   */
  public void incrementReferenceCount() {
    references++;
  }

  /**
   * Decrement the reference count.
   */
  public void decrementReferenceCount() {
    if (references <= 1) {
      throw new IllegalStateException();
    }
    references--;
  }

  /**
   * Get the kind of path (element, attribute or namespace).
   *
   * @return path kind
   */
  public NodeKind getPathKind() {
    return kind;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PATH;
  }

  @Override
  public int getPrefixKey() {
    return nameNodeDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameNodeDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameNodeDel.getURIKey();
  }

  @Override
  public void setLocalNameKey(final int nameKey) {
    nameNodeDel.setLocalNameKey(nameKey);
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    nameNodeDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    nameNodeDel.setURIKey(uriKey);
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return nodeDel;
  }

  /**
   * Get the name node delegate.
   *
   * @return name node delegate.
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameNodeDel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeDel, nameNodeDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof PathNode other) {
      return Objects.equals(nodeDel, other.nodeDel) && Objects.equals(nameNodeDel, other.nameNodeDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                      .add("node delegate", nodeDel)
                      .add("struct delegate", structNodeDel)
                      .add("name delegate", nameNodeDel)
                      .add("references", references)
                      .add("kind", kind)
                      .add("level", level)
                      .toString();
  }

  @Override
  public void setPathNodeKey(final long nodeKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPathNodeKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QNm getName() {
    return name;
  }

  public void setFirstChild(PathNode pathNode) {
    firstChild = pathNode;
  }

  public void setLastChild(PathNode pathNode) {
    lastChild = pathNode;
  }

  public void setParent(PathNode pathNode) {
    parent = pathNode;
  }

  public void setLeftSibling(PathNode pathNode) {
    leftSibling = pathNode;
  }

  public void setRightSibling(PathNode pathNode) {
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

  public void setName(QNm qNm) {
    name = qNm;
  }

  // =====================================================================
  // Statistics maintenance (package-private — called by PathSummaryWriter).
  // =====================================================================

  /** Record a long value observation (numeric path). */
  void recordLongValue(final long value) {
    statsCount++;
    statsSum += value;
    if (value < statsMin) {
      statsMin = value;
    }
    if (value > statsMax) {
      statsMax = value;
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.add(value);
  }

  /**
   * Record a byte-sequence value observation (string path). The caller's array is
   * cloned on min/max update so later mutations don't corrupt the bound.
   */
  void recordBytesValue(final byte[] value) {
    statsCount++;
    if (statsMinBytes == null || Arrays.compareUnsigned(value, statsMinBytes) < 0) {
      statsMinBytes = value.clone();
    }
    if (statsMaxBytes == null || Arrays.compareUnsigned(value, statsMaxBytes) > 0) {
      statsMaxBytes = value.clone();
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.add(value);
  }

  /** Record a boolean value (treated as 0/1 in numeric aggregates). */
  void recordBooleanValue(final boolean value) {
    recordLongValue(value ? 1L : 0L);
  }

  /** Record a null value observation. */
  void recordNullValue() {
    statsNullCount++;
  }

  /**
   * Decrement value-count / sum on delete. If the deleted value matches the current
   * min or max bound, mark it dirty — the bound is possibly loose, reader rebounds on
   * demand. HLL is never decremented; estimate drifts upward on heavy deletion until a
   * rebuild runs.
   */
  void removeLongValue(final long value) {
    if (statsCount > 0) {
      statsCount--;
    }
    statsSum -= value;
    if (value == statsMin) {
      statsMinDirty = true;
    }
    if (value == statsMax) {
      statsMaxDirty = true;
    }
  }

  void removeBytesValue(final byte[] value) {
    if (statsCount > 0) {
      statsCount--;
    }
    if (statsMinBytes != null && Arrays.equals(value, statsMinBytes)) {
      statsMinDirty = true;
    }
    if (statsMaxBytes != null && Arrays.equals(value, statsMaxBytes)) {
      statsMaxDirty = true;
    }
  }

  void removeBooleanValue(final boolean value) {
    removeLongValue(value ? 1L : 0L);
  }

  void removeNullValue() {
    if (statsNullCount > 0) {
      statsNullCount--;
    }
  }

  // =====================================================================
  // Bulk-merge mutators — used by PathSummaryWriter.flushPendingStats to
  // splice a batched-up set of deferred observations into this node in
  // a single prepareRecordForModification call. Package-private; callers
  // must hold the write-side PathNode reference (not a reader snapshot).
  // =====================================================================

  /**
   * Merge a precomputed long-value aggregate. Equivalent to calling
   * {@link #recordLongValue(long)} {@code count} times with values whose
   * sum is {@code sum} and whose extremes are {@code min}/{@code max}.
   * HLL is merged via {@link #unionHll(HyperLogLogSketch)} separately by
   * the caller so the batch's HLL can be union'd in one pass.
   */
  void mergeLongStats(final long count, final long sum, final long min, final long max) {
    statsCount += count;
    statsSum += sum;
    if (min < statsMin) {
      statsMin = min;
    }
    if (max > statsMax) {
      statsMax = max;
    }
  }

  /**
   * Merge a precomputed bytes-value aggregate. Caller owns the passed
   * arrays; we clone on min/max update so later mutation doesn't corrupt
   * the stored bound.
   */
  void mergeBytesStats(final long count, final byte @Nullable [] minBytes, final byte @Nullable [] maxBytes) {
    statsCount += count;
    if (minBytes != null
        && (statsMinBytes == null || Arrays.compareUnsigned(minBytes, statsMinBytes) < 0)) {
      statsMinBytes = minBytes.clone();
    }
    if (maxBytes != null
        && (statsMaxBytes == null || Arrays.compareUnsigned(maxBytes, statsMaxBytes) > 0)) {
      statsMaxBytes = maxBytes.clone();
    }
  }

  /** Add {@code delta} to the null-value counter. Bulk equivalent of N {@link #recordNullValue()} calls. */
  void incrementNullCount(final long delta) {
    statsNullCount += delta;
  }

  /** Union {@code other} into this node's HLL sketch, creating one if absent. No-op if {@code other} is null. */
  void unionHll(final HyperLogLogSketch other) {
    if (other == null) {
      return;
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.union(other);
  }

  /**
   * Bulk setter — used by the PATH-node deserializer (in {@code io.sirix.node}, a
   * different package, so this must be public) and by the reader after a dirty-bound
   * rebound. Not part of the user-facing API; callers other than the serializer and
   * rebound path should use the {@code recordValue}/{@code removeValue} mutators.
   */
  public void setStatsState(final long count, final long nullCount, final long sum,
      final long min, final long max,
      final byte @Nullable [] minBytes, final byte @Nullable [] maxBytes,
      final @Nullable HyperLogLogSketch hll,
      final boolean minDirty, final boolean maxDirty) {
    this.statsCount = count;
    this.statsNullCount = nullCount;
    this.statsSum = sum;
    this.statsMin = min;
    this.statsMax = max;
    this.statsMinBytes = minBytes;
    this.statsMaxBytes = maxBytes;
    this.hll = hll;
    this.statsMinDirty = minDirty;
    this.statsMaxDirty = maxDirty;
  }

  /** Replace the tracked minimum after a rebound and clear the dirty flag. */
  void clearMinDirty(final long newMin) {
    this.statsMin = newMin;
    this.statsMinDirty = false;
  }

  /** Replace the tracked maximum after a rebound and clear the dirty flag. */
  void clearMaxDirty(final long newMax) {
    this.statsMax = newMax;
    this.statsMaxDirty = false;
  }

  /**
   * Merge the set of {@code leafPageKeys} into this PathNode's presence
   * bitmap. Each entry is a page key — {@code nodeKey >>> INP_REFERENCE_COUNT_EXPONENT}
   * for the leaf page where a node with this pathNodeKey was stored.
   *
   * <p>Lazily allocates the bitmap on first call. Updates are idempotent
   * (RoaringBitmap dedupes). Caller must hold a writable reference to this
   * PathNode — typically via {@code prepareRecordForModification}.
   */
  void mergePageKeys(final IntSet leafPageKeys) {
    if (leafPageKeys == null || leafPageKeys.isEmpty()) return;
    if (pageKeys == null) pageKeys = new RoaringBitmap();
    final IntIterator it = leafPageKeys.iterator();
    while (it.hasNext()) pageKeys.add(it.nextInt());
  }

  /**
   * Serializer-accessible setter. Package-exposed via a public accessor
   * on the {@link io.sirix.node.NodeKind} enum. Takes ownership of the
   * supplied bitmap.
   */
  public void setPageKeys(final @Nullable RoaringBitmap pageKeys) {
    this.pageKeys = pageKeys;
  }

  /**
   * @return the presence bitmap, or {@code null} if this pathNode has
   *         no page-key tracking (either never recorded or the resource
   *         was built without path statistics).
   */
  public @Nullable RoaringBitmap getPageKeys() {
    return pageKeys;
  }

  /**
   * @return sorted array copy of the page keys where this pathNodeKey
   *         is present, or {@code null} if the bitmap is absent. Empty
   *         array (distinct from {@code null}) signals "definitely no
   *         page has this pathNodeKey".
   */
  public int @Nullable [] getPageKeysArray() {
    return pageKeys == null ? null : pageKeys.toArray();
  }

  // =====================================================================
  // Statistics accessors (public — used by query-time short-circuit).
  // =====================================================================

  public long getStatsValueCount() {
    return statsCount;
  }

  public long getStatsNullCount() {
    return statsNullCount;
  }

  public long getStatsSum() {
    return statsSum;
  }

  /**
   * @return the tracked minimum long value, or {@link Long#MAX_VALUE} if no numeric
   *         values have been recorded.
   */
  public long getStatsMin() {
    return statsMin;
  }

  /**
   * @return the tracked maximum long value, or {@link Long#MIN_VALUE} if no numeric
   *         values have been recorded.
   */
  public long getStatsMax() {
    return statsMax;
  }

  public byte @Nullable [] getStatsMinBytes() {
    return statsMinBytes;
  }

  public byte @Nullable [] getStatsMaxBytes() {
    return statsMaxBytes;
  }

  public @Nullable HyperLogLogSketch getHllSketch() {
    return hll;
  }

  public boolean isStatsMinDirty() {
    return statsMinDirty;
  }

  public boolean isStatsMaxDirty() {
    return statsMaxDirty;
  }

  /** True if any numeric value has been recorded (i.e. min/max are real bounds). */
  public boolean hasNumericStats() {
    return statsMin != Long.MAX_VALUE;
  }

  /** True if any byte/string value has been recorded. */
  public boolean hasBytesStats() {
    return statsMinBytes != null;
  }
}
