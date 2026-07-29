package io.sirix.index.redblacktree.keyvalue;

import io.sirix.utils.ToStringHelper;
import java.util.Objects;
import io.sirix.index.redblacktree.interfaces.References;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Set;

/**
 * Text node-ID references.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NodeReferences implements References {
  /** A {@link Set} of node-keys. */
  private final Roaring64Bitmap nodeKeys;

  /**
   * Default constructor.
   */
  public NodeReferences() {
    nodeKeys = new Roaring64Bitmap();
  }

  /**
   * Constructor taking a defensive copy of {@code nodeKeys}.
   *
   * <p>Use this when the bitmap belongs to someone else — notably another {@code NodeReferences}
   * reached through {@link #getNodeKeys()}, which hands out the live set. Since
   * {@link #addNodeKey} and {@link #removeNodeKey} mutate in place, sharing the instance would
   * let one reference set silently rewrite another's.
   *
   * <p>When the caller built the bitmap itself and nothing else can see it, the copy is pure
   * waste on a hot path — the index writer merges a reference set per indexed node, so an O(n)
   * copy and a whole duplicate bitmap allocation per merge is money burned. Use
   * {@link #owning(Roaring64Bitmap)} there instead.
   *
   * @param nodeKeys node keys, copied
   */
  public NodeReferences(final Roaring64Bitmap nodeKeys) {
    this(nodeKeys, true);
  }

  /**
   * Wrap a bitmap the caller is handing over, without copying it.
   *
   * <p>The returned instance takes ownership: the caller must not retain the reference or mutate
   * the bitmap afterwards. Intended for freshly-built bitmaps that have not escaped — a
   * deserialize result, or a set merged together locally.
   *
   * @param nodeKeys node keys, adopted rather than copied
   * @return references over {@code nodeKeys}
   */
  public static NodeReferences owning(final Roaring64Bitmap nodeKeys) {
    return new NodeReferences(nodeKeys, false);
  }

  private NodeReferences(final Roaring64Bitmap nodeKeys, final boolean copy) {
    Objects.requireNonNull(nodeKeys, "nodeKeys");
    this.nodeKeys = copy ? nodeKeys.clone() : nodeKeys;
  }

  @Override
  public boolean isPresent(final long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }

  @Override
  public Roaring64Bitmap getNodeKeys() {
    return nodeKeys;
  }

  @Override
  public NodeReferences addNodeKey(final long nodeKey) {
    nodeKeys.add(nodeKey);
    return this;
  }

  @Override
  public boolean removeNodeKey(long nodeKey) {
    boolean containsNodeKey = nodeKeys.contains(nodeKey);
    nodeKeys.removeLong(nodeKey);
    return containsNodeKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKeys);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final NodeReferences refs) {
      return nodeKeys.equals(refs.nodeKeys);
    }
    return false;
  }

  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this);
    final LongIterator iterator = nodeKeys.getLongIterator();
    while (iterator.hasNext()) {
      final var nodeKey = iterator.next();
      helper.add("referenced node key", nodeKey);
    }
    return helper.toString();
  }

  @Override
  public boolean hasNodeKeys() {
    return !nodeKeys.isEmpty();
  }

  @Override
  public boolean contains(long nodeKey) {
    return nodeKeys.contains(nodeKey);
  }
}
