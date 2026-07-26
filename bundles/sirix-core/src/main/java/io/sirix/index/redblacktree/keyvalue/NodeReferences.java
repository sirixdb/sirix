package io.sirix.index.redblacktree.keyvalue;

import io.sirix.utils.ToStringHelper;
import java.util.Objects;
import io.sirix.index.redblacktree.interfaces.References;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Set;

import static java.util.Objects.requireNonNull;

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
   * Constructor. <b>Defensively copies</b> {@code nodeKeys} — use {@link #adopt} instead when the
   * bitmap was just built for this instance and is not shared.
   *
   * @param nodeKeys node keys
   */
  public NodeReferences(final Roaring64Bitmap nodeKeys) {
    assert nodeKeys != null;
    this.nodeKeys = nodeKeys.clone();
  }

  /** Marker for the non-copying constructor; distinguishes it from the defensive-copy one. */
  private enum Adopt { INSTANCE }

  private NodeReferences(final Roaring64Bitmap nodeKeys, final Adopt marker) {
    this.nodeKeys = requireNonNull(nodeKeys);
  }

  /**
   * Wrap {@code nodeKeys} WITHOUT copying it, taking ownership of the bitmap.
   *
   * <p>{@link #NodeReferences(Roaring64Bitmap)} clones, which is right when the caller keeps using
   * its bitmap but pure waste at the many sites that build one solely to hand it over — every
   * index reassembly, and both deserialization paths, allocate a fresh bitmap and immediately
   * clone it away. On a large index entry that is a full second copy of the whole bitmap per
   * lookup.
   *
   * <p><b>Contract:</b> the caller must not mutate {@code nodeKeys} afterwards; this instance owns
   * it. Use the constructor when that cannot be guaranteed.
   *
   * @param nodeKeys the bitmap to take ownership of
   * @return references backed directly by {@code nodeKeys}
   */
  public static NodeReferences adopt(final Roaring64Bitmap nodeKeys) {
    return new NodeReferences(nodeKeys, Adopt.INSTANCE);
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
