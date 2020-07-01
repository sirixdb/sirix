package org.sirix.xquery.json;

import org.brackit.xquery.atomic.AbstractAtomic;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.Type;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.xquery.StructuredDBItem;
import com.google.common.base.Preconditions;

public final class AtomicJsonDBItem extends AbstractAtomic
    implements JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Sirix node key. */
  private final long nodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /** The atomic value delegate. */
  private final AbstractAtomic atomic;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic the atomic value delegate
   */
  public AtomicJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final AbstractAtomic atomic) {
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);
    nodeKey = this.rtx.getNodeKey();
    this.atomic = Preconditions.checkNotNull(atomic);
  }

  private final void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return rtx.getResourceManager();
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    moveRtx();

    return rtx;
  }

  @Override
  public JsonDBCollection getCollection() {
    return collection;
  }

  @Override
  public boolean booleanValue() {
    return atomic.booleanValue();
  }

  @Override
  public Type type() {
    return atomic.type();
  }

  @Override
  public int cmp(Atomic atomic) {
    return this.atomic.cmp(atomic);
  }

  @Override
  public int atomicCode() {
    return atomic.atomicCode();
  }

  @Override
  public String stringValue() {
    return atomic.stringValue();
  }

  @Override
  public Atomic asType(Type type) {
    return atomic.asType(type);
  }

  @Override
  public int hashCode() {
    return atomic.hashCode();
  }

  @Override
  public int atomicCmpInternal(Atomic atomic) {
    return this.atomic.atomicCmp(atomic);
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }
}
