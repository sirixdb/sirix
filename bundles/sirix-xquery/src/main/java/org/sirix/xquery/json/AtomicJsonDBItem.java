package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.AbstractAtomic;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.Type;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.xquery.StructuredDBItem;

public final class AtomicJsonDBItem extends AbstractAtomic
    implements JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx mRtx;

  /** Sirix node key. */
  private final long mNodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection mCollection;

  /** The atomic value delegate. */
  private final AbstractAtomic mAtomic;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic the atomic value delegate
   */
  public AtomicJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final AbstractAtomic atomic) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mNodeKey = mRtx.getNodeKey();
    mAtomic = Preconditions.checkNotNull(atomic);
  }

  private final void moveRtx() {
    mRtx.moveTo(mNodeKey);
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return mRtx.getResourceManager();
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    moveRtx();

    return mRtx;
  }

  @Override
  public JsonDBCollection getCollection() {
    return mCollection;
  }

  @Override
  public boolean booleanValue() {
    return mAtomic.booleanValue();
  }

  @Override
  public Type type() {
    return mAtomic.type();
  }

  @Override
  public int cmp(Atomic atomic) {
    return mAtomic.cmp(atomic);
  }

  @Override
  public int atomicCode() {
    return mAtomic.atomicCode();
  }

  @Override
  public String stringValue() {
    return mAtomic.stringValue();
  }

  @Override
  public Atomic asType(Type type) {
    return mAtomic.asType(type);
  }

  @Override
  public int hashCode() {
    return mAtomic.hashCode();
  }

  @Override
  public int atomicCmpInternal(Atomic atomic) {
    return mAtomic.atomicCmp(atomic);
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }
}
