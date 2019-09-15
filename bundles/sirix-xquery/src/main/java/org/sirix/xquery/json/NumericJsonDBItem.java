package org.sirix.xquery.json;

import java.math.BigDecimal;
import org.brackit.xquery.atomic.AbstractNumeric;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.atomic.Numeric;
import org.brackit.xquery.xdm.Type;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.xquery.StructuredDBItem;
import com.google.common.base.Preconditions;

public final class NumericJsonDBItem extends AbstractNumeric
    implements JsonDBItem, Numeric, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx mRtx;

  /** Sirix node key. */
  private final long mNodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection mCollection;

  /** The atomic value delegate. */
  private final Numeric mAtomic;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic the atomic value delegate
   */
  public NumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final Numeric atomic) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mNodeKey = mRtx.getNodeKey();
    mAtomic = atomic;
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return mRtx.getResourceManager();
  }

  private final void moveRtx() {
    mRtx.moveTo(mNodeKey);
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
  public String stringValue() {
    return mAtomic.stringValue();
  }

  @Override
  public Atomic asType(Type type) {
    return mAtomic.asType(type);
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public double doubleValue() {
    return mAtomic.doubleValue();
  }

  @Override
  public float floatValue() {
    return mAtomic.floatValue();
  }

  @Override
  public BigDecimal integerValue() {
    return mAtomic.integerValue();
  }

  @Override
  public BigDecimal decimalValue() {
    return mAtomic.decimalValue();
  }

  @Override
  public long longValue() {
    return mAtomic.longValue();
  }

  @Override
  public int intValue() {
    return mAtomic.intValue();
  }

  @Override
  public IntNumeric asIntNumeric() {
    return mAtomic.asIntNumeric();
  }

  @Override
  public Numeric add(Numeric other) {
    return mAtomic.add(other);
  }

  @Override
  public Numeric subtract(Numeric other) {
    return mAtomic.subtract(other);
  }

  @Override
  public Numeric multiply(Numeric other) {
    return mAtomic.multiply(other);
  }

  @Override
  public Numeric div(Numeric other) {
    return mAtomic.div(other);
  }

  @Override
  public Numeric idiv(Numeric other) {
    return mAtomic.idiv(other);
  }

  @Override
  public Numeric mod(Numeric other) {
    return mAtomic.mod(other);
  }

  @Override
  public Numeric negate() {
    return mAtomic.negate();
  }

  @Override
  public Numeric round() {
    return mAtomic.round();
  }

  @Override
  public Numeric abs() {
    return mAtomic.abs();
  }

  @Override
  public Numeric floor() {
    return mAtomic.floor();
  }

  @Override
  public Numeric ceiling() {
    return mAtomic.ceiling();
  }

  @Override
  public Numeric roundHalfToEven(int precision) {
    return mAtomic.roundHalfToEven(precision);
  }

  @Override
  public int atomicCmpInternal(Atomic atomic) {
    return mAtomic.atomicCmp(atomic);
  }
}
