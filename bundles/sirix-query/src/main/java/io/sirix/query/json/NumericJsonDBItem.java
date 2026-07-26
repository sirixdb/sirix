package io.sirix.query.json;

import io.brackit.query.atomic.AbstractNumeric;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.jdm.Type;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.StructuredDBItem;

import java.math.BigDecimal;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a JSON number sourced from a Sirix document as a brackit {@link Numeric} while keeping its
 * {@link JsonDBItem}/{@link StructuredDBItem} node identity.
 *
 * <p>This base class only declares the GENERIC {@link Numeric} interface. brackit's arithmetic,
 * range and comparison operators, however, dispatch on the SPECIFIC numeric sub-interfaces
 * ({@code IntNumeric}, {@code DecNumeric}, {@code DblNumeric}, {@code FltNumeric}) that brackit's own
 * atomics implement — e.g. {@code Int32.mod} falls through to a division when its argument is not an
 * {@code IntNumeric}/{@code DecNumeric}/{@code Dbl}, {@code RangeExpr} requires both bounds to be
 * {@code IntNumeric}, and {@code Dec.atomicCmp} refuses to compare against anything that is not a
 * {@code DecNumeric}/{@code DblNumeric}/{@code FltNumeric}. A single wrapper class cannot
 * conditionally implement one of these mutually exclusive interfaces, so the concrete instance is
 * picked per kind by {@link JsonItemFactory}: integers become {@link IntNumericJsonDBItem}, decimals
 * {@link DecNumericJsonDBItem}, doubles {@link DblNumericJsonDBItem} and floats
 * {@link FltNumericJsonDBItem}. Each subclass simply tags the matching brackit sub-interface and
 * delegates every numeric operation to the wrapped brackit atomic, so {@code instanceof} dispatch
 * routes document-sourced numbers exactly like brackit's literals do.
 */
public abstract class NumericJsonDBItem extends AbstractNumeric
    implements JsonDBItem, Numeric, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Sirix node key. */
  private final long nodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /** The atomic value delegate. */
  final Numeric atomic;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic the atomic value delegate
   */
  NumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final Numeric atomic) {
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
    nodeKey = this.rtx.getNodeKey();
    this.atomic = requireNonNull(atomic);
  }

  @Override
  public JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
  }

  private void moveRtx() {
    rtx.moveTo(nodeKey);
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
  public String stringValue() {
    return atomic.stringValue();
  }

  @Override
  public Atomic asType(Type type) {
    return atomic.asType(type);
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public double doubleValue() {
    return atomic.doubleValue();
  }

  @Override
  public float floatValue() {
    return atomic.floatValue();
  }

  @Override
  public BigDecimal integerValue() {
    return atomic.integerValue();
  }

  @Override
  public BigDecimal decimalValue() {
    return atomic.decimalValue();
  }

  @Override
  public long longValue() {
    return atomic.longValue();
  }

  @Override
  public int intValue() {
    return atomic.intValue();
  }

  @Override
  public IntNumeric asIntNumeric() {
    return atomic.asIntNumeric();
  }

  @Override
  public Numeric add(Numeric other) {
    return atomic.add(other);
  }

  @Override
  public Numeric subtract(Numeric other) {
    return atomic.subtract(other);
  }

  @Override
  public Numeric multiply(Numeric other) {
    return atomic.multiply(other);
  }

  @Override
  public Numeric div(Numeric other) {
    return atomic.div(other);
  }

  @Override
  public Numeric idiv(Numeric other) {
    return atomic.idiv(other);
  }

  @Override
  public Numeric mod(Numeric other) {
    return atomic.mod(other);
  }

  @Override
  public Numeric negate() {
    return atomic.negate();
  }

  @Override
  public Numeric round() {
    return atomic.round();
  }

  @Override
  public Numeric abs() {
    return atomic.abs();
  }

  @Override
  public Numeric floor() {
    return atomic.floor();
  }

  @Override
  public Numeric ceiling() {
    return atomic.ceiling();
  }

  @Override
  public Numeric roundHalfToEven(int precision) {
    return atomic.roundHalfToEven(precision);
  }

  @Override
  public int atomicCmpInternal(Atomic atomic) {
    return this.atomic.atomicCmp(atomic);
  }
}
