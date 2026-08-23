package io.sirix.access.trx.node.json.objectvalue;

/**
 * Internal number-kind tags and object-record carrier for exposing a parsed {@code int} or
 * {@code long} without first boxing it as a {@link Number}.
 *
 * <p>
 * The ordinary {@link #getValue()} contract remains available for compatibility. Hot fused
 * object-number insertion checks {@link #primitiveType()} first and consumes
 * {@link #primitiveValue()} synchronously, avoiding an otherwise per-field
 * {@link Integer}/{@link Long} allocation. The read-side projection cursor uses the same tags when
 * it copies a bound payload into caller-owned primitive scratch.
 * </p>
 */
public interface PrimitiveNumberValue extends ObjectRecordValue<Number> {

  /** No unboxed integral value is available; use {@link #getValue()}. */
  byte NONE = 0;

  /** {@link #primitiveValue()} contains a sign-extended {@code int}. */
  byte INT = 1;

  /** {@link #primitiveValue()} contains a {@code long}. */
  byte LONG = 2;

  /**
   * Returns the unboxed representation available from this carrier.
   *
   * @return {@link #NONE}, {@link #INT}, or {@link #LONG}
   */
  byte primitiveType();

  /**
   * Returns the current integral value. The result is meaningful only when {@link #primitiveType()}
   * is {@link #INT} or {@link #LONG}.
   *
   * @return the sign-extended primitive value
   */
  long primitiveValue();
}
