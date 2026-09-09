package io.sirix.index.redblacktree.keyvalue;

import io.sirix.utils.ToStringHelper;
import io.sirix.utils.ComparisonChain;
import java.util.Objects;
import io.sirix.index.AtomicUtil;
import io.sirix.utils.LogWrapper;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.Type;
import org.jspecify.annotations.Nullable;
import io.sirix.exception.SirixException;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Value representing a text value, attribute value, element QName or any other byte encoded value.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CASValue implements Comparable<CASValue> {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASValue.class));

  /** Atomic value. */
  private final Atomic value;

  /** Path node key this text value belongs to (that is the parent path). */
  private final long pathNodeKey;

  /** Type of value. */
  private final Type type;

  /**
   * Constructor.
   *
   * @param value the atomic value
   * @param type the type of the value
   * @param pathNodeKey the path node-key
   */
  public CASValue(final Atomic value, final Type type, final long pathNodeKey) {
    this.value = requireNonNull(value);
    this.type = requireNonNull(type);
    this.pathNodeKey = pathNodeKey;
  }

  /**
   * Get the value.
   *
   * @return the value
   */
  public byte[] getValue() {
    if (value == null || type == null) {
      return null;
    }
    try {
      return AtomicUtil.toBytes(value, type);
    } catch (final SirixException e) {
      // Do NOT swallow and return byte[]{0}: a 1-byte zero key would be persisted as a different
      // HOT index key. Fail loudly.
      throw new IllegalStateException("Failed to serialize CAS index value " + value + " as " + type, e);
    }
  }

  public Atomic getAtomicValue() {
    if (value == null || type == null) {
      return null;
    }
    try {
      return value;
    } catch (final QueryException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public int compareTo(final @Nullable CASValue other) {
    if (other == null) {
      return 1;
    }

    final Atomic thisAtomic = asDeclaredType(value, type);
    final Atomic otherAtomic = asDeclaredType(other.value, other.type);

    return ComparisonChain.start().compare(pathNodeKey, other.pathNodeKey).compare(thisAtomic, otherAtomic).result();
  }

  /**
   * {@code value} as {@code type}, skipping the conversion when it already IS that type.
   *
   * <p>
   * Not merely an optimization. brackit's instant atomics throw {@code BIDY0005} from
   * {@link Atomic#asType} when the target is their OWN type, so converting an already-typed
   * {@code xs:dateTime} is an error rather than a no-op. That went unnoticed while the index stored
   * every value as a {@link io.brackit.query.atomic.Str} and this method was the only thing that ever
   * typed it; once the builder began storing the converted value — so that the stored and probed
   * sides share one shape — the unconditional cast started failing on the instant family.
   * </p>
   *
   * @param value the value to view as {@code type}
   * @param type the declared content type
   * @return the typed value
   */
  private static Atomic asDeclaredType(final Atomic value, final Type type) {
    return value.type() == type
        ? value
        : value.asType(type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, type, pathNodeKey);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final CASValue otherValue) {
      return Objects.equals(otherValue.value, value) && Objects.equals(otherValue.type, type)
          && otherValue.pathNodeKey == pathNodeKey;
    }
    return false;
  }

  /**
   * Get path node key.
   *
   * @return path node key
   */
  public long getPathNodeKey() {
    return pathNodeKey;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("value", value).add("pathNodeKey", pathNodeKey).toString();
  }

  public Type getType() {
    return type;
  }
}
