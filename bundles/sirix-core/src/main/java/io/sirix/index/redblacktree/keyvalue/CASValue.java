package io.sirix.index.redblacktree.keyvalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import io.sirix.index.AtomicUtil;
import io.sirix.utils.LogWrapper;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.Type;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
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
  public CASValue(final Atomic value, final Type type, final @NonNegative long pathNodeKey) {
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
    byte[] retVal = new byte[1];
    try {
      retVal = AtomicUtil.toBytes(value, type);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return retVal;
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

    Atomic thisAtomic = value.asType(type);
    Atomic otherAtomic = other.value.asType(other.type);

    return ComparisonChain.start().compare(pathNodeKey, other.pathNodeKey).compare(thisAtomic, otherAtomic).result();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value, type, pathNodeKey);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final CASValue otherValue) {
      return Objects.equal(otherValue.value, value) && Objects.equal(otherValue.type, type)
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
    return MoreObjects.toStringHelper(this).add("value", value).add("pathNodeKey", pathNodeKey).toString();
  }

  public Type getType() {
    return type;
  }
}
