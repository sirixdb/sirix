package org.sirix.index.avltree.keyvalue;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.Type;
import org.sirix.exception.SirixException;
import org.sirix.index.AtomicUtil;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

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
  private final Atomic mValue;

  /** Path node key this text value belongs to (that is the parent path). */
  private final long mPathNodeKey;

  /** Type of value. */
  private final Type mType;

  /**
   * Constructor.
   *
   * @param value the atomic value
   * @param type the type of the value
   * @param pathNodeKey the path node-key
   */
  public CASValue(final Atomic value, final Type type, final @Nonnegative long pathNodeKey) {
    mValue = value;
    mType = type;
    mPathNodeKey = pathNodeKey;
  }

  /**
   * Get the value.
   *
   * @return the value
   */
  public byte[] getValue() {
    if (mValue == null || mType == null) {
      return null;
    }
    byte[] retVal = new byte[1];
    try {
      retVal = AtomicUtil.toBytes(mValue, mType);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return retVal;
  }

  public Atomic getAtomicValue() {
    if (mValue == null || mType == null) {
      return null;
    }
    try {
      return mValue;
    } catch (final QueryException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public int compareTo(final @Nullable CASValue other) {
    final CASValue otherValue = other;
    Atomic thisAtomic = mValue != null && mType != null ? mValue.asType(mType) : null;
    Atomic otherAtomic =
        otherValue.mValue != null && otherValue.mType != null ? otherValue.mValue.asType(otherValue.mType) : null;

    return ComparisonChain.start()
                          .compare(mPathNodeKey, otherValue.mPathNodeKey)
                          .compare(thisAtomic, otherAtomic)
                          .result();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mValue, mType, mPathNodeKey);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof CASValue) {
      final CASValue otherValue = (CASValue) obj;
      return Objects.equal(otherValue.mValue, mValue) && Objects.equal(otherValue.mType, mType)
          && otherValue.mPathNodeKey == mPathNodeKey;
    }
    return false;
  }

  /**
   * Get path node key.
   *
   * @return path node key
   */
  public long getPathNodeKey() {
    return mPathNodeKey;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("value", mValue)
                      .add("pathNodeKey", mPathNodeKey)
                      .toString();
  }

  public Type getType() {
    return mType;
  }
}
