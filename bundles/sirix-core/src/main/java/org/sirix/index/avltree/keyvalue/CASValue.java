package org.sirix.index.avltree.keyvalue;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Type;
import org.sirix.exception.SirixException;
import org.sirix.index.AtomicUtil;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Value representing a text value, attribute value, element QName or any other
 * byte encoded value.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class CASValue implements Comparable<CASValue> {

	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(CASValue.class));

	/** Atomic value. */
	private final Atomic mValue;

	/** Path node key this text value belongs to (that is the parent path). */
	private final long mPathNodeKey;

	/** Type of value. */
	private final Type mType;

	/**
	 * Constructor.
	 * 
	 * @param value
	 *          the String value in bytes
	 * @param nodeKey
	 *          the unique node-key
	 * @param pathNodeKey
	 *          the path node-key
	 */
	public CASValue(final Atomic value, final Type type,
			final @Nonnegative long pathNodeKey) {
		mValue = checkNotNull(value);
		mType = checkNotNull(type);
		mType.atomicCode();
		mPathNodeKey = pathNodeKey;
	}

	/**
	 * Get the value.
	 * 
	 * @return the value
	 */
	public byte[] getValue() {
		byte[] retVal = new byte[1];
		try {
			retVal = AtomicUtil.toBytes(mValue, mType);
		} catch (final SirixException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return retVal;
	}
	
	public Atomic getAtomicValue() {
		try {
			return mValue.asType(mType);
		} catch (final QueryException e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}
	}

	@Override
	public int compareTo(final @Nullable CASValue other) {
		final CASValue otherValue = (CASValue) other;
		Atomic thisAtomic = null;
		Atomic otherAtomic = null;
		try {
			thisAtomic = mValue.asType(mType);
			otherAtomic = otherValue.mValue.asType(otherValue.mType);
		} catch (final QueryException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return ComparisonChain.start().compare(thisAtomic, otherAtomic)
				.compare((Long) mPathNodeKey, (Long) otherValue.mPathNodeKey).result();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mValue, mType, mPathNodeKey);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof CASValue) {
			final CASValue otherValue = (CASValue) obj;
			return Objects.equal(otherValue.mValue, mValue)
					&& Objects.equal(otherValue.mType, mType)
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
		return Objects.toStringHelper(this).add("value", mValue)
				.add("pathNodeKey", mPathNodeKey).toString();
	}

	public Type getType() {
		return mType;
	}
}
