package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.settings.Constants;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * TextValue which saves the value of a text node.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class Value implements Comparable<Value> {
	/** Value in bytes. */
	private final byte[] mValue;

	/** Path node key this text value belongs to (that is the parent path). */
	private long mPathNodeKey;

	/** Kind of value. */
	private ValueKind mKind;

	/**
	 * Constructor.
	 * 
	 * @param value
	 *          the String value in bytes
	 * @param nodeKey
	 *          the unique node-key
	 * @param pathNodeKey
	 *          the path node-key
	 * @param kind
	 *          value kind (attribute/text)
	 */
	public Value(final @Nonnull byte[] value,
			final @Nonnegative long pathNodeKey, final @Nonnull ValueKind kind) {
		mValue = checkNotNull(value);
		mPathNodeKey = pathNodeKey;
		mKind = checkNotNull(kind);
	}

	/**
	 * Get the kind of value (text/attribute).
	 * 
	 * @return kind of value
	 */
	public ValueKind getKind() {
		return mKind;
	}

	/**
	 * Get the value.
	 * 
	 * @return the value
	 */
	public byte[] getValue() {
		return mValue;
	}

	@Override
	public int compareTo(final @Nullable Value other) {
		final Value value = (Value) other;
		return ComparisonChain
				.start()
				.compare(new String(mValue, Constants.DEFAULT_ENCODING),
						new String(value.mValue, Constants.DEFAULT_ENCODING))
				.compare((Long) mPathNodeKey, (Long) value.mPathNodeKey).result();
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(mValue);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof Value) {
			final Value otherValue = (Value) obj;
			return Arrays.equals(otherValue.mValue, mValue);
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
		return Objects.toStringHelper(this).add("value", new String(mValue))
				.toString();
	}
}
