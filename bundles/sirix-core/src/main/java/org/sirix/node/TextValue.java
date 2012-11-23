package org.sirix.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.node.interfaces.Record;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * TextValue which saves the value of a text node.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class TextValue implements Comparable<Record>, Record {
	/** Value in bytes. */
	private final byte[] mValue;

	/** Unique node-key. */
	private final long mNodeKey;

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
	public TextValue(final @Nonnull byte[] value,
			final @Nonnegative long nodeKey, final @Nonnegative long pathNodeKey,
			final @Nonnull ValueKind kind) {
		mValue = checkNotNull(value);
		checkArgument(nodeKey >= 0, "pNodeKey must be >= 0!");
		mNodeKey = nodeKey;
		mPathNodeKey = pathNodeKey;
		mKind = checkNotNull(kind);
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
	public int compareTo(final @Nullable Record other) {
		final TextValue value = (TextValue) other;
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
		if (obj instanceof TextValue) {
			final TextValue otherValue = (TextValue) obj;
			return Arrays.equals(otherValue.mValue, mValue);
		}
		return false;
	}

	@Override
	public long getNodeKey() {
		return mNodeKey;
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
	public Kind getKind() {
		return mKind.getKind();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("nodeKey", mNodeKey)
				.add("value", new String(mValue)).toString();
	}

	@Override
	public long getRevision() {
		return -1; // Not needed over here
	}
}
