package org.sirix.node;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;

import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.node.interfaces.NodeBase;

/**
 * TextValue which saves the value of a text node.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class TextValue implements Comparable<NodeBase>, NodeBase {
	/** Value in bytes. */
	private final byte[] mValue;

	/** Unique node-key. */
	private final long mNodeKey;

	/** Path node key this text value belongs to (that is the parent path). */
	private long mPathNodeKey;

	/**
	 * Constructor.
	 * 
	 * @param pValue
	 *          the String value in bytes
	 * @param pNodeKey
	 *          the unique node-key
	 * @param pNodeKey
	 *          the path node-key
	 */
	public TextValue(final @Nonnull byte[] pValue,
			final @Nonnegative long pNodeKey, final @Nonnegative long pPathNodeKey) {
		mValue = checkNotNull(pValue);
		checkArgument(pNodeKey >= 0, "pNodeKey must be >= 0!");
		mNodeKey = pNodeKey;
		mPathNodeKey = pPathNodeKey;
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
	public int compareTo(final @Nullable NodeBase pOther) {
		final TextValue value = (TextValue) pOther;
		return new String(mValue).compareTo(new String(value.mValue));
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(mValue);
	}

	@Override
	public boolean equals(final @Nullable Object pObj) {
		if (pObj instanceof TextValue) {
			final TextValue otherValue = (TextValue) pObj;
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
		return Kind.TEXT_VALUE;
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
