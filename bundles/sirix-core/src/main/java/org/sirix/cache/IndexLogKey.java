package org.sirix.cache;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

/**
 * Index log key.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class IndexLogKey {
	/** Unique number. */
	private final int mIndex;

	/** Record page key. */
	private final long mRecordPageKey;

	/**
	 * Constructor.
	 * 
	 * @param pageKind
	 *          the page kind (kind of the index)
	 * @param recordPageKey
	 *          the record page key
	 * @param index
	 *          the index number
	 */
	public IndexLogKey(final long recordPageKey, final @Nonnegative int index) {
		assert recordPageKey >= -1;
		assert index >= 0;
		mRecordPageKey = recordPageKey;
		mIndex = index;
	}

	public long getRecordPageKey() {
		return mRecordPageKey;
	}

	public int getIndex() {
		return mIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mRecordPageKey, mIndex);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof IndexLogKey) {
			final IndexLogKey other = (IndexLogKey) obj;
			return mRecordPageKey == other.mRecordPageKey && mIndex == other.mIndex;
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("recordPageKey", mRecordPageKey)
				.add("index", mIndex).toString();
	}

}
