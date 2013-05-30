package org.sirix.cache;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.page.PageKind;

import com.google.common.base.Objects;

/**
 * Log key.
 * 
 * @author johannes
 * 
 */
public final class IndirectPageLogKey {
	private int mLevel;
	private int mOffset;
	private PageKind mPageKind;
	private int mIndex;

	public IndirectPageLogKey(final @Nonnull PageKind pageKind, final int index,
			final int level, final @Nonnegative int offset) {
		assert level >= -1;
		assert offset >= 0;
		assert pageKind != null;
		mPageKind = pageKind;
		mIndex = index;
		mLevel = level;
		mOffset = offset;
	}

	public int getLevel() {
		return mLevel;
	}

	public int getOffset() {
		return mOffset;
	}

	public PageKind getPageKind() {
		return mPageKind;
	}

	public int getIndex() {
		return mIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mPageKind, mIndex, mLevel, mOffset);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof IndirectPageLogKey) {
			final IndirectPageLogKey other = (IndirectPageLogKey) obj;
			return mPageKind == other.mPageKind && mIndex == other.mIndex
					&& mLevel == other.mLevel && mOffset == other.mOffset;
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("pageKind", mPageKind)
				.add("index", mIndex).add("level", mLevel).add("offset", mOffset)
				.toString();
	}

}
