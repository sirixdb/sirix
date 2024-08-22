package io.sirix.cache;

import io.sirix.index.IndexType;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * Index log key.
 *
 * @author Johannes Lichtenberger
 */
public final class IndexLogKey {
	private IndexType indexType;
	private long recordPageKey;
	private @NonNegative int indexNumber;
	private @NonNegative int revisionNumber;

	public IndexLogKey(IndexType indexType, long recordPageKey, @NonNegative int indexNumber,
			@NonNegative int revisionNumber) {
		this.indexType = indexType;
		this.recordPageKey = recordPageKey;
		this.indexNumber = indexNumber;
		this.revisionNumber = revisionNumber;
	}

	public IndexLogKey setIndexType(IndexType indexType) {
		this.indexType = indexType;
		return this;
	}

	public IndexLogKey setRecordPageKey(long recordPageKey) {
		this.recordPageKey = recordPageKey;
		return this;
	}

	public IndexLogKey setIndexNumber(int indexNumber) {
		this.indexNumber = indexNumber;
		return this;
	}

	public IndexLogKey setRevisionNumber(int revisionNumber) {
		this.revisionNumber = revisionNumber;
		return this;
	}

	private int hash;

	@Override
	public int hashCode() {
		if (hash == 0) {
			hash = indexType.getID() + Long.hashCode(recordPageKey) + indexNumber + revisionNumber;
		}
		return hash;
	}

	public long getRecordPageKey() {
		return recordPageKey;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public int getIndexNumber() {
		return indexNumber;
	}

	public int getRevisionNumber() {
		return revisionNumber;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || obj.getClass() != this.getClass())
			return false;
		var that = (IndexLogKey) obj;
		return this.indexType == that.indexType && this.recordPageKey == that.recordPageKey
				&& this.indexNumber == that.indexNumber && this.revisionNumber == that.revisionNumber;
	}

	@Override
	public String toString() {
		return "IndexLogKey[" + "indexType=" + indexType + ", " + "recordPageKey=" + recordPageKey + ", "
				+ "indexNumber=" + indexNumber + ", " + "revisionNumber=" + revisionNumber + ']';
	}

}
