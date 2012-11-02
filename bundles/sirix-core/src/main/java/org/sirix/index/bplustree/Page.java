package org.sirix.index.bplustree;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.io.ByteArrayDataOutput;

public class Page<K, V extends Record> implements KeyValuePage<K, V>{
	
	private final long mRevision;
	private long mRecordPageKey;
	private HashMap<Object, Object> mRecords;
	private boolean mIsDirty;
	private PageReadTrx mPageReadTrx;
	
	/**
	 * Create record page.
	 * 
	 * @param recordPageKey
	 *          base key assigned to this node page
	 * @param revision
	 *          revision the page belongs to
	 */
	public Page(final @Nonnegative long recordPageKey,
			final @Nonnegative int revision, final @Nonnull PageReadTrx pageReadTrx) {
		// Assertions instead of checkNotNull(...) checks as it's part of the
		// internal flow.
		assert recordPageKey >= 0 : "recordPageKey must not be negative!";
		assert revision >= 0 : "revision must not be negative!";
		assert pageReadTrx != null : "pageReadTrx must not be null!";
		mRevision = revision;
		mRecordPageKey = recordPageKey;
		mRecords = new HashMap<>();
		mIsDirty = true;
		mPageReadTrx = pageReadTrx;
	}

	@Override
	public void serialize(@Nonnull ByteArrayDataOutput out) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getRevision() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public PageReference[] getReferences() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commit(@Nonnull PageWriteTrx pageWriteTrx) throws SirixException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PageReference getReference(@Nonnegative int offset) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isDirty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public org.sirix.page.interfaces.Page setDirty(boolean dirty) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getRecordPageKey() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Record getRecord(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRecord(Record record) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <C extends KeyValuePage<K, V>> C newInstance(long recordPageKey,
			int revision, PageReadTrx pageReadTrx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PageReadTrx getPageReadTrx() {
		// TODO Auto-generated method stub
		return null;
	}
}
