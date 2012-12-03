package org.sirix.index.bplustree;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.EnumSet;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.AbstractForwardingPageReadTrx;
import org.sirix.access.MultipleWriteTrx;
import org.sirix.access.Restore;
import org.sirix.access.conf.ResourceConfiguration.Indexes;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.Session;
import org.sirix.cache.Cache;
import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.TransactionLogCache;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.settings.Constants;

/**
 * BPlusTree page write transaction with method implementations specific to
 * writing a BPlusTree.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class BPlusTreePageWriteTrx<K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>>
		extends AbstractForwardingPageReadTrx implements PageWriteTrx<K, V> {

	/** {@link PageReadTrx} for retrieval from persistent storage. */
	private final PageReadTrx mPageReadTrx;

	private RecordPageContainer<S> mRecordPageCon;

	/** Cache to store text value changes in this transaction log. */
	private final Cache<Long, RecordPageContainer<S>> mTextValueLog;

	/** Cache to store attribute value changes in this transaction log. */
	private final Cache<Long, RecordPageContainer<S>> mAttributeValueLog;

	/** Indexes to update. */
	private final EnumSet<Indexes> mIndexes;

	/**
	 * Constructor.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} for retrieval from persistent storage
	 * @throws SirixIOException
	 *           if an I/O error occured
	 */
	private BPlusTreePageWriteTrx(final @Nonnull PageReadTrx pageReadTrx)
			throws SirixIOException {
		mPageReadTrx = pageReadTrx;

		final Session session = mPageReadTrx.getSession();
		mIndexes = session.getResourceConfig().mIndexes;
		if (mIndexes.contains(Indexes.TEXT_VALUE)) {
			mTextValueLog = new TransactionLogCache<>(
					session.getResourceConfig().mPath, mPageReadTrx.getRevisionNumber(),
					"textValue", mPageReadTrx);
		} else {
			mTextValueLog = null;
		}
		if (mIndexes.contains(Indexes.ATTRIBUTE_VALUE)) {
			mAttributeValueLog = new TransactionLogCache<>(
					session.getResourceConfig().mPath, mPageReadTrx.getRevisionNumber(),
					"attributeValue", mPageReadTrx);
		} else {
			mAttributeValueLog = null;
		}
	}

	/**
	 * Get a new instance.
	 * 
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} for persistent storage
	 * @param kind
	 *          kind of page (of subtree root)
	 * @return new {@link BPlusTreePageWriteTrx} instance
	 * @throws SirixIOException
	 *           if an I/O error occured
	 * 
	 * @throws NullPointerException
	 *           if {@code pageWriteTrx} or {@code kind} is {@code null}, S
	 */
	public static <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> BPlusTreePageWriteTrx<K, V, S> getInstance(
			final @Nonnull PageReadTrx pageReadTrx, final @Nonnull PageKind kind)
			throws SirixIOException {
		return new BPlusTreePageWriteTrx<K, V, S>(checkNotNull(pageReadTrx));
	}

	@Override
	public V createEntry(final @Nonnull K key, final @Nonnull V value,
			final @Nonnull PageKind pageKind) throws SirixIOException {
		// Allocate record key and increment record count.
		long entryKey;
		final RevisionRootPage root = mPageReadTrx.getActualRevisionRootPage();
		switch (pageKind) {
		case TEXTVALUEPAGE:
			root.incrementMaxTextValueNodeKey();
			entryKey = root.getMaxTextValueNodeKey();
			break;
		case ATTRIBUTEVALUEPAGE:
			root.incrementMaxAttributeValueNodeKey();
			entryKey = root.getMaxAttributeValueNodeKey();
			break;
		default:
			throw new IllegalStateException();
		}

		final long pageKey = mPageReadTrx.pageKey(entryKey);
		prepareRecordPage(pageKey, pageKind);
		final KeyValuePage<K, V> modified = mRecordPageCon.getModified();
		modified.setEntry(key, value);
		finishEntryModification(key, pageKind);
		return value;
	}

	/**
	 * Prepare record page.
	 * 
	 * @param recordPageKey
	 *          the key of the record page
	 * @param pageKind
	 *          the kind of page (used to determine the right subtree)
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	private void prepareRecordPage(final @Nonnegative long recordPageKey,
			final @Nonnull PageKind pageKind) throws SirixIOException {
		assert recordPageKey >= 0;
		assert pageKind != null;
//		RecordPageContainer<S> cont = getOrderedRecordPageContainer(pageKind,
//				recordPageKey);
//		if (cont.equals(RecordPageContainer.EMPTY_INSTANCE)) {
//			// Indirect reference.
//			final PageReference reference = prepareLeafOfTree(
//					getPageReference(mPageReadTrx.getActualRevisionRootPage(), pageKind),
//					recordPageKey, pageKind);
//			@SuppressWarnings("unchecked")
//			final S recordPage = (S) reference.getPage();
//			if (recordPage == null) {
//				if (reference.getKey() == Constants.NULL_ID) {
//					cont = new RecordPageContainer<>(new UnorderedKeyValuePage(
//							recordPageKey, Constants.UBP_ROOT_REVISION_NUMBER, mPageReadTrx));
//				} else {
//					cont = dereferenceRecordPageForModification(recordPageKey, pageKind);
//				}
//			} else {
//				cont = new RecordPageContainer<>(recordPage);
//			}
//
//			assert cont != null;
//			reference.setKeyValuePageKey(recordPageKey);
//			reference.setPageKind(pageKind);
//
//			switch (pageKind) {
//			case TEXTVALUEPAGE:
//				mTextValueLog.put(recordPageKey, cont);
//				break;
//			case ATTRIBUTEVALUEPAGE:
//				mAttributeValueLog.put(recordPageKey, cont);
//				break;
//			default:
//				throw new IllegalStateException("Page kind not known!");
//			}
//		}
//		mRecordPageCon = cont;
	}

	/**
	 * Get the page reference which points to the right subtree (usual nodes, path
	 * summary nodes, value index nodes).
	 * 
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 * @param pPage
	 *          the page type to determine the right subtree
	 */
	PageReference getPageReference(final @Nonnull RevisionRootPage revisionRoot,
			final @Nonnull PageKind pPage) throws SirixIOException {
		assert revisionRoot != null;
		PageReference ref = null;
//		switch (pPage) {
//		case TEXTVALUEPAGE:
//			ref = getTextValuePage(revisionRoot).getIndirectPageReference();
//			break;
//		case ATTRIBUTEVALUEPAGE:
//			ref = getAttributeValuePage(revisionRoot).getIndirectPageReference();
//			break;
//		default:
//			throw new IllegalStateException(
//					"Only defined for node, path summary, text value and attribute value pages!");
//		}
		return ref;
	}

	@Override
	protected PageReadTrx delegate() {
		return mPageReadTrx;
	}

	@Override
	public void clearCaches() {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeCaches() {
		// TODO Auto-generated method stub

	}

	@Override
	public V prepareEntryForModification(K key, PageKind page)
			throws SirixIOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void finishEntryModification(K key, PageKind pageKind) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeEntry(K key, PageKind pageKind) throws SirixIOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int createNameKey(String name, Kind kind) throws SirixIOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public UberPage commit(MultipleWriteTrx multipleWriteTrx)
			throws SirixException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateDataContainer(
			RecordPageContainer<UnorderedKeyValuePage> nodePageCont, PageKind page) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit(PageReference reference) throws SirixException {
		// TODO Auto-generated method stub

	}

	@Override
	public void restore(Restore restore) {
		// TODO Auto-generated method stub

	}

	@Override
	public PageReadTrx getPageReadTrx() {
		return mPageReadTrx;
	}
}
