package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.CASPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.PathPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

import com.google.common.base.Optional;
import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingPageReadTrx extends ForwardingObject
		implements PageReadTrx {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingPageReadTrx() {
	}

	@Override
	public void clearCaches() {
		delegate().clearCaches();
	}

	@Override
	public void closeCaches() {
		delegate().closeCaches();
	}

	@Override
	public PageReference getPageReferenceForPage(
			@Nonnull PageReference startReference, @Nonnegative long pageKey,
			int index, @Nonnull PageKind pageKind) throws SirixIOException {
		return delegate().getPageReferenceForPage(startReference, pageKey, index,
				pageKind);
	}

	@Override
	public Session getSession() {
		return delegate().getSession();
	}

	@Override
	public CASPage getCASPage(
			@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		return delegate().getCASPage(revisionRoot);
	}

	@Override
	public NamePage getNamePage(@Nonnull RevisionRootPage revisionRoot)
			throws SirixIOException {
		return delegate().getNamePage(revisionRoot);
	}

	@Override
	public PathSummaryPage getPathSummaryPage(
			@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
		return delegate().getPathSummaryPage(revisionRoot);
	}

	@Override
	public PathPage getPathPage(@Nonnull RevisionRootPage revisionRoot)
			throws SirixIOException {
		return delegate().getPathPage(revisionRoot);
	}

	@Override
	public Optional<? extends Record> getRecord(@Nonnegative long key,
			@Nonnull PageKind page, @Nonnegative int index) throws SirixIOException {
		return delegate().getRecord(key, page, index);
	}

	@Override
	public long pageKey(@Nonnegative long recordKey) {
		return delegate().pageKey(recordKey);
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() throws SirixIOException {
		return delegate().getActualRevisionRootPage();
	}

	@Override
	public String getName(int nameKey, @Nonnull Kind kind) {
		return delegate().getName(nameKey, kind);
	}

	@Override
	public int getNameCount(int nameKey, @Nonnull Kind kind) {
		return delegate().getNameCount(nameKey, kind);
	}

	@Override
	public byte[] getRawName(int nameKey, @Nonnull Kind kind) {
		return delegate().getRawName(nameKey, kind);
	}

	@Override
	public void close() throws SirixIOException {
		delegate().close();
	}

	@Override
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> RecordPageContainer<S> getRecordPageContainer(
			@Nonnull @Nonnegative Long key, int index, @Nonnull PageKind pageKind)
			throws SirixIOException {
		return delegate().<K, V, S> getRecordPageContainer(key, index, pageKind);
	}

	@Override
	public UberPage getUberPage() {
		return delegate().getUberPage();
	}

	@Override
	public boolean isClosed() {
		return delegate().isClosed();
	}

	@Override
	public int getRevisionNumber() {
		return delegate().getRevisionNumber();
	}

	@Override
	public Page getFromPageCache(@Nonnull PageReference reference)
			throws SirixIOException {
		return delegate().getFromPageCache(reference);
	}

	@Override
	public void putPageCache(@Nonnull TransactionLogPageCache pageLog) {
		delegate().putPageCache(pageLog);
	}

	@Override
	public Reader getReader() {
		return delegate().getReader();
	}

	@Override
	protected abstract PageReadTrx delegate();
}
