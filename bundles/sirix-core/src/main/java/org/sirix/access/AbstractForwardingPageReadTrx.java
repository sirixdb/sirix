package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedRecordPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
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
	public Session getSession() {
		return delegate().getSession();
	}

	@Override
	public Optional<? extends Record> getNode(@Nonnegative long key,
			@Nonnull PageKind page) throws SirixIOException {
		return delegate().getNode(key, page);
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
	public RecordPageContainer<UnorderedRecordPage> getNodeFromPage(
			@Nonnegative long key, @Nonnull PageKind page) throws SirixIOException {
		return delegate().getNodeFromPage(key, page);
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
	public Page getFromPageCache(@Nonnegative long key) throws SirixIOException {
		return delegate().getFromPageCache(key);
	}

	@Override
	public void putPageCache(@Nonnull TransactionLogPageCache pageLog) {
		delegate().putPageCache(pageLog);
	}

	@Override
	protected abstract PageReadTrx delegate();
}
