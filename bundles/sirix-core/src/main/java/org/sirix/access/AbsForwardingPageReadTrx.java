package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.EPage;
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
public abstract class AbsForwardingPageReadTrx extends ForwardingObject
		implements PageReadTrx {

	/** Constructor for use by subclasses. */
	protected AbsForwardingPageReadTrx() {
	}

	@Override
	public Session getSession() {
		return delegate().getSession();
	}

	@Override
	public Optional<? extends NodeBase> getNode(@Nonnegative long pKey,
			@Nonnull EPage pPage) throws SirixIOException {
		return delegate().getNode(pKey, pPage);
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() throws SirixIOException {
		return delegate().getActualRevisionRootPage();
	}

	@Override
	public String getName(int pNameKey, @Nonnull Kind pKind) {
		return delegate().getName(pNameKey, pKind);
	}

	@Override
	public int getNameCount(int pNameKey, @Nonnull Kind pKind) {
		return delegate().getNameCount(pNameKey, pKind);
	}

	@Override
	public byte[] getRawName(int pNameKey, @Nonnull Kind pKind) {
		return delegate().getRawName(pNameKey, pKind);
	}

	@Override
	public void close() throws SirixIOException {
		delegate().close();
	}

	@Override
	public PageContainer getNodeFromPage(@Nonnegative long pKey,
			@Nonnull EPage pPage) throws SirixIOException {
		return delegate().getNodeFromPage(pKey, pPage);
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
	public Page getFromPageCache(@Nonnegative long pKey) throws SirixIOException {
		return delegate().getFromPageCache(pKey);
	}

	@Override
	public void putPageCache(@Nonnull TransactionLogPageCache pPageLog) {
		delegate().putPageCache(pPageLog);
	}

	@Override
	protected abstract PageReadTrx delegate();
}
