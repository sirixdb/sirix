package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageReadTrx;
import org.sirix.api.ISession;
import org.sirix.cache.BerkeleyPersistencePageCache;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixIOException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.IPage;

import com.google.common.base.Optional;
import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingPageReadTrx extends ForwardingObject
		implements IPageReadTrx {

	/** Constructor for use by subclasses. */
	protected AbsForwardingPageReadTrx() {
	}

	@Override
	public ISession getSession() {
		return delegate().getSession();
	}

	@Override
	public Optional<? extends INodeBase> getNode(@Nonnegative long pKey,
			@Nonnull EPage pPage) throws SirixIOException {
		return delegate().getNode(pKey, pPage);
	}

	@Override
	public RevisionRootPage getActualRevisionRootPage() throws SirixIOException {
		return delegate().getActualRevisionRootPage();
	}

	@Override
	public String getName(int pNameKey, @Nonnull EKind pKind) {
		return delegate().getName(pNameKey, pKind);
	}

	@Override
	public int getNameCount(int pNameKey, @Nonnull EKind pKind) {
		return delegate().getNameCount(pNameKey, pKind);
	}

	@Override
	public byte[] getRawName(int pNameKey, @Nonnull EKind pKind) {
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
	public IPage getFromPageCache(@Nonnegative long pKey) throws SirixIOException {
		return delegate().getFromPageCache(pKey);
	}

	@Override
	public void putPageCache(@Nonnull BerkeleyPersistencePageCache pPageLog) {
		delegate().putPageCache(pPageLog);
	}

	@Override
	protected abstract IPageReadTrx delegate();
}
