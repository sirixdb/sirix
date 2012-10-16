package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.Session;
import org.sirix.cache.NodePageContainer;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.PageKind;
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
	public Optional<? extends NodeBase> getNode(@Nonnegative long key,
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
	public NodePageContainer getNodeFromPage(@Nonnegative long key,
			@Nonnull PageKind page) throws SirixIOException {
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
