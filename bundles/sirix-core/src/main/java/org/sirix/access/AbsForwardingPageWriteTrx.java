package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.cache.NodePageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingPageWriteTrx extends
		AbsForwardingPageReadTrx implements PageWriteTrx {

	/** Constructor for use by subclasses. */
	protected AbsForwardingPageWriteTrx() {
	}

	@Override
	public NodeBase createNode(@Nonnull NodeBase node, @Nonnull PageKind page)
			throws SirixIOException {
		return delegate().createNode(node, page);
	}

	@Override
	public NodeBase prepareNodeForModification(@Nonnegative long nodeKey,
			@Nonnull PageKind page) throws SirixIOException {
		return delegate().prepareNodeForModification(nodeKey, page);
	}

	@Override
	public void finishNodeModification(@Nonnegative long nodeKey,
			@Nonnull PageKind page) {
		delegate().finishNodeModification(nodeKey, page);
	}

	@Override
	public void removeNode(@Nonnegative long nodeKey, @Nonnull PageKind page)
			throws SirixIOException {
		delegate().removeNode(nodeKey, page);
	}

	@Override
	public int createNameKey(@Nonnull String name, @Nonnull Kind kind)
			throws SirixIOException {
		return delegate().createNameKey(name, kind);
	}

	@Override
	public UberPage commit(@Nonnull MultipleWriteTrx multipleWriteTrx)
			throws SirixException {
		return delegate().commit(multipleWriteTrx);
	}

	@Override
	public void updateDateContainer(@Nonnull NodePageContainer nodePageContainer,
			@Nonnull PageKind page) {
		delegate().updateDateContainer(nodePageContainer, page);
	}

	@Override
	public void commit(@Nonnull PageReference reference) throws SirixException {
		delegate().commit(reference);
	}

	@Override
	public void restore(@Nonnull Restore restore) {
		delegate().restore(restore);
	}

	@Override
	protected abstract PageWriteTrx delegate();

}
