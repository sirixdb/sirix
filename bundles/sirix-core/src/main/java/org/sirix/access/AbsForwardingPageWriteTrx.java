package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsForwardingPageWriteTrx extends
		AbsForwardingPageReadTrx implements IPageWriteTrx {

	/** Constructor for use by subclasses. */
	protected AbsForwardingPageWriteTrx() {
	}

	@Override
	public INodeBase createNode(@Nonnull INodeBase pNode, @Nonnull EPage pPage)
			throws SirixIOException {
		return delegate().createNode(pNode, pPage);
	}

	@Override
	public INodeBase prepareNodeForModification(@Nonnegative long pNodeKey,
			@Nonnull EPage pPage) throws SirixIOException {
		return delegate().prepareNodeForModification(pNodeKey, pPage);
	}

	@Override
	public void finishNodeModification(@Nonnegative long pNode,
			@Nonnull EPage pPage) {
		delegate().finishNodeModification(pNode, pPage);
	}

	@Override
	public void removeNode(@Nonnegative long pNode, @Nonnull EPage pPage)
			throws SirixIOException {
		delegate().removeNode(pNode, pPage);
	}

	@Override
	public int createNameKey(@Nonnull String pName, @Nonnull EKind pKind)
			throws SirixIOException {
		return delegate().createNameKey(pName, pKind);
	}

	@Override
	public UberPage commit(@Nonnull EMultipleWriteTrx pMultipleWriteTrx)
			throws SirixException {
		return delegate().commit(pMultipleWriteTrx);
	}

	@Override
	public void updateDateContainer(@Nonnull PageContainer pNodePageCont,
			@Nonnull EPage pPage) {
		delegate().updateDateContainer(pNodePageCont, pPage);
	}

	@Override
	public void commit(@Nonnull PageReference pReference) throws SirixException {
		delegate().commit(pReference);
	}

	@Override
	public void restore(@Nonnull ERestore pRestore) {
		delegate().restore(pRestore);
	}

	@Override
	protected abstract IPageWriteTrx delegate();

}
