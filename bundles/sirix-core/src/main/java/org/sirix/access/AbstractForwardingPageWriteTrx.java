package org.sirix.access;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.cache.RecordPageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.RecordPageImpl;
import org.sirix.page.UberPage;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingPageWriteTrx extends
		AbstractForwardingPageReadTrx implements PageWriteTrx {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingPageWriteTrx() {
	}

	@Override
	public Record createNode(@Nonnull Record node, @Nonnull PageKind page)
			throws SirixIOException {
		return delegate().createNode(node, page);
	}

	@Override
	public Record prepareNodeForModification(@Nonnegative long nodeKey,
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
	public void updateDataContainer(@Nonnull RecordPageContainer<Long, RecordPageImpl> nodePageContainer,
			@Nonnull PageKind page) {
		delegate().updateDataContainer(nodePageContainer, page);
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
