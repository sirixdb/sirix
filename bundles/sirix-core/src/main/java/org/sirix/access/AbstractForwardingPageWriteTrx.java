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
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.base.Optional;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingPageWriteTrx<K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>>
		extends AbstractForwardingPageReadTrx implements PageWriteTrx<K, V, S> {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingPageWriteTrx() {
	}

	@Override
	public void closeCaches() {
		delegate().closeCaches();
	}

	@Override
	public void clearCaches() {
		delegate().clearCaches();
	}

	@Override
	public void close() throws SirixIOException {
		delegate().close();
	}

	@Override
	public V createEntry(@Nonnull K key, @Nonnull V record, @Nonnull PageKind pageKind, @Nonnull Optional<S> keyValuePage)
			throws SirixIOException {
		return delegate().createEntry(key, record, pageKind, keyValuePage);
	}

	@Override
	public V prepareEntryForModification(@Nonnegative K recordKey,
			@Nonnull PageKind pageKind, @Nonnull Optional<S> keyValuePage) throws SirixIOException {
		return delegate().prepareEntryForModification(recordKey, pageKind, keyValuePage);
	}

	@Override
	public void finishEntryModification(@Nonnegative K recordKey,
			@Nonnull PageKind pageKind) {
		delegate().finishEntryModification(recordKey, pageKind);
	}

	@Override
	public void removeEntry(@Nonnegative K recordKey, @Nonnull PageKind pageKind, @Nonnull Optional<S> keyValuePage)
			throws SirixIOException {
		delegate().removeEntry(recordKey, pageKind, keyValuePage);
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
	public void updateDataContainer(
			@Nonnull RecordPageContainer<UnorderedKeyValuePage> recordPageContainer,
			@Nonnull PageKind pageKind) {
		delegate().updateDataContainer(recordPageContainer, pageKind);
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
	protected abstract PageWriteTrx<K, V, S> delegate();

}
