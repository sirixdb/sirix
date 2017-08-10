package org.sirix.io;

import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingReader extends ForwardingObject implements Reader {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingReader() {}

	@Override
	public Page read(long key, @Nullable PageReadTrx pageReadTrx) throws SirixIOException {
		return delegate().read(key, pageReadTrx);
	}

	@Override
	public PageReference readUberPageReference() throws SirixIOException {
		return delegate().readUberPageReference();
	}

	@Override
	protected abstract Reader delegate();
}
