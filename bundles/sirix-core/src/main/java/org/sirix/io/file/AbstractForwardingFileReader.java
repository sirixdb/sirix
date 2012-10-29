package org.sirix.io.file;

import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractForwardingFileReader extends ForwardingObject implements Reader {

	/** Constructor for use by subclasses. */
	protected AbstractForwardingFileReader() {
	}
	
	@Override
	public Page read(long key, @Nullable PageReadTrx pageReadTrx) throws SirixIOException {
		return delegate().read(key, pageReadTrx);
	}
	
	@Override
	public PageReference readFirstReference() throws SirixIOException {
		return delegate().readFirstReference();
	}
	
	@Override
	protected abstract Reader delegate();
}
