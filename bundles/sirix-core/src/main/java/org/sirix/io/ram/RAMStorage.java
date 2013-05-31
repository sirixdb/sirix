package org.sirix.io.ram;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

/**
 * In memory storage.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class RAMStorage implements Storage {

	/** Storage, mapping a resource to the pageKey/page mapping. */
	private final Map<String, Map<Long, Page>> mStorage;

	/** Mapping pageKey to the page. */
	private final Map<Long, Page> mResourceStorage;

	/** {@link ByteHandlePipeline} reference. */
	private final ByteHandlePipeline mHandler;

	/** {@link RAMAccess} reference. */
	private final RAMAccess mAccess;

	/** Determines if the storage already exists or not. */
	private final boolean mExists;

	/** The unique page key. */
	private long mPageKey;

	/**
	 * Constructor
	 * 
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public RAMStorage(final ResourceConfiguration resourceConfig) {
		mStorage = new ConcurrentHashMap<String, Map<Long, Page>>();
		mHandler = resourceConfig.mByteHandler;
		final String resource = resourceConfig.getResource().getName();
		final Map<Long, Page> resourceStorage = mStorage.get(resource);
		if (resourceStorage == null) {
			mResourceStorage = new ConcurrentHashMap<Long, Page>();
			mStorage.put(resource, mResourceStorage);
			mExists = false;
		} else {
			mResourceStorage = resourceStorage;
			mExists = true;
		}
		mAccess = new RAMAccess();
	}

	@Override
	public Writer getWriter() throws SirixIOException {
		return mAccess;
	}

	@Override
	public Reader getReader() throws SirixIOException {
		return mAccess;
	}

	@Override
	public void close() throws SirixIOException {
	}

	@Override
	public ByteHandlePipeline getByteHandler() {
		return mHandler;
	}

	@Override
	public boolean exists() throws SirixIOException {
		return mExists;
	}

	/** Provides RAM access. */
	public class RAMAccess implements Writer {
		@Override
		public Page read(long key, @Nullable PageReadTrx pageReadTrx) {
			return mResourceStorage.get(key);
		}

		@Override
		public PageReference readFirstReference() {
			final Page page = mResourceStorage.get(new Long(-1));
			final PageReference uberPageReference = new PageReference();
			uberPageReference.setKey(-1);
			uberPageReference.setPage(page);
			return uberPageReference;
		}

		@Override
		public void write(final PageReference pageReference)
				throws SirixIOException {
			final Page page = pageReference.getPage();
			pageReference.setKey(mPageKey);
			mResourceStorage.put(mPageKey++, page);
		}

		@Override
		public void writeFirstReference(final PageReference pageReference)
				throws SirixIOException {
			final Page page = pageReference.getPage();
			pageReference.setKey(-1);
			mResourceStorage.put(-1l, page);
		}

		@Override
		public void close() throws SirixIOException {
		}
	}
}
