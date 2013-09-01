package org.sirix.io.chronicle;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersistenter;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

public final class ChronicleReader implements Reader {

	/** Beacon of the other references. */
	final static int OTHER_BEACON = 4;

	private final Chronicle mChronicle;
	final ByteHandler mByteHandler;

	public ChronicleReader(final File concreteStorage, final ByteHandler handler)
			throws SirixIOException {
		try {
			if (!concreteStorage.exists()) {
				concreteStorage.getParentFile().mkdirs();
				concreteStorage.createNewFile();
			}

			mChronicle = new IndexedChronicle(concreteStorage.getAbsolutePath());
			mByteHandler = checkNotNull(handler);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public PageReference readFirstReference() throws SirixIOException {
		final PageReference uberPageReference = new PageReference();
		// Read primary beacon.
		final Excerpt excerpt = mChronicle.createExcerpt();
		final long lastIndex = excerpt.size() - 1;
		final boolean indexSet = excerpt.index(lastIndex);
		assert indexSet : "Index couldn't be set to last index!";
		uberPageReference.setKey(lastIndex);
		excerpt.close();
		final UberPage page = (UberPage) read(uberPageReference.getKey(), null);
		uberPageReference.setPage(page);
		return uberPageReference;
	}

	@Override
	public Page read(long key, @Nullable PageReadTrx pageReadTrx)
			throws SirixIOException {
		try {
			// Read page from excerpt.
			final Excerpt excerpt = mChronicle.createExcerpt();
			final boolean opened = excerpt.index(key);
			assert opened : "Index couldn't be opened!";
			final int dataLength = excerpt.readInt();
			final byte[] page = new byte[dataLength];
			excerpt.read(page);
			excerpt.finish();

			// Perform byte operations.
			final DataInputStream input = new DataInputStream(
					mByteHandler.deserialize(new ByteArrayInputStream(page)));

			// Return reader required to instantiate and deserialize page.
			return PagePersistenter.deserializePage(input, pageReadTrx);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public void close() throws SirixIOException {
		mChronicle.close();
	}
}
