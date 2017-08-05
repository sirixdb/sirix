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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.IndexedChronicle;

public final class ChronicleReader implements Reader {

	/** Beacon of the other references. */
	final static int OTHER_BEACON = 4;

	private final Chronicle mChronicle;

	final ByteHandler mByteHandler;

	private final Excerpt mExcerpt;

	public ChronicleReader(final File file, final ByteHandler handler)
			throws IOException {
		mChronicle = new IndexedChronicle(file.getAbsolutePath());
		mByteHandler = checkNotNull(handler);
		mExcerpt = mChronicle.createExcerpt();
	}

	@Override
	public PageReference readUberPageReference() throws SirixIOException {
		final PageReference uberPageReference = new PageReference();
		// Read primary beacon.
		final long lastIndex = mExcerpt.size() - 1;
		uberPageReference.setKey(lastIndex);
		final UberPage page = (UberPage) read(lastIndex, null);
		uberPageReference.setPage(page);
		return uberPageReference;
	}

	@Override
	public Page read(long key, @Nullable PageReadTrx pageReadTrx)
			throws SirixIOException {
		try {
			// Read page from excerpt.
			final boolean opened = mExcerpt.index(key);
			assert opened : "Index couldn't be opened!";
			final int dataLength = mExcerpt.readInt();
			final byte[] page = new byte[dataLength];
			mExcerpt.read(page);
			mExcerpt.finish();

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
		try {
			mExcerpt.close();
			mChronicle.close();
		} catch (final IOException e) {
			throw new SirixIOException(e.getCause());
		}
	}
}
