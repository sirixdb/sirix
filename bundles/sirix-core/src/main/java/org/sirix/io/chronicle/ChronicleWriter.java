package org.sirix.io.chronicle;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.IndexedChronicle;

import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersistenter;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

public final class ChronicleWriter extends AbstractForwardingReader implements
		Writer {

	private final ChronicleReader mReader;
	private final ExcerptAppender mExcerpt;
	private final IndexedChronicle mChronicle;

	/**
	 * Constructor.
	 * 
	 * @param storage
	 *          the concrete storage
	 * @param handler
	 *          the byte handler
	 * @throws IOException
	 *           if an I/O error occurs
	 */
	public ChronicleWriter(final File file, final ByteHandler handler)
			throws IOException {
		mReader = new ChronicleReader(file, handler);
		mChronicle = new IndexedChronicle(file.getAbsolutePath());
		mExcerpt = mChronicle.createAppender();
	}

	@Override
	public void close() throws SirixIOException {
		try {
			mExcerpt.close();
			mChronicle.close();
			mReader.close();
		} catch (final IOException e) {
			throw new SirixIOException(e.getCause());
		}
	}

	@Override
	public void write(final PageReference pageReference) throws SirixIOException {
		// Perform byte operations.
		try {
			// Serialize page.
			final Page page = pageReference.getPage();
			assert page != null;
			final ByteArrayOutputStream output = new ByteArrayOutputStream();
			final DataOutputStream dataOutput = new DataOutputStream(
					mReader.mByteHandler.serialize(output));
			PagePersistenter.serializePage(dataOutput, page);
			output.flush();
			output.close();
			dataOutput.close();

			final byte[] serializedPage = output.toByteArray();

			mExcerpt.startExcerpt(serializedPage.length
					+ ChronicleReader.OTHER_BEACON);
			mExcerpt.writeInt(serializedPage.length);
			mExcerpt.write(serializedPage);
			mExcerpt.finish();
			final long index = mExcerpt.index() - 1;
			assert index != -1 : "Index nr. not valid!";

			// Remember page coordinates.
			pageReference.setKey(index);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public void writeUberPageReference(final PageReference pageReference)
			throws SirixIOException {
		write(pageReference);
	}

	@Override
	protected Reader delegate() {
		return mReader;
	}
}
