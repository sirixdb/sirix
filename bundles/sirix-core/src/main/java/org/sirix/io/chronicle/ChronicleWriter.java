package org.sirix.io.chronicle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersistenter;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

import com.google.common.io.ByteStreams;
import com.higherfrequencytrading.chronicle.Chronicle;
import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

public final class ChronicleWriter extends AbstractForwardingReader implements
		Writer {

	private final ChronicleReader mReader;
	private final Chronicle mChronicle;
	private final Excerpt mExcerpt;

	/**
	 * Constructor.
	 * 
	 * @param storage
	 *          the concrete storage
	 * @param handler
	 *          the byte handler
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public ChronicleWriter(final File storage, final ByteHandler handler)
			throws SirixIOException {
		try {
			mChronicle = new IndexedChronicle(storage.getAbsolutePath());
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
		mReader = new ChronicleReader(storage, handler);
		mExcerpt = mChronicle.createExcerpt();
	}

	@Override
	public void close() throws SirixIOException {
		mReader.close();
		mChronicle.close();
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
			final long index = mExcerpt.index();
			assert index != -1 : "Index nr. not valid!";
			mExcerpt.finish();

			// Remember page coordinates.
			pageReference.setKey(index);
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public void writeFirstReference(final PageReference pageReference)
			throws SirixIOException {
		write(pageReference);
	}

	@Override
	protected Reader delegate() {
		return mReader;
	}
}
