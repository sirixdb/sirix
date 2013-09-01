package org.sirix.io.chronicle;

import java.io.File;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;

/**
 * Chronicle storage.
 * 
 * @author Johannes Lichtenberger
 *
 */
public final class ChronicleStorage implements Storage {

	/** File name. */
	private static final String FILENAME = "sirix.data";

	/** Instance to storage. */
	private final File mFile;

	/** Byte handler pipeline. */
	private final ByteHandlePipeline mByteHandler;
	
	/** Reading from the storage. */
	private Reader mReader;
	
	/** Writing to the storage. */
	private Writer mWriter;

	/**
	 * Constructor.
	 * 
	 * @param file
	 *          the location of the database
	 * @param byteHandler
	 *          byte handler pipeline
	 */
	public ChronicleStorage(final ResourceConfiguration resourceConfig) {
		assert resourceConfig != null : "resourceConfig must not be null!";
		mFile = resourceConfig.mPath;
		mByteHandler = resourceConfig.mByteHandler;
	}

	@Override
	public Reader getReader() throws SirixIOException {
		mReader = new ChronicleReader(getConcreteStorage(), new ByteHandlePipeline(
				mByteHandler));
		return mReader;
	}

	@Override
	public Writer getWriter() throws SirixIOException {
		mWriter = new ChronicleWriter(getConcreteStorage(), new ByteHandlePipeline(
				mByteHandler));
		return mWriter;
	}

	@Override
	public void close() throws SirixIOException {
		if (mReader != null)
			mReader.close();
		if (mWriter != null)
			mWriter.close();
	}

	/**
	 * Getting concrete storage for this file.
	 * 
	 * @return the concrete storage for this database
	 */
	private File getConcreteStorage() {
		return new File(mFile, new StringBuilder(ResourceConfiguration.Paths.DATA
				.getFile().getName()).append(File.separator).append(FILENAME)
				.toString());
	}

	@Override
	public boolean exists() throws SirixIOException {
		final File file = getConcreteStorage();
		return file.exists();
	}

	@Override
	public ByteHandler getByteHandler() {
		return mByteHandler;
	}
}
