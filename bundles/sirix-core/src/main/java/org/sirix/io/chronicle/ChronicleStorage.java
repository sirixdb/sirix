package org.sirix.io.chronicle;

import java.io.File;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;

public final class ChronicleStorage implements Storage {

	/** File name. */
	private static final String FILENAME = "sirix.data";

	/** Instance to storage. */
	private final File mFile;

	/** Byte handler pipeline. */
	private final ByteHandlePipeline mByteHandler;

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
		return new ChronicleReader(getConcreteStorage(), new ByteHandlePipeline(
				mByteHandler));
	}

	@Override
	public Writer getWriter() throws SirixIOException {
		return new ChronicleWriter(getConcreteStorage(), new ByteHandlePipeline(
				mByteHandler));
	}

	@Override
	public void close() {
		// not used over here
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
		return file.exists() && file.length() > 0;
	}

	@Override
	public ByteHandler getByteHandler() {
		return mByteHandler;
	}

}
