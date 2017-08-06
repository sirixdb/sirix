package org.sirix.io.chronicle;

import java.io.File;
import java.io.IOException;

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

	/**
	 * Constructor.
	 * 
	 * @param file
	 *          the location of the database
	 * @param byteHandler
	 *          byte handler pipeline
	 * @throws SirixIOException
	 */
	public ChronicleStorage(final ResourceConfiguration resourceConfig)
			throws SirixIOException {
		assert resourceConfig != null : "resourceConfig must not be null!";
		mFile = resourceConfig.mPath;
		mByteHandler = resourceConfig.mByteHandler;
	}

	@Override
	public Reader createReader() throws SirixIOException {
		try {
			final File concreteStorage = getConcreteStorage();

			if (!concreteStorage.exists()) {
				concreteStorage.getParentFile().mkdirs();
				concreteStorage.createNewFile();
			}

			return new ChronicleReader(concreteStorage, new ByteHandlePipeline(
					mByteHandler));
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public Writer createWriter() throws SirixIOException {
		try {
			final File concreteStorage = getConcreteStorage();

			if (!concreteStorage.exists()) {
				concreteStorage.getParentFile().mkdirs();
				concreteStorage.createNewFile();
			}

			return new ChronicleWriter(concreteStorage, new ByteHandlePipeline(
					mByteHandler));
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public void close() throws SirixIOException {
	}

	/**
	 * Getting concrete storage for this file.
	 * 
	 * @return the concrete storage for this database
	 */
	private File getConcreteStorage() {
		return new File(new File(mFile, ResourceConfiguration.Paths.DATA.getFile()
				.getName()), FILENAME);
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
