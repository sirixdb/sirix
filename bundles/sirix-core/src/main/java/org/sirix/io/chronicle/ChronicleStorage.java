package org.sirix.io.chronicle;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;

import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

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
	private List<Reader> mReaders;

	/** Writing to the storage. */
	private List<Writer> mWriters;

	/** Chronicle storage. */
	private IndexedChronicle mChronicle;

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
	public Reader getReader() throws SirixIOException {
		try {
			final File concreteStorage = getConcreteStorage();

			if (!concreteStorage.exists()) {
				concreteStorage.getParentFile().mkdirs();
				concreteStorage.createNewFile();
			}

			if (mReaders == null) {
				mReaders = new LinkedList<>();
				mChronicle = new IndexedChronicle(concreteStorage.getAbsolutePath());
			}
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
		return new ChronicleReader(mChronicle, new ByteHandlePipeline(
				mByteHandler));
	}

	@Override
	public Writer getWriter() throws SirixIOException {
		try {
			final File concreteStorage = getConcreteStorage();

			if (!concreteStorage.exists()) {
				concreteStorage.getParentFile().mkdirs();
				concreteStorage.createNewFile();
			}
			if (mWriters == null) {
				mWriters = new LinkedList<>();
				mChronicle = new IndexedChronicle(concreteStorage.getAbsolutePath());
			}
		} catch (final IOException e) {
			throw new SirixIOException(e);
		}
		return new ChronicleWriter(mChronicle, new ByteHandlePipeline(
				mByteHandler));
	}

	@Override
	public void close() throws SirixIOException {
		if (mReaders != null) {
			for (final Reader reader : mReaders)
				reader.close();
		}
//		if (mWriters != null) 
//		{
//			for (final Writer writer : mWriters);
//				writer.close();
//		}
		mChronicle.close();
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
