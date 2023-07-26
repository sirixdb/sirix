package org.sirix.io.combined;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.cloud.amazon.AmazonS3StorageReader;
import org.sirix.page.PageReference;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.bytes.Bytes;

public class CombinedStorageWriter extends AbstractForwardingReader implements Writer {
	
	private Writer localStorageWriter, remoteStorageWriter;
	private Reader storageReader;
	/** Logger. */
	private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CombinedStorageWriter.class));
	
	public CombinedStorageWriter(Writer localWriter, Writer remoteWriter, Reader storageReader) {
		this.localStorageWriter = localWriter;
		this.remoteStorageWriter = remoteWriter;
		this.storageReader = storageReader;
	}
	
	@Override
	public Writer write(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference, Bytes<ByteBuffer> bufferedBytes) {
		Writer writer = localStorageWriter.write(pageReadOnlyTrx, pageReference, bufferedBytes);
		CompletableFuture.supplyAsync(() -> remoteStorageWriter.write(pageReadOnlyTrx, pageReference, bufferedBytes));
		return writer;
	}

	@Override
	public Writer writeUberPageReference(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference,
			Bytes<ByteBuffer> bufferedBytes) {
		Writer writer = localStorageWriter.writeUberPageReference(pageReadOnlyTrx, pageReference, bufferedBytes);
		CompletableFuture.supplyAsync(() -> remoteStorageWriter.writeUberPageReference(pageReadOnlyTrx, pageReference, bufferedBytes));
		return writer;
	}

	@Override
	public Writer truncateTo(PageReadOnlyTrx pageReadOnlyTrx, int revision) {
		Writer writer = localStorageWriter.truncateTo(pageReadOnlyTrx, revision);
		CompletableFuture.supplyAsync(() -> remoteStorageWriter.truncateTo(pageReadOnlyTrx, revision));
		return writer;
	}

	@Override
	public Writer truncate() {
		Writer writer = localStorageWriter.truncate();
		CompletableFuture<Writer> remoteWriterTask = CompletableFuture.supplyAsync(() -> remoteStorageWriter.truncate());
		return writer;
	}

	@Override
	public void close() {
		localStorageWriter.close();
		remoteStorageWriter.close();
		
	}

	@Override
	protected Reader delegate() {
		return storageReader;
	}
	
}
