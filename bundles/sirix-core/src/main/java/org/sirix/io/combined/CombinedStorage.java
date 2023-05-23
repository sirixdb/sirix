package org.sirix.io.combined;

import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.cloud.ICloudStorage;

public class CombinedStorage implements IOStorage {
	
	private final IOStorage localStorage;
	
	private final ICloudStorage remoteStorage;
	
	public CombinedStorage(final IOStorage localStorage, 
			final ICloudStorage remoteStorage) {
		this.localStorage = localStorage;
		this.remoteStorage = remoteStorage;
	}

	@Override
	public Writer createWriter() {
		return new CombinedStorageWriter(localStorage.createWriter(), remoteStorage.createWriter(), localStorage.createReader());
	}

	@Override
	public Reader createReader() {
		return new CombinedStorageReader(localStorage.createReader(), remoteStorage.createReader());
	}

	@Override
	public void close() {
		localStorage.close();
		remoteStorage.close();
	}

	@Override
	public boolean exists() {
		if(!localStorage.exists()) return remoteStorage.exists();
		return localStorage.exists();
	}

	@Override
	public ByteHandler getByteHandler() {
		return localStorage.getByteHandler();
	}

}
