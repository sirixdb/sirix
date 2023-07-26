package org.sirix.io.combined;

import java.time.Instant;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public class CombinedStorageReader implements Reader {

	
	private Reader localReader, remoteReader;
	
	public CombinedStorageReader(Reader localReader, Reader remoteReader) {
		this.localReader = localReader;
		this.remoteReader = remoteReader;
	}
	
	@Override
	public PageReference readUberPageReference() {
		PageReference pageReference = localReader.readUberPageReference();
		if(pageReference==null) {
			pageReference = remoteReader.readUberPageReference();
		}
		return pageReference;
	}

	@Override
	public Page read(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx) {
		Page page = localReader.read(key, pageReadTrx);
		if(page==null) {
			page = remoteReader.read(key, pageReadTrx);
		}
		return page;
	}

	@Override
	public void close() {
		localReader.close();
		remoteReader.close();
	}

	@Override
	public RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx) {
		RevisionRootPage revRootPage = localReader.readRevisionRootPage(revision, pageReadTrx);
		if(revRootPage==null) {
			revRootPage = remoteReader.readRevisionRootPage(revision, pageReadTrx);
		}
		return revRootPage;
	}

	@Override
	public Instant readRevisionRootPageCommitTimestamp(int revision) {
		Instant revRootPageCommitTS = localReader.readRevisionRootPageCommitTimestamp(revision);
		if(revRootPageCommitTS==null) {
			revRootPageCommitTS = remoteReader.readRevisionRootPageCommitTimestamp(revision);
		}
		return revRootPageCommitTS;
	}

	@Override
	public RevisionFileData getRevisionFileData(int revision) {
		RevisionFileData revFileData = localReader.getRevisionFileData(revision);
		if(revFileData == null) {
			revFileData = remoteReader.getRevisionFileData(revision);
		}
		return revFileData;
	}

}
