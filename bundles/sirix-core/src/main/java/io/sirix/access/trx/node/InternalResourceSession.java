package io.sirix.access.trx.node;

import io.sirix.api.*;
import io.sirix.io.Reader;
import io.sirix.page.UberPage;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

public interface InternalResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
		extends
			ResourceSession<R, W> {
	/**
	 * Abort the write-transaction.
	 */
	enum Abort {
		/**
		 * Yes, abort.
		 */
		YES,

		/**
		 * No, don't abort.
		 */
		NO
	}

	Reader createReader();

	Path getCommitFile();

	void assertAccess(int revision);

	PageTrx createPageTransaction(long trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx);

	Lock getCommitLock();

	void setLastCommittedUberPage(UberPage lastUberPage);

	void closeWriteTransaction(long transactionID);

	void setNodePageWriteTransaction(long transactionID, PageTrx pageTrx);

	void closeNodePageWriteTransaction(long transactionID);

	void closeReadTransaction(long trxId);

	void closePageReadTransaction(Long trxId);

	void closePageWriteTransaction(Long transactionID);
}
