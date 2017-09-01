/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Writer;
import org.sirix.io.berkeley.binding.PageBinding;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.page.PageReference;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.Page;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * This class represents a write instance of the Sirix-Application implementing the
 * {@link Writer}-interface. It inherits and overrides some reader methods because of the
 * transaction layer.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class BerkeleyWriter implements Writer {

	/** Current {@link Database} to write to. */
	private final Database mDatabase;

	/** Current {@link Transaction} to write with. */
	private final Transaction mTxn;

	/** Current {@link BerkeleyReader} to read with. */
	private final BerkeleyReader mReader;

	/** Key of nodepage. */
	private long mNodepagekey;

	private PageBinding mPageBinding;

	/**
	 * Simple constructor starting with an {@link Environment} and a {@link Database}.
	 *
	 * @param env {@link Environment} reference for the write
	 * @param database {@link Database} reference where the data should be written to
	 * @throws SirixIOException if something odd happens@Nonnull
	 */
	public BerkeleyWriter(final Environment env, final Database database,
			final ByteHandlePipeline byteHandler) throws SirixIOException {
		try {
			mTxn = env.beginTransaction(null, null);
			mDatabase = checkNotNull(database);
			mNodepagekey = getLastNodePage();
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}

		mReader = new BerkeleyReader(mDatabase, mTxn, byteHandler);
		mPageBinding = new PageBinding(byteHandler);
	}

	@Override
	public void close() throws SirixIOException {
		try {
			setLastNodePage(mNodepagekey);
			mTxn.commit();
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}
	}

	@Override
	public Writer write(final PageReference pageReference) throws SirixIOException {
		final Page page = pageReference.getPage();

		final DatabaseEntry valueEntry = new DatabaseEntry();
		final DatabaseEntry keyEntry = new DatabaseEntry();

		mNodepagekey++;

		mPageBinding.objectToEntry(page, valueEntry);
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(mNodepagekey, keyEntry);

		final OperationStatus status = mDatabase.put(mTxn, keyEntry, valueEntry);
		if (status != OperationStatus.SUCCESS) {
			throw new SirixIOException(new StringBuilder("Write of ").append(pageReference.toString())
					.append(" failed!").toString());
		}

		pageReference.setKey(mNodepagekey);

		return this;
	}

	/**
	 * Setting the last {@link UnorderedKeyValuePage} to the persistent storage.
	 *
	 * @param data key to be stored
	 * @throws SirixIOException if can't set last {@link UnorderedKeyValuePage}
	 */
	private void setLastNodePage(@Nonnegative final long data) throws SirixIOException {
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry valueEntry = new DatabaseEntry();

		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-2l, keyEntry);
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(data, valueEntry);

		try {
			mDatabase.put(mTxn, keyEntry, valueEntry);
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}
	}

	/**
	 * Getting the last nodePage from the persistent storage.
	 *
	 * @throws SirixIOException If can't get last Node page
	 * @return the last nodepage-key
	 */
	private long getLastNodePage() throws SirixIOException {
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry valueEntry = new DatabaseEntry();

		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-2l, keyEntry);

		try {
			final OperationStatus status = mDatabase.get(mTxn, keyEntry, valueEntry, LockMode.DEFAULT);
			return status == OperationStatus.SUCCESS
					? BerkeleyStorage.DATAINFO_VAL_B.entryToObject(valueEntry)
					: 0L;
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}
	}

	@Override
	public Writer writeUberPageReference(final PageReference pageReference) throws SirixIOException {
		write(pageReference);

		final DatabaseEntry keyEntry = new DatabaseEntry();
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-1l, keyEntry);

		final DatabaseEntry valueEntry = new DatabaseEntry();
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(pageReference.getKey(), valueEntry);

		try {
			mDatabase.put(mTxn, keyEntry, valueEntry);
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}

		return this;
	}

	@Override
	public Page read(final PageReference key, final PageReadTrx pageReadTrx) throws SirixIOException {
		return mReader.read(key, pageReadTrx);
	}

	@Override
	public PageReference readUberPageReference() throws SirixIOException {
		return mReader.readUberPageReference();
	}

	@Override
	public int hashCode() {
		return Objects.hash(mDatabase, mTxn, mReader);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof BerkeleyWriter) {
			final BerkeleyWriter other = (BerkeleyWriter) obj;
			return Objects.equals(mDatabase, other.mDatabase) && Objects.equals(mTxn, other.mTxn)
					&& Objects.equals(mReader, other.mReader);
		}
		return false;
	}

	@Override
	public Writer truncateTo(int revision) {
		// TODO Auto-generated method stub
		return null;
	}

}
