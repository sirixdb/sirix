/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.io.berkeley;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.berkeley.binding.PageBinding;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
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
 * This class represents an reading instance of the Sirix-Application
 * implementing the {@link Reader}-interface.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class BerkeleyReader implements Reader {

	/** {@link Database} reference. */
	private final Database mDatabase;

	/** {@link Transaction} reference. */
	private final Transaction mTxn;

	/** {@link PageBinding} reference. */
	private final PageBinding mPageBinding;

	/**
	 * Constructor.
	 * 
	 * @param database
	 *          {@link Database} reference to be connected to
	 * @param trx
	 *          {@link Transaction} to be used
	 * @param pageBinding
	 *          page binding
	 * @throws NullPointerException
	 *           if {@code pDatabase}, {@code pTxn} or {@code pBinding} is
	 *           {@code null}
	 */
	public BerkeleyReader(final @Nonnull Database database,
			final @Nonnull Transaction trx, final @Nonnull PageBinding pageBinding) {
		mTxn = checkNotNull(trx);
		mDatabase = checkNotNull(database);
		mPageBinding = checkNotNull(pageBinding);
	}

	/**
	 * Constructor.
	 * 
	 * @param pEnv
	 *          {@link Envirenment} to be used
	 * @param pDatabase
	 *          {@link Database} to be connected to
	 * @param pPageBinding
	 *          page binding
	 * @throws DatabaseException
	 *           if something weird happens
	 */
	public BerkeleyReader(@Nonnull final Environment pEnv,
			@Nonnull final Database pDatabase, final @Nonnull PageBinding pPageBinding)
			throws DatabaseException {
		this(pDatabase, pEnv.beginTransaction(null, null), pPageBinding);
	}

	@Override
	public Page read(final long pKey, final @Nonnull ResourceConfiguration resourceConfig) throws SirixIOException {
		final DatabaseEntry valueEntry = new DatabaseEntry();
		final DatabaseEntry keyEntry = new DatabaseEntry();

		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(pKey, keyEntry);

		Page page = null;
		try {
			final OperationStatus status = mDatabase.get(mTxn, keyEntry, valueEntry,
					LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				page = mPageBinding.entryToObject(valueEntry);
			}
			return page;
		} catch (final DatabaseException exc) {
			throw new SirixIOException(exc);
		}
	}

	@Override
	public PageReference readFirstReference(final @Nonnull ResourceConfiguration resourceConfig) throws SirixIOException {
		final DatabaseEntry valueEntry = new DatabaseEntry();
		final DatabaseEntry keyEntry = new DatabaseEntry();
		TupleBinding.getPrimitiveBinding(Long.class).objectToEntry(-1l, keyEntry);

		try {
			final OperationStatus status = mDatabase.get(mTxn, keyEntry, valueEntry,
					LockMode.DEFAULT);
			PageReference uberPageReference = new PageReference();
			if (status == OperationStatus.SUCCESS) {
				uberPageReference.setKey(TupleBinding.getPrimitiveBinding(Long.class)
						.entryToObject(valueEntry));
			}
			final UberPage page = (UberPage) read(uberPageReference.getKey(), resourceConfig);

			if (uberPageReference != null) {
				uberPageReference.setPage(page);
			}

			return uberPageReference;
		} catch (final DatabaseException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public void close() throws SirixIOException {
		try {
			mTxn.abort();
		} catch (final DatabaseException e) {
			throw new SirixIOException(e);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(mDatabase, mTxn);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof BerkeleyReader) {
			final BerkeleyReader other = (BerkeleyReader) obj;
			return Objects.equals(mDatabase, other.mDatabase)
					&& Objects.equals(mTxn, other.mTxn);
		}
		return false;
	}

}
