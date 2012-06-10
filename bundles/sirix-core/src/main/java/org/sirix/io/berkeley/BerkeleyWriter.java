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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.sirix.exception.TTIOException;
import org.sirix.io.IKey;
import org.sirix.io.IWriter;
import org.sirix.page.NodePage;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.IPage;

/**
 * This class represents an reading instance of the sirix-Application
 * implementing the {@link IWriter}-interface. It inherits and overrides some
 * reader methods because of the transaction layer.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class BerkeleyWriter implements IWriter {

  /** Current {@link Database} to write to. */
  private final Database mDatabase;

  /** Current {@link Transaction} to write with. */
  private final Transaction mTxn;

  /** Current {@link BerkeleyReader} to read with. */
  private final BerkeleyReader mReader;

  /** Key of nodepage. */
  private long mNodepagekey;

  /**
   * Simple constructor starting with an {@link Environment} and a {@link Database}.
   * 
   * @param pEnv
   *          {@link Environment} reference for the write
   * @param pDatabase
   *          {@link Database} reference where the data should be written to
   * @throws TTIOException
   *           if something odd happens
   */
  public BerkeleyWriter(final Environment pEnv, final Database pDatabase) throws TTIOException {
    try {
      mTxn = pEnv.beginTransaction(null, null);
      mDatabase = checkNotNull(pDatabase);
      mNodepagekey = getLastNodePage();
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }

    mReader = new BerkeleyReader(mDatabase, mTxn);
  }

  @Override
  public void close() throws TTIOException {
    try {
      setLastNodePage(mNodepagekey);
      mTxn.commit();
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public void write(final PageReference pageReference) throws TTIOException {
    final IPage page = pageReference.getPage();

    final DatabaseEntry valueEntry = new DatabaseEntry();
    final DatabaseEntry keyEntry = new DatabaseEntry();

    // TODO make this better
    mNodepagekey++;
    final BerkeleyKey key = new BerkeleyKey(mNodepagekey);

    BerkeleyFactory.PAGE_VAL_B.objectToEntry(page, valueEntry);
    BerkeleyFactory.KEY.objectToEntry(key, keyEntry);

    final OperationStatus status = mDatabase.put(mTxn, keyEntry, valueEntry);
    if (status != OperationStatus.SUCCESS) {
      throw new TTIOException(new StringBuilder("Write of ").append(pageReference.toString()).append(
        " failed!").toString());
    }

    pageReference.setKey(key);

  }

  /**
   * Setting the last {@link NodePage} to the persistent storage.
   * 
   * @param pData
   *          key to be stored
   * @throws TTIOException
   *           if can't set last {@link NodePage}
   */
  private void setLastNodePage(final long pData) throws TTIOException {
    final DatabaseEntry keyEntry = new DatabaseEntry();
    final DatabaseEntry valueEntry = new DatabaseEntry();

    final BerkeleyKey key = BerkeleyKey.getDataInfoKey();
    BerkeleyFactory.KEY.objectToEntry(key, keyEntry);
    BerkeleyFactory.DATAINFO_VAL_B.objectToEntry(pData, valueEntry);
    try {
      mDatabase.put(mTxn, keyEntry, valueEntry);
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
  }

  /**
   * Getting the last nodePage from the persistent storage.
   * 
   * @throws TTIOException
   *           If can't get last Node page
   * @return the last nodepage-key
   */
  private long getLastNodePage() throws TTIOException {
    final DatabaseEntry keyEntry = new DatabaseEntry();
    final DatabaseEntry valueEntry = new DatabaseEntry();

    final BerkeleyKey key = BerkeleyKey.getDataInfoKey();
    BerkeleyFactory.KEY.objectToEntry(key, keyEntry);

    try {
      final OperationStatus status = mDatabase.get(mTxn, keyEntry, valueEntry, LockMode.DEFAULT);
      return status == OperationStatus.SUCCESS ? BerkeleyFactory.DATAINFO_VAL_B.entryToObject(valueEntry)
        : 0L;
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public void writeFirstReference(final PageReference pPageReference) throws TTIOException {
    write(pPageReference);

    final DatabaseEntry keyEntry = new DatabaseEntry();
    BerkeleyFactory.KEY.objectToEntry(BerkeleyKey.getFirstRevKey(), keyEntry);

    final DatabaseEntry valueEntry = new DatabaseEntry();
    BerkeleyFactory.FIRST_REV_VAL_B.objectToEntry(pPageReference, valueEntry);

    try {
      mDatabase.put(mTxn, keyEntry, valueEntry);
    } catch (final DatabaseException exc) {
      throw new TTIOException(exc);
    }

  }

  @Override
  public IPage read(final IKey pKey) throws TTIOException {
    return mReader.read(pKey);
  }

  @Override
  public PageReference readFirstReference() throws TTIOException {
    return mReader.readFirstReference();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mDatabase == null) ? 0 : mDatabase.hashCode());
    result = prime * result + ((mTxn == null) ? 0 : mTxn.hashCode());
    result = prime * result + ((mReader == null) ? 0 : mReader.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object mObj) {
    boolean returnVal = true;
    if (mObj == null) {
      returnVal = false;
    } else if (getClass() != mObj.getClass()) {
      returnVal = false;
    }
    final BerkeleyWriter other = (BerkeleyWriter)mObj;
    if (mDatabase == null) {
      if (other.mDatabase != null) {
        returnVal = false;
      }
    } else if (!mDatabase.equals(other.mDatabase)) {
      returnVal = false;
    }
    if (mTxn == null) {
      if (other.mTxn != null) {
        returnVal = false;
      }
    } else if (!mTxn.equals(other.mTxn)) {
      returnVal = false;
    }
    if (mReader == null) {
      if (other.mReader != null) {
        returnVal = false;
      }
    } else if (!mReader.equals(other.mReader)) {
      returnVal = false;
    }
    return returnVal;
  }

}
