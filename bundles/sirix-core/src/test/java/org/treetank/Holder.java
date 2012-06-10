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
package org.treetank;

import org.treetank.TestHelper.PATHS;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IDatabase;
import org.treetank.api.INodeReadTrx;
import org.treetank.api.ISession;
import org.treetank.api.INodeWriteTrx;
import org.treetank.exception.AbsTTException;

/**
 * Generating a standard resource within the {@link PATHS#PATH1} path. It also
 * generates a standard resource defined within {@link TestHelper#RESOURCE}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class Holder {

  private IDatabase mDatabase;

  private ISession mSession;

  private INodeReadTrx mRtx;

  private INodeWriteTrx mWtx;

  public static Holder generateSession() throws AbsTTException {
    final IDatabase database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH1.getConfig())
      .build());
    final ISession session =
      database.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
    final Holder holder = new Holder();
    holder.setDatabase(database);
    holder.setSession(session);
    return holder;
  }

  public static Holder generateWtx() throws AbsTTException {
    final Holder holder = generateSession();
    final INodeWriteTrx wtx = holder.mSession.beginNodeWriteTrx();
    holder.setWtx(wtx);
    return holder;
  }

  public static Holder generateRtx() throws AbsTTException {
    final Holder holder = generateSession();
    final INodeReadTrx rtx = holder.mSession.beginNodeReadTrx();
    holder.setRtx(rtx);
    return holder;
  }

  public void close() throws AbsTTException {
    if (mRtx != null && !mRtx.isClosed()) {
      mRtx.close();
    }
    if (mWtx != null && !mWtx.isClosed()) {
      mWtx.abort();
      mWtx.close();
    }
    mSession.close();
    mDatabase.close();
  }

  public IDatabase getDatabase() {
    return mDatabase;
  }

  public ISession getSession() {
    return mSession;
  }

  public INodeReadTrx getRtx() {
    return mRtx;
  }

  public INodeWriteTrx getWtx() {
    return mWtx;
  }

  private void setWtx(final INodeWriteTrx paramWtx) {
    this.mWtx = paramWtx;
  }

  private void setRtx(final INodeReadTrx paramRtx) {
    this.mRtx = paramRtx;
  }

  private void setSession(final ISession paramSession) {
    this.mSession = paramSession;
  }

  private void setDatabase(final IDatabase paramDatabase) {
    this.mDatabase = paramDatabase;
  }

}
