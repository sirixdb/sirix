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
package org.sirix;

import org.sirix.XmlTestHelper.PATHS;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generating a standard resource within the {@link PATHS#PATH1} path. It also generates a standard
 * resource defined within {@link XmlTestHelper#RESOURCE}.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public class Holder {

  /** {@link Database} implementation. */
  private Database<XmlResourceManager> mDatabase;

  /** {@link XmlResourceManager} implementation. */
  private XmlResourceManager mResMgr;

  /** {@link XmlNodeReadOnlyTrx} implementation. */
  private XmlNodeReadOnlyTrx mRtx;

  /** {@link XmlNodeTrx} implementation. */
  private XmlNodeTrx mWtx;

  /**
   * Generate a resource with deweyIDs for resources and open a resource.
   *
   * @return this holder instance
   */
  public static Holder generateDeweyIDResourceMgr() {
    final Path file = PATHS.PATH1.getFile();
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (!Files.exists(file)) {
      Databases.createXmlDatabase(config);
    }
    final var database = Databases.openXmlDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).useDeweyIDs(true).build());
    final XmlResourceManager resourceManager = database.openResourceManager(XmlTestHelper.RESOURCE);
    final Holder holder = new Holder();
    holder.setDatabase(database);
    holder.setResourceManager(resourceManager);
    return holder;
  }

  /**
   * Generate a resource with a path summary.
   *
   * @return this holder instance
   */
  public static Holder generatePathSummary() {
    final Path file = PATHS.PATH1.getFile();
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (!Files.exists(file)) {
      Databases.createXmlDatabase(config);
    }
    final var database = Databases.openXmlDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).buildPathSummary(true).build());
    final XmlResourceManager resourceManager = database.openResourceManager(XmlTestHelper.RESOURCE);
    final Holder holder = new Holder();
    holder.setDatabase(database);
    holder.setResourceManager(resourceManager);
    return holder;
  }

  /**
   * Open a resource manager.
   *
   * @return this holder instance
   */
  public static Holder openResourceManager() {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    final XmlResourceManager resMgr = database.openResourceManager(XmlTestHelper.RESOURCE);
    final Holder holder = new Holder();
    holder.setDatabase(database);
    holder.setResourceManager(resMgr);
    return holder;
  }

  /**
   * Generate a {@link XmlNodeTrx}.
   *
   * @return this holder instance
   */
  public static Holder generateWtx() {
    final Holder holder = openResourceManager();
    final XmlNodeTrx writer = holder.mResMgr.beginNodeTrx();
    holder.setXdmNodeWriteTrx(writer);
    return holder;
  }

  /**
   * Generate a {@link XmlNodeReadOnlyTrx}.
   *
   * @return this holder instance
   */
  public static Holder generateRtx() {
    final Holder holder = openResourceManager();
    final XmlNodeReadOnlyTrx reader = holder.mResMgr.beginNodeReadOnlyTrx();
    holder.setXdmNodeReadTrx(reader);
    return holder;
  }

  /**
   * Close the database, session, read transaction and/or write transaction.
   */
  public void close() {
    if (mRtx != null && !mRtx.isClosed()) {
      mRtx.close();
    }
    if (mWtx != null && !mWtx.isClosed()) {
      mWtx.rollback();
      mWtx.close();
    }
    if (mResMgr != null && !mResMgr.isClosed()) {
      mResMgr.close();
    }
    if (mDatabase != null) {
      mDatabase.close();
    }
  }

  /**
   * Get the {@link Database} handle.
   *
   * @return {@link Database} handle
   */
  public Database<XmlResourceManager> getDatabase() {
    return mDatabase;
  }

  /**
   * Get the {@link ResourceManager} handle.
   *
   * @return {@link ResourceManager} handle
   */
  public XmlResourceManager getResourceManager() {
    return mResMgr;
  }

  /**
   * Get the {@link XmlNodeReadOnlyTrx} handle.
   *
   * @return {@link XmlNodeReadOnlyTrx} handle
   */
  public XmlNodeReadOnlyTrx getXmlNodeReadTrx() {
    return mRtx;
  }

  /**
   * Get the {@link XmlNodeTrx} handle.
   *
   * @return {@link XmlNodeTrx} handle
   */
  public XmlNodeTrx getXdmNodeWriteTrx() {
    return mWtx;
  }

  /**
   * Set the working {@link XmlNodeTrx}.
   *
   * @param wtx {@link XmlNodeTrx} instance
   */
  private void setXdmNodeWriteTrx(final XmlNodeTrx wtx) {
    mWtx = wtx;
  }

  /**
   * Set the working {@link XmlNodeReadOnlyTrx}.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} instance
   */
  private void setXdmNodeReadTrx(final XmlNodeReadOnlyTrx rtx) {
    mRtx = rtx;
  }

  /**
   * Set the working {@link ResourceManager}.
   *
   * @param resourceManager {@link XmlResourceManager} instance
   */
  private void setResourceManager(final XmlResourceManager resourceManager) {
    mResMgr = resourceManager;
  }

  /**
   * Set the working {@link Database}.
   *
   * @param database {@link Database} instance
   */
  private void setDatabase(final Database<XmlResourceManager> database) {
    mDatabase = database;
  }

}
