/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generating a standard resource within the {@link XmlTestHelper.PATHS#PATH1} path. It also generates a standard
 * resource defined within {@link XmlTestHelper#RESOURCE}.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public final class Holder {

  /** {@link Database} implementation. */
  private Database<XmlResourceSession> database;

  /** {@link XmlResourceSession} implementation. */
  private XmlResourceSession resMgr;

  /** {@link XmlNodeReadOnlyTrx} implementation. */
  private XmlNodeReadOnlyTrx rtx;

  /** {@link XmlNodeTrx} implementation. */
  private XmlNodeTrx wtx;

  /**
   * Generate a resource with deweyIDs for resources and open a resource.
   *
   * @return this holder instance
   */
  public static Holder generateDeweyIDResourceMgr() {
    final Path file = XmlTestHelper.PATHS.PATH1.getFile();
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (!Files.exists(file)) {
      Databases.createXmlDatabase(config);
    }
    final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).useDeweyIDs(true)
                                                                                     .hashKind(HashType.ROLLING)
                                                                                     .build());
    final XmlResourceSession resourceManager = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
    final Path file = XmlTestHelper.PATHS.PATH1.getFile();
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (!Files.exists(file)) {
      Databases.createXmlDatabase(config);
    }
    final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).buildPathSummary(true).build());
    final XmlResourceSession resourceManager = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
    final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
    final XmlResourceSession resMgr = database.beginResourceSession(XmlTestHelper.RESOURCE);
    final Holder holder = new Holder();
    holder.setDatabase(database);
    holder.setResourceManager(resMgr);
    return holder;
  }

  /**
   * Open a resource manager.
   *
   * @return this holder instance
   */
  public static Holder openResourceManagerWithHashes() {
    final var database = XmlTestHelper.getDatabaseWithRollingHashesEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    final XmlResourceSession resMgr = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
    final XmlNodeTrx writer = holder.resMgr.beginNodeTrx();
    holder.setXdmNodeWriteTrx(writer);
    return holder;
  }

  /**
   * Generate a {@link XmlNodeTrx}.
   *
   * @return this holder instance
   */
  public static Holder generateWtxAndResourceWithHashes() {
    final Holder holder = openResourceManagerWithHashes();
    final XmlNodeTrx writer = holder.resMgr.beginNodeTrx();
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
    final XmlNodeReadOnlyTrx reader = holder.resMgr.beginNodeReadOnlyTrx();
    holder.setXdmNodeReadTrx(reader);
    return holder;
  }

  /**
   * Close the database, session, read transaction and/or write transaction.
   */
  public void close() {
    if (rtx != null && !rtx.isClosed()) {
      rtx.close();
    }
    if (wtx != null && !wtx.isClosed()) {
      wtx.rollback();
      wtx.close();
    }
    if (resMgr != null && !resMgr.isClosed()) {
      resMgr.close();
    }
    if (database != null) {
      database.close();
    }
  }

  /**
   * Get the {@link Database} handle.
   *
   * @return {@link Database} handle
   */
  public Database<XmlResourceSession> getDatabase() {
    return database;
  }

  /**
   * Get the {@link ResourceSession} handle.
   *
   * @return {@link ResourceSession} handle
   */
  public XmlResourceSession getResourceManager() {
    return resMgr;
  }

  /**
   * Get the {@link XmlNodeReadOnlyTrx} handle.
   *
   * @return {@link XmlNodeReadOnlyTrx} handle
   */
  public XmlNodeReadOnlyTrx getXmlNodeReadTrx() {
    return rtx;
  }

  /**
   * Get the {@link XmlNodeTrx} handle.
   *
   * @return {@link XmlNodeTrx} handle
   */
  public XmlNodeTrx getXdmNodeWriteTrx() {
    return wtx;
  }

  /**
   * Set the working {@link XmlNodeTrx}.
   *
   * @param wtx {@link XmlNodeTrx} instance
   */
  private void setXdmNodeWriteTrx(final XmlNodeTrx wtx) {
    this.wtx = wtx;
  }

  /**
   * Set the working {@link XmlNodeReadOnlyTrx}.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} instance
   */
  private void setXdmNodeReadTrx(final XmlNodeReadOnlyTrx rtx) {
    this.rtx = rtx;
  }

  /**
   * Set the working {@link ResourceSession}.
   *
   * @param resourceManager {@link XmlResourceSession} instance
   */
  private void setResourceManager(final XmlResourceSession resourceManager) {
    resMgr = resourceManager;
  }

  /**
   * Set the working {@link Database}.
   *
   * @param database {@link Database} instance
   */
  private void setDatabase(final Database<XmlResourceSession> database) {
    this.database = database;
  }

}
