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

import static org.junit.Assert.fail;

import com.google.common.collect.HashBiMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;
import org.treetank.access.Database;
import org.treetank.access.Session;
import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.access.conf.ResourceConfiguration;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IDatabase;
import org.treetank.api.INodeWriteTrx;
import org.treetank.api.ISession;
import org.treetank.exception.AbsTTException;
import org.treetank.node.AttributeNode;
import org.treetank.node.DeletedNode;
import org.treetank.node.DocumentRootNode;
import org.treetank.node.ElementNode;
import org.treetank.node.NamespaceNode;
import org.treetank.node.TextNode;
import org.treetank.node.delegates.NameNodeDelegate;
import org.treetank.node.delegates.NodeDelegate;
import org.treetank.node.delegates.StructNodeDelegate;
import org.treetank.node.delegates.ValNodeDelegate;
import org.treetank.page.NodePage;
import org.treetank.settings.ECharsForSerializing;
import org.treetank.utils.DocumentCreater;

/**
 * 
 * Helper class for offering convenient usage of {@link Session}s for test
 * cases.
 * 
 * This includes instantiation of databases plus resources.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class TestHelper {

  /** Common resource name. */
  public static final String RESOURCE = "shredded";

  /** Paths where the data is stored to. */
  public enum PATHS {

    // PATH1 (TNK)
      PATH1(new File(new StringBuilder(File.separator).append("tmp").append(File.separator).append("tnk")
        .append(File.separator).append("path1").toString())),

      // PATH2 (TNK)
      PATH2(new File(new StringBuilder(File.separator).append("tmp").append(File.separator).append("tnk")
        .append(File.separator).append("path2").toString())),

      // PATH3 (XML)
      PATH3(new File(new StringBuilder(File.separator).append("tmp").append(File.separator).append("xml")
        .append(File.separator).append("test.xml").toString()));

    final File file;

    final DatabaseConfiguration config;

    PATHS(final File paramFile) {
      file = paramFile;
      config = new DatabaseConfiguration(paramFile);
    }

    public File getFile() {
      return file;
    }

    public DatabaseConfiguration getConfig() {
      return config;
    }

  }

  /** Common random instance for generating common tag names. */
  public final static Random random = new Random();

  private final static Map<File, IDatabase> INSTANCES = new Hashtable<File, IDatabase>();

  @Test
  public void testDummy() {
    // Just empty to ensure maven running
  }

  /**
   * Getting a database and create one of not existing. This includes the
   * creation of a resource with the settings in the builder as standard.
   * 
   * @param file
   *          to be created
   * @return a database-obj
   */
  @Ignore
  public static final IDatabase getDatabase(final File file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      try {
        final DatabaseConfiguration config = new DatabaseConfiguration(file);
        if (!file.exists()) {
          Database.createDatabase(config);
        }
        final IDatabase database = Database.openDatabase(file);
        database.createResource(new ResourceConfiguration.Builder(RESOURCE, config).build());
        INSTANCES.put(file, database);
        return database;
      } catch (final AbsTTException exc) {
        fail(exc.toString());
        return null;
      }
    }
  }

  /**
   * Deleting all resources as defined in the enum {@link PATHS}.
   * 
   * @throws AbsTTException
   */
  @Ignore
  public static final void deleteEverything() throws AbsTTException {
    closeEverything();
    Database.truncateDatabase(PATHS.PATH1.config);
    Database.truncateDatabase(PATHS.PATH2.config);
  }

  /**
   * Closing all resources as defined in the enum {@link PATHS}.
   * 
   * @throws AbsTTException
   */
  @Ignore
  public static final void closeEverything() throws AbsTTException {
    if (INSTANCES.containsKey(PATHS.PATH1.getFile())) {
      final IDatabase database = INSTANCES.remove(PATHS.PATH1.getFile());
      database.close();
    }
    if (INSTANCES.containsKey(PATHS.PATH2.getFile())) {
      final IDatabase database = INSTANCES.remove(PATHS.PATH2.getFile());
      database.close();
    }
  }

  @Ignore
  public static NodePage getNodePage(final long revision, final int offset, final int length,
    final long nodePageKey) {
    final NodePage page = new NodePage(nodePageKey, revision);
    NodeDelegate nodeDel;
    NameNodeDelegate nameDel;
    StructNodeDelegate strucDel;
    ValNodeDelegate valDel;
    for (int i = offset; i < length; i++) {
      switch (random.nextInt(6)) {
      case 0:
        nodeDel = new NodeDelegate(random.nextLong(), random.nextLong(), random.nextLong());
        nameDel = new NameNodeDelegate(nodeDel, random.nextInt(), random.nextInt());
        valDel = new ValNodeDelegate(nodeDel, new byte[] {
          0, 1, 2, 3, 4
        }, false);
        page.setNode(i, new AttributeNode(nodeDel, nameDel, valDel));
        break;
      case 1:
        page.setNode(i, new DeletedNode(new NodeDelegate(random.nextLong(), random.nextLong(), random
          .nextLong())));
        break;
      case 2:
        nodeDel = new NodeDelegate(random.nextLong(), random.nextLong(), random.nextLong());
        nameDel = new NameNodeDelegate(nodeDel, random.nextInt(), random.nextInt());
        strucDel =
          new StructNodeDelegate(nodeDel, random.nextLong(), random.nextLong(), random.nextLong(), random
            .nextLong(), random.nextLong());
        page.setNode(i, new ElementNode(nodeDel, strucDel, nameDel, new ArrayList<Long>(), HashBiMap
          .<Integer, Long> create(), new ArrayList<Long>()));
        break;
      case 3:
        nodeDel = new NodeDelegate(random.nextLong(), random.nextLong(), random.nextLong());
        nameDel = new NameNodeDelegate(nodeDel, random.nextInt(), random.nextInt());
        page.setNode(i, new NamespaceNode(nodeDel, nameDel));
        break;
      case 4:
        nodeDel = new NodeDelegate(random.nextLong(), random.nextLong(), random.nextLong());
        strucDel =
          new StructNodeDelegate(nodeDel, random.nextLong(), random.nextLong(), random.nextLong(), random
            .nextLong(), random.nextLong());
        page.setNode(i, new DocumentRootNode(nodeDel, strucDel));
        break;
      case 5:
        nodeDel = new NodeDelegate(random.nextLong(), random.nextLong(), random.nextLong());
        valDel = new ValNodeDelegate(nodeDel, new byte[] {
          0, 1
        }, false);
        strucDel =
          new StructNodeDelegate(nodeDel, random.nextLong(), random.nextLong(), random.nextLong(), random
            .nextLong(), random.nextLong());
        page.setNode(i, new TextNode(nodeDel, valDel, strucDel));
        break;
      }

    }
    return page;
  }

  /**
   * Read a file into a StringBuilder.
   * 
   * @param paramFile
   *          The file to read.
   * @param paramWhitespaces
   *          Retrieve file and don't remove any whitespaces.
   * @return StringBuilder instance, which has the string representation of
   *         the document.
   * @throws IOException
   *           throws an IOException if any I/O operation fails.
   */
  @Ignore("Not a test, utility method only")
  public static StringBuilder readFile(final File paramFile, final boolean paramWhitespaces)
    throws IOException {
    final BufferedReader in = new BufferedReader(new FileReader(paramFile));
    final StringBuilder sBuilder = new StringBuilder();
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      if (paramWhitespaces) {
        sBuilder.append(line + ECharsForSerializing.NEWLINE);
      } else {
        sBuilder.append(line.trim());
      }
    }

    // Remove last newline.
    if (paramWhitespaces) {
      sBuilder.replace(sBuilder.length() - 1, sBuilder.length(), "");
    }
    in.close();

    return sBuilder;
  }

  /**
   * Creating a test document at {@link PATHS#PATH1}.
   * 
   * @throws AbsTTException
   */
  public static void createTestDocument() throws AbsTTException {
    final IDatabase database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(RESOURCE, PATHS.PATH1.config).build());
    final ISession session = database.getSession(new SessionConfiguration.Builder(RESOURCE).build());
    final INodeWriteTrx wtx = session.beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    session.close();
  }
}
