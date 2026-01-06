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

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;

import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.xml.XmlResourceSessionImpl;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.settings.CharsForSerializing;
import io.sirix.utils.XmlDocumentCreator;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Ignore;
import org.junit.Test;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.IndexBackendType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;

/**
 * Helper class for offering convenient usage of {@link XmlResourceSessionImpl}s for test cases.
 * <p>
 * This includes instantiation of databases plus resources.
 *
 * @author Sebastian Graf, University of Konstanz
 */
public final class XmlTestHelper {

  /**
   * Temporary directory path.
   */
  private static final String TMPDIR = System.getProperty("java.io.tmpdir");

  /**
   * Common resource name.
   */
  public static final String RESOURCE = "shredded";

  /**
   * Paths where the data is stored to.
   */
  public enum PATHS {
    // PATH1 (Sirix)
    PATH1(Paths.get(TMPDIR, "sirix", "path1")),

    // PATH2 (Sirix)
    PATH2(Paths.get(TMPDIR, "sirix", "path2")),

    // PATH3 (XML)
    PATH3(Paths.get(TMPDIR, "xml", "test.xml"));

    final Path file;

    final DatabaseConfiguration config;

    PATHS(final Path file) {
      this.file = file;
      config = new DatabaseConfiguration(file);
    }

    public Path getFile() {
      return file;
    }

    public DatabaseConfiguration getConfig() {
      return config.setDatabaseType(DatabaseType.XML);
    }

  }

  /**
   * Common random instance for generating common tag names.
   */
  public final static Random random = new Random();

  /**
   * Path <=> Database instances.
   */
  private final static Map<Path, Database<XmlResourceSession>> INSTANCES = new Hashtable<>();

  @Test
  public void testDummy() {
    // Just empty to ensure maven running
  }

  /**
   * Getting a database and create one if not existing. This includes the creation of a resource with
   * the settings in the builder as standard.
   *
   * @param file to be created
   * @return a database-obj
   */
  @Ignore
  public static Database<XmlResourceSession> getDatabase(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createXmlDatabase(config);
      }
      final var database = Databases.openXmlDatabase(file);
      database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Getting a database and create one if not existing. This includes the creation of a resource with
   * the settings in the builder as standard.
   *
   * @param file to be created
   * @return a database-obj
   */
  @Ignore
  public static Database<XmlResourceSession> getDatabase(final Path file, final User user) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createXmlDatabase(config);
      }
      final var database = Databases.openXmlDatabase(file, user);
      database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Getting a database and create one if not existing. This includes the creation of a resource with
   * the settings in the builder as standard.
   *
   * @param file to be created
   * @return a database-obj
   */
  @Ignore
  public static Database<XmlResourceSession> getDatabaseWithRollingHashesEnabled(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createXmlDatabase(config);
      }
      final var database = Databases.openXmlDatabase(file);
      database.createResource(new ResourceConfiguration.Builder(RESOURCE).hashKind(HashType.ROLLING).build());
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Getting a database and create one if not existing. This includes the creation of a resource with
   * the settings in the builder as standard.
   *
   * @param file to be created
   * @return a database-obj
   */
  @Ignore
  public static Database<XmlResourceSession> getDatabaseWithDeweyIDsEnabled(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createXmlDatabase(config);
      }
      final var database = Databases.openXmlDatabase(file);
      database.createResource(new ResourceConfiguration.Builder(RESOURCE).useDeweyIDs(true).build());
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Getting a database with Red-Black tree indexes enabled for testing RBTree integration.
   *
   * @param file to be created
   * @return a database-obj with RBTREE index backend
   */
  @Ignore
  public static Database<XmlResourceSession> getDatabaseWithRedBlackTreeIndexes(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createXmlDatabase(config);
      }
      final var database = Databases.openXmlDatabase(file);
      database.createResource(new ResourceConfiguration.Builder(RESOURCE)
          .indexBackendType(IndexBackendType.RBTREE)
          .build());
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Deleting all resources as defined in the enum {@link PATHS}.
   *
   * @throws SirixException
   */
  @Ignore
  public static void deleteEverything() {
    closeEverything();
    Databases.removeDatabase(PATHS.PATH1.getFile());
    Databases.removeDatabase(PATHS.PATH2.getFile());
  }

  /**
   * Closing all resources as defined in the enum {@link PATHS}.
   *
   * @throws SirixException
   */
  @Ignore
  public static void closeEverything() {
    if (INSTANCES.containsKey(PATHS.PATH1.getFile())) {
      final var database = INSTANCES.remove(PATHS.PATH1.getFile());
      database.close();
    }
    if (INSTANCES.containsKey(PATHS.PATH2.getFile())) {
      final var database = INSTANCES.remove(PATHS.PATH2.getFile());
      database.close();
    }
  }

  /**
   * Read a file into a StringBuilder.
   *
   * @param file        the file to read
   * @param whitespaces retrieve file and don't remove any whitespaces
   * @return StringBuilder instance, which has the string representation of the document
   * @throws IOException if an I/O operation fails
   */
  @Ignore("Not a test, utility method only")
  public static StringBuilder readFile(final Path file, final boolean whitespaces) throws IOException {
    final BufferedReader in = new BufferedReader(new FileReader(file.toFile()));
    final StringBuilder sBuilder = new StringBuilder();
    for (String line = in.readLine(); line != null; line = in.readLine()) {
      if (whitespaces) {
        sBuilder.append(line).append(CharsForSerializing.NEWLINE);
      } else {
        sBuilder.append(line.trim());
      }
    }

    // Remove last newline.
    if (whitespaces) {
      sBuilder.replace(sBuilder.length() - 1, sBuilder.length(), "");
    }
    in.close();

    return sBuilder;
  }

  /**
   * Creating a test document at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createTestDocument() {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
    try (final XmlResourceSession manager = database.beginResourceSession(RESOURCE);
         final XmlNodeTrx wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
    }
  }

  /**
   * Creating a test document with comments and processing instructions at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createPICommentTestDocument() {
    final var database = XmlTestHelper.getDatabaseWithRollingHashesEnabled(PATHS.PATH1.getFile());
    try (final XmlResourceSession manager = database.beginResourceSession(RESOURCE);
         final XmlNodeTrx wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.createCommentPI(wtx);
      wtx.commit();
    }
  }

  /**
   * Generating random bytes.
   *
   * @return the random bytes
   */
  public static @NonNull byte[] generateRandomBytes(final int size) {
    final byte[] returnVal = new byte[size];
    random.nextBytes(returnVal);
    return returnVal;
  }

  /**
   * Generating a single {@link NodeKind.DumbNode} with random values.
   *
   * @return a {@link NodeKind.DumbNode} with random values
   */
  public static DataRecord generateOne() {
    return new NodeKind.DumbNode(XmlTestHelper.random.nextInt(Integer.MAX_VALUE));
  }
}
