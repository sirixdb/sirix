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
import javax.annotation.Nonnull;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.DatabaseType;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.xdm.XdmResourceManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.node.Kind.DumbNode;
import org.sirix.node.interfaces.Record;
import org.sirix.settings.CharsForSerializing;
import org.sirix.utils.XdmDocumentCreator;

/**
 *
 * Helper class for offering convenient usage of {@link XdmResourceManagerImpl}s for test cases.
 *
 * This includes instantiation of databases plus resources.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class XdmTestHelper {

  /** Temporary directory path. */
  private static final String TMPDIR = System.getProperty("java.io.tmpdir");

  /** Common resource name. */
  public static final String RESOURCE = "shredded";

  /** Paths where the data is stored to. */
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
      return config.setDatabaseType(DatabaseType.XDM);
    }

  }

  /** Common random instance for generating common tag names. */
  public final static Random random = new Random();

  /** Path <=> Database instances. */
  private final static Map<Path, Database<XdmResourceManager>> INSTANCES = new Hashtable<>();

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
  public static final Database<XdmResourceManager> getDatabase(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      try {
        final DatabaseConfiguration config = new DatabaseConfiguration(file);
        if (!Files.exists(file)) {
          Databases.createXdmDatabase(config);
        }
        final var database = Databases.openXdmDatabase(file);
        database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
        INSTANCES.put(file, database);
        return database;
      } catch (final SirixRuntimeException e) {
        fail(e.toString());
        return null;
      }
    }
  }

  /**
   * Deleting all resources as defined in the enum {@link PATHS}.
   *
   * @throws SirixException
   */
  @Ignore
  public static final void deleteEverything() {
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
  public static final void closeEverything() {
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
   * @param file the file to read
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
        sBuilder.append(line + CharsForSerializing.NEWLINE);
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
    final var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
    try (final XdmResourceManager manager = database.getResourceManager(RESOURCE);
        final XdmNodeTrx wtx = manager.beginNodeTrx()) {
      XdmDocumentCreator.create(wtx);
      wtx.commit();
    }
  }

  /**
   * Creating a test document with comments and processing instructions at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createPICommentTestDocument() {
    final var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
    try (final XdmResourceManager manager = database.getResourceManager(RESOURCE);
        final XdmNodeTrx wtx = manager.beginNodeTrx()) {
      XdmDocumentCreator.createCommentPI(wtx);
      wtx.commit();
    }
  }

  /**
   * Generating random bytes.
   *
   * @return the random bytes
   */
  public static final @Nonnull byte[] generateRandomBytes(final int size) {
    final byte[] returnVal = new byte[size];
    random.nextBytes(returnVal);
    return returnVal;
  }

  /**
   * Generating a single {@link DumbNode} with random values.
   *
   * @return a {@link DumbNode} with random values
   */
  public static final Record generateOne() {
    return new DumbNode(XdmTestHelper.random.nextInt(Integer.MAX_VALUE));
  }
}
