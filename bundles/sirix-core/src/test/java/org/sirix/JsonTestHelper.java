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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.DatabaseType;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.utils.JsonDocumentCreator;

/**
 *
 * Helper class for offering convenient usage of the {@link JsonResourceManager} for test cases.
 *
 * This includes instantiation of databases plus resources.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class JsonTestHelper {

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
      config = new DatabaseConfiguration(file).setDatabaseType(DatabaseType.JSON);
    }

    public Path getFile() {
      return file;
    }

    public DatabaseConfiguration getConfig() {
      return config;
    }

  }

  /** Common random instance for generating common tag names. */
  public final static Random random = new Random();

  /** Path <=> Database instances. */
  private final static Map<Path, Database<JsonResourceManager>> INSTANCES = new HashMap<>();

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
  public static final Database<JsonResourceManager> getDatabase(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      try {
        final DatabaseConfiguration config = new DatabaseConfiguration(file);
        if (!Files.exists(file)) {
          Databases.createJsonDatabase(config);
        }
        final var database = Databases.openJsonDatabase(file);
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
   * @throws SirixException if anything went wrong
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
   * @throws SirixException if anything went wrong
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
   * Creating a test document at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createTestDocument() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(RESOURCE).build());
    try (final JsonResourceManager manager = database.openResourceManager(RESOURCE);
        final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
    }
  }
}
