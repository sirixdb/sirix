/*
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

package io.sirix;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Helper class for offering convenient usage of the {@link JsonResourceSession} for test cases.
 * This includes instantiation of databases plus resources.
 *
 * @author Johannes Lichtenberger
 */
public final class JsonTestHelper {

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
    PATH1(Paths.get(TMPDIR, "sirix", "json-path1")),

    // PATH2 (Sirix)
    PATH2(Paths.get(TMPDIR, "sirix", "json-path2")),

    // PATH3 (JSON)
    PATH3(Paths.get(TMPDIR, "json", "test.json"));

    final Path file;

    PATHS(final Path file) {
      this.file = file;
    }

    public Path getFile() {
      return file;
    }
  }

  /**
   * Common random instance for generating common tag names.
   */
  public final static Random random = new Random();

  /**
   * Path <=> Database instances.
   */
  private final static Map<Path, Database<JsonResourceSession>> INSTANCES = new HashMap<>();

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
  public static Database<JsonResourceSession> getDatabase(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createJsonDatabase(config);
      }
      final var database = Databases.openJsonDatabase(file);
      if (!database.existsResource(RESOURCE)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }
      INSTANCES.put(file, database);
      return database;
    }
  }

  public static void createDatabase(final Path file) {
    final DatabaseConfiguration config = new DatabaseConfiguration(file);
    if (!Files.exists(file)) {
      Databases.createJsonDatabase(config);
    }
  }

  /**
   * Getting a database and create one if not existing, using a custom ResourceConfiguration.
   *
   * @param file           to be created
   * @param resourceConfig the custom resource configuration to use
   * @return a database-obj
   */
  @Ignore
  public static Database<JsonResourceSession> getDatabaseWithResourceConfig(final Path file,
      final ResourceConfiguration resourceConfig) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createJsonDatabase(config);
      }
      final var database = Databases.openJsonDatabase(file);
      if (!database.existsResource(RESOURCE)) {
        database.createResource(resourceConfig);
      }
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
  public static Database<JsonResourceSession> getDatabaseWithDeweyIdsEnabled(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createJsonDatabase(config);
      }
      final var database = Databases.openJsonDatabase(file);
      if (!database.existsResource(RESOURCE)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).useDeweyIDs(true).build());
      }
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
  public static Database<JsonResourceSession> getDatabaseWithHashesEnabled(final Path file) {
    if (INSTANCES.containsKey(file)) {
      return INSTANCES.get(file);
    } else {
      final DatabaseConfiguration config = new DatabaseConfiguration(file);
      if (!Files.exists(file)) {
        Databases.createJsonDatabase(config);
      }
      final var database = Databases.openJsonDatabase(file);
      if (!database.existsResource(RESOURCE)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).hashKind(HashType.ROLLING).build());
      }
      INSTANCES.put(file, database);
      return database;
    }
  }

  /**
   * Deleting all resources as defined in the enum {@link PATHS}.
   *
   * @throws SirixException if anything went wrong
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
   * @throws SirixException if anything went wrong
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
   * Creating a test document at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createTestDocument() {
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
    }
  }

  /**
   * Creating a test document at {@link PATHS#PATH1}.
   *
   * @throws SirixException if anything went wrong
   */
  public static void createTestDocumentWithDeweyIdsEnabled() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
    }
  }
}
