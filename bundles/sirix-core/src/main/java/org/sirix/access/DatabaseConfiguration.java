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

package org.sirix.access;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.exception.SirixIOException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Represents a configuration of a database. Includes all settings which have to be made during the
 * creation of the database.
 *
 * @author Sebastian Graf, University of Konstanz
 */
public final class DatabaseConfiguration {

  /**
   * Paths for a {@link org.sirix.api.Database}. Each {@link org.sirix.api.Database} has the same folder
   * layout.
   */
  public enum DatabasePaths {
    /**
     * File to store db settings.
     */
    CONFIGBINARY(Paths.get("dbsetting.obj"), false),
    /**
     * File to store encryption db settings.
     */
    KEYSELECTOR(Paths.get("keyselector"), true),
    /**
     * File to store the data.
     */
    DATA(Paths.get("resources"), true),
    /**
     * Lock file.
     */
    LOCK(Paths.get(".lock"), false);

    /**
     * Location of the file.
     */
    private final Path mFile;

    /**
     * Is the location a folder or no?
     */
    private final boolean mIsFolder;

    /**
     * Constructor.
     *
     * @param file     to be set
     * @param isFolder determines if the file is a folder instead
     */
    DatabasePaths(final Path file, final boolean isFolder) {
      mFile = requireNonNull(file);
      mIsFolder = isFolder;
    }

    /**
     * Getting the file for the kind.
     *
     * @return the file to the kind
     */
    public Path getFile() {
      return mFile;
    }

    /**
     * Check if file is denoted as folder or not.
     *
     * @return boolean if file is folder
     */
    public boolean isFolder() {
      return mIsFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this enum.
     *
     * @param file to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to the one expected, 1 if the
     * structure has more folders
     */
    public static int compareStructure(final Path file) {
      requireNonNull(file);
      int existing = 0;
      for (final DatabasePaths paths : values()) {
        final Path currentFile = file.resolve(paths.getFile());
        if (Files.exists(currentFile) && !DatabasePaths.LOCK.getFile().equals(currentFile)) {
          existing++;
        }
      }
      return existing - values().length + 1;
    }
  }

  // STATIC STANDARD FIELDS
  /**
   * Identification for string.
   */
  public static final String BINARY = "0.1.0";

  /**
   * Binary version of storage.
   */
  private final String binaryVersion;

  /**
   * Path to file.
   */
  private final Path file;

  /**
   * Maximum unique resource ID.
   */
  private long maxResourceID;

  /**
   * The database type.
   */
  private DatabaseType databaseType;

  /**
   * Constructor with the path to be set.
   *
   * @param file file to be set
   */
  public DatabaseConfiguration(final Path file) {
    binaryVersion = BINARY;
    this.file = file;
  }

  /**
   * Set the database type.
   *
   * @param type the database type.
   * @return this {@link DatabaseConfiguration} instance
   */
  public DatabaseConfiguration setDatabaseType(final DatabaseType type) {
    databaseType = requireNonNull(type);
    return this;
  }

  /**
   * Get the database type.
   *
   * @return the database type
   */
  public DatabaseType getDatabaseType() {
    return databaseType;
  }

  /**
   * Set unique maximum resource ID.
   *
   * @param id maximum resource ID
   * @return this {@link DatabaseConfiguration} instance
   */
  public DatabaseConfiguration setMaximumResourceID(final long id) {
    checkArgument(id >= 0, "ID must be >= 0!");
    maxResourceID = id;
    return this;
  }

  /**
   * Get maximum resource transactions.
   *
   * @return maximum resource ID
   */
  public long getMaxResourceID() {
    return maxResourceID;
  }

  /**
   * Getting the database file.
   *
   * @return the database file
   */
  public Path getDatabaseFile() {
    return file;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("File", file).add("Binary Version", binaryVersion).toString();
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof DatabaseConfiguration))
      return false;

    final var other = (DatabaseConfiguration) obj;
    return Objects.equal(file, other.file) && Objects.equal(binaryVersion, other.binaryVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(file, binaryVersion);
  }

  /**
   * Get the configuration file.
   *
   * @return configuration file
   */
  public Path getConfigFile() {
    return file.resolve(DatabasePaths.CONFIGBINARY.getFile());
  }

  /**
   * Get the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return file.getFileName().toString();
  }

  /**
   * Serializing a {@link DatabaseConfiguration} to a json file.
   *
   * @param config to be serialized
   * @throws SirixIOException if an I/O error occurs
   */
  public static void serialize(final DatabaseConfiguration config) throws SirixIOException {
    try (final FileWriter fileWriter = new FileWriter(config.getConfigFile().toFile());
         final JsonWriter jsonWriter = new JsonWriter(fileWriter)) {
      jsonWriter.beginObject();
      final String filePath = config.file.toAbsolutePath().toString();
      jsonWriter.name("file").value(filePath);
      jsonWriter.name("ID").value(config.maxResourceID);
      jsonWriter.name("databaseType").value(config.databaseType.toString());
      jsonWriter.endObject();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Generate a DatabaseConfiguration out of a file.
   *
   * @param file where the DatabaseConfiguration lies in as json
   * @return a new {@link DatabaseConfiguration} class
   * @throws SirixIOException if an I/O error occurs
   */
  public static DatabaseConfiguration deserialize(final Path file) {
    try (final FileReader fileReader = new FileReader(
        file.toAbsolutePath().resolve(DatabasePaths.CONFIGBINARY.getFile()).toFile());
         final JsonReader jsonReader = new JsonReader(fileReader)) {
      jsonReader.beginObject();
      final String fileName = jsonReader.nextName();
      assert fileName.equals("file");
      final Path dbFile = Paths.get(jsonReader.nextString());
      final String IDName = jsonReader.nextName();
      assert IDName.equals("ID");
      final int ID = jsonReader.nextInt();
      final String databaseType = jsonReader.nextName();
      assert databaseType.equals("databaseType");
      final String type = jsonReader.nextString();
      jsonReader.endObject();
      final DatabaseType dbType = DatabaseType.fromString(type)
                                              .orElseThrow(() -> new IllegalStateException("Type can not be unknown."));
      return new DatabaseConfiguration(dbFile).setMaximumResourceID(ID).setDatabaseType(dbType);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
