/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.access;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.sirix.api.Database;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.exception.SirixIOException;

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
   * Paths for a {@link Database}. Each {@link Database} has the same folder layout.
   */
  public enum DatabasePaths {
    /**
     * File to store db settings.
     */
    CONFIG_BINARY(Paths.get("dbsetting.obj"), false),
    /**
     * File to store encryption db settings.
     */
    KEY_SELECTOR(Paths.get("keyselector"), true),
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
     * @param file to be set
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
     *         structure has more folders
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
   * Path to file.
   */
  private final Path file;

  /**
   * Maximum unique resource ID.
   */
  private long maxResourceID;

  /**
   * Unique database ID to distinguish this database from others in a global BufferManager.
   */
  private long databaseId;

  /**
   * The database type.
   */
  private DatabaseType databaseType;

  /**
   * System property to configure maximum segment allocation size. Value should be in bytes, e.g.,
   * "17179869184" for 16GB. Alternatively, use suffixes: "16G", "16GB", "16384M", "16384MB".
   */
  public static final String SEGMENT_ALLOCATION_SIZE_PROPERTY = "sirix.allocator.maxSize";

  /**
   * Default maximum segment allocation size: 16GB. Can be overridden via system property
   * {@link #SEGMENT_ALLOCATION_SIZE_PROPERTY}.
   */
  private static final long DEFAULT_SEGMENT_ALLOCATION_SIZE =
      parseSegmentSize(System.getProperty(SEGMENT_ALLOCATION_SIZE_PROPERTY, "16G"));

  /**
   * Parse segment size from string with optional suffix (G, GB, M, MB, K, KB).
   */
  private static long parseSegmentSize(String value) {
    if (value == null || value.isEmpty()) {
      return 16L * (1L << 30); // 16GB fallback
    }
    value = value.trim().toUpperCase();
    long multiplier = 1;
    if (value.endsWith("GB")) {
      multiplier = 1L << 30;
      value = value.substring(0, value.length() - 2);
    } else if (value.endsWith("G")) {
      multiplier = 1L << 30;
      value = value.substring(0, value.length() - 1);
    } else if (value.endsWith("MB")) {
      multiplier = 1L << 20;
      value = value.substring(0, value.length() - 2);
    } else if (value.endsWith("M")) {
      multiplier = 1L << 20;
      value = value.substring(0, value.length() - 1);
    } else if (value.endsWith("KB")) {
      multiplier = 1L << 10;
      value = value.substring(0, value.length() - 2);
    } else if (value.endsWith("K")) {
      multiplier = 1L << 10;
      value = value.substring(0, value.length() - 1);
    }
    try {
      return Long.parseLong(value.trim()) * multiplier;
    } catch (NumberFormatException e) {
      return 16L * (1L << 30); // 16GB fallback on parse error
    }
  }

  /**
   * Maximum buffer size for memory segment allocation. Default is 16GB (configurable via
   * -Dsirix.allocator.maxSize=XXG). With global buffer pool, this budget is shared across all
   * databases, so larger is better.
   */
  private long maxSegmentAllocationSize = DEFAULT_SEGMENT_ALLOCATION_SIZE;

  /**
   * Constructor with the path to be set.
   *
   * @param file file to be set
   */
  public DatabaseConfiguration(final Path file) {
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
   * Set the maximum buffer size for memory segment allocation.
   *
   * @param size maximum buffer size in bytes
   * @return this {@link DatabaseConfiguration} instance
   */
  public DatabaseConfiguration setMaxSegmentAllocationSize(final long size) {
    checkArgument(size > 0, "Max buffer size must be positive");
    this.maxSegmentAllocationSize = size;
    return this;
  }

  /**
   * Get the maximum buffer size for memory segment allocation.
   *
   * @return maximum buffer size in bytes
   */
  public long getMaxSegmentAllocationSize() {
    return maxSegmentAllocationSize;
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
   * Set unique database ID.
   *
   * @param id database ID
   * @return this {@link DatabaseConfiguration} instance
   */
  public DatabaseConfiguration setDatabaseId(final long id) {
    checkArgument(id >= 0, "Database ID must be >= 0!");
    databaseId = id;
    return this;
  }

  /**
   * Get the unique database ID.
   *
   * @return database ID
   */
  public long getDatabaseId() {
    return databaseId;
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
    return MoreObjects.toStringHelper(this).add("File", file).toString();
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof DatabaseConfiguration other))
      return false;

    return Objects.equal(file, other.file);
  }

  @Override
  public int hashCode() {
    return file.hashCode();
  }

  /**
   * Get the configuration file.
   *
   * @return configuration file
   */
  public Path getConfigFile() {
    return file.resolve(DatabasePaths.CONFIG_BINARY.getFile());
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
      jsonWriter.name("databaseId").value(config.databaseId);
      jsonWriter.name("databaseType").value(config.databaseType.toString());
      jsonWriter.name("maxSegmentAllocationSize").value(config.maxSegmentAllocationSize);
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
    try (
        final FileReader fileReader =
            new FileReader(file.toAbsolutePath().resolve(DatabasePaths.CONFIG_BINARY.getFile()).toFile());
        final JsonReader jsonReader = new JsonReader(fileReader)) {
      jsonReader.beginObject();
      final String fileName = jsonReader.nextName();
      assert fileName.equals("file");
      final Path dbFile = Paths.get(jsonReader.nextString());
      final String IDName = jsonReader.nextName();
      assert IDName.equals("ID");
      final int ID = jsonReader.nextInt();

      // Read databaseId if present (for backward compatibility)
      long databaseId = -1;
      String nextName = jsonReader.nextName();
      if (nextName.equals("databaseId")) {
        databaseId = jsonReader.nextLong();
        nextName = jsonReader.nextName();
      }

      assert nextName.equals("databaseType");
      final String type = jsonReader.nextString();
      final String maxSegmentAllocationSizeName = jsonReader.nextName();
      assert maxSegmentAllocationSizeName.equals("maxSegmentAllocationSize");
      final long maxSegmentAllocationSize = jsonReader.nextLong();
      jsonReader.endObject();
      final DatabaseType dbType =
          DatabaseType.fromString(type).orElseThrow(() -> new IllegalStateException("Type can not be unknown."));

      final DatabaseConfiguration config =
          new DatabaseConfiguration(dbFile).setMaximumResourceID(ID)
                                           .setDatabaseType(dbType)
                                           .setMaxSegmentAllocationSize(maxSegmentAllocationSize);

      // If databaseId was present in file, use it; otherwise it will be assigned later
      if (databaseId >= 0) {
        config.setDatabaseId(databaseId);
      }

      return config;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
