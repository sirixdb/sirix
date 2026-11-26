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

import io.sirix.exception.UnsupportedFormatVersionException;
import io.sirix.utils.ToStringHelper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import io.sirix.BinaryEncodingVersion;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.HashAlgorithm;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerKind;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.node.NodeSerializerImpl;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.settings.RegionCompressionType;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import io.sirix.utils.ChildCounts;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

import static io.sirix.utils.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Holds the settings for a resource which acts as a base for session that can not change. This
 * includes all settings which are persistent. Each {@link ResourceConfiguration} is furthermore
 * bound to one fixed database denoted by a related {@link DatabaseConfiguration}.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class ResourceConfiguration {

  /**
   * Paths in a resource. Each resource has the same folder layout.
   */
  public enum ResourcePaths {

    /**
     * Folder for storage of data.
     */
    DATA(Paths.get("data"), true),

    /**
     * Folder for the transaction log.
     */
    TRANSACTION_INTENT_LOG(Paths.get("log"), true),

    /**
     * File to store the resource settings.
     */
    CONFIG_BINARY(Paths.get("ressetting.obj"), false),

    /**
     * File to store index definitions.
     */
    INDEXES(Paths.get("indexes"), true),

    /**
     * Folder to store the encryption key.
     */
    ENCRYPTION_KEY(Paths.get("encryption"), true),

    /**
     * Folder to store the update operations.
     */
    UPDATE_OPERATIONS(Paths.get("update-operations"), true);

    /**
     * Location of the file.
     */
    private final Path path;

    /**
     * Is the location a folder or no?
     */
    private final boolean isFolder;

    /**
     * Constructor.
     *
     * @param path the path
     * @param isFolder determines if the path denotes a filer or not
     */
    ResourcePaths(final Path path, final boolean isFolder) {
      this.path = path;
      this.isFolder = isFolder;
    }

    /**
     * Getting the path.
     *
     * @return the path
     */
    public Path getPath() {
      return path;
    }

    /**
     * Check if file is denoted as folder or not.
     *
     * @return {@code true} if file is a folder, {@code false} otherwise
     */
    public boolean isFolder() {
      return isFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this enum.
     *
     * @param file to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to the one expected, 1 if the
     *         structure has more folders
     * @throws NullPointerException if {@code file} is {@code null}
     */
    public static int compareStructure(final Path file) {
      int existing = 0;
      for (final ResourcePaths paths : values()) {
        final Path currentFile = file.resolve(paths.getPath());
        if (Files.exists(currentFile)) {
          existing++;
        }
      }
      return existing - values().length;
    }
  }

  // FIXED STANDARD FIELDS
  /**
   * Standard storage.
   */
  private static final StorageType STORAGE = StorageType.FILE_CHANNEL;
  // OS.isWindows() ? StorageType.FILE_CHANNEL : OS.is64Bit() ? StorageType.MEMORY_MAPPED :
  // StorageType.FILE_CHANNEL;

  /**
   * Standard versioning approach.
   */
  private static final VersioningType VERSIONING = VersioningType.SLIDING_SNAPSHOT;

  /** Name of the system property that overrides {@link #DEFAULT_HASH_TYPE} for a whole JVM. */
  public static final String HASH_TYPE_PROPERTY = "sirix.hashType";

  /**
   * Default node-hash kind for a newly created resource: {@link HashType#NONE}. Hashing is an
   * explicit choice — call {@link Builder#hashKind(HashType)} to maintain an incremental Merkle-tree
   * hash ({@link HashType#ROLLING}) or a postorder hash. Override the default for a whole JVM with
   * {@code -Dsirix.hashType=ROLLING}.
   *
   * <p>
   * An <em>existing</em> resource is unaffected: its hash kind is persisted in the resource settings
   * and read back by {@link #deserialize(Path)}, so a store created with hashes keeps them.
   *
   * <p>
   * Hashing is a change-detection aid, not cryptographic tamper evidence. Enabling it retains
   * value-derived ancestor hashes even after a value is deleted. See
   * <a href="https://github.com/sirixdb/sirix/blob/main/docs/ERASURE_PREPARATION.md">erasure
   * preparation</a> for capabilities, storage costs and privacy consequences.
   */
  public static final HashType DEFAULT_HASH_TYPE = defaultHashType(System.getProperty(HASH_TYPE_PROPERTY));

  /**
   * Resolve {@link #DEFAULT_HASH_TYPE} from the {@value #HASH_TYPE_PROPERTY} system property.
   *
   * @param property the property's value, or {@code null} when it is unset
   * @return {@link HashType#NONE} when the property is unset, else the named hash type
   * @throws IllegalArgumentException if the property names no hash type
   */
  static HashType defaultHashType(final String property) {
    return property == null
        ? HashType.NONE
        : HashType.fromString(property);
  }

  /**
   * Versions to restore.
   */
  private static final int VERSIONS_TO_RESTORE = 3;

  /**
   * Serializer for records.
   */
  private static final RecordSerializer NODE_SERIALIZER = new NodeSerializerImpl();

  /**
   * The current binary encoding version. Single version (V0) — the on-disk format includes a version
   * byte to reserve room for future bumps, but there are no alternative encodings to select at
   * runtime.
   */
  public static final BinaryEncodingVersion BINARY_ENCODING_VERSION = BinaryEncodingVersion.V0;

  /**
   * Stable identifier of the node-hash function baked into the on-disk format (XXH3-64 via
   * {@code LongHashFunction.xx3()}). Persisted in the resource settings and validated on open so that
   * a future change of the hash function is detectable instead of silently re-verifying old resources
   * against the wrong function.
   */
  public static final String NODE_HASH_FUNCTION_ID = "XX3";

  // END FIXED STANDARD FIELDS

  // MEMBERS FOR FIXED FIELDS
  /**
   * Type of Storage (File, BerkeleyDB).
   */
  public final StorageType storageType;

  /**
   * Kind of revisioning (Full, Incremental, Differential).
   */
  public final VersioningType versioningType;

  /**
   * Kind of integrity hash (rolling, postorder).
   */
  public final HashType hashType;

  /**
   * Number of revisions to restore a complete set of data.
   */
  public final int maxNumberOfRevisionsToRestore;

  /**
   * Byte handler pipeline.
   */
  public final ByteHandlerPipeline byteHandlePipeline;

  /**
   * Path for the resource to be associated.
   */
  public Path resourcePath;

  /**
   * DatabaseConfiguration for this {@link ResourceConfiguration}.
   */
  private DatabaseConfiguration databaseConfig;

  /**
   * Determines if text-compression should be used or not (default is true).
   */
  public final boolean useTextCompression;

  /**
   * Determines if a path summary should be build and kept up to date or not.
   */
  public final boolean withPathSummary;

  /**
   * Determines if per-path value statistics (count, nullCount, sum, min, max, HLL) are maintained on
   * the PathSummary nodes. Requires {@link #withPathSummary} to be true. When enabled, aggregate
   * queries over unfiltered paths ({@code sum/avg/min/max}, {@code countDistinct}) can short-circuit
   * via the PathSummary without scanning data pages — typically microseconds instead of seconds.
   */
  public final boolean withPathStatistics;

  /**
   * Retained bulk-loader tuning request. JSON auto-committing imports now maintain hashes and
   * descendant counts incrementally at every publication boundary regardless of this flag: a final
   * repair cannot fix already-published intermediate revisions. Non-auto-committing JSON imports
   * repair the complete newly inserted forest once, without walking unchanged sibling subtrees.
   */
  public final boolean repairBulkInsertHashes;

  /**
   * Persistents records / commonly nodes.
   */
  public final RecordSerializer recordPersister;

  /**
   * Unique ID.
   */
  private long id;

  /**
   * Determines if dewey IDs are generated and stored or not.
   */
  public final boolean areDeweyIDsStored;

  /**
   * The distance between sibling DeweyIDs when generating new IDs.
   * 
   * <p>
   * Larger values (e.g., 64, 128) are better for high-insert workloads as they reduce the chance of
   * needing to overflow into deeper levels. Smaller values (e.g., 8, 16) result in more compact
   * DeweyIDs for read-mostly workloads.
   * </p>
   * 
   * <p>
   * Default is 16, which provides a good balance for mixed workloads.
   * </p>
   */
  public final int deweyIdSiblingDistance;

  /**
   * The hash function used for hashing nodes.
   */
  public final LongHashFunction nodeHashFunction;

  /**
   * The name of the resource.
   */
  private final String resourceName;

  /**
   * Determines whether resource child count should be tracked
   */
  private final boolean storeChildCount;

  /**
   * Determines if diffs are going to be stored or not.
   */
  private final boolean storeDiffs;

  /**
   * Determines if custom commit timestamps should be stored or not.
   */
  private final boolean customCommitTimestamps;

  /**
   * Store the full node history of each record.
   */
  private final boolean storeNodeHistory;

  private final boolean aggregateKernel;

  /**
   * Binary encoding version of storage.
   */
  private final BinaryEncodingVersion binaryVersion;

  /**
   * String compression type for string-containing nodes.
   */
  public final StringCompressionType stringCompressionType;

  /**
   * How PAX region payloads are compressed on the wire — the explicit speed/size dial.
   * Self-describing per payload on disk, so changing it never invalidates existing pages.
   */
  public final RegionCompressionType regionCompressionType;

  /**
   * Whether to verify page checksums on read operations.
   * 
   * <p>
   * When enabled, each page read from storage is verified against its stored XXH3 hash to detect
   * corruption. Uses zero-copy verification for native memory segments (~15 GB/s throughput).
   * </p>
   * 
   * <p>
   * Default is {@code true} for data integrity. Disable only if benchmarking.
   * </p>
   */
  public final boolean verifyChecksumsOnRead;

  /** Salt record pages on first serialization; disable only for reproducible test fixtures. */
  public final boolean saltRecordPages;

  /**
   * The hash algorithm used for page checksums.
   *
   * <p>
   * Default is {@link HashAlgorithm#XXH3}, which provides ~15 GB/s throughput. The algorithm can be
   * changed for future extensibility, though changing it requires re-writing all pages.
   * </p>
   */
  public final HashAlgorithm hashAlgorithm;

  /**
   * Configuration for valid time support in bitemporal queries.
   *
   * <p>
   * When set, this enables first-class valid time support, allowing optimized bitemporal queries
   * using functions like jn:valid-at() and jn:open-bitemporal().
   * </p>
   *
   * <p>
   * The configuration specifies which JSON paths hold the validFrom and validTo timestamps for valid
   * time intervals.
   * </p>
   */
  public final ValidTimeConfig validTimeConfig;

  /**
   * Per-resource identity UUID, generated at resource creation and written into both files'
   * superblocks (reserved bytes [40, 56)). Cross-links the binary files to this settings file:
   * opening a data file with a different resource's settings — the classic "restored the JSON from
   * the wrong backup" corruption — fails fast instead of misreading. Never {@code null} and never the
   * all-zero UUID: 1.0 settings must identify their resource (FMT-102).
   */
  public final UUID resourceUuid;

  /**
   * The resource incarnation id (FMT-209): an independent uniform unsigned 64-bit draw from a
   * cryptographically secure RNG, persisted in the settings as {@code incarnationId}, 16 lowercase
   * hexadecimal digits (CONF-102). It identifies the incarnation and timeline the revision numbers
   * belong to, so it changes only where that timeline ends: resource creation (including recreation),
   * truncate (TXN-404), whole-resource replacement (REST-210, which creates a new resource) and
   * backup restore (TXN-613/614). Ordinary commits, reopen and settings rewrites keep it. Read at
   * open and settings level only, never per record.
   */
  private volatile long incarnationId;

  // END MEMBERS FOR FIXED FIELDS

  /** One shared generator; {@link SecureRandom} is thread-safe and the draws are rare. */
  private static final SecureRandom INCARNATION_ID_RANDOM = new SecureRandom();

  /** CONF-102: the encoded length of an incarnation id. */
  private static final int INCARNATION_ID_DIGITS = 16;

  private static final HexFormat INCARNATION_ID_FORMAT = HexFormat.of();

  /**
   * Get a new builder instance.
   *
   * @param resource the name of the resource
   * @return {@link Builder} instance
   * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
   */
  public static Builder newBuilder(final String resource) {
    return new Builder(resource);
  }

  static String canonicalResourceName(final String name) {
    final Path path = Path.of(requireNonNull(name)).normalize();
    final String canonicalName = path.toString();
    if (path.getRoot() != null || path.getNameCount() != 1 || canonicalName.isEmpty() || canonicalName.equals("..")) {
      throw new SirixUsageException("Invalid resource name: " + name);
    }
    return canonicalName;
  }

  /**
   * Convenience constructor using the standard settings.
   *
   * @param builder {@link Builder} reference
   */
  private ResourceConfiguration(final ResourceConfiguration.Builder builder) {
    storageType = builder.type;
    byteHandlePipeline = builder.byteHandler;
    versioningType = builder.revisionKind;
    hashType = builder.hashType;
    maxNumberOfRevisionsToRestore = builder.maxNumberOfRevisionsToRestore;
    useTextCompression = builder.useTextCompression;
    withPathSummary = builder.pathSummary;
    withPathStatistics = builder.pathStatistics;
    repairBulkInsertHashes = builder.repairBulkInsertHashes;
    areDeweyIDsStored = builder.useDeweyIDs;
    deweyIdSiblingDistance = builder.deweyIdSiblingDistance;
    recordPersister = builder.persistenter;
    resourceName = builder.resource;
    nodeHashFunction = builder.hashFunction;
    storeChildCount = builder.storeChildCount;
    storeDiffs = builder.storeDiffs;
    customCommitTimestamps = builder.customCommitTimestamps;
    storeNodeHistory = builder.storeNodeHistory;
    aggregateKernel = builder.aggregateKernel;
    binaryVersion = builder.binaryEncodingVersion;
    stringCompressionType = builder.stringCompressionType;
    regionCompressionType = builder.regionCompressionType;
    verifyChecksumsOnRead = builder.verifyChecksumsOnRead;
    saltRecordPages = builder.saltRecordPages;
    hashAlgorithm = builder.hashAlgorithm;
    validTimeConfig = builder.validTimeConfig;
    resourceUuid = builder.resourceUuid;
    // Never a sentinel (FMT-209): even a configuration that is never persisted carries a real draw.
    incarnationId = drawIncarnationId();
  }

  /**
   * The resource incarnation id (FMT-209), an unsigned 64-bit value.
   *
   * @return the id as a signed {@code long} carrying the unsigned bits
   */
  public long getIncarnationId() {
    return incarnationId;
  }

  /**
   * Encodes an incarnation id as CONF-102 persists it.
   *
   * @param incarnationId the unsigned 64-bit id
   * @return exactly 16 lowercase hexadecimal digits
   */
  public static String formatIncarnationId(final long incarnationId) {
    return INCARNATION_ID_FORMAT.toHexDigits(incarnationId);
  }

  private static long drawIncarnationId() {
    return INCARNATION_ID_RANDOM.nextLong();
  }

  /**
   * Draws the id of a newly created resource (FMT-209: creation, including recreation). Runs before
   * the first settings write, so a configuration object reused for several creations never hands one
   * incarnation id to two resources.
   */
  void drawIncarnationIdForCreation() {
    incarnationId = drawIncarnationId();
  }

  /**
   * Draws a fresh incarnation id for this persisted resource and publishes it in its settings
   * (FMT-209, TXN-404), durably where the platform provides a directory barrier. On Windows the
   * rename has no durability barrier; see {@code docs/KNOWN_LIMITATIONS.md#platform}. A failure
   * before the rename leaves both the settings and this configuration unchanged. After the rename
   * this configuration adopts the installed id even if the directory barrier or channel close fails;
   * the failure still propagates.
   *
   * @throws SirixIOException if the settings cannot be read or durably rewritten
   */
  public void publishFreshIncarnationId() {
    final long fresh = drawIncarnationId();
    writeIncarnationId(resourcePath, fresh, this);
  }

  /**
   * Draws a fresh incarnation id for the persisted resource in {@code resourceDirectory} and
   * publishes it in its settings (FMT-209: backup restore draws per restored resource before exposing
   * it), durably where the platform provides a directory barrier. On Windows the rename has no
   * durability barrier; see {@code docs/KNOWN_LIMITATIONS.md#platform}. Only the
   * {@code incarnationId} member changes; every other setting is kept as written. A failure before
   * the rename leaves the settings unchanged; a failure after it leaves the new id installed and
   * still propagates.
   *
   * @param resourceDirectory the resource directory holding {@code ressetting.obj}
   * @throws SirixIOException if the settings are not a valid 1.0 settings document or cannot be
   *         durably rewritten
   */
  public static void publishFreshIncarnationId(final Path resourceDirectory) {
    writeIncarnationId(requireNonNull(resourceDirectory), drawIncarnationId(), null);
  }

  /**
   * Replaces the persisted {@code incarnationId} member with {@code incarnationId}: write a sibling
   * temporary file, force it, atomically move it over the settings and force the directory, so a
   * crash leaves the old or the new document in full, and the new one durably once this returns where
   * the platform provides a directory barrier. On Windows the rename has no durability barrier; see
   * {@code docs/KNOWN_LIMITATIONS.md#platform}.
   */
  private static void writeIncarnationId(final Path resourceDirectory, final long incarnationId,
      final @Nullable ResourceConfiguration configuration) {
    final Path configFile = resourceDirectory.resolve(ResourcePaths.CONFIG_BINARY.getPath());
    final JsonObject settings;
    try {
      settings = JsonParser.parseString(Files.readString(configFile, StandardCharsets.UTF_8)).getAsJsonObject();
    } catch (final IOException | RuntimeException e) {
      throw new SirixIOException(configFile + ": cannot read the resource settings", e);
    }
    // The member already exists, so the replacement keeps its position in the document.
    parseIncarnationId(settings.get(INCARNATION_ID_NAME), configFile);
    settings.addProperty(INCARNATION_ID_NAME, formatIncarnationId(incarnationId));
    Path tmpFile = null;
    try {
      tmpFile = Files.createTempFile(resourceDirectory, "ressetting", ".tmp");
      try (final FileChannel channel = FileChannel.open(tmpFile, StandardOpenOption.WRITE)) {
        final ByteBuffer bytes = ByteBuffer.wrap(settings.toString().getBytes(StandardCharsets.UTF_8));
        while (bytes.hasRemaining()) {
          channel.write(bytes);
        }
        channel.force(true);
      }
      DatabaseConfiguration.moveIntoPlace(tmpFile, configFile);
      tmpFile = null;
      if (configuration != null) {
        configuration.incarnationId = incarnationId;
      }
      forceDirectory(resourceDirectory);
    } catch (final IOException e) {
      throw new SirixIOException(configFile + ": cannot publish a new incarnation id", e);
    } finally {
      if (tmpFile != null) {
        try {
          Files.deleteIfExists(tmpFile);
        } catch (final IOException ignored) {
          // Best effort: a leftover temporary file is not a settings document.
        }
      }
    }
  }

  /**
   * Makes the rename durable where a directory barrier is available. On Windows MoveFileEx runs
   * without MOVEFILE_WRITE_THROUGH and no directory handle is flushed, so a crash after return can
   * lose the new id. This is part of the engine-wide directory-metadata durability gap, including the
   * commit path's index catalogue and .commit marker, tracked as sirix-durable-file-metadata.
   * Accepted for FMT-209 by the captain decision of 2026-11-26 (option C); TXN-616 closes it before
   * 1.0 with a shared DurableFiles helper that this method will delegate to.
   */
  private static void forceDirectory(final Path directory) throws IOException {
    if (System.getProperty("os.name", "").startsWith("Windows")) {
      return;
    }
    try (final FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  /**
   * Validates a persisted {@code incarnationId} member (CONF-102): a JSON string of exactly 16
   * lowercase hexadecimal digits. Absence, any other JSON type and any other spelling are refused;
   * nothing defaults to a sentinel (FMT-209).
   */
  private static long parseIncarnationId(final @Nullable JsonElement member, final Path configFile) {
    if (member == null) {
      throw missingIncarnationId(configFile);
    }
    if (!member.isJsonPrimitive() || !member.getAsJsonPrimitive().isString()) {
      throw new SirixIOException(configFile + ": settings field '" + INCARNATION_ID_NAME
          + "' must be a string of 16 lowercase hexadecimal digits");
    }
    return parseIncarnationId(member.getAsString(), configFile);
  }

  private static long parseIncarnationId(final String value, final Path configFile) {
    if (value.length() != INCARNATION_ID_DIGITS) {
      throw malformedIncarnationId(configFile);
    }
    for (int i = 0; i < INCARNATION_ID_DIGITS; i++) {
      final char c = value.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
        throw malformedIncarnationId(configFile);
      }
    }
    return Long.parseUnsignedLong(value, 16);
  }

  private static SirixIOException malformedIncarnationId(final Path configFile) {
    return new SirixIOException(configFile + ": settings field '" + INCARNATION_ID_NAME
        + "' must be exactly 16 lowercase hexadecimal digits (CONF-102)");
  }

  private static SirixIOException missingIncarnationId(final Path configFile) {
    return new SirixIOException(configFile + ": the resource settings lack the required '" + INCARNATION_ID_NAME
        + "' member (FMT-209); every 1.0 resource carries one and none is assumed");
  }

  public BinaryEncodingVersion getBinaryEncodingVersion() {
    return binaryVersion;
  }

  public boolean customCommitTimestamps() {
    return customCommitTimestamps;
  }

  ResourceConfiguration setDatabaseConfiguration(final DatabaseConfiguration config) {
    databaseConfig = requireNonNull(config);
    resourcePath = databaseConfig.getDatabaseFile()
                                 .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                 .resolve(resourceName);
    return this;
  }

  /**
   * Set a unique ID.
   *
   * @param id the ID to set
   * @return this instance
   */
  public ResourceConfiguration setID(final long id) {
    checkArgument(id >= 0, "The ID must be >= 0!");
    this.id = id;
    return this;
  }

  /**
   * Get the storage type.
   *
   * @return The storage type.
   */
  public StorageType getStorageType() {
    return storageType;
  }

  /**
   * Get the unique ID.
   *
   * @return the unique resource ID
   */
  public long getID() {
    return id;
  }

  /**
   * Get the database ID from the parent database configuration.
   *
   * <p>
   * Returns {@code 0} when no configuration is attached, and that value is a HAZARD rather than a
   * neutral default: this id is the leading component of every JVM-global cache key in the engine —
   * {@code PageReference}, {@code HOTLookupKey}, {@code NamesCacheKey}, {@code PathSummaryCacheKey},
   * {@code RevisionRootPageCacheKey} and the HOT lookup cache. Resource ids restart at 0 in every
   * database, so two unattached resources collapse onto the same {@code (0, 0, revision, …)} key
   * space and one is served the other's cached data.
   * </p>
   *
   * <p>
   * Only the HOT lookup cache currently guards against it ({@code AbstractHOTIndexReader} disables
   * memoization for id 0). Making the unattached state impossible here is the real fix, but it cannot
   * be done from this accessor alone: {@link #setDatabaseConfiguration} is package-private and
   * {@code NodeStorageEngineReaderTest} legitimately builds a configuration without a database, so
   * throwing breaks callers that never touch a cache. Closing it properly means either widening that
   * setter and requiring it, or applying the id-0 guard at the other five key sites.
   * </p>
   *
   * @return the database ID, or 0 if database configuration not set
   */
  public long getDatabaseId() {
    return databaseConfig != null
        ? databaseConfig.getDatabaseId()
        : 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageType, versioningType, hashType, resourcePath, databaseConfig);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ResourceConfiguration other))
      return false;

    return Objects.equals(storageType, other.storageType) && Objects.equals(versioningType, other.versioningType)
        && Objects.equals(hashType, other.hashType) && Objects.equals(resourcePath, other.resourcePath)
        && Objects.equals(databaseConfig, other.databaseConfig);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("Resource", resourcePath)
                         .add("Type", storageType)
                         .add("Revision", versioningType)
                         .add("HashKind", hashType)
                         .toString();
  }

  /**
   * Get the resource.
   *
   * @return the resource
   */
  public Path getResource() {
    return resourcePath;
  }

  /**
   * Get the resource name.
   *
   * @return the resource name
   */
  public String getName() {
    return resourceName;
  }

  public boolean storeDiffs() {
    return storeDiffs;
  }

  /**
   * Get the configuration file.
   *
   * @return configuration file
   */
  public Path getConfigFile() {
    return resourcePath.resolve(ResourcePaths.CONFIG_BINARY.getPath());
  }

  public boolean storeChildCount() {
    return storeChildCount;
  }

  /** Whether this internal resource runs the aggregate kernel directly. */
  public boolean aggregateKernel() {
    return aggregateKernel;
  }

  public boolean storeNodeHistory() {
    return storeNodeHistory;
  }

  /**
   * Get the valid time configuration.
   *
   * @return the valid time configuration, or null if not configured
   */
  public ValidTimeConfig getValidTimeConfig() {
    return validTimeConfig;
  }

  /**
   * Check if valid time support is enabled for this resource.
   *
   * @return true if valid time is configured, false otherwise
   */
  public boolean hasValidTimeSupport() {
    return validTimeConfig != null;
  }

  /**
   * JSON names.
   */
  private static final String[] JSONNAMES = {"binaryEncoding", "revisioning", "revisioningClass",
      "numbersOfRevisiontoRestore", "byteHandlerClasses", "storageKind", "hashKind", "hashFunction", "compression",
      "pathSummary", "resourceID", "deweyIDsStored", "persistenter", "storeDiffs", "customCommitTimestamps",
      "storeNodeHistory", "storeChildCount", "stringCompressionType", "deweyIdSiblingDistance", "verifyChecksumsOnRead",
      "hashAlgorithm", "validTimeConfig", "validFromPath", "validToPath", "pathStatistics", "repairBulkInsertHashes",
      "resourceUuid", "regionCompression", "aggregateKernel", "saltRecordPages", "incarnationId"};

  /** CONF-102 / FMT-209 member name. */
  private static final String INCARNATION_ID_NAME = JSONNAMES[30];

  /**
   * Serialize the configuration.
   *
   * @param config configuration to serialize
   * @throws SirixIOException if an I/O error occurs
   */
  public static void serialize(final ResourceConfiguration config) throws SirixIOException {
    final Path configFile = config.getConfigFile();
    try (final FileWriter fileWriter = new FileWriter(configFile.toFile());
        final JsonWriter jsonWriter = new JsonWriter(fileWriter)) {
      jsonWriter.beginObject();
      // Binary encoding | page layout.
      jsonWriter.name(JSONNAMES[0]).value(config.binaryVersion.name());
      // Versioning.
      jsonWriter.name(JSONNAMES[1]);
      jsonWriter.beginObject();
      jsonWriter.name(JSONNAMES[2]).value(config.versioningType.name());
      jsonWriter.name(JSONNAMES[3]).value(config.maxNumberOfRevisionsToRestore);
      jsonWriter.endObject();
      // ByteHandlers.
      jsonWriter.name(JSONNAMES[4]);
      jsonWriter.beginArray();
      for (final ByteHandler handler : config.byteHandlePipeline.getComponents()) {
        ByteHandlerKind.getKind(handler.getClass()).serialize(handler, jsonWriter);
      }
      jsonWriter.endArray();
      // Storage type.
      jsonWriter.name(JSONNAMES[5]).value(config.storageType.name());
      // Hashing type.
      jsonWriter.name(JSONNAMES[6]).value(config.hashType.name());
      // Hash function — stable identifier, validated on deserialize.
      jsonWriter.name(JSONNAMES[7]).value(NODE_HASH_FUNCTION_ID);
      // Text compression.
      jsonWriter.name(JSONNAMES[8]).value(config.useTextCompression);
      // Path summary.
      jsonWriter.name(JSONNAMES[9]).value(config.withPathSummary);
      // ID.
      jsonWriter.name(JSONNAMES[10]).value(config.id);
      // Dewey IDs stored or not.
      jsonWriter.name(JSONNAMES[11]).value(config.areDeweyIDsStored);
      // Persistenter.
      jsonWriter.name(JSONNAMES[12]).value(config.recordPersister.getClass().getName());
      // Diffs.
      jsonWriter.name(JSONNAMES[13]).value(config.storeDiffs);
      // Custom commit timestamps.
      jsonWriter.name(JSONNAMES[14]).value(config.customCommitTimestamps);
      // Node history.
      jsonWriter.name(JSONNAMES[15]).value(config.storeNodeHistory);
      // Child count.
      jsonWriter.name(JSONNAMES[16]).value(config.storeChildCount);
      // String compression type.
      jsonWriter.name(JSONNAMES[17]).value(config.stringCompressionType.name());
      // DeweyID sibling distance.
      jsonWriter.name(JSONNAMES[18]).value(config.deweyIdSiblingDistance);
      // Verify checksums on read.
      jsonWriter.name(JSONNAMES[19]).value(config.verifyChecksumsOnRead);
      // Hash algorithm for checksums.
      jsonWriter.name(JSONNAMES[20]).value(config.hashAlgorithm.name());
      // Valid time configuration.
      if (config.validTimeConfig != null) {
        jsonWriter.name(JSONNAMES[21]);
        jsonWriter.beginObject();
        jsonWriter.name(JSONNAMES[22]).value(config.validTimeConfig.getValidFromPath());
        jsonWriter.name(JSONNAMES[23]).value(config.validTimeConfig.getValidToPath());
        jsonWriter.endObject();
      }
      // Path statistics.
      jsonWriter.name(JSONNAMES[24]).value(config.withPathStatistics);
      // Retained bulk-loader tuning request.
      jsonWriter.name(JSONNAMES[25]).value(config.repairBulkInsertHashes);
      // Resource identity UUID (cross-linked to both superblocks); always written (FMT-102).
      jsonWriter.name(JSONNAMES[26]).value(config.resourceUuid.toString());
      // Region wire compression (speed/size dial).
      jsonWriter.name(JSONNAMES[27]).value(config.regionCompressionType.name());
      jsonWriter.name(JSONNAMES[28]).value(config.aggregateKernel);
      jsonWriter.name(JSONNAMES[29]).value(config.saltRecordPages);
      // Resource incarnation id (FMT-209), a string: JSON numbers cannot hold every u64 (CONF-102).
      jsonWriter.name(INCARNATION_ID_NAME).value(formatIncarnationId(config.incarnationId));
      jsonWriter.endObject();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    // Database config.
    DatabaseConfiguration.serialize(config.databaseConfig);
  }

  /**
   * Reads the next field name and fails fast when it is not the expected one. The resource settings
   * are format identity: required fields must not be silently misparsed (this used to be an
   * {@code assert}, a no-op in production). Tail fields are dispatched separately by name; see
   * {@code spec/CONF-configuration.md} for the settings contract.
   */
  private static void expectField(final JsonReader jsonReader, final String expected, final Path configFile)
      throws IOException {
    final String name = jsonReader.nextName();
    if (!name.equals(expected)) {
      throw new SirixIOException(
          configFile + ": malformed resource settings — expected field '" + expected + "' but found '" + name + "'");
    }
  }

  /**
   * Deserializing a Resource configuration from a JSON-file from the persistent storage. //todo add
   * track child count parameter here
   *
   * @param file where the resource lies in.
   * @return a complete {@link ResourceConfiguration} instance
   * @throws SirixIOException if an I/O error occurs or the resource identity is missing or zero
   * @throws UnsupportedFormatVersionException if the persisted binary encoding is not supported
   */
  public static ResourceConfiguration deserialize(final Path file) throws SirixIOException {
    return deserialize(file, false);
  }

  public static ResourceConfiguration deserializeForTool(final Path file) {
    return deserialize(file, true);
  }

  private static ResourceConfiguration deserialize(final Path file, final boolean forTool) {
    final Path configFile = file.resolve(ResourcePaths.CONFIG_BINARY.getPath());
    try (final FileReader fileReader = new FileReader(configFile.toFile());
        final JsonReader jsonReader = new JsonReader(fileReader)) {
      jsonReader.beginObject();
      // Binary encoding version.
      expectField(jsonReader, JSONNAMES[0], configFile);
      final BinaryEncodingVersion binaryEncodingVersion = readBinaryEncodingVersion(jsonReader, configFile);
      // Versioning.
      expectField(jsonReader, JSONNAMES[1], configFile);
      jsonReader.beginObject();
      expectField(jsonReader, JSONNAMES[2], configFile);
      final VersioningType revisioning = VersioningType.valueOf(jsonReader.nextString());
      expectField(jsonReader, JSONNAMES[3], configFile);
      final int revisionToRestore = jsonReader.nextInt();
      if (jsonReader.hasNext()) {
        throw new SirixIOException(configFile + ": unknown revisioning settings field '" + jsonReader.nextName() + "'");
      }
      jsonReader.endObject();
      // ByteHandlers.
      final List<ByteHandler> handlerList = new ArrayList<>();
      expectField(jsonReader, JSONNAMES[4], configFile);
      jsonReader.beginArray();
      while (jsonReader.hasNext()) {
        jsonReader.beginObject();
        @SuppressWarnings("unchecked")
        final Class<ByteHandler> clazzName = (Class<ByteHandler>) Class.forName(jsonReader.nextName());
        handlerList.add(ByteHandlerKind.getKind(clazzName).deserialize(jsonReader));
        jsonReader.endObject();
      }
      jsonReader.endArray();
      final ByteHandlerPipeline pipeline = new ByteHandlerPipeline(handlerList.toArray(new ByteHandler[0]));
      // Storage type.
      expectField(jsonReader, JSONNAMES[5], configFile);
      final StorageType storage = StorageType.valueOf(jsonReader.nextString());
      // Hashing type.
      expectField(jsonReader, JSONNAMES[6], configFile);
      final HashType hashing = HashType.valueOf(jsonReader.nextString());
      // Hashing function identity — validated so a future function change is detectable. Legacy
      // configs stored LongHashFunction.xx3().toString(); both spellings mean XXH3.
      expectField(jsonReader, JSONNAMES[7], configFile);
      final String hashFunctionId = jsonReader.nextString();
      if (!NODE_HASH_FUNCTION_ID.equals(hashFunctionId) && !hashFunctionId.toLowerCase(Locale.ROOT).contains("xx3")) {
        throw new SirixIOException(configFile + ": resource was written with node-hash function '" + hashFunctionId
            + "' but this build only supports '" + NODE_HASH_FUNCTION_ID
            + "' (XXH3-64) — node hashes would not verify");
      }
      // Text compression.
      expectField(jsonReader, JSONNAMES[8], configFile);
      final boolean compression = jsonReader.nextBoolean();
      // Path summary.
      expectField(jsonReader, JSONNAMES[9], configFile);
      final boolean pathSummary = jsonReader.nextBoolean();
      // Unique ID.
      expectField(jsonReader, JSONNAMES[10], configFile);
      final int ID = jsonReader.nextInt();
      expectField(jsonReader, JSONNAMES[11], configFile);
      final boolean deweyIDsStored = jsonReader.nextBoolean();
      expectField(jsonReader, JSONNAMES[12], configFile);
      final Class<?> persistenterClazz = Class.forName(jsonReader.nextString());
      final Constructor<?> persistenterConstr = persistenterClazz.getConstructors()[0];
      final RecordSerializer serializer = (RecordSerializer) persistenterConstr.newInstance();
      expectField(jsonReader, JSONNAMES[13], configFile);
      final boolean storeDiffs = jsonReader.nextBoolean();
      expectField(jsonReader, JSONNAMES[14], configFile);
      final boolean customCommitTimestamps = jsonReader.nextBoolean();
      expectField(jsonReader, JSONNAMES[15], configFile);
      final boolean storeNodeHistory = jsonReader.nextBoolean();
      expectField(jsonReader, JSONNAMES[16], configFile);
      final boolean storeChildCount = jsonReader.nextBoolean();

      // Tail fields use names so omissions cannot shift decoding; CONF-102 owns required members
      // and read defaults. Unknown names are refused; only the extensions object is safe to skip.
      StringCompressionType stringCompressionType = StringCompressionType.NONE;
      int deweyIdSiblingDistance = 16;
      boolean verifyChecksumsOnRead = true;
      HashAlgorithm hashAlgorithm = HashAlgorithm.XXH3;
      String name;

      ValidTimeConfig validTimeConfig = null;
      boolean pathStatistics = false;
      boolean repairBulkInsertHashes = false;
      UUID resourceUuid = null;
      RegionCompressionType regionCompressionType = RegionCompressionType.LZ77;
      boolean aggregateKernel = false;
      boolean saltRecordPages = true;
      boolean hasIncarnationId = false;
      long incarnationId = 0L;
      while (jsonReader.hasNext()) {
        name = jsonReader.nextName();
        if (name.equals(JSONNAMES[17])) {
          stringCompressionType = StringCompressionType.valueOf(jsonReader.nextString());
        } else if (name.equals(JSONNAMES[18])) {
          deweyIdSiblingDistance = jsonReader.nextInt();
        } else if (name.equals(JSONNAMES[19])) {
          verifyChecksumsOnRead = jsonReader.nextBoolean();
        } else if (name.equals(JSONNAMES[20])) {
          hashAlgorithm = HashAlgorithm.valueOf(jsonReader.nextString());
        } else if (name.equals(JSONNAMES[21])) {
          jsonReader.beginObject();
          String validFromPath = null;
          String validToPath = null;
          while (jsonReader.hasNext()) {
            String fieldName = jsonReader.nextName();
            if (fieldName.equals(JSONNAMES[22])) {
              validFromPath = jsonReader.nextString();
            } else if (fieldName.equals(JSONNAMES[23])) {
              validToPath = jsonReader.nextString();
            } else {
              throw new SirixIOException(configFile + ": unknown valid-time settings field '" + fieldName + "'");
            }
          }
          jsonReader.endObject();
          if (validFromPath != null && validToPath != null) {
            validTimeConfig = new ValidTimeConfig(validFromPath, validToPath);
          }
        } else if (name.equals(JSONNAMES[24])) {
          pathStatistics = jsonReader.nextBoolean();
        } else if (name.equals(JSONNAMES[25])) {
          repairBulkInsertHashes = jsonReader.nextBoolean();
        } else if (name.equals(JSONNAMES[26])) {
          resourceUuid = UUID.fromString(jsonReader.nextString());
        } else if (name.equals(JSONNAMES[27])) {
          regionCompressionType = RegionCompressionType.valueOf(jsonReader.nextString());
        } else if (name.equals(JSONNAMES[28])) {
          aggregateKernel = jsonReader.nextBoolean();
        } else if (name.equals(JSONNAMES[29])) {
          saltRecordPages = jsonReader.nextBoolean();
        } else if (name.equals(INCARNATION_ID_NAME)) {
          if (hasIncarnationId) {
            throw new SirixIOException(configFile + ": duplicate settings field '" + INCARNATION_ID_NAME + "'");
          }
          // peek first: nextString() would silently accept a JSON number.
          if (jsonReader.peek() != JsonToken.STRING) {
            throw new SirixIOException(configFile + ": settings field '" + INCARNATION_ID_NAME
                + "' must be a string of 16 lowercase hexadecimal digits");
          }
          incarnationId = parseIncarnationId(jsonReader.nextString(), configFile);
          hasIncarnationId = true;
        } else if (name.equals("extensions")) {
          if (jsonReader.peek() != JsonToken.BEGIN_OBJECT) {
            throw new SirixIOException(configFile + ": settings field 'extensions' must be an object");
          }
          jsonReader.skipValue();
        } else {
          throw new SirixIOException(configFile + ": unknown resource settings field '" + name + "'");
        }
      }

      jsonReader.endObject();
      if (!hasIncarnationId) {
        throw missingIncarnationId(configFile);
      }

      requireResourceIdentity(resourceUuid, configFile);

      // Deserialize database config.
      final Path database = file.getParent().getParent();
      final DatabaseConfiguration dbConfig = forTool
          ? DatabaseConfiguration.deserializeForTool(database)
          : DatabaseConfiguration.deserialize(database);

      // Builder.
      final ResourceConfiguration.Builder builder = ResourceConfiguration.newBuilder(file.getFileName().toString());
      builder.binaryEncodingVersion(binaryEncodingVersion)
             .byteHandlerPipeline(pipeline)
             .hashKind(hashing)
             .versioningApproach(revisioning)
             .maxNumberOfRevisionsToRestore(revisionToRestore)
             .storageType(storage)
             .persistenter(serializer)
             .useTextCompression(compression)
             .buildPathSummary(pathSummary)
             .useDeweyIDs(deweyIDsStored)
             .storeDiffs(storeDiffs)
             .storeChildCount(storeChildCount)
             .customCommitTimestamps(customCommitTimestamps)
             .storeNodeHistory(storeNodeHistory)
             .stringCompressionType(stringCompressionType)
             .regionCompressionType(regionCompressionType)
             .aggregateKernel(aggregateKernel)
             .saltRecordPages(saltRecordPages)
             .deweyIdSiblingDistance(deweyIdSiblingDistance)
             .verifyChecksumsOnRead(verifyChecksumsOnRead)
             .hashAlgorithm(hashAlgorithm)
             .validTimeConfig(validTimeConfig)
             .buildPathStatistics(pathStatistics)
             .repairBulkInsertHashes(repairBulkInsertHashes)
             .resourceUuid(resourceUuid);

      // Deserialized instance.
      final ResourceConfiguration config = new ResourceConfiguration(builder);
      config.incarnationId = incarnationId;
      config.setDatabaseConfiguration(dbConfig);
      return config.setID(ID);
    } catch (IOException | ClassNotFoundException | IllegalArgumentException | InstantiationException
        | IllegalAccessException | InvocationTargetException e) {
      throw new SirixIOException(e);
    }
  }

  public static void requireSupportedBinaryEncoding(final Path resource) {
    final Path configFile = resource.resolve(ResourcePaths.CONFIG_BINARY.getPath());
    try (final FileReader fileReader = new FileReader(configFile.toFile(), Charset.defaultCharset());
        final JsonReader reader = new JsonReader(fileReader)) {
      reader.beginObject();
      while (reader.hasNext()) {
        final String name = reader.nextName();
        if (name.equals(JSONNAMES[0])) {
          readBinaryEncodingVersion(reader, configFile);
        } else {
          reader.skipValue();
        }
      }
    } catch (final IOException | IllegalStateException unreadable) {
      return;
    }
  }

  private static BinaryEncodingVersion readBinaryEncodingVersion(final JsonReader reader, final Path configFile)
      throws IOException {
    final JsonToken token = reader.peek();
    if (token != JsonToken.STRING && token != JsonToken.NUMBER) {
      throw new UnsupportedFormatVersionException(configFile + ": unsupported format version '" + token + "'");
    }
    return parseBinaryEncodingVersion(reader.nextString(), configFile);
  }

  /**
   * Resolves the persisted format version. 1.0 is the only on-disk format (FMT-401), so any other
   * name is refused with a plain message instead of the raw enum lookup failure.
   */
  private static BinaryEncodingVersion parseBinaryEncodingVersion(final String name, final Path configFile) {
    for (final BinaryEncodingVersion version : BinaryEncodingVersion.values()) {
      if (version.name().equals(name)) {
        return version;
      }
    }
    throw new UnsupportedFormatVersionException(configFile + ": unsupported format version '" + name + "'");
  }

  /** 1.0 settings MUST identify their resource with a nonzero UUID (FMT-102); there is no default. */
  private static void requireResourceIdentity(final UUID resourceUuid, final Path configFile) {
    if (resourceUuid == null) {
      throw new SirixIOException(configFile + ": missing required resource settings field '" + JSONNAMES[26] + "'");
    }
    if (resourceUuid.getMostSignificantBits() == 0 && resourceUuid.getLeastSignificantBits() == 0) {
      throw new SirixIOException(configFile + ": zero resource UUID in settings field '" + JSONNAMES[26] + "'");
    }
  }

  /**
   * Builder class for generating new {@link ResourceConfiguration} instance.
   */
  public static final class Builder {

    /**
     * Determines if diffs should be stored or not.
     */
    public boolean storeDiffs = true;

    /**
     * Hashing function for hashing nodes.
     */
    private final LongHashFunction hashFunction = LongHashFunction.xx3();

    /**
     * Type of Storage (File, Berkeley).
     */
    private StorageType type = STORAGE;

    /**
     * Kind of revisioning (Incremental, Differential).
     */
    private VersioningType revisionKind = VERSIONING;

    /**
     * Kind of node hashing; defaults to {@link #DEFAULT_HASH_TYPE}.
     */
    private HashType hashType = DEFAULT_HASH_TYPE;

    /**
     * Number of revisions to restore a complete set of data.
     */
    private int maxNumberOfRevisionsToRestore = VERSIONS_TO_RESTORE;

    /**
     * Record/Node persistenter.
     */
    private RecordSerializer persistenter = NODE_SERIALIZER;

    /**
     * Resource for this session.
     */
    private final String resource;

    /**
     * Determines if text-compression should be used or not (default is true).
     */
    private boolean useTextCompression;

    /**
     * Byte handler pipeline.
     */
    private ByteHandlerPipeline byteHandler;

    /**
     * Determines if DeweyIDs should be used or not.
     */
    private boolean useDeweyIDs = false;

    /**
     * The distance between sibling DeweyIDs when generating new IDs. Default is 16, which provides a
     * good balance for mixed workloads.
     */
    private int deweyIdSiblingDistance = 16;

    /**
     * Determines if a path summary should be build or not.
     *
     * <p>
     * On by default, matching {@code BasicJsonDBStore}. It used to default to {@code false} here, so a
     * resource created straight through this builder silently lost every optimisation that reads the
     * summary — path-based index selection, the count/sum answers, the scan's page-skip — while the
     * same resource created through the query store kept them. Two defaults for the same decision is
     * how a system ends up only being fast in the configuration its benchmarks happen to use.
     */
    private boolean pathSummary = true;

    /**
     * Determines if per-path value statistics are maintained on PathSummary nodes. Requires
     * {@link #pathSummary} to be {@code true}.
     */
    private boolean pathStatistics;

    /** Retained tuning request; JSON hashes/counts are maintained regardless of this value. */
    private boolean repairBulkInsertHashes = false;

    /**
     * Determines whether child count should be tracked or not.
     */
    private boolean storeChildCount;

    /**
     * Determines if custom commit timestamps should be stored or not.
     */
    private boolean customCommitTimestamps;

    /**
     * Determines if node history should be stored or not.
     */
    private boolean storeNodeHistory = true;

    private boolean aggregateKernel;

    private BinaryEncodingVersion binaryEncodingVersion = BINARY_ENCODING_VERSION;

    /**
     * String compression type for string-containing nodes.
     */
    private StringCompressionType stringCompressionType = StringCompressionType.NONE;

    /**
     * Region wire compression defaults ON: the measured trade (−37% database size for ~13% ingest) is
     * the right default for a database whose stated goal is small storage, and the dial exists
     * precisely for resources that want the speed back.
     */
    private RegionCompressionType regionCompressionType = RegionCompressionType.LZ77;

    /**
     * Whether to verify page checksums on read operations. Default is true for data integrity.
     */
    private boolean verifyChecksumsOnRead = true;

    private boolean saltRecordPages = true;

    /**
     * Hash algorithm for page checksums. Default is XXH3 for maximum performance.
     */
    private HashAlgorithm hashAlgorithm = HashAlgorithm.XXH3;

    /**
     * Configuration for valid time support in bitemporal queries. Default is null (no valid time
     * support).
     */
    private ValidTimeConfig validTimeConfig = null;

    /**
     * Per-resource identity UUID. A fresh random UUID for new resources; overwritten with the persisted
     * value when deserializing an existing configuration, which must carry a nonzero one (FMT-102).
     */
    private UUID resourceUuid = UUID.randomUUID();

    /**
     * Constructor, setting the mandatory fields.
     *
     * @param resource the name of the resource
     * @throws NullPointerException if {@code resource} or {@code config} is {@code null}
     */
    public Builder(final String resource) {
      this.resource = canonicalResourceName(resource);
      pathSummary = true;
      storeChildCount = true;
      byteHandler = selectDefaultByteHandler();
    }

    /**
     * Set the storage type.
     *
     * @param type storage type to use
     * @return reference to the builder object
     */
    public Builder storageType(final StorageType type) {
      this.type = requireNonNull(type);
      return this;
    }

    // White-box per-region encoders keep scans SARGable, so outer block compression is off by
    // default; set -Dsirix.compression=lz4 for storage-size A/Bs that re-enable the legacy pipeline.
    private ByteHandlerPipeline selectDefaultByteHandler() {
      final String choice = System.getProperty("sirix.compression", "none").toLowerCase();
      switch (choice) {
        case "none":
          return new ByteHandlerPipeline();
        case "lz4":
          if (!FFILz4Compressor.isNativeAvailable()) {
            // Graceful degrade: missing native → uncompressed pages, operator sees a warning.
            System.err.println("[sirix] WARN: liblz4 not available, falling back to uncompressed pages.");
            return new ByteHandlerPipeline();
          }
          return new ByteHandlerPipeline(new FFILz4Compressor());
        default:
          throw new IllegalArgumentException("Unknown sirix.compression value: '" + choice + "' (expected: none, lz4)");
      }
    }

    /**
     * Set the record persistenter.
     *
     * @param persistenter the record persistenter
     * @return reference to the builder object
     */
    public Builder persistenter(final RecordSerializer persistenter) {
      this.persistenter = requireNonNull(persistenter);
      return this;
    }

    /**
     * Set to {@code false} if no diffs should be stored.
     *
     * @param storeDiffs {code true}, if diffs should be stored, {@code false} if not
     * @return reference to the builder object
     */
    public Builder storeDiffs(final boolean storeDiffs) {
      this.storeDiffs = storeDiffs;
      return this;
    }

    /**
     * Set the versioning algorithm to use.
     *
     * @param versioning versioning algorithm to use
     * @return reference to the builder object
     */
    public Builder versioningApproach(final VersioningType versioning) {
      revisionKind = requireNonNull(versioning);
      return this;
    }

    /**
     * Set the hash kind to use for the nodes. The default is {@link #DEFAULT_HASH_TYPE}
     * ({@link HashType#NONE}): node hashing is opt-in.
     *
     * <p>
     * See {@link #DEFAULT_HASH_TYPE} for configuration and privacy consequences.
     *
     * @param hashKind hash kind to use
     * @return reference to the builder object
     */
    public Builder hashKind(final HashType hashKind) {
      this.hashType = requireNonNull(hashKind);
      return this;
    }

    /**
     * Set the byte handler pipeline.
     *
     * @param byteHandler byte handler pipeline
     * @return reference to the builder object
     */
    public Builder byteHandlerPipeline(final ByteHandlerPipeline byteHandler) {
      this.byteHandler = requireNonNull(byteHandler);
      return this;
    }

    /**
     * Set the maximum number of revisions to restore.
     *
     * @param revisionsToRestore number of versions to restore
     * @return reference to the builder object
     */
    public Builder maxNumberOfRevisionsToRestore(final int revisionsToRestore) {
      checkArgument(revisionsToRestore > 0, "revisionsToRestore must be > 0!");
      this.maxNumberOfRevisionsToRestore = revisionsToRestore;
      return this;
    }

    /**
     * Determines if DeweyIDs should be stored or not.
     *
     * @param useDeweyIDs flag whihc represents to use the deweyIds
     * @return reference to the builder object
     */
    public Builder useDeweyIDs(final boolean useDeweyIDs) {
      this.useDeweyIDs = useDeweyIDs;
      return this;
    }

    /**
     * Sets the distance between sibling DeweyIDs when generating new IDs.
     * 
     * <p>
     * Larger values (e.g., 64, 128) are better for high-insert workloads as they reduce the chance of
     * needing to overflow into deeper levels. Smaller values (e.g., 8, 16) result in more compact
     * DeweyIDs for read-mostly workloads.
     * </p>
     * 
     * <p>
     * Must be a positive even number. Default is 16.
     * </p>
     *
     * @param distance the distance between siblings (must be positive and even)
     * @return reference to the builder object
     * @throws IllegalArgumentException if distance is not positive or not even
     */
    public Builder deweyIdSiblingDistance(final int distance) {
      checkArgument(distance > 0, "deweyIdSiblingDistance must be positive!");
      checkArgument(distance % 2 == 0, "deweyIdSiblingDistance must be even!");
      this.deweyIdSiblingDistance = distance;
      return this;
    }

    /**
     * Determines if text-compression should be used or not.
     *
     * @param useTextCompression use text compression or not (default: yes)
     * @return reference to the builder object
     */
    public Builder useTextCompression(final boolean useTextCompression) {
      this.useTextCompression = useTextCompression;
      return this;
    }

    /**
     * Determines if a path summary should be build.
     *
     * @return reference to the builder object
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      pathSummary = buildPathSummary;
      return this;
    }

    /**
     * Determines if per-path value statistics (count, nullCount, sum, min, max, HLL) are maintained on
     * PathSummary nodes. Requires {@code buildPathSummary(true)}.
     *
     * <p>
     * When enabled, aggregate queries ({@code sum/avg/min/max}, {@code countDistinct}) over unfiltered
     * paths short-circuit through the PathSummary — microseconds instead of a full scan. Costs ~10-20%
     * write-path overhead on top of plain PathSummary maintenance.
     *
     * @return reference to the builder object
     * @throws IllegalStateException if enabled without a path summary
     */
    public Builder buildPathStatistics(final boolean buildPathStatistics) {
      if (buildPathStatistics && !pathSummary) {
        throw new IllegalStateException(
            "buildPathStatistics requires buildPathSummary(true) — enable the path summary first");
      }
      this.pathStatistics = buildPathStatistics;
      return this;
    }

    /**
     * Retains the former final-repair preference in the resource configuration. JSON imports always
     * maintain the configured hash/count invariants, including intermediate auto-commit revisions; this
     * flag no longer defers that maintenance until the end of a JSON import.
     */
    public Builder repairBulkInsertHashes(final boolean repair) {
      this.repairBulkInsertHashes = repair;
      return this;
    }

    /**
     * Determines if the child count of a node should be stored or not.
     *
     * <p>
     * Disabling storage preserves JSON array access, container lengths, and serialized child-count
     * metadata. Exact counts are then derived from structural links as needed; see
     * {@link ChildCounts#get(NodeReadOnlyTrx)} for the work involved and
     * {@link NodeReadOnlyTrx#getChildCount()} for the raw metadata contract.
     *
     * @param storeChildCount store child count or not
     * @return reference to the builder object
     */
    public Builder storeChildCount(final boolean storeChildCount) {
      this.storeChildCount = storeChildCount;
      return this;
    }

    /**
     * Set to {@code true} if custom commit timestamps should be stored.
     *
     * @param customCommitTimestamps {code true}, if custom commit timestamps should be stored,
     *        {@code false} if not
     * @return reference to the builder object
     */
    public Builder customCommitTimestamps(final boolean customCommitTimestamps) {
      this.customCommitTimestamps = customCommitTimestamps;
      return this;
    }

    Builder aggregateKernel(final boolean enabled) {
      aggregateKernel = enabled;
      return this;
    }

    /**
     * Set to {@code true} if node history should be stored.
     *
     * @param storeNodeHistory {code true}, if node history should be stored, {@code false} if not
     * @return reference to the builder object
     */
    public Builder storeNodeHistory(boolean storeNodeHistory) {
      this.storeNodeHistory = storeNodeHistory;
      return this;
    }

    /**
     * Set the binary encoding version number.
     *
     * @param binaryEncodingVersion the binary encoding version number
     * @return reference to the builder object
     */
    public Builder binaryEncodingVersion(BinaryEncodingVersion binaryEncodingVersion) {
      this.binaryEncodingVersion = requireNonNull(binaryEncodingVersion);
      return this;
    }

    /**
     * Set the string compression type for string-containing nodes.
     * 
     * <p>
     * This controls whether and how strings in JSON/XML nodes are compressed:
     * <ul>
     * <li>{@link StringCompressionType#NONE} - No per-string compression (default)</li>
     * <li>{@link StringCompressionType#FSST} - Fast Static Symbol Table compression</li>
     * </ul>
     *
     * @param stringCompressionType the compression type to use
     * @return reference to the builder object
     */
    /**
     * Choose how PAX region payloads are compressed on the wire.
     *
     * @param regionCompressionType the compression type to use
     * @return this builder
     */
    public Builder regionCompressionType(RegionCompressionType regionCompressionType) {
      this.regionCompressionType = requireNonNull(regionCompressionType);
      return this;
    }

    public Builder stringCompressionType(StringCompressionType stringCompressionType) {
      this.stringCompressionType = requireNonNull(stringCompressionType);
      return this;
    }

    /**
     * Enable or disable page checksum verification on read operations.
     * 
     * <p>
     * When enabled, each page read from storage is verified against its stored XXH3 hash to detect
     * corruption. Uses zero-copy verification for native memory segments (~15 GB/s throughput).
     * </p>
     * 
     * <p>
     * Default is {@code true} for data integrity. Only disable for benchmarking or if external
     * integrity mechanisms are in place.
     * </p>
     *
     * @param verify true to enable checksum verification, false to disable
     * @return reference to the builder object
     */
    public Builder verifyChecksumsOnRead(boolean verify) {
      this.verifyChecksumsOnRead = verify;
      return this;
    }

    /**
     * Salt record pages on their first serialization (default: true). Disabling this writes zero salt
     * bytes for deterministic test fixtures; resources created this way lack the preparation for future
     * erasure of page-checksum residues.
     *
     * @param enabled whether record pages receive a random salt
     * @return this builder
     */
    public Builder saltRecordPages(final boolean enabled) {
      saltRecordPages = enabled;
      return this;
    }

    /**
     * Set the hash algorithm for page checksums.
     *
     * <p>
     * Default is {@link HashAlgorithm#XXH3}. Changing the algorithm after pages have been written
     * requires re-writing all pages.
     * </p>
     *
     * @param algorithm the hash algorithm to use
     * @return reference to the builder object
     */
    public Builder hashAlgorithm(HashAlgorithm algorithm) {
      this.hashAlgorithm = requireNonNull(algorithm);
      return this;
    }

    /**
     * Set the valid time configuration for bitemporal queries.
     *
     * <p>
     * When set, this enables first-class valid time support with optimized bitemporal queries using
     * functions like jn:valid-at() and jn:open-bitemporal().
     * </p>
     *
     * <p>
     * The persistent valid-time interval index and two {@code xs:dateTime} CAS indexes over the
     * valid-time fields are created automatically by the store layers (JSONiq
     * {@code jn:store}/{@code jn:load} with valid-time options and the REST create handler); resources
     * created directly through the Java API can create them explicitly via
     * {@code jn:create-valid-time-index}/{@code jn:create-cas-index} or the
     * {@code io.sirix.query.json.ValidTimeIndexes} helper. Setting this configuration alone does not
     * create any index.
     * </p>
     *
     * @param validTimeConfig the valid time configuration, or null to disable
     * @return reference to the builder object
     */
    public Builder validTimeConfig(ValidTimeConfig validTimeConfig) {
      this.validTimeConfig = validTimeConfig;
      return this;
    }

    /**
     * Sets the resource identity UUID. Deserialization passes the persisted value; new resources keep
     * the generated random default. The all-zero UUID is not an identity (FMT-102).
     *
     * @param resourceUuid the persisted nonzero UUID
     * @return reference to the builder object
     * @throws NullPointerException if {@code resourceUuid} is {@code null}
     * @throws IllegalArgumentException if {@code resourceUuid} is the all-zero UUID
     */
    public Builder resourceUuid(final UUID resourceUuid) {
      requireNonNull(resourceUuid);
      checkArgument(resourceUuid.getMostSignificantBits() != 0 || resourceUuid.getLeastSignificantBits() != 0,
          "The resource identity UUID must not be the all-zero UUID.");
      this.resourceUuid = resourceUuid;
      return this;
    }

    /**
     * Set the valid time paths for bitemporal queries.
     *
     * <p>
     * This is a convenience method equivalent to calling
     * {@code validTimeConfig(new ValidTimeConfig(validFromPath, validToPath))}.
     * </p>
     *
     * <p>
     * See {@link #validTimeConfig(ValidTimeConfig)} for how the valid-time interval index is created —
     * setting the paths alone does not create any index.
     * </p>
     *
     * @param validFromPath JSON path to the validFrom field (e.g., "$.validFrom" or "validFrom")
     * @param validToPath JSON path to the validTo field (e.g., "$.validTo" or "validTo")
     * @return reference to the builder object
     * @throws NullPointerException if any argument is null
     */
    public Builder validTimePaths(String validFromPath, String validToPath) {
      this.validTimeConfig = new ValidTimeConfig(validFromPath, validToPath);
      return this;
    }

    /**
     * Enable valid time support using convention-based field names.
     *
     * <p>
     * This uses the standard field names "_validFrom" and "_validTo".
     * </p>
     *
     * <p>
     * This is a convenience method equivalent to calling
     * {@code validTimeConfig(ValidTimeConfig.withConventionalPaths())}.
     * </p>
     *
     * @return reference to the builder object
     */
    public Builder useConventionalValidTimePaths() {
      this.validTimeConfig = ValidTimeConfig.withConventionalPaths();
      return this;
    }

    @Override
    public String toString() {
      return ToStringHelper.of(this)
                           .add("binaryEncodingVersion", binaryEncodingVersion)
                           .add("Type", type)
                           .add("RevisionKind", revisionKind)
                           .add("HashKind", hashType)
                           .add("HashFunction", hashFunction)
                           .add("PathSummary", pathSummary)
                           .add("TextCompression", useTextCompression)
                           .add("Store diffs", storeDiffs)
                           .add("Store child count", storeChildCount)
                           .add("Store node history", storeNodeHistory)
                           .add("Custom commit timestamps", customCommitTimestamps)
                           .add("Max number of revisions to restore", maxNumberOfRevisionsToRestore)
                           .add("Use deweyIDs", useDeweyIDs)
                           .add("Byte handler pipeline", byteHandler)
                           .add("Verify checksums on read", verifyChecksumsOnRead)
                           .add("Valid time config", validTimeConfig)
                           .toString();
    }

    /**
     * Building a new {@link ResourceConfiguration} with immutable fields.
     *
     * @return a new {@link ResourceConfiguration} instance
     */
    public ResourceConfiguration build() {
      return new ResourceConfiguration(this);
    }
  }
}
