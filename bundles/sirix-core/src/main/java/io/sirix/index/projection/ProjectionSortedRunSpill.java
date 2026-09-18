/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.utils.SirixFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where, and through which byte handlers, the initial build of a resource's sorted projection view
 * spills its sorted runs.
 *
 * <p>Runs live in a directory of the resource itself, {@code <resource>/projection-sort-spill}. When
 * {@code -Dsirix.projection.sortedRun.spillDirectory} is set it replaces that location's root, and
 * each resource still spills into its own subdirectory under it, named after the resource and a
 * digest of its path, so cleaning one resource can never touch another resource's runs. Every run
 * is written and read through the resource's {@link ByteHandlerPipeline} exactly as its pages are:
 * an empty pipeline stores the keys as they are, a pipeline whose handlers all work on memory
 * segments encodes independent blocks, and any other pipeline, an {@code Encryptor} included, wraps
 * the whole run in its stream handlers, so a run never holds a plaintext key the resource's pages
 * would not.</p>
 *
 * <p>Each build owns one directory below the resource's spill directory, registered as live for as
 * long as it exists. {@link #removeOrphanedRuns} deletes every other entry there when a resource is
 * opened: the runs of a build whose process died, and anything else found in the directory. It never
 * deletes a live build of this JVM or a build owned by another live process.</p>
 */
public final class ProjectionSortedRunSpill {

  static final String SPILL_DIRECTORY_PROPERTY = "sirix.projection.sortedRun.spillDirectory";

  /** Name of the spill directory inside a resource's own directory. */
  static final String RESOURCE_SPILL_DIRECTORY = "projection-sort-spill";

  private static final String BUILD_PREFIX = "build-";
  private static final int MAX_BUILD_DIRECTORY_ATTEMPTS = 1024;
  private static final int STREAM_BUFFER_BYTES = 1 << 16;
  private static final int MAX_BLOCK_BYTES = 1 << 26;
  private static final long PROCESS_ID = ProcessHandle.current().pid();
  private static final AtomicLong BUILD_SEQUENCE = new AtomicLong();
  private static final Set<String> LIVE_BUILDS = ConcurrentHashMap.newKeySet();
  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionSortedRunSpill.class);

  private final Path directory;
  private final ByteHandlerPipeline pipeline;

  /**
   * Spill into {@code directory} through {@code pipeline}.
   *
   * @param directory the resource's spill directory; build directories are created below it
   * @param pipeline the byte handlers every run is written and read through
   */
  ProjectionSortedRunSpill(final Path directory, final ByteHandlerPipeline pipeline) {
    this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
  }

  /** The spill target of {@code resourceConfig}: its spill directory and its byte handlers. */
  static ProjectionSortedRunSpill forResource(final ResourceConfiguration resourceConfig) {
    Objects.requireNonNull(resourceConfig, "resourceConfig");
    return new ProjectionSortedRunSpill(directoryOf(resourceConfig), resourceConfig.byteHandlePipeline);
  }

  /** The resource's spill directory under the configured root, or inside the resource by default. */
  static Path directoryOf(final ResourceConfiguration resourceConfig) {
    final Path resource = resourceDirectory(resourceConfig);
    final String configuredRoot = System.getProperty(SPILL_DIRECTORY_PROPERTY);
    if (configuredRoot == null || configuredRoot.isBlank()) {
      return resource.resolve(RESOURCE_SPILL_DIRECTORY);
    }
    return Path.of(configuredRoot.trim()).toAbsolutePath().normalize().resolve(externalName(resource));
  }

  /**
   * Delete the sorted runs that builds of this resource left behind when their process died.
   *
   * <p>Both the resource's own spill directory and, when
   * {@code -Dsirix.projection.sortedRun.spillDirectory} is set, the resource's subdirectory under it
   * are cleaned. Builds live in this JVM or owned by another live process are kept. Cleanup is
   * best effort: a failure is logged and never fails the open.</p>
   *
   * @param resourceConfig the resource being opened
   */
  public static void removeOrphanedRuns(final ResourceConfiguration resourceConfig) {
    Objects.requireNonNull(resourceConfig, "resourceConfig");
    final Path local = resourceDirectory(resourceConfig).resolve(RESOURCE_SPILL_DIRECTORY);
    removeOrphansIn(local);
    final Path configured;
    try {
      configured = directoryOf(resourceConfig);
    } catch (final RuntimeException e) {
      LOGGER.warn("Cannot resolve the configured sorted projection spill directory of {}", local.getParent(), e);
      return;
    }
    if (!configured.equals(local)) {
      removeOrphansIn(configured);
    }
  }

  /** The resource's spill directory; build directories are created below it. */
  Path directory() {
    return directory;
  }

  /**
   * Create a build's private run directory, registered as live before it exists so that no
   * concurrent cleanup in this JVM can remove it.
   */
  Path createBuildDirectory() throws IOException {
    Files.createDirectories(directory);
    for (int attempt = 0; attempt < MAX_BUILD_DIRECTORY_ATTEMPTS; attempt++) {
      final String name = BUILD_PREFIX + PROCESS_ID + '-' + BUILD_SEQUENCE.incrementAndGet();
      LIVE_BUILDS.add(name);
      boolean created = false;
      try {
        final Path build = Files.createDirectory(directory.resolve(name));
        created = true;
        return build;
      } catch (final FileAlreadyExistsException ignored) {
        // A leftover of an earlier process with this process id; the next sequence number is free.
      } finally {
        if (!created) {
          LIVE_BUILDS.remove(name);
        }
      }
    }
    throw new IOException("no free sorted-run build directory name under " + directory);
  }

  /** Stop protecting a build directory; call after its runs and the directory were deleted. */
  static void retireBuildDirectory(final Path buildDirectory) {
    final Path name = buildDirectory.getFileName();
    if (name != null) {
      LIVE_BUILDS.remove(name.toString());
    }
  }

  /** Open a new run file for writing; the stream encodes through the resource's byte handlers. */
  OutputStream openWriter(final Path file) throws IOException {
    Objects.requireNonNull(file, "file");
    if (pipeline.isEmpty()) {
      return Files.newOutputStream(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }
    if (pipeline.supportsMemorySegments()) {
      return new BlockOutputStream(
          FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), pipeline);
    }
    final OutputStream raw = new BufferedOutputStream(
        Files.newOutputStream(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), STREAM_BUFFER_BYTES);
    try {
      return pipeline.serialize(raw);
    } catch (final RuntimeException e) {
      final IOException failure = new IOException("the resource's byte handlers cannot encode a sorted run", e);
      closeAfterFailure(raw, failure);
      throw failure;
    }
  }

  /** Open a run file written by {@link #openWriter} for sequential reading of its plain keys. */
  InputStream openReader(final Path file) throws IOException {
    Objects.requireNonNull(file, "file");
    if (pipeline.isEmpty()) {
      return Files.newInputStream(file, StandardOpenOption.READ);
    }
    if (pipeline.supportsMemorySegments()) {
      return new BlockInputStream(FileChannel.open(file, StandardOpenOption.READ), pipeline);
    }
    final InputStream raw =
        new BufferedInputStream(Files.newInputStream(file, StandardOpenOption.READ), STREAM_BUFFER_BYTES);
    try {
      return pipeline.deserialize(raw);
    } catch (final RuntimeException e) {
      final IOException failure = new IOException("the resource's byte handlers cannot decode a sorted run", e);
      closeAfterFailure(raw, failure);
      throw failure;
    }
  }

  private static Path resourceDirectory(final ResourceConfiguration resourceConfig) {
    return Objects.requireNonNull(resourceConfig.getResource(), "resource directory").toAbsolutePath().normalize();
  }

  private static String externalName(final Path resource) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
    final byte[] hash = digest.digest(resource.toString().getBytes(StandardCharsets.UTF_8));
    final Path fileName = resource.getFileName();
    return (fileName == null
        ? "resource"
        : fileName.toString()) + '-' + HexFormat.of().formatHex(hash, 0, 16);
  }

  private static void removeOrphansIn(final Path spillDirectory) {
    try {
      if (!Files.isDirectory(spillDirectory)) {
        return;
      }
      try (DirectoryStream<Path> entries = Files.newDirectoryStream(spillDirectory)) {
        for (final Path entry : entries) {
          final Path name = entry.getFileName();
          if (name != null && isOrphan(name.toString())) {
            SirixFiles.recursiveRemove(entry);
            if (Files.exists(entry)) {
              LOGGER.warn("Cannot remove the orphaned sorted projection runs at {}", entry);
            }
          }
        }
      }
    } catch (final IOException | RuntimeException e) {
      LOGGER.warn("Cannot remove orphaned sorted projection runs under {}", spillDirectory, e);
    }
  }

  private static boolean isOrphan(final String name) {
    if (LIVE_BUILDS.contains(name)) {
      return false;
    }
    final long owner = ownerProcess(name);
    return owner < 0 || owner == PROCESS_ID || !isAlive(owner);
  }

  /** The process id encoded in a build directory name, or {@code -1} for any other name. */
  private static long ownerProcess(final String name) {
    if (!name.startsWith(BUILD_PREFIX)) {
      return -1L;
    }
    final int end = name.indexOf('-', BUILD_PREFIX.length());
    if (end <= BUILD_PREFIX.length()) {
      return -1L;
    }
    try {
      return Long.parseLong(name, BUILD_PREFIX.length(), end, 10);
    } catch (final NumberFormatException e) {
      return -1L;
    }
  }

  private static boolean isAlive(final long processId) {
    try {
      final Optional<ProcessHandle> process = ProcessHandle.of(processId);
      return process.isPresent() && process.get().isAlive();
    } catch (final RuntimeException e) {
      return true;
    }
  }

  private static void closeAfterFailure(final Closeable closeable, final IOException failure) {
    try {
      closeable.close();
    } catch (final IOException e) {
      failure.addSuppressed(e);
    }
  }

  private static void writeFully(final FileChannel channel, final ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  /** Fill {@code buffer}; {@code false} only for an end of file before its first byte, if allowed. */
  private static boolean readFully(final FileChannel channel, final ByteBuffer buffer, final boolean endAllowed)
      throws IOException {
    final int start = buffer.position();
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) < 0) {
        if (endAllowed && buffer.position() == start) {
          return false;
        }
        throw new IOException("truncated sorted projection run block");
      }
    }
    return true;
  }

  /**
   * Blocks encoded on their own by a memory-segment pipeline, each prefixed by its encoded and its
   * plain length. A decoder may return a larger segment than it decoded, so a block's plain length is
   * never taken from the decoded segment.
   */
  private static final class BlockOutputStream extends OutputStream {
    private final FileChannel channel;
    private final ByteHandlerPipeline pipeline;
    private final ByteBuffer header = ByteBuffer.allocate(2 * Integer.BYTES);
    private final byte[] single = new byte[1];

    BlockOutputStream(final FileChannel channel, final ByteHandlerPipeline pipeline) {
      this.channel = channel;
      this.pipeline = pipeline;
    }

    @Override
    public void write(final int value) throws IOException {
      single[0] = (byte) value;
      write(single, 0, 1);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      if (length == 0) {
        return;
      }
      if (length > MAX_BLOCK_BYTES) {
        throw new IOException("a sorted run block of " + length + " bytes exceeds " + MAX_BLOCK_BYTES + " bytes");
      }
      final MemorySegment encoded;
      try {
        encoded = pipeline.compress(MemorySegment.ofArray(bytes).asSlice(offset, length));
      } catch (final RuntimeException e) {
        throw new IOException("the resource's byte handlers cannot encode a sorted run block", e);
      }
      final long encodedBytes = encoded.byteSize();
      if (encodedBytes <= 0 || encodedBytes > MAX_BLOCK_BYTES) {
        throw new IOException("the resource's byte handlers encoded a sorted run block as " + encodedBytes + " bytes");
      }
      header.clear();
      header.putInt((int) encodedBytes);
      header.putInt(length);
      header.flip();
      writeFully(channel, header);
      writeFully(channel, encoded.asByteBuffer());
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }

  /** Reads the blocks of a {@link BlockOutputStream}, keeping one decoded block of its exact plain length. */
  private static final class BlockInputStream extends InputStream {
    private final FileChannel channel;
    private final ByteHandlerPipeline pipeline;
    private final ByteBuffer header = ByteBuffer.allocate(2 * Integer.BYTES);
    private final byte[] single = new byte[1];
    private byte[] encoded = new byte[0];
    private ByteBuffer encodedBuffer = ByteBuffer.wrap(encoded);
    private byte[] decoded = new byte[0];
    private int position;
    private int limit;

    BlockInputStream(final FileChannel channel, final ByteHandlerPipeline pipeline) {
      this.channel = channel;
      this.pipeline = pipeline;
    }

    @Override
    public int read() throws IOException {
      return read(single, 0, 1) < 0
          ? -1
          : single[0] & 0xFF;
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      if (length == 0) {
        return 0;
      }
      while (position == limit) {
        if (!nextBlock()) {
          return -1;
        }
      }
      final int copied = Math.min(length, limit - position);
      System.arraycopy(decoded, position, bytes, offset, copied);
      position += copied;
      return copied;
    }

    private boolean nextBlock() throws IOException {
      header.clear();
      if (!readFully(channel, header, true)) {
        return false;
      }
      header.flip();
      final int size = header.getInt();
      final int plainBytes = header.getInt();
      if (size <= 0 || size > MAX_BLOCK_BYTES || plainBytes <= 0 || plainBytes > MAX_BLOCK_BYTES) {
        throw new IOException("corrupt sorted projection run block lengths " + size + " / " + plainBytes);
      }
      if (encoded.length < size) {
        encoded = new byte[size];
        encodedBuffer = ByteBuffer.wrap(encoded);
      }
      encodedBuffer.clear();
      encodedBuffer.limit(size);
      readFully(channel, encodedBuffer, false);
      try (ByteHandler.DecompressionResult result =
          pipeline.decompressScoped(MemorySegment.ofArray(encoded).asSlice(0, size))) {
        final MemorySegment plain = result.segment();
        if (plain.byteSize() < plainBytes) {
          throw new IOException("corrupt sorted projection run block: decoded " + plain.byteSize()
              + " of its " + plainBytes + " plain bytes");
        }
        if (decoded.length < plainBytes) {
          decoded = new byte[plainBytes];
        }
        MemorySegment.copy(plain, ValueLayout.JAVA_BYTE, 0, decoded, 0, plainBytes);
      } catch (final RuntimeException e) {
        throw new IOException("the resource's byte handlers cannot decode a sorted run block", e);
      }
      position = 0;
      limit = plainBytes;
      return true;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
