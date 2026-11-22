package io.sirix.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.BinaryEncodingVersion;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.UnsupportedFormatVersionException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandler.DecompressionResult;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.page.PageKind;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Side-effect-free FMT-401 gate shared by persistent storage and destructive recovery. */
public final class ResourceFormatCompatibility {
  private ResourceFormatCompatibility() {}

  /** Only this gate can construct the resource-bound evidence used by destructive callers. */
  public static final class Proof {
    private final Path resource;
    private final UberPage uberPage;

    private Proof(final Path resource, final UberPage uberPage) {
      this.resource = resource;
      this.uberPage = uberPage;
    }

    public UberPage uberPage() {
      return uberPage;
    }

    public void requireResource(final Path path) {
      if (!resource.equals(path)) {
        throw new IllegalArgumentException("Compatibility proof belongs to another resource");
      }
    }
  }

  public static Proof verify(final ResourceConfiguration config) {
    return verify(config, true);
  }

  /** With unreadable settings, both files must still name the same nonzero resource UUID. */
  public static Proof verifyWithoutSettings(final ResourceConfiguration config) {
    ResourceConfiguration.requireSupportedBinaryEncoding(config.resourcePath);
    return verify(config, false);
  }

  private static Proof verify(final ResourceConfiguration config, final boolean knownIdentity) {
    final Path directory = config.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath());
    final Path data = directory.resolve(IOStorage.FILENAME);
    final Path revisions = directory.resolve(IOStorage.REVISIONS_FILENAME);
    try {
      final long msb = knownIdentity
          ? config.resourceUuid.getMostSignificantBits()
          : 0L;
      final long lsb = knownIdentity
          ? config.resourceUuid.getLeastSignificantBits()
          : 0L;
      InterruptedFirstCommitRecovery.requireValidFileIdentity(data, Superblock.ROLE_DATA, msb, lsb);
      InterruptedFirstCommitRecovery.requireValidFileIdentity(revisions, Superblock.ROLE_REVISIONS, msb, lsb);
      if (!knownIdentity && Files.exists(data) && Files.size(data) >= Superblock.BYTES) {
        final ByteBuffer header = ByteBuffer.allocate(Superblock.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        try (final FileChannel channel = FileChannel.open(data, StandardOpenOption.READ)) {
          readFully(channel, header, 0L);
        }
        InterruptedFirstCommitRecovery.requireValidFileIdentity(revisions, Superblock.ROLE_REVISIONS,
            header.getLong(40), header.getLong(48));
      }
      final UberPage uber = InterruptedFirstCommitRecovery.probeUberPage(config);
      uber.getFeatures().requireReadable();
      if (!Files.exists(data) || !Files.exists(revisions)) {
        uber.getFeatures().requireWritable();
      }
      if (uber.isBootstrap()) {
        if (Files.exists(data)) {
          try (final FileChannel channel = FileChannel.open(data, StandardOpenOption.READ)) {
            requireFramedPrefix(channel, new ByteHandlerPipeline(config.byteHandlePipeline));
          } catch (final UnsupportedFormatVersionException unsupported) {
            throw unsupported;
          } catch (final IOException | RuntimeException incomplete) {
            return new Proof(config.resourcePath, uber);
          }
        }
        return new Proof(config.resourcePath, uber);
      }
      try (final FileChannel channel = FileChannel.open(data, StandardOpenOption.READ);
          final FileChannel records = FileChannel.open(Files.exists(revisions)
              ? revisions
              : data, StandardOpenOption.READ);
          final FileChannelReader reader =
              new FileChannelReader(channel, records, new ByteHandlerPipeline(config.byteHandlePipeline),
                  SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())) {
        reader.setStoreFeatures(uber.getFeatures());
        final RevisionFileData root;
        try {
          root = reader.getRevisionFileDataWithoutHealing(uber.getRevisionNumber());
          reader.requireCompatibleRevisionRoots(uber.getRevisionNumber());
        } catch (final SirixIOException missing) {
          // A backend without a tail-log copy must inspect surviving envelopes before recovery.
          requireFramedPrefix(channel, reader.getByteHandler());
          if (uber.getRevisionNumber() != 0) {
            throw missing;
          }
          return new Proof(config.resourcePath, uber);
        }
        root.requireReadable(uber.getRevisionNumber());
        requireRootEnvelope(channel, root.offset(), reader.getByteHandler());
      }
      return new Proof(config.resourcePath, uber);
    } catch (final IOException failure) {
      throw new SirixIOException(failure);
    }
  }

  /** Compatibility evidence bound to the exact descriptors a direct repair will modify. */
  public static final class RepairProof {
    private final FileChannel data;
    private final FileChannel revisions;

    private RepairProof(final FileChannel data, final FileChannel revisions) {
      this.data = data;
      this.revisions = revisions;
    }

    @SuppressWarnings("ReferenceEquality") // Proofs authorize these exact descriptors, not equal wrappers.
    public void requireChannels(final FileChannel dataChannel, final FileChannel revisionsChannel) {
      if (data != dataChannel || revisions != revisionsChannel) {
        throw new IllegalArgumentException("Compatibility proof belongs to other storage files");
      }
    }
  }

  public static RepairProof verifyRepair(final FileChannel data, final FileChannel revisions,
      final FileChannelReader reader) throws IOException {
    final ByteBuffer header = ByteBuffer.allocate(Superblock.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    readFully(data, header, 0L);
    header.flip();
    Superblock.validate(header, Superblock.ROLE_DATA, IOStorage.FILENAME);
    final long msb = header.getLong(40);
    final long lsb = header.getLong(48);
    if (revisions.size() != 0) {
      header.clear();
      readFully(revisions, header, 0L);
      header.flip();
      Superblock.validate(header, Superblock.ROLE_REVISIONS, IOStorage.REVISIONS_FILENAME, msb, lsb);
    }
    final UberPage uber = reader.readOpeningUberPage();
    uber.getFeatures().requireWritable();
    final RevisionFileData root = reader.getRevisionFileDataWithoutHealing(uber.getRevisionNumber());
    reader.requireCompatibleRevisionRoots(uber.getRevisionNumber());
    root.requireReadable(uber.getRevisionNumber());
    requireRootEnvelope(data, root.offset(), reader.getByteHandler());
    return new RepairProof(data, revisions);
  }

  /** Also guards direct reader healing and writer truncation, which need no resource settings. */
  public static void requireRootEnvelope(final FileChannel channel, final long offset, final ByteHandler handler)
      throws IOException {
    if (requireEnvelope(channel, offset, handler) != PageKind.REVISIONROOTPAGE) {
      throw new SirixIOException("Committed root does not reference a revision-root page");
    }
  }

  private static PageKind requireEnvelope(final FileChannel channel, final long offset, final ByteHandler handler)
      throws IOException {
    final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    readFully(channel, length, offset);
    final int size = length.getInt(0);
    if (offset < IOStorage.DATA_REGION_START || size < 2 || size > channel.size() - offset - Integer.BYTES) {
      throw new SirixIOException("Invalid page frame while checking format compatibility");
    }
    final byte[] payload = new byte[size];
    readFully(channel, ByteBuffer.wrap(payload), offset + Integer.BYTES);
    if (handler.supportsMemorySegments()) {
      try (final DecompressionResult decoded = handler.decompressScoped(MemorySegment.ofArray(payload))) {
        if (decoded.segment().byteSize() < 2) {
          throw new SirixIOException("Truncated page envelope while checking format compatibility");
        }
        BinaryEncodingVersion.fromByte(decoded.segment().get(ValueLayout.JAVA_BYTE, 1));
        return PageKind.getKind(decoded.segment().get(ValueLayout.JAVA_BYTE, 0));
      }
    }
    try (final InputStream decoded = handler.deserialize(new ByteArrayInputStream(payload))) {
      final int kind = decoded.read();
      final int version = decoded.read();
      if (kind < 0 || version < 0) {
        throw new SirixIOException("Truncated page envelope while checking format compatibility");
      }
      BinaryEncodingVersion.fromByte((byte) version);
      return PageKind.getKind((byte) kind);
    }
  }

  private static void requireFramedPrefix(final FileChannel channel, final ByteHandler handler) throws IOException {
    final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    final long size = channel.size();
    for (long offset = IOStorage.DATA_REGION_START; offset + Integer.BYTES <= size;) {
      length.clear();
      readFully(channel, length, offset);
      final int bytes = length.getInt(0);
      if (bytes == 0) {
        return; // Unwritten preallocation follows the framed prefix.
      }
      requireEnvelope(channel, offset, handler);
      offset = (offset + Integer.BYTES + bytes + Writer.PAGE_FRAGMENT_BYTE_ALIGN - 1)
          & -((long) Writer.PAGE_FRAGMENT_BYTE_ALIGN);
    }
  }

  private static void readFully(final FileChannel channel, final ByteBuffer buffer, final long offset)
      throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer, offset + buffer.position()) <= 0) {
        throw new SirixIOException("Truncated file while checking format compatibility");
      }
    }
  }
}
