package io.sirix.crash;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.StorageProvider;
import io.sirix.io.StorageProviders;
import io.sirix.io.StorageType;
import io.sirix.io.Superblock;
import io.sirix.io.SuperblockValidator;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.io.filechannel.FileChannelWriter;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link IOStorage} that wires a REAL {@link FileChannelWriter}/{@link FileChannelReader}
 * pair exactly like {@code FileChannelStorage.createWriter()} does, but with the two underlying
 * channels wrapped in {@link RecordingFileChannel}s. Every write/force the production commit
 * protocol issues is thereby captured while still being applied to the real files.
 *
 * <p>Injection is purely test-scoped: {@link #install(Path, PowerLossRecorder)} registers a
 * {@link StorageProvider} named {@code FILE_CHANNEL} in the {@link StorageProviders} registry
 * (consulted by {@code StorageType.getStorage} BEFORE the built-in types). The provider returns a
 * recording storage only for resources under the registered recording root and delegates to the
 * untouched built-in {@code StorageType.FILE_CHANNEL} storage for every other path — so the
 * verification opens of materialized crash states use the production I/O stack end to end.
 * {@link #uninstall()} removes the provider again.
 */
final class PowerLossRecordingStorage implements IOStorage {

  private final Path resourceFile;
  private final ByteHandlerPipeline byteHandlerPipeline;
  private final AsyncCache<Integer, RevisionFileData> cache;
  private final RevisionIndexHolder revisionIndexHolder;
  private final PowerLossRecorder recorder;

  private PowerLossRecordingStorage(final ResourceConfiguration resourceConfig,
      final AsyncCache<Integer, RevisionFileData> cache, final RevisionIndexHolder revisionIndexHolder,
      final PowerLossRecorder recorder) {
    this.resourceFile = resourceConfig.resourcePath;
    this.byteHandlerPipeline = resourceConfig.byteHandlePipeline;
    this.cache = cache;
    this.revisionIndexHolder = revisionIndexHolder;
    this.recorder = recorder;
  }

  private Path dataFilePath() {
    return resourceFile.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(IOStorage.FILENAME);
  }

  private Path revisionsFilePath() {
    return resourceFile.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                       .resolve(IOStorage.REVISIONS_FILENAME);
  }

  private void createFilesIfMissing() throws IOException {
    final Path dataFilePath = dataFilePath();
    if (!Files.exists(dataFilePath)) {
      Files.createDirectories(dataFilePath.getParent());
      Files.createFile(dataFilePath);
    }
    final Path revisionsFilePath = revisionsFilePath();
    if (!Files.exists(revisionsFilePath)) {
      Files.createFile(revisionsFilePath);
    }
  }

  private RecordingFileChannel openRecording(final Path path, final PowerLossRecorder.TargetFile target,
      final boolean sparse) throws IOException {
    return openRecording(path, target, sparse, PowerLossRecorder.WriteDurability.NONE);
  }

  private RecordingFileChannel openRecording(final Path path, final PowerLossRecorder.TargetFile target,
      final boolean sparse, final PowerLossRecorder.WriteDurability writeDurability) throws IOException {
    // The REAL channel is opened buffered regardless of the modeled durability — the simulation
    // owns durability semantics; actual write-through latency is irrelevant for recording.
    final FileChannel channel = sparse
        ? FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE)
        : FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    return new RecordingFileChannel(channel, recorder, target, writeDurability);
  }

  @Override
  public Reader createReader() {
    try {
      SuperblockValidator.validateOnce(dataFilePath(), Superblock.ROLE_DATA);
      SuperblockValidator.validateOnce(revisionsFilePath(), Superblock.ROLE_REVISIONS);
      createFilesIfMissing();
      final FileChannel revisionsChannel =
          openRecording(revisionsFilePath(), PowerLossRecorder.TargetFile.REVISIONS, false);
      final FileChannel dataChannel = openRecording(dataFilePath(), PowerLossRecorder.TargetFile.DATA, true);
      return new FileChannelReader(dataChannel, revisionsChannel, new ByteHandlerPipeline(byteHandlerPipeline),
          SerializationType.DATA, new PagePersister(), cache.synchronous());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer createWriter() {
    try {
      SuperblockValidator.validateOnce(dataFilePath(), Superblock.ROLE_DATA);
      SuperblockValidator.validateOnce(revisionsFilePath(), Superblock.ROLE_REVISIONS);
      createFilesIfMissing();
      // Mirrors FileChannelStorage.createWriter: SYNC-modeled revisions channel (record write
      // durable incl. size at return), DSYNC-modeled beacon channel (in-place slot writes
      // durable at return), buffered bulk data channel.
      final FileChannel revisionsChannel =
          openRecording(revisionsFilePath(), PowerLossRecorder.TargetFile.REVISIONS, false,
                        PowerLossRecorder.WriteDurability.SYNC);
      final FileChannel dataChannel = openRecording(dataFilePath(), PowerLossRecorder.TargetFile.DATA, true);
      final FileChannel beaconChannel = openRecording(dataFilePath(), PowerLossRecorder.TargetFile.DATA, true,
                                                      PowerLossRecorder.WriteDurability.DSYNC);

      final var pipeline = new ByteHandlerPipeline(byteHandlerPipeline);
      final var pagePersister = new PagePersister();
      final var reader = new FileChannelReader(dataChannel, revisionsChannel, pipeline, SerializationType.DATA,
          pagePersister, cache.synchronous());
      return new FileChannelWriter(dataChannel, revisionsChannel, beaconChannel, SerializationType.DATA,
          pagePersister, cache, revisionIndexHolder, reader);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    // Nothing to do — mirrors FileChannelStorage.
  }

  @Override
  public boolean exists() {
    final Path storage = dataFilePath();
    try {
      return Files.exists(storage) && Files.size(storage) > 0;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public ByteHandler getByteHandler() {
    return byteHandlerPipeline;
  }

  @Override
  public RevisionIndexHolder getRevisionIndexHolder() {
    return revisionIndexHolder;
  }

  // ---- provider installation ----

  private static final AtomicReference<Path> RECORDING_ROOT = new AtomicReference<>();
  private static final AtomicReference<PowerLossRecorder> RECORDER = new AtomicReference<>();
  private static final String PROVIDER_NAME = "FILE_CHANNEL";

  /** The provider instance currently registered, for exact removal on uninstall. */
  private static final AtomicReference<StorageProvider> INSTALLED = new AtomicReference<>();

  /**
   * Registers the recording provider; resources whose {@code resourcePath} lives under
   * {@code recordingRoot} get recording storages, everything else gets the untouched built-in
   * FILE_CHANNEL storage.
   */
  static void install(final Path recordingRoot, final PowerLossRecorder recorder) {
    RECORDING_ROOT.set(recordingRoot.toAbsolutePath().normalize());
    RECORDER.set(recorder);

    final StorageProvider provider = new StorageProvider() {
      @Override
      public String getName() {
        return PROVIDER_NAME;
      }

      @Override
      public boolean isAvailable() {
        return true;
      }

      @Override
      public IOStorage createStorage(final ResourceConfiguration resourceConfig) {
        final Path root = RECORDING_ROOT.get();
        final PowerLossRecorder activeRecorder = RECORDER.get();
        if (root != null && activeRecorder != null
            && resourceConfig.resourcePath.toAbsolutePath().normalize().startsWith(root)) {
          // Same cache/index wiring as StorageType.FILE_CHANNEL.getInstance, with a plain
          // per-path AsyncCache registered in the shared repository so readers and writers of
          // this resource share one RevisionFileData view.
          final Path cacheKey = resourceConfig.resourcePath
              .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
              .resolve(IOStorage.FILENAME);
          final AsyncCache<Integer, RevisionFileData> cache = StorageType.CACHE_REPOSITORY
              .computeIfAbsent(cacheKey, p -> Caffeine.newBuilder()
                                                      .maximumSize(10_000)
                                                      .<Integer, RevisionFileData>buildAsync());
          final RevisionIndexHolder holder = StorageType.getRevisionIndexHolder(resourceConfig);
          final var storage = new PowerLossRecordingStorage(resourceConfig, cache, holder, activeRecorder);
          storage.loadRevisionFileDataIntoMemory(cache);
          storage.loadRevisionIndex(holder);
          return storage;
        }
        return StorageType.FILE_CHANNEL.getInstance(resourceConfig);
      }
    };

    final Map<String, StorageProvider> providers = providersMap();
    final StorageProvider previous = providers.putIfAbsent(PROVIDER_NAME, provider);
    if (previous != null) {
      throw new IllegalStateException("A FILE_CHANNEL storage provider is already registered: " + previous
          + " — refusing to shadow it");
    }
    INSTALLED.set(provider);
  }

  static void uninstall() {
    final StorageProvider installed = INSTALLED.getAndSet(null);
    if (installed != null) {
      providersMap().remove(PROVIDER_NAME, installed);
    }
    RECORDING_ROOT.set(null);
    RECORDER.set(null);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, StorageProvider> providersMap() {
    try {
      final Field field = StorageProviders.class.getDeclaredField("PROVIDERS");
      field.setAccessible(true);
      return (Map<String, StorageProvider>) field.get(null);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot access StorageProviders.PROVIDERS registry", e);
    }
  }
}
