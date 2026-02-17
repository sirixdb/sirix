/*
 * Copyright (c) 2022, University of Konstanz, Distributed Systems Group All rights reserved.
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

package io.sirix.io.memorymapped;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.filechannel.FileChannelWriter;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.exception.SirixIOException;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.IOStorage;
import io.sirix.io.Writer;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.io.filechannel.FileChannelReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Storage, to provide offheap memory mapped access.
 * <p>
 * Uses a shared Arena for memory-mapped segments to avoid creating a new Arena per reader. The
 * Arena is managed at the storage level and shared across all readers. When the file grows (after
 * commits), the mapping is remapped to cover the new file size.
 * <p>
 * Uses reference counting to ensure arenas are not closed while readers are still using them. Old
 * arenas are kept alive until all readers using them are closed.
 *
 * @author Johannes Lichtenberger
 */
public final class MMStorage implements IOStorage {

  /**
   * Data file name.
   */
  private static final String FILENAME = "sirix.data";

  /**
   * Revisions file name.
   */
  private static final String REVISIONS_FILENAME = "sirix.revisions";

  /**
   * Byte handler pipeline.
   */
  private final ByteHandlerPipeline byteHandlerPipeline;

  final Semaphore semaphore = new Semaphore(1);

  /**
   * Revision file data cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  private final Path revisionsFilePath;

  private final Path dataFilePath;

  /**
   * Current arena generation for new readers.
   */
  private volatile ArenaGeneration currentGeneration;

  /**
   * List of old arena generations that still have active readers. These will be closed when their
   * reference count reaches zero.
   */
  private final List<ArenaGeneration> oldGenerations = new ArrayList<>();

  /**
   * Last known size of the data file when mapped.
   */
  private volatile long lastDataFileSize;

  /**
   * Last known size of the revisions file when mapped.
   */
  private volatile long lastRevisionsFileSize;

  /**
   * Lock for synchronizing remap operations.
   */
  private final Object remapLock = new Object();

  /**
   * Revision index holder for fast timestamp lookups.
   */
  private final RevisionIndexHolder revisionIndexHolder;

  /**
   * Represents a generation of memory-mapped segments with reference counting. When the file grows, a
   * new generation is created and old generations are kept alive until all readers using them are
   * closed.
   */
  static final class ArenaGeneration {
    private final Arena arena;
    private final MemorySegment dataSegment;
    private final MemorySegment revisionsSegment;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean isOldGeneration = false;

    ArenaGeneration(Arena arena, MemorySegment dataSegment, MemorySegment revisionsSegment) {
      this.arena = arena;
      this.dataSegment = dataSegment;
      this.revisionsSegment = revisionsSegment;
    }

    void incrementRefCount() {
      refCount.incrementAndGet();
    }

    /**
     * Decrements the reference count. For OLD generations only: closes the arena if it reaches zero.
     * Current generation is kept alive even with refCount=0 until storage is closed.
     * 
     * @return true if the arena was closed (only happens for old generations), false otherwise
     */
    boolean decrementRefCount() {
      int newCount = refCount.decrementAndGet();
      if (newCount == 0 && isOldGeneration) {
        arena.close();
        return true;
      }
      return false;
    }

    /**
     * Marks this generation as old (replaced by a new generation). Old generations will be closed when
     * their refCount reaches 0.
     */
    void markAsOld() {
      this.isOldGeneration = true;
    }

    MemorySegment getDataSegment() {
      return dataSegment;
    }

    MemorySegment getRevisionsSegment() {
      return revisionsSegment;
    }

    int getRefCount() {
      return refCount.get();
    }

    void close() {
      arena.close();
    }
  }

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   * @param cache the revision file data cache
   * @param revisionIndexHolder the revision index holder
   */
  public MMStorage(final ResourceConfiguration resourceConfig, final AsyncCache<Integer, RevisionFileData> cache,
      final RevisionIndexHolder revisionIndexHolder) {
    assert resourceConfig != null : "resourceConfig must not be null!";
    final Path file = resourceConfig.resourcePath;
    revisionsFilePath = file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(REVISIONS_FILENAME);
    dataFilePath = file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(FILENAME);
    byteHandlerPipeline = resourceConfig.byteHandlePipeline;
    this.cache = cache;
    this.revisionIndexHolder = revisionIndexHolder;
  }

  /**
   * Constructor (backward compatibility).
   *
   * @param resourceConfig the resource configuration
   * @param cache the revision file data cache
   */
  public MMStorage(final ResourceConfiguration resourceConfig, final AsyncCache<Integer, RevisionFileData> cache) {
    this(resourceConfig, cache, new RevisionIndexHolder());
  }

  @Override
  public Reader createReader() {
    try {
      final var sempahoreAcquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);

      if (!sempahoreAcquired) {
        throw new IllegalStateException("Couldn't acquire semaphore.");
      }

      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);

      final ArenaGeneration generation;

      synchronized (remapLock) {
        final long currentDataFileSize = Files.size(dataFilePath);
        final long currentRevisionsFileSize = Files.size(revisionsOffsetFilePath);

        // Check if we need to create or remap the segments
        if (currentGeneration == null || currentDataFileSize > lastDataFileSize
            || currentRevisionsFileSize > lastRevisionsFileSize) {

          // Move old generation to oldGenerations list (don't close it yet - readers may still use it)
          if (currentGeneration != null) {
            currentGeneration.markAsOld();
            oldGenerations.add(currentGeneration);
          }

          // Create new shared arena and generation
          final Arena newArena = Arena.ofShared();
          final MemorySegment dataSegment;
          final MemorySegment revisionsSegment;

          try (final var dataFileChannel = FileChannel.open(dataFilePath);
              final var revisionsOffsetFileChannel = FileChannel.open(revisionsOffsetFilePath)) {
            dataSegment = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, currentDataFileSize, newArena);
            revisionsSegment =
                revisionsOffsetFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, currentRevisionsFileSize, newArena);
          }

          currentGeneration = new ArenaGeneration(newArena, dataSegment, revisionsSegment);
          lastDataFileSize = currentDataFileSize;
          lastRevisionsFileSize = currentRevisionsFileSize;
        }

        generation = currentGeneration;
        generation.incrementRefCount();
      }

      // Create reader with reference-counted generation
      return new MMFileReader(generation.getDataSegment(), generation.getRevisionsSegment(),
          new ByteHandlerPipeline(byteHandlerPipeline), SerializationType.DATA, new PagePersister(),
          cache.synchronous(), generation, this);
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  /**
   * Called by MMFileReader when it is closed to decrement the reference count for its arena
   * generation.
   *
   * @param generation the arena generation to release
   */
  void releaseGeneration(ArenaGeneration generation) {
    synchronized (remapLock) {
      if (generation.decrementRefCount()) {
        // Arena was closed, remove from old generations list if present
        oldGenerations.remove(generation);
      }
    }
  }

  private void createRevisionsOffsetFileIfItDoesNotExist(Path revisionsOffsetFilePath) throws IOException {
    if (!Files.exists(revisionsOffsetFilePath)) {
      Files.createFile(revisionsOffsetFilePath);
    }
  }

  private Path createDirectoriesAndFile() throws IOException {
    final Path concreteStorage = getDataFilePath();

    if (!Files.exists(concreteStorage)) {
      Files.createDirectories(concreteStorage.getParent());
      Files.createFile(concreteStorage);
    }

    return concreteStorage;
  }

  @Override
  public synchronized Writer createWriter() {
    try {
      final var sempahoreAcquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);

      if (!sempahoreAcquired) {
        throw new IllegalStateException("Couldn't acquire semaphore.");
      }

      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);
      final var dataFileChannel = createDataFileChannel(dataFilePath);
      final var revisionsOffsetFileChannel = createRevisionsOffsetFileChannel(revisionsOffsetFilePath);

      final var byteHandlePipeline = new ByteHandlerPipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      final var reader = new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel, byteHandlePipeline,
          serializationType, pagePersister, cache.synchronous());

      return new FileChannelWriter(dataFileChannel, revisionsOffsetFileChannel, serializationType, pagePersister, cache,
          revisionIndexHolder, reader);
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  private FileChannel createDataFileChannel(Path dataFilePath) throws IOException {
    return FileChannel.open(dataFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
  }

  private FileChannel createRevisionsOffsetFileChannel(Path revisionsOffsetFilePath) throws IOException {
    return FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
  }

  @Override
  public void close() {
    synchronized (remapLock) {
      // Close current generation if no readers are using it
      if (currentGeneration != null) {
        if (currentGeneration.getRefCount() == 0) {
          currentGeneration.close();
        } else {
          // Readers are still using it - mark as old so it will be closed when they're done
          currentGeneration.markAsOld();
          oldGenerations.add(currentGeneration);
        }
        currentGeneration = null;
      }

      // Close any old generations with no active readers
      oldGenerations.removeIf(gen -> {
        if (gen.getRefCount() == 0) {
          gen.close();
          return true;
        }
        return false;
      });
    }
  }

  /**
   * Getting path for data file.
   *
   * @return the path for this data file
   */
  private Path getDataFilePath() {
    return dataFilePath;
  }

  /**
   * Getting concrete storage for this file.
   *
   * @return the concrete storage for this database
   */
  private Path getRevisionFilePath() {
    return revisionsFilePath;
  }

  @Override
  public boolean exists() {
    final Path storage = getDataFilePath();
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
  public @NonNull RevisionIndexHolder getRevisionIndexHolder() {
    return revisionIndexHolder;
  }
}
