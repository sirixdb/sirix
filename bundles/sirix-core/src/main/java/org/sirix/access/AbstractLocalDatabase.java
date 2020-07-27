package org.sirix.access;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
import org.sirix.access.trx.TransactionManagerImpl;
import org.sirix.api.*;
import org.sirix.cache.BufferManager;
import org.sirix.cache.BufferManagerImpl;
import org.sirix.exception.SirixIOException;
import org.sirix.io.StorageType;
import org.sirix.io.bytepipe.Encryptor;
import org.sirix.utils.SirixFiles;

import javax.annotation.Nonnegative;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractLocalDatabase<T extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements Database<T> {

  /**
   * Unique ID of a resource.
   */
  private final AtomicLong resourceID = new AtomicLong();

  /**
   * The transaction manager.
   */
  protected final TransactionManager transactionManager;

  /**
   * Determines if the database instance is in the closed state or not.
   */
  protected boolean isClosed;

  /**
   * Buffers / page cache for each resource.
   */
  protected final ConcurrentMap<Path, BufferManager> bufferManagers;

  /**
   * Central repository of all resource-ID/resource-name tuples.
   */
  protected final BiMap<Long, String> resourceIDsToResourceNames;

  /**
   * DatabaseConfiguration with fixed settings.
   */
  protected final DatabaseConfiguration dbConfig;

  /**
   * Constructor.
   *
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   */
  public AbstractLocalDatabase(final DatabaseConfiguration dbConfig) {
    this.dbConfig = checkNotNull(dbConfig);
    resourceIDsToResourceNames = Maps.synchronizedBiMap(HashBiMap.create());
    bufferManagers = new ConcurrentHashMap<>();
    transactionManager = new TransactionManagerImpl();
  }

  protected void addResourceToBufferManagerMapping(Path resourceFile, ResourceConfiguration resourceConfig) {
    if (resourceConfig.getStorageType() == StorageType.MEMORY_MAPPED) {
      bufferManagers.put(resourceFile, new BufferManagerImpl(100, 50, 150));
    } else {
      bufferManagers.put(resourceFile, new BufferManagerImpl(5_000, 1_000, 1_000));
    }
  }

  @Override
  public String getName() {
    return dbConfig.getDatabaseName();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resConfig) {
    assertNotClosed();

    boolean returnVal = true;
    resConfig.setDatabaseConfiguration(dbConfig);
    final Path path =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resConfig.resourcePath);
    // If file is existing, skip.
    if (Files.exists(path)) {
      return false;
    } else {
      try {
        Files.createDirectory(path);
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }

      if (returnVal) {
        // Creation of the folder structure.
        for (final ResourceConfiguration.ResourcePaths resourcePath : ResourceConfiguration.ResourcePaths.values()) {
          final Path toCreate = path.resolve(resourcePath.getPath());

          try {
            if (resourcePath.isFolder()) {
              Files.createDirectory(toCreate);

              if (resourcePath == ResourceConfiguration.ResourcePaths.ENCRYPTION_KEY)
                createAndStoreKeysetIfNeeded(resConfig, toCreate);
            } else {
              Files.createFile(toCreate);
            }
          } catch (UnsupportedOperationException | IOException | SecurityException e) {
            returnVal = false;
          }

          if (!returnVal)
            break;
        }
      }
    }

    if (returnVal) {
      // If everything was correct so far, initialize storage.

      // Serialization of the config.
      resourceID.set(dbConfig.getMaxResourceID());
      ResourceConfiguration.serialize(resConfig.setID(resourceID.getAndIncrement()));
      dbConfig.setMaximumResourceID(resourceID.get());
      resourceIDsToResourceNames.forcePut(resourceID.get(), resConfig.getResource().getFileName().toString());

      returnVal = bootstrapResource(resConfig);
    }

    if (!returnVal) {
      // If something was not correct, delete the partly created substructure.
      SirixFiles.recursiveRemove(resConfig.resourcePath);
    }

    return returnVal;
  }

  void createAndStoreKeysetIfNeeded(final ResourceConfiguration resConfig, final Path createdPath) {
    final Path encryptionKeyPath = createdPath.resolve("encryptionKey.json");
    if (resConfig.byteHandlePipeline.getComponents().contains(new Encryptor(createdPath.getParent()))) {
      try {
        Files.createFile(encryptionKeyPath);
        final KeysetHandle handle = KeysetHandle.generateNew(StreamingAeadKeyTemplates.AES256_CTR_HMAC_SHA256_4KB);
        CleartextKeysetHandle.write(handle, JsonKeysetWriter.withPath(encryptionKeyPath));
      } catch (final GeneralSecurityException | IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  protected abstract boolean bootstrapResource(final ResourceConfiguration resConfig);

  @Override
  public boolean isOpen() {
    return !isClosed;
  }

  @Override
  public synchronized Database<T> removeResource(final String name) {
    assertNotClosed();
    checkNotNull(name);

    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);

    // Check that no running resource managers / sessions are opened.
    final var resourceManagers = new HashSet<>(DatabasesInternals.getOpenResourceManagers(resourceFile));
    if (!resourceManagers.isEmpty()) {
      throw new IllegalStateException(
          "Open resource managers found, must be closed first: " + resourceManagers);
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0) {
      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      DatabasesInternals.removeWriteLock(resourceFile);

      bufferManagers.remove(resourceFile);
    }

    return this;
  }

  @Override
  public synchronized String getResourceName(final @Nonnegative long id) {
    assertNotClosed();
    checkArgument(id >= 0, "The ID must be >= 0!");
    return resourceIDsToResourceNames.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    return resourceIDsToResourceNames.inverse().get(checkNotNull(name));
  }

  protected void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Database is already closed.");
    }
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    assertNotClosed();
    return dbConfig;
  }

  @Override
  public synchronized boolean existsResource(final String resourceName) {
    assertNotClosed();
    final Path resourceFile =
        dbConfig.getDatabaseFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resourceName);
    return Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    try (final Stream<Path> stream = Files.list(dbConfig.getDatabaseFile()
                                                        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.collect(Collectors.toList());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Transaction beginTransaction() {
    // FIXME
    return null;
  }

}
