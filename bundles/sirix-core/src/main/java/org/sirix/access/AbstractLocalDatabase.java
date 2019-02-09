package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import org.sirix.access.trx.TransactionManagerImpl;
import org.sirix.api.Database;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;
import org.sirix.cache.BufferManager;
import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.Encryptor;
import org.sirix.utils.SirixFiles;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;

public abstract class AbstractLocalDatabase<T extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements Database<T> {

  /** Unique ID of a resource. */
  private final AtomicLong mResourceID = new AtomicLong();

  /** The transaction manager. */
  protected final TransactionManager mTransactionManager;

  /** Determines if the database instance is in the closed state or not. */
  protected boolean mClosed;

  /** Buffers / page cache for each resource. */
  protected final ConcurrentMap<Path, BufferManager> mBufferManagers;

  /** Central repository of all resource-ID/resource-name tuples. */
  protected final BiMap<Long, String> mResources;

  /** DatabaseConfiguration with fixed settings. */
  protected final DatabaseConfiguration mDBConfig;

  /**
   * Constructor.
   *
   * @param dbConfig {@link ResourceConfiguration} reference to configure the {@link Database}
   */
  public AbstractLocalDatabase(final DatabaseConfiguration dbConfig) {
    mDBConfig = checkNotNull(dbConfig);
    mResources = Maps.synchronizedBiMap(HashBiMap.create());
    mBufferManagers = new ConcurrentHashMap<>();
    mTransactionManager = new TransactionManagerImpl();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration resConfig) {
    assertNotClosed();

    boolean returnVal = true;
    resConfig.setDatabaseConfiguration(mDBConfig);
    final Path path =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resConfig.resourcePath);
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
      mResourceID.set(mDBConfig.getMaxResourceID());
      ResourceConfiguration.serialize(resConfig.setID(mResourceID.getAndIncrement()));
      mDBConfig.setMaximumResourceID(mResourceID.get());
      mResources.forcePut(mResourceID.get(), resConfig.getResource().getFileName().toString());

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
  public synchronized Database<T> removeResource(final String name) {
    assertNotClosed();

    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(name);
    // Check that no running resource managers / sessions are opened.
    if (Databases.hasOpenResourceManagers(resourceFile)) {
      throw new IllegalStateException("Opened resource managers found, must be closed first.");
    }

    // If file is existing and folder is a Sirix-dataplace, delete it.
    if (Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0) {
      // Instantiate the database for deletion.
      SirixFiles.recursiveRemove(resourceFile);

      // mReadSemaphores.remove(resourceFile);
      // mWriteSemaphores.remove(resourceFile);
      mBufferManagers.remove(resourceFile);
    }

    return this;
  }

  @Override
  public synchronized String getResourceName(final @Nonnegative long id) {
    assertNotClosed();
    checkArgument(id >= 0, "The ID must be >= 0!");
    return mResources.get(id);
  }

  @Override
  public synchronized long getResourceID(final String name) {
    assertNotClosed();
    return mResources.inverse().get(checkNotNull(name));
  }

  protected void assertNotClosed() {
    if (mClosed) {
      throw new IllegalStateException("Database is already closed.");
    }
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    assertNotClosed();
    return mDBConfig;
  }

  @Override
  public synchronized boolean existsResource(final String resourceName) {
    assertNotClosed();
    final Path resourceFile =
        mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(resourceName);
    return Files.exists(resourceFile) && ResourceConfiguration.ResourcePaths.compareStructure(resourceFile) == 0
        ? true
        : false;
  }

  @Override
  public List<Path> listResources() {
    assertNotClosed();
    try (final Stream<Path> stream =
        Files.list(mDBConfig.getFile().resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()))) {
      return stream.collect(Collectors.toList());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  BufferManager getPageCache(final Path resourceFile) {
    return mBufferManagers.get(resourceFile);
  }

  @Override
  public Transaction beginTransaction() {
    // FIXME
    return null;
  }

}
