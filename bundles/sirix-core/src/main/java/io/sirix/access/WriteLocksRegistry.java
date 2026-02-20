package io.sirix.access;

import io.sirix.api.ResourceSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * A registry for write locks used for {@link ResourceSession resource sessions}.
 *
 * <p>
 * Each {@link ResourceSession}, identified by its {@link Path resource path}, will be assigned a
 * unique write lock.
 *
 * @author Joao Sousa
 */
@Singleton
@ThreadSafe
public class WriteLocksRegistry {

  /**
   * Logger for {@link WriteLocksRegistry}.
   */
  private static final Logger logger = LoggerFactory.getLogger(WriteLocksRegistry.class);

  /** Central repository of all resource {@code <=>} write locks mappings. */
  private final Map<Path, Semaphore> locks;

  @Inject
  WriteLocksRegistry() {
    locks = new ConcurrentHashMap<>();
  }

  /**
   * Fetches the write lock for the provider resource, identified by its {@code resourcePath}.
   *
   * <p>
   * This method will create a new write lock if no lock exists for the provided resource.
   *
   * @param resourcePath The path that identifies the resource.
   * @return The lock to be used to perform write operations to the given resource.
   */
  public Semaphore getWriteLock(final Path resourcePath) {
    logger.trace("Getting lock for resource with path {}", resourcePath);
    return this.locks.computeIfAbsent(resourcePath, res -> new Semaphore(1));
  }

  /**
   * De-registers the lock for the provided resource, identified by its {@code resourcePath}.
   *
   * <p>
   * <b>Note</b> that this method does not prevent the de-registered lock from still being used.
   *
   * @param resourcePath The path that identifies the resource.
   */
  public void removeWriteLock(final Path resourcePath) {
    logger.trace("Removing lock for resource with path {}", resourcePath);
    this.locks.remove(resourcePath);
  }
}
