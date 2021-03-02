package org.sirix.access;

import org.sirix.api.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A registry for write locks used for {@link ResourceManager resource managers}.
 *
 * <p>Each {@link ResourceManager}, identified by its {@link Path resource path}, will be
 * assigned a unique write lock.
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
    private final Map<Path, Lock> locks;

    @Inject
    WriteLocksRegistry() {
        locks = new ConcurrentHashMap<>();
    }

    /**
     * Fetches the write lock for the provider resource, identified by its {@code resourcePath}.
     *
     * <p>This method will create a new write lock if no lock exists for the provided resource.
     *
     * @param resourcePath The path that identifies the resource.
     * @return The lock to be used to perform write operations to the given resource.
     */
    public Lock getWriteLock(final Path resourcePath) {
        logger.trace("Fetching log for resource with path {}", resourcePath);

        return this.locks.computeIfAbsent(resourcePath, res -> new ReentrantLock());
    }

    /**
     * De-registers the lock for the provided resource, identified by its {@code resourcePath}.
     *
     * <p><b>Note</b> that this method does not prevent the de-registered lock from still being used.
     *
     * @param resourcePath The path that identifies the resource.
     */
    public void removeWriteLock(final Path resourcePath) {
        this.locks.remove(resourcePath);
    }
}
