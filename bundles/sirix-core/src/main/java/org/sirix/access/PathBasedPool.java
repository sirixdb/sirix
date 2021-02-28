package org.sirix.access;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableMap;

/**
 * Manages a pool of objects which can be indexed by a {@link Path}.
 *
 * @author Joao Sousa
 */
@ThreadSafe
public class PathBasedPool<E> {

    /**
     * Logger for {@link PathBasedPool}.
     */
    private static final Logger logger = LoggerFactory.getLogger(PathBasedPool.class);

    /**
     * The thread safe map of objects in the pool.
     */
    private final Map<Path, Set<E>> sessions;

    /**
     * Default Constructor
     */
    public PathBasedPool() {
        this.sessions = new ConcurrentHashMap<>();
    }

    /**
     * Persists a file/object into the pool.
     *
     * @param file   the file that identifies the object.
     * @param object object to be persisted to the pool.
     */
    public void putObject(final Path file, final E object) {
        this.sessions.compute(file, (key, value) -> append(key, value, object));
    }

    /**
     * Checks if the provided {@code file} has any registered object.
     *
     * @param file The file which might have registered objects.
     * @return {@code true} if the provided {@code file} has 1 or more registered objects.
     */
    public boolean containsAnyEntry(final Path file) {
        return this.sessions.containsKey(file);
    }

    /**
     * Package private method to remove a database.
     *
     * @param file   file that represents object.
     * @param object The object to be removed from the pool.
     */
    public void removeObject(final Path file, final E object) {
        this.sessions.computeIfPresent(file, (key, value) -> remove(key, value, object));
    }

    @Nullable
    private Set<E> remove(final Path path, final Set<E> objects, final E object) {

        logger.trace("Removing session in path {}", path);
        objects.remove(object);

        // coalesce to null if empty
        return objects.isEmpty() ? null : objects;
    }

    private Set<E> append(final Path path, @Nullable final Set<E> objects,
                          final E object) {
        final Set<E> coalescedSessions = objects == null ? new HashSet<>() : objects;

        logger.trace("Registering new session in path {}", path);
        coalescedSessions.add(object);
        return coalescedSessions;
    }

    public Map<Path, Set<E>> asMap() {
        return unmodifiableMap(this.sessions);
    }
}
