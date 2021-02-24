package org.sirix.access;

import org.sirix.api.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableMap;

/**
 * Manages all active database sessions in the current component.
 *
 * @author Joao Sousa
 */
@ThreadSafe
@Singleton
class DatabaseSessionPool {

    /**
     * Logger for {@link DatabaseSessionPool}.
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseSessionPool.class);

    /**
     * The thread safe map of active sessions.
     */
    private final Map<Path, Set<Database<?>>> sessions;

    /**
     * Default Constructor
     */
    @Inject
    DatabaseSessionPool() {
        this.sessions = new ConcurrentHashMap<>();
    }

    /**
     * Package private method to put a file/database into the internal map.
     *
     * @param file database file to put into the map
     * @param database database handle to put into the map
     */
    void putDatabase(final Path file, final Database<?> database) {
        this.sessions.compute(file, (key, value) -> append(key, value, database));
    }

    /**
     * Checks if the provided {@code file} has any active session.
     *
     * @param file The file which might have registered sessions.
     * @return {@code true} if the provided {@code file} has 1 or more active sessions associated.
     */
    boolean containsSessions(final Path file) {
        return this.sessions.containsKey(file);
    }

    /**
     * Package private method to remove a database.
     *
     * @param file database file to remove
     */
    void removeDatabase(final Path file, final Database<?> database) {
        this.sessions.compute(file, (key, value) -> remove(key, value, database));
    }

    @Nullable
    private Set<Database<?>> remove(final Path path, @Nullable final Set<Database<?>> sessions,
                                    final Database<?> database) {
        if (sessions == null) {
            logger.debug("No sessions registered for path {}", path);
            return null;
        }

        logger.trace("Removing session in path {}", path);
        sessions.remove(database);

        // coalesce to null if empty
        return sessions.isEmpty() ? null : sessions;
    }

    private Set<Database<?>> append(final Path path, @Nullable final Set<Database<?>> sessions,
                                    final Database<?> database) {
        final Set<Database<?>> coalescedSessions = sessions == null ? new HashSet<>() : sessions;

        logger.trace("Registering new session in path {}", path);
        coalescedSessions.add(database);
        return coalescedSessions;
    }

    public Map<Path, Set<Database<?>>> asMap() {
        return unmodifiableMap(this.sessions);
    }
}
