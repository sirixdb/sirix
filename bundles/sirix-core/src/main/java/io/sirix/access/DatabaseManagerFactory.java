package io.sirix.access;

import io.sirix.access.json.LocalJsonDatabaseFactory;
import io.sirix.access.xml.LocalXmlDatabaseFactory;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;

/**
 * Manual replacement for the Dagger-generated {@code DaggerDatabaseManager}. Creates and holds
 * the singleton dependencies that were previously managed by the Dagger component graph.
 *
 * <p>Dependency wiring (previously 3 Dagger levels):
 * <ul>
 *   <li>Level 1 (root singletons): {@link WriteLocksRegistry}, {@link PathBasedPool} instances</li>
 *   <li>Level 2 (database scope): wired per-call inside the database factory lambdas</li>
 *   <li>Level 3 (resource session scope): wired per-call inside the resource session factory lambdas</li>
 * </ul>
 */
public final class DatabaseManagerFactory {

  private DatabaseManagerFactory() {
    throw new AssertionError("Factory class, not instantiable");
  }

  /**
   * Creates a new {@link DatabaseManager} with all dependencies wired manually.
   *
   * @return a fully wired database manager
   */
  public static DatabaseManager create() {
    // Level 1 singletons (previously @Singleton in DatabaseModule)
    final WriteLocksRegistry writeLocksRegistry = new WriteLocksRegistry();
    final PathBasedPool<Database<?>> databaseSessions = new PathBasedPool<>();
    final PathBasedPool<ResourceSession<?, ?>> resourceSessions = new PathBasedPool<>();

    // JSON database factory (replaces JsonLocalDatabaseComponent + JsonLocalDatabaseModule)
    final LocalDatabaseFactory<JsonResourceSession> jsonFactory =
        new LocalJsonDatabaseFactory(writeLocksRegistry, databaseSessions, resourceSessions);

    // XML database factory (replaces XmlLocalDatabaseComponent + XmlLocalDatabaseModule)
    final LocalDatabaseFactory<XmlResourceSession> xmlFactory =
        new LocalXmlDatabaseFactory(writeLocksRegistry, databaseSessions, resourceSessions);

    return new DatabaseManagerImpl(jsonFactory, xmlFactory, databaseSessions);
  }

  /**
   * Concrete implementation of {@link DatabaseManager} holding the wired singletons.
   */
  private static final class DatabaseManagerImpl implements DatabaseManager {

    private final LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory;
    private final LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory;
    private final PathBasedPool<Database<?>> sessions;

    DatabaseManagerImpl(final LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory,
        final LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory,
        final PathBasedPool<Database<?>> sessions) {
      this.jsonDatabaseFactory = jsonDatabaseFactory;
      this.xmlDatabaseFactory = xmlDatabaseFactory;
      this.sessions = sessions;
    }

    @Override
    public LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory() {
      return jsonDatabaseFactory;
    }

    @Override
    public LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory() {
      return xmlDatabaseFactory;
    }

    @Override
    public PathBasedPool<Database<?>> sessions() {
      return sessions;
    }
  }
}
