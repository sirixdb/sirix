package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;

/**
 * Manages database dependencies: factory methods for creating JSON and XML databases, and a pool
 * of open database sessions.
 *
 * @author Joao Sousa
 */
public interface DatabaseManager {

  /**
   * Returns the factory used to create JSON database instances.
   *
   * @return the JSON database factory
   */
  LocalDatabaseFactory<JsonResourceSession> jsonDatabaseFactory();

  /**
   * Returns the factory used to create XML database instances.
   *
   * @return the XML database factory
   */
  LocalDatabaseFactory<XmlResourceSession> xmlDatabaseFactory();

  /**
   * Returns the pool of open database sessions, indexed by path.
   *
   * @return the database session pool
   */
  PathBasedPool<Database<?>> sessions();
}
