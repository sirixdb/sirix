package io.sirix.access.json;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.LocalDatabaseFactory;
import io.sirix.access.User;
import io.sirix.access.json.JsonLocalDatabaseComponent.Builder;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * A database session factory for Json databases.
 *
 * @author Joao Sousa
 */
@Singleton
public class LocalJsonDatabaseFactory implements LocalDatabaseFactory<JsonResourceSession> {

	/**
	 * Logger for {@link LocalJsonDatabaseFactory}.
	 */
	private static final Logger logger = LoggerFactory.getLogger(LocalJsonDatabaseFactory.class);

	private final Provider<JsonLocalDatabaseComponent.Builder> subComponentBuilder;

	@Inject
	LocalJsonDatabaseFactory(final Provider<Builder> subComponentBuilder) {
		this.subComponentBuilder = subComponentBuilder;
	}

	@Override
	public Database<JsonResourceSession> createDatabase(final DatabaseConfiguration configuration, final User user) {
		logger.trace("Creating new local JSON database instance (open)");
		return this.subComponentBuilder.get().databaseConfiguration(configuration).user(user).build().database();
	}
}
