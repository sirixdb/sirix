package io.sirix.access.json;

import dagger.Subcomponent;
import io.sirix.access.GenericLocalDatabaseComponent;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.dagger.DatabaseScope;

/**
 * A Dagger subcomponent that manages {@link Database json database sessions}.
 *
 * @author Joao Sousa
 */
@DatabaseScope
@Subcomponent(modules = JsonLocalDatabaseModule.class)
public interface JsonLocalDatabaseComponent
		extends
			GenericLocalDatabaseComponent<JsonResourceSession, JsonResourceSessionComponent.Builder> {

	@Subcomponent.Builder
	interface Builder extends GenericLocalDatabaseComponent.Builder<Builder> {

		@Override
		JsonLocalDatabaseComponent build();
	}
}
