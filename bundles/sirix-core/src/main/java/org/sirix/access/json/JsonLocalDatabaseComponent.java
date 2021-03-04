package org.sirix.access.json;

import dagger.Subcomponent;
import org.sirix.access.GenericLocalDatabaseComponent;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.dagger.DatabaseScope;

/**
 * A Dagger subcomponent that manages {@link Database json database sessions}.
 *
 * @author Joao Sousa
 */
@DatabaseScope
@Subcomponent(modules = JsonLocalDatabaseModule.class)
public interface JsonLocalDatabaseComponent extends GenericLocalDatabaseComponent<JsonResourceManager> {

    @Subcomponent.Builder
    interface Builder extends GenericLocalDatabaseComponent.Builder<Builder> {

        @Override
        JsonLocalDatabaseComponent build();
    }
}
