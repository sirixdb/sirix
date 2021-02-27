package org.sirix.access.json;

import dagger.Subcomponent;
import org.sirix.access.GenericResourceManagerComponent;
import org.sirix.access.ResourceManagerModule;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.dagger.ResourceManagerScope;

/**
 * A {@link Subcomponent dagger subcomponent} that manages the lifecycle of a {@link JsonResourceManager}.
 *
 * @author Joao Sousa
 */
@ResourceManagerScope
@Subcomponent(modules = {JsonResourceManagerModule.class, ResourceManagerModule.class})
public interface JsonResourceManagerComponent extends GenericResourceManagerComponent<JsonResourceManager> {
    
    @Subcomponent.Builder
    interface Builder extends GenericResourceManagerComponent.Builder<Builder, JsonResourceManager,
            JsonResourceManagerComponent> {

    }
}
