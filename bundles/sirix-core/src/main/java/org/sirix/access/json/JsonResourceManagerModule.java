package org.sirix.access.json;

import dagger.Binds;
import dagger.Module;
import org.sirix.access.trx.node.json.JsonResourceManagerImpl;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.dagger.ResourceManagerScope;

/**
 * The module for {@link JsonResourceManagerComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface JsonResourceManagerModule {

    @Binds
    @ResourceManagerScope
    JsonResourceManager resourceManager(JsonResourceManagerImpl resourceManager);

}
