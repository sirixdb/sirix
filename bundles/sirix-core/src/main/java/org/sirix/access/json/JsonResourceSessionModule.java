package org.sirix.access.json;

import dagger.Binds;
import dagger.Module;
import org.sirix.access.trx.node.json.JsonResourceSessionImpl;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.dagger.ResourceManagerScope;

/**
 * The module for {@link JsonResourceSessionComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface JsonResourceSessionModule {
  @Binds
  @ResourceManagerScope
  JsonResourceSession resourceManager(JsonResourceSessionImpl resourceManager);
}
