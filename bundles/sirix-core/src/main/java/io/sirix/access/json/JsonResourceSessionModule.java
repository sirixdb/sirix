package io.sirix.access.json;

import dagger.Binds;
import dagger.Module;
import io.sirix.access.trx.node.json.JsonResourceSessionImpl;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.dagger.ResourceSessionScope;

/**
 * The module for {@link JsonResourceSessionComponent}.
 *
 * @author Joao Sousa
 */
@Module
public interface JsonResourceSessionModule {
  @Binds
  @ResourceSessionScope
  JsonResourceSession resourceSession(JsonResourceSessionImpl resourceSession);
}
