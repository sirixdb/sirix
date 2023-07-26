package io.sirix.access.json;

import dagger.Subcomponent;
import io.sirix.access.GenericResourceSessionComponent;
import io.sirix.access.ResourceSessionModule;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.dagger.ResourceSessionScope;

/**
 * A {@link Subcomponent dagger subcomponent} that manages the lifecycle of a {@link JsonResourceSession}.
 *
 * @author Joao Sousa
 */
@ResourceSessionScope
@Subcomponent(modules = { JsonResourceSessionModule.class, ResourceSessionModule.class })
public interface JsonResourceSessionComponent extends GenericResourceSessionComponent<JsonResourceSession> {
  @Subcomponent.Builder
  interface Builder
      extends GenericResourceSessionComponent.Builder<Builder, JsonResourceSession, JsonResourceSessionComponent> {
  }
}
