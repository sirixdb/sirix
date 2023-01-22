package org.sirix.access.json;

import dagger.Subcomponent;
import org.sirix.access.GenericResourceSessionComponent;
import org.sirix.access.ResourceSessionModule;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.dagger.ResourceSessionScope;

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
