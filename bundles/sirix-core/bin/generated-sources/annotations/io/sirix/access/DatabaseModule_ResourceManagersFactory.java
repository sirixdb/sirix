package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.api.ResourceSession;
import javax.annotation.processing.Generated;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class DatabaseModule_ResourceManagersFactory implements Factory<PathBasedPool<ResourceSession<?, ?>>> {
  @Override
  public PathBasedPool<ResourceSession<?, ?>> get() {
    return resourceManagers();
  }

  public static DatabaseModule_ResourceManagersFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PathBasedPool<ResourceSession<?, ?>> resourceManagers() {
    return Preconditions.checkNotNullFromProvides(DatabaseModule.resourceManagers());
  }

  private static final class InstanceHolder {
    private static final DatabaseModule_ResourceManagersFactory INSTANCE = new DatabaseModule_ResourceManagersFactory();
  }
}
