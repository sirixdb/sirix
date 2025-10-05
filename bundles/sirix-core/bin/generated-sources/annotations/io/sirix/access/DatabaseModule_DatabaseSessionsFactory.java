package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.api.Database;
import javax.annotation.processing.Generated;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class DatabaseModule_DatabaseSessionsFactory implements Factory<PathBasedPool<Database<?>>> {
  @Override
  public PathBasedPool<Database<?>> get() {
    return databaseSessions();
  }

  public static DatabaseModule_DatabaseSessionsFactory create() {
    return InstanceHolder.INSTANCE;
  }

  public static PathBasedPool<Database<?>> databaseSessions() {
    return Preconditions.checkNotNullFromProvides(DatabaseModule.databaseSessions());
  }

  private static final class InstanceHolder {
    private static final DatabaseModule_DatabaseSessionsFactory INSTANCE = new DatabaseModule_DatabaseSessionsFactory();
  }
}
