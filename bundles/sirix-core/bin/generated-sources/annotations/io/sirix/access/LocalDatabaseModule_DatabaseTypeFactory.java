package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import javax.annotation.processing.Generated;
import javax.inject.Provider;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class LocalDatabaseModule_DatabaseTypeFactory implements Factory<DatabaseType> {
  private final Provider<DatabaseConfiguration> configurationProvider;

  public LocalDatabaseModule_DatabaseTypeFactory(
      Provider<DatabaseConfiguration> configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public DatabaseType get() {
    return databaseType(configurationProvider.get());
  }

  public static LocalDatabaseModule_DatabaseTypeFactory create(
      Provider<DatabaseConfiguration> configurationProvider) {
    return new LocalDatabaseModule_DatabaseTypeFactory(configurationProvider);
  }

  public static DatabaseType databaseType(DatabaseConfiguration configuration) {
    return Preconditions.checkNotNullFromProvides(LocalDatabaseModule.databaseType(configuration));
  }
}
