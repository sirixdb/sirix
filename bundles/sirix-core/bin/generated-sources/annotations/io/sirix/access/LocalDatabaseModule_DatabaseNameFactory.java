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
public final class LocalDatabaseModule_DatabaseNameFactory implements Factory<String> {
  private final Provider<DatabaseConfiguration> configurationProvider;

  public LocalDatabaseModule_DatabaseNameFactory(
      Provider<DatabaseConfiguration> configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public String get() {
    return databaseName(configurationProvider.get());
  }

  public static LocalDatabaseModule_DatabaseNameFactory create(
      Provider<DatabaseConfiguration> configurationProvider) {
    return new LocalDatabaseModule_DatabaseNameFactory(configurationProvider);
  }

  public static String databaseName(DatabaseConfiguration configuration) {
    return Preconditions.checkNotNullFromProvides(LocalDatabaseModule.databaseName(configuration));
  }
}
