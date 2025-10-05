package io.sirix.access.trx.page;

import dagger.internal.Factory;
import io.sirix.access.DatabaseType;
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
public final class PageTrxFactory_Factory implements Factory<PageTrxFactory> {
  private final Provider<DatabaseType> databaseTypeProvider;

  public PageTrxFactory_Factory(Provider<DatabaseType> databaseTypeProvider) {
    this.databaseTypeProvider = databaseTypeProvider;
  }

  @Override
  public PageTrxFactory get() {
    return newInstance(databaseTypeProvider.get());
  }

  public static PageTrxFactory_Factory create(Provider<DatabaseType> databaseTypeProvider) {
    return new PageTrxFactory_Factory(databaseTypeProvider);
  }

  public static PageTrxFactory newInstance(DatabaseType databaseType) {
    return new PageTrxFactory(databaseType);
  }
}
