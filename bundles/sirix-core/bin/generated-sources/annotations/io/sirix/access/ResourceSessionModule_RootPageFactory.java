package io.sirix.access;

import dagger.internal.Factory;
import dagger.internal.Preconditions;
import io.sirix.io.IOStorage;
import io.sirix.page.UberPage;
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
public final class ResourceSessionModule_RootPageFactory implements Factory<UberPage> {
  private final Provider<IOStorage> storageProvider;

  public ResourceSessionModule_RootPageFactory(Provider<IOStorage> storageProvider) {
    this.storageProvider = storageProvider;
  }

  @Override
  public UberPage get() {
    return rootPage(storageProvider.get());
  }

  public static ResourceSessionModule_RootPageFactory create(Provider<IOStorage> storageProvider) {
    return new ResourceSessionModule_RootPageFactory(storageProvider);
  }

  public static UberPage rootPage(IOStorage storage) {
    return Preconditions.checkNotNullFromProvides(ResourceSessionModule.rootPage(storage));
  }
}
