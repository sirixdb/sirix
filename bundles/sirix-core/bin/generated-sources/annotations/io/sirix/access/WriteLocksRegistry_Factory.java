package io.sirix.access;

import dagger.internal.Factory;
import javax.annotation.processing.Generated;

@Generated(
    value = "dagger.internal.codegen.ComponentProcessor",
    comments = "https://dagger.dev"
)
@SuppressWarnings({
    "unchecked",
    "rawtypes"
})
public final class WriteLocksRegistry_Factory implements Factory<WriteLocksRegistry> {
  @Override
  public WriteLocksRegistry get() {
    return newInstance();
  }

  public static WriteLocksRegistry_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static WriteLocksRegistry newInstance() {
    return new WriteLocksRegistry();
  }

  private static final class InstanceHolder {
    private static final WriteLocksRegistry_Factory INSTANCE = new WriteLocksRegistry_Factory();
  }
}
