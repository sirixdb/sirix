package io.sirix.access.trx;

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
public final class TransactionManagerImpl_Factory implements Factory<TransactionManagerImpl> {
  @Override
  public TransactionManagerImpl get() {
    return newInstance();
  }

  public static TransactionManagerImpl_Factory create() {
    return InstanceHolder.INSTANCE;
  }

  public static TransactionManagerImpl newInstance() {
    return new TransactionManagerImpl();
  }

  private static final class InstanceHolder {
    private static final TransactionManagerImpl_Factory INSTANCE = new TransactionManagerImpl_Factory();
  }
}
