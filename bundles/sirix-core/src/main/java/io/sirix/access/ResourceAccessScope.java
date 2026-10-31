package io.sirix.access;

import io.sirix.api.ResourceSession;
import java.nio.file.Path;
import java.util.Objects;

/** Request-scoped resource authorization. Unbound callers retain the ordinary storage contract. */
public final class ResourceAccessScope {
  private static final ScopedValue<Policy> POLICY = ScopedValue.newInstance();

  private ResourceAccessScope() {}

  public interface Policy {
    void checkDatabase(Path database, boolean write);

    void checkResource(Path database, String resource, boolean write);

    void checkWrite();
  }

  public static ScopedValue.Carrier where(final Policy policy) {
    return ScopedValue.where(POLICY, Objects.requireNonNull(policy));
  }

  /** Preserve a bound request policy when submitting work to an unstructured executor. */
  public static Runnable capture(final Runnable action) {
    Objects.requireNonNull(action);
    if (!POLICY.isBound())
      return action;
    final ScopedValue.Carrier carrier = where(POLICY.get());
    return () -> carrier.run(action);
  }

  public static void checkDatabase(final Path database, final boolean write) {
    if (POLICY.isBound())
      POLICY.get().checkDatabase(database, write);
  }

  public static void checkResource(final Path database, final String resource, final boolean write) {
    if (POLICY.isBound()) {
      POLICY.get().checkResource(database, resource, write);
    }
  }

  public static void checkWrite() {
    if (POLICY.isBound())
      POLICY.get().checkWrite();
  }

  public static void checkWrite(final ResourceSession<?, ?> session) {
    if (POLICY.isBound()) {
      final ResourceConfiguration config = session.getResourceConfig();
      POLICY.get().checkResource(config.getResource().getParent().getParent(), config.getName(), true);
    }
  }

}
