package io.sirix.query.bench;

import io.sirix.api.json.JsonResourceSession;

/** Test bridge: exposes the package-private wildcard projection installer. */
public final class ScaleBenchProjectionSetupAccess {
  private ScaleBenchProjectionSetupAccess() {
  }

  public static int installWildcard(final JsonResourceSession session) {
    return ScaleBenchProjectionSetup.installWildcard(session);
  }
}
