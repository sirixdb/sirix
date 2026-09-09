package io.sirix.query.bench;

import io.sirix.api.json.JsonResourceSession;

/** Test bridge: exposes the package-private production projection setup. */
public final class ScaleBenchProjectionSetupAccess {
  private ScaleBenchProjectionSetupAccess() {}

  public static int ensureProjection(final JsonResourceSession session) {
    return ScaleBenchProjectionSetup.ensureProjection(session);
  }
}
