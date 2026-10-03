package io.sirix.api.json;

import io.sirix.api.ResourceSession;

public interface JsonResourceSession extends ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> {
  /**
   * Rebuild obsolete valid-time indexes from the latest revision into fresh physical roots. This
   * maintenance operation opens a write transaction and commits a new revision only when an obsolete
   * definition exists. Callers must enforce write authorization before invoking it; read-only
   * resource opening never invokes maintenance.
   */
  void rebuildValidTimeIndexes();
}
