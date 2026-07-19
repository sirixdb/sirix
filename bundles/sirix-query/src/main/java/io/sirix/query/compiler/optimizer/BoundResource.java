package io.sirix.query.compiler.optimizer;

import java.util.Objects;

/**
 * The single {@code (database, resource, revision)} a {@code SirixVectorizedExecutor} is bound to
 * when a {@link io.sirix.query.SirixCompileChain} auto-wires the analytical fast paths.
 *
 * <p>Brackit's vectorized detection captures only a pipeline's source <em>path</em>, never which
 * document it dereferences, so a resource-bound executor would otherwise answer a same-shaped
 * query over a different resource with the wrong data. {@link VectorizedResourceScopeStage} uses
 * this identity to strip the vectorized annotations off any scan that does not provably target it.
 *
 * @param databaseName      the bound database/collection name (the first {@code jn:doc} argument)
 * @param resourceName      the bound resource name (the second {@code jn:doc} argument)
 * @param revision          the committed revision the executor answers at
 * @param revisionIsLatest  whether {@code revision} was the resource's most-recent revision when
 *                          the chain resolved it — a {@code jn:doc(...)} with no explicit revision
 *                          opens the most-recent revision, so it may only serve when this is true
 */
public record BoundResource(String databaseName, String resourceName, int revision,
                            boolean revisionIsLatest) {
  public BoundResource {
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(resourceName, "resourceName");
  }
}
