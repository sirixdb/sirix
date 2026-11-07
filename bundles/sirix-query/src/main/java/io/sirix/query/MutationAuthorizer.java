package io.sirix.query;

/**
 * Authorizes a mutation that a query is about to perform on one resource.
 *
 * <p>
 * A query store implements this when the caller's identity decides whether a mutation may happen at
 * all. The REST API's session stores do: they are created per request and hold the authenticated
 * user, so they can answer for the database the mutation targets. Stores used by embedded callers
 * (the CLI, benchmarks, tests) do not implement it, and {@link MutationAuthorization} then performs
 * no check — an embedded caller already holds the database.
 * </p>
 *
 * <p>
 * This is the authorization point for mutating function calls such as index DDL, {@code sdb:commit}
 * and {@code sdb:rollback}. A query-level {@code isUpdating} check alone does not reliably identify
 * these calls, so permission must be checked at the mutation itself.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public interface MutationAuthorizer {

  /**
   * Authorize a mutation of {@code resourceName} in {@code databaseName}, which must happen before
   * the mutation takes any effect — before a write transaction is begun, reverted or committed.
   *
   * @param databaseName the target's full collection name used for REST roles, never a path basename
   * @param resourceName the resource the mutation targets
   * @throws RuntimeException if the caller may not mutate it; the implementation chooses the type
   */
  void authorizeWrite(String databaseName, String resourceName);
}
