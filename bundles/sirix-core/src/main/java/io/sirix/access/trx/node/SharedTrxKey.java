package io.sirix.access.trx.node;

/**
 * Key for the per-thread shared read-only transaction pool in
 * {@link AbstractResourceSession}. Combines the thread identity with the
 * revision number so that each worker thread gets exactly one transaction
 * per revision.
 *
 * @param threadId the {@link Thread#threadId()} of the owning thread
 * @param revision the revision number the transaction reads
 */
public record SharedTrxKey(long threadId, int revision) {
}
