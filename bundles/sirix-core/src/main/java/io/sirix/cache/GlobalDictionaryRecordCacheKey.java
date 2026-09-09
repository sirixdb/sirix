package io.sirix.cache;

/**
 * Identifies one record of a global projection value dictionary — a value block, a reverse bucket,
 * a radix node or a spilled entry — for cross-transaction retention.
 *
 * <p>
 * {@code databaseId} and {@code resourceId} are present for the same reason {@link NamesCacheKey}
 * carries them: a node key is unique only within a resource. {@code revision} is present because
 * the dictionary sub-trie is copy-on-write with freshly minted keys, with ONE exception — the
 * generation header is rewritten under a stable key — so a revision-free key could serve a
 * pre-rewrite header. The write path evicts that key explicitly as well; the revision is the belt
 * to that braces.
 * </p>
 *
 * @param databaseId the database the dictionary belongs to
 * @param resourceId the resource within it
 * @param revision the revision the record was read from
 * @param nodeKey the record's node key
 */
public record GlobalDictionaryRecordCacheKey(long databaseId, long resourceId, int revision, long nodeKey) {
}
