package io.sirix.cache;

/**
 * Identifies one string-predicate verdict over a global projection value dictionary.
 *
 * <p>
 * A verdict answers "which dictionary ids satisfy this predicate" and is a PURE FUNCTION of the
 * dictionary's content and the question asked, so every input to that function is in the key.
 * {@code databaseId} and {@code resourceId} are both present for the same reason
 * {@link NamesCacheKey} carries both: node keys are only unique within a resource, and a cache that
 * outlives one would otherwise serve one resource's answer to another. {@code revision} is what
 * makes an entry go stale rather than wrong — a later revision asks under a different key and the
 * bounded cache retires the older one on its own.
 * </p>
 *
 * @param databaseId the database the dictionary belongs to
 * @param resourceId the resource within it
 * @param revision the revision the verdict was computed against
 * @param entryCount the dictionary's cardinality when the verdict was computed
 * @param headerNodeKey the dictionary's header node key
 * @param op the predicate operator's name
 * @param literalHex the literal's UTF-8 bytes, hex-encoded so the key has value equality
 */
public record GlobalVerdictCacheKey(long databaseId, long resourceId, int revision, long headerNodeKey,
    int entryCount, String op, String literalHex) {
}
