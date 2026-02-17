package io.sirix.page;

import io.sirix.page.interfaces.PageFragmentKey;

/**
 * The page fragment key implementation (simple immutable record/data class). Includes database and
 * resource IDs for global BufferManager support.
 *
 * @author Johannes Lichtenberger
 */
public record PageFragmentKeyImpl(int revision, long key, long databaseId, long resourceId) implements PageFragmentKey {
}
