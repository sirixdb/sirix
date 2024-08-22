package io.sirix.page;

import io.sirix.page.interfaces.PageFragmentKey;

/**
 * The page fragment key implementation (simple immutable record/data class).
 *
 * @author Johannes Lichtenberger
 */
public record PageFragmentKeyImpl(int revision, long key) implements PageFragmentKey {
}
