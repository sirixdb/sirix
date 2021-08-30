package org.sirix.page;

import org.sirix.page.interfaces.PageFragmentKey;

/**
 * The page fragment key implementation (simple immutable record/data class).
 *
 * @author Johannes Lichtenberger
 */
public record PageFragmentKeyImpl(int revision, long key) implements PageFragmentKey {}