package io.sirix.page.interfaces;

/**
 * A revision root's mutable index container. Its child references belong to that revision, so the
 * shared page cache must never retain an instance across transactions. Implementing this marker
 * keeps cache admission independent of the concrete (possibly optional) index implementations.
 */
public interface IndexRootPage extends Page {
}
