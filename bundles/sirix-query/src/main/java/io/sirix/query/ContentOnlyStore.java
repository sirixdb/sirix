package io.sirix.query;

/**
 * Marks request-scoped stores whose query functions accept inline content only. Embedded stores
 * deliberately do not implement this interface, retaining local file/URI loading.
 */
public interface ContentOnlyStore {
}
