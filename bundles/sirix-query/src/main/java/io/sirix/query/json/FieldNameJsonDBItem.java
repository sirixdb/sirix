package io.sirix.query.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;

/**
 * The NAME of an object field read from a Sirix document, as produced for a key iteration
 * ({@code bit:fields($object)}). It behaves exactly like any stored string; the type only records
 * that the item stands for the field name of its node rather than the node's value, so that
 * temporal navigation re-reads the name in other revisions (see {@link AtomicStrJsonDBItem}).
 */
public final class FieldNameJsonDBItem extends AtomicStrJsonDBItem {

  /**
   * Constructor.
   *
   * @param rtx cursor positioned on a node that plays the object-key role
   * @param collection the collection the node belongs to
   * @param name the field name
   */
  FieldNameJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final String name) {
    super(rtx, collection, name, StoredDateTimeParser.NOT_FIXED_UTC);
  }
}
