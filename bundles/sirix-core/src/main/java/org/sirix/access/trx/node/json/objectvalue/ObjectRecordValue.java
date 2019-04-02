package org.sirix.access.trx.node.json.objectvalue;

import org.sirix.node.Kind;

/**
 * Marker interface.
 *
 * @author Johannes Lichtenberger <johannes.lichtenberger@sirix.io>
 *
 */
public interface ObjectRecordValue<T> {

  Kind getKind();

  T getValue();
}
