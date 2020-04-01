package org.sirix.access.trx.node.json.objectvalue;

import org.sirix.node.NodeKind;

/**
 * Marker interface.
 *
 * @author Johannes Lichtenberger <johannes.lichtenberger@sirix.io>
 *
 */
public interface ObjectRecordValue<T> {
  NodeKind getKind();

  T getValue();
}
