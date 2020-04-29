package org.sirix.access.trx.node.json.objectvalue;

import org.sirix.node.NodeKind;

/**
 * Marker interface.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public interface ObjectRecordValue<T> {
  NodeKind getKind();

  T getValue();
}
