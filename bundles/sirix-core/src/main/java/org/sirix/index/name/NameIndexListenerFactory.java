package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.UnorderedKeyValuePage;

public final class NameIndexListenerFactory {

  public NameIndexListener create(final PageTrx pageWriteTrx,
      final IndexDef indexDefinition) {
    final var includes = checkNotNull(indexDefinition.getIncluded());
    final var excludes = checkNotNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;
    final var avlTreeWriter = AVLTreeWriter.<QNm, NodeReferences>getInstance(pageWriteTrx, indexDefinition.getType(),
        indexDefinition.getID());

    return new NameIndexListener(includes, excludes, avlTreeWriter);
  }
}
