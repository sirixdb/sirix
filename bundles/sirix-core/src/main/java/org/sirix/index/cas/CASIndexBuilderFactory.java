package org.sirix.index.cas;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

import static com.google.common.base.Preconditions.checkNotNull;

public final class CASIndexBuilderFactory {

  public CASIndexBuilder create(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    final var avlTreeWriter =
        AVLTreeWriter.<CASValue, NodeReferences>getInstance(pageWriteTrx, indexDef.getType(), indexDef.getID());
    final var pathSummary = checkNotNull(pathSummaryReader);
    final var paths = checkNotNull(indexDef.getPaths());
    final var type = checkNotNull(indexDef.getContentType());

    return new CASIndexBuilder(avlTreeWriter, pathSummary, paths, type);
  }
}
