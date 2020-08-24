package org.sirix.index;

import org.sirix.index.redblacktree.RBNode;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;

public interface Filter {

  <K extends Comparable<? super K>> boolean filter(RBNode<K, NodeReferences> node);
}
