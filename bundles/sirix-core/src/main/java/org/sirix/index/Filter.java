package org.sirix.index;

import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.NodeReferences;

public interface Filter {

  <K extends Comparable<? super K>> boolean filter(AVLNode<K, NodeReferences> node);
}
