package org.sirix.index;

import org.sirix.index.redblacktree.RBNodeKey;

public interface Filter {

  <K extends Comparable<? super K>> boolean filter(RBNodeKey<K> node);
}
