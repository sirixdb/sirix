package org.sirix.index.name;

import org.brackit.xquery.atomic.QNm;
import org.sirix.index.Filter;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.NodeReferences;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public final class NameFilter implements Filter {

  private final Set<QNm> includes;

  private final Set<QNm> excludes;

  public NameFilter(final Set<QNm> included, final Set<QNm> excluded) {
    includes = checkNotNull(included);
    excludes = checkNotNull(excluded);
  }

  public Set<QNm> getIncludes() {
    return Collections.unmodifiableSet(includes);
  }

  public Set<QNm> getExcludes() {
    return Collections.unmodifiableSet(excludes);
  }

  @Override
  public <K extends Comparable<? super K>> boolean filter(final AVLNode<K, NodeReferences> node) {
    if (!(node.getKey() instanceof QNm))
      throw new IllegalStateException("Key is not of type QNm!");

    final QNm name = (QNm) node.getKey();
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return false;
    }

    return true;
  }
}
